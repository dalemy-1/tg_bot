import os
import re
import json
import time
import asyncio
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from aiohttp import web, ClientSession
from langdetect import detect, DetectorFactory

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ChatType, ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

DetectorFactory.seed = 0  # 让识别更稳定

# ================== ENV ==================
TOKEN = (os.getenv("TG_BOT_TOKEN") or "").strip()
ADMIN_ID = int(os.getenv("TG_ADMIN_ID", "0") or "0")  # 必填：管理员 user_id

PUBLIC_URL = (os.getenv("RENDER_EXTERNAL_URL") or os.getenv("PUBLIC_URL") or "").strip().rstrip("/")
WEBHOOK_SECRET = (os.getenv("WEBHOOK_SECRET") or "").strip()
PORT = int(os.getenv("PORT", "10000"))
HEALTH_PATH = "/healthz"

# 给用户“一键联系管理员”按钮（保留）
ADMIN_CONTACT_URL = (os.getenv("ADMIN_CONTACT_URL") or "https://t.me/Adalemy").strip()

# 自动回复：24小时一次
AUTO_REPLY_COOLDOWN_SEC = int(os.getenv("AUTO_REPLY_COOLDOWN_SEC", str(24 * 3600)))

# 多语言自动回复（不翻译时也能用）
AUTO_REPLY_ZH = (os.getenv("AUTO_REPLY_ZH") or "你好，已收到你的消息，我们会尽快回复。").strip()
AUTO_REPLY_EN = (os.getenv("AUTO_REPLY_EN") or "Hello, we received your message and will reply soon.").strip()
AUTO_REPLY_JA = (os.getenv("AUTO_REPLY_JA") or "メッセージを受け取りました。できるだけ早く返信します。").strip()
AUTO_REPLY_DEFAULT = (os.getenv("AUTO_REPLY_TEXT") or "已收到，请联系管理员。").strip()

# 免费翻译（可选）：MyMemory（不保证稳定，可能限流）
TRANSLATE_ENABLED = (os.getenv("TRANSLATE_ENABLED", "0").strip() == "1")
ADMIN_SOURCE_LANG = (os.getenv("ADMIN_SOURCE_LANG") or "zh-CN").strip()   # 管理员发出消息默认语言
ADMIN_TARGET_LANG = (os.getenv("ADMIN_TARGET_LANG") or "zh-CN").strip()   # 管理员看到用户消息翻成中文

# ================== FILES ==================
BASE_DIR = Path(__file__).resolve().parent
STATE_FILE = BASE_DIR / "support_state.json"
LOG_FILE = BASE_DIR / "history.jsonl"
MAX_MSG_INDEX = 8000


# ================== UTILS ==================
def _now_ts() -> int:
    return int(time.time())


def html_escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def load_state() -> Dict[str, Any]:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {
        "ticket_seq": 0,
        "tickets": {},          # user_id(str) -> {ticket_id, status, created_at, header_msg_id}
        "msg_index": {},        # admin_message_id(str) -> user_id(int)
        "last_user": 0,
        "user_lang": {},        # user_id(str) -> auto|zh|en|ja|fr|de|es|it|pt|ru
        "last_auto_reply": {},  # user_id(str) -> ts
        "user_meta": {},        # user_id(str) -> {name, username, language_code, first_seen, last_seen, msg_count}
        "user_status": {},      # user_id(str) -> 已下单/退货退款/已返款/黑名单/用户来信
        "user_note": {},        # user_id(str) -> "..."
    }


def save_state(state: Dict[str, Any]) -> None:
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def is_admin(update: Update) -> bool:
    return bool(update.effective_user and update.effective_user.id == ADMIN_ID and ADMIN_ID > 0)


def fmt_time(ts: int) -> str:
    if not ts:
        return "-"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))


def log_event(direction: str, user_id: int, payload: Dict[str, Any]) -> None:
    rec = {"ts": _now_ts(), "direction": direction, "user_id": user_id, **payload}
    try:
        with LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception:
        pass


def remember_msg_index(state: Dict[str, Any], admin_message_id: int, user_id: int) -> None:
    mi = state.setdefault("msg_index", {})
    mi[str(admin_message_id)] = int(user_id)
    if len(mi) > MAX_MSG_INDEX:
        keys = list(mi.keys())
        for k in keys[: len(keys) - MAX_MSG_INDEX]:
            mi.pop(k, None)


def message_type_name(msg) -> str:
    if msg.photo:
        return "photo"
    if msg.sticker:
        return "sticker"
    if msg.voice:
        return "voice"
    if msg.video:
        return "video"
    if msg.document:
        return "document"
    if msg.animation:
        return "animation"
    if msg.audio:
        return "audio"
    if msg.video_note:
        return "video_note"
    if msg.contact:
        return "contact"
    if msg.location:
        return "location"
    if msg.poll:
        return "poll"
    if msg.text:
        return "text"
    return "unknown"


def auto_reply_text(lang: str) -> str:
    if lang == "zh":
        return AUTO_REPLY_ZH
    if lang == "ja":
        return AUTO_REPLY_JA
    if lang == "en":
        return AUTO_REPLY_EN
    return AUTO_REPLY_DEFAULT


def normalize_lang(code: str) -> str:
    c = (code or "").lower()
    if c.startswith("zh"):
        return "zh"
    if c.startswith("ja"):
        return "ja"
    if c.startswith("en"):
        return "en"
    if c.startswith("fr"):
        return "fr"
    if c.startswith("de"):
        return "de"
    if c.startswith("es"):
        return "es"
    if c.startswith("it"):
        return "it"
    if c.startswith("pt"):
        return "pt"
    if c.startswith("ru"):
        return "ru"
    return "en"


def to_mymemory_lang(lang: str) -> str:
    if lang == "zh":
        return "zh-CN"
    return lang


def detect_lang_local(text: str) -> str:
    """
    本地语言识别（免费，不依赖外部 API）
    返回：en/de/fr/es/it/pt/ru/zh/ja ... 不在支持范围就返回 'en'
    """
    t = (text or "").strip()
    if len(t) < 3:
        return "en"
    try:
        d = (detect(t) or "").lower()
        # langdetect 可能给 zh-cn / zh-tw 或 zh
        if d.startswith("zh"):
            return "zh"
        if d.startswith("ja"):
            return "ja"
        if d.startswith("en"):
            return "en"
        if d.startswith("de"):
            return "de"
        if d.startswith("fr"):
            return "fr"
        if d.startswith("es"):
            return "es"
        if d.startswith("it"):
            return "it"
        if d.startswith("pt"):
            return "pt"
        if d.startswith("ru"):
            return "ru"
        return "en"
    except Exception:
        return "en"


# ================== FREE TRANSLATE (MyMemory) ==================
_http: Optional[ClientSession] = None
_translate_cache: Dict[Tuple[str, str, str], str] = {}


async def translate_text(text: str, src: str, dst: str) -> str:
    if not TRANSLATE_ENABLED:
        return text
    text = (text or "").strip()
    if not text or src == dst:
        return text

    key = (text, src, dst)
    if key in _translate_cache:
        return _translate_cache[key]

    global _http
    if _http is None:
        _http = ClientSession(timeout=None)

    try:
        import urllib.parse
        q = urllib.parse.quote(text)
        url = f"https://api.mymemory.translated.net/get?q={q}&langpair={src}|{dst}"
        async with _http.get(url) as resp:
            data = await resp.json(content_type=None)
        out = ((data.get("responseData") or {}).get("translatedText") or "").strip()
        out = out or text
        _translate_cache[key] = out
        if len(_translate_cache) > 2000:
            _translate_cache.clear()
        return out
    except Exception:
        return text


# ================== UI ==================
def language_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("✅ Auto", callback_data="ulang|auto"),
            InlineKeyboardButton("中文", callback_data="ulang|zh"),
        ],
        [
            InlineKeyboardButton("English", callback_data="ulang|en"),
            InlineKeyboardButton("日本語", callback_data="ulang|ja"),
        ],
        [
            InlineKeyboardButton("Français", callback_data="ulang|fr"),
            InlineKeyboardButton("Deutsch", callback_data="ulang|de"),
        ],
        [
            InlineKeyboardButton("Español", callback_data="ulang|es"),
            InlineKeyboardButton("Italiano", callback_data="ulang|it"),
        ],
        [
            InlineKeyboardButton("Português", callback_data="ulang|pt"),
            InlineKeyboardButton("Русский", callback_data="ulang|ru"),
        ],
    ]
    return InlineKeyboardMarkup(rows)


def start_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("联系管理员", url=ADMIN_CONTACT_URL)],
        [InlineKeyboardButton("选择语言 / Choose language", callback_data="ulang_menu")],
    ])


# ================== ADMIN: STATUS BUTTONS (精简) ==================
def admin_ticket_keyboard(uid: int) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("已下单", callback_data=f"status|{uid}|已下单"),
         InlineKeyboardButton("退货退款", callback_data=f"status|{uid}|退货退款")],
        [InlineKeyboardButton("已返款", callback_data=f"status|{uid}|已返款"),
         InlineKeyboardButton("黑名单", callback_data=f"status|{uid}|黑名单")],
        [InlineKeyboardButton("清空状态", callback_data=f"status|{uid}|清空状态"),
         InlineKeyboardButton("Profile", callback_data=f"profile|{uid}|-")],
    ]
    return InlineKeyboardMarkup(rows)


def render_ticket_header(state: Dict[str, Any], uid: int) -> str:
    uid_key = str(uid)
    t = (state.get("tickets") or {}).get(uid_key, {})
    meta = (state.get("user_meta") or {}).get(uid_key, {})
    note = (state.get("user_note") or {}).get(uid_key, "")
    status = (state.get("user_status") or {}).get(uid_key, "用户来信")

    ticket_id = t.get("ticket_id", "-")
    t_status = t.get("status", "open")

    name = meta.get("name", "Unknown")
    username = meta.get("username")
    lang_code = meta.get("language_code", "") or "-"
    user_link = f"tg://user?id={uid}"

    first_seen = fmt_time(int(meta.get("first_seen", 0) or 0))
    last_seen = fmt_time(int(meta.get("last_seen", 0) or 0))
    msg_count = int(meta.get("msg_count", 0) or 0)

    lines = []
    lines.append(f"<b>Ticket #{ticket_id}</b>   <b>Status:</b> <code>{html_escape(t_status)}</code>")
    lines.append(f"<b>工单状态:</b> <code>{html_escape(status)}</code>")
    lines.append(f"<b>Name:</b> {html_escape(name)}")
    if username:
        lines.append(f"<b>Username:</b> @{html_escape(username)}")
    lines.append(f"<b>UserID:</b> <code>{uid}</code>   <b>Open:</b> <a href=\"{user_link}\">Click</a>")
    lines.append(f"<b>User lang:</b> <code>{html_escape(lang_code)}</code>")
    lines.append(f"<b>First seen:</b> <code>{html_escape(first_seen)}</code>")
    lines.append(f"<b>Last seen:</b> <code>{html_escape(last_seen)}</code>   <b>Msg count:</b> <code>{msg_count}</code>")
    lines.append(f"<b>Note:</b> {html_escape(note) if note else '-'}")
    lines.append("")
    lines.append("推荐：在管理员私聊里 <b>Reply</b> 下面那条“转发自用户”的消息，即可回复对方（支持文字/图片/文件/贴纸/语音等）。")
    lines.append("命令：<code>/note &lt;uid&gt; ...</code>  <code>/history &lt;uid&gt; 20</code>  <code>/close &lt;uid&gt;</code>  <code>/reopen &lt;uid&gt;</code>")
    lines.append(f"快捷文本：<code>/reply &lt;uid&gt; 你的回复</code> 或 <code>/r 你的回复</code>（回复最近用户）")
    return "\n".join(lines)


async def ensure_ticket(state: Dict[str, Any], context: ContextTypes.DEFAULT_TYPE, uid: int) -> Dict[str, Any]:
    tickets = state.setdefault("tickets", {})
    uid_key = str(uid)
    t = tickets.get(uid_key)

    need_new = True
    if t and t.get("status") == "open" and t.get("header_msg_id"):
        need_new = False

    if need_new:
        state["ticket_seq"] = int(state.get("ticket_seq", 0)) + 1
        ticket_id = state["ticket_seq"]

        msg = await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=render_ticket_header(state, uid),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=admin_ticket_keyboard(uid),
        )

        tickets[uid_key] = {
            "ticket_id": ticket_id,
            "status": "open",
            "created_at": _now_ts(),
            "header_msg_id": msg.message_id,
        }

    return tickets[uid_key]


async def refresh_header(state: Dict[str, Any], context: ContextTypes.DEFAULT_TYPE, uid: int) -> None:
    t = (state.get("tickets") or {}).get(str(uid))
    if not t or not t.get("header_msg_id"):
        return
    try:
        await context.bot.edit_message_text(
            chat_id=ADMIN_ID,
            message_id=int(t["header_msg_id"]),
            text=render_ticket_header(state, uid),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=admin_ticket_keyboard(uid),
        )
    except Exception:
        pass


# ================== COMMANDS ==================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    if update.effective_chat and update.effective_chat.type == ChatType.PRIVATE and not is_admin(update):
        await update.message.reply_text(
            "Hello, thank you for contacting us.\nPlease contact the administrator:",
            reply_markup=start_keyboard(),
        )
        return

    await update.message.reply_text(
        "管理员模式已启用。\n"
        "请在管理员私聊中 Reply 那条“转发自用户”的消息来回复用户（支持多媒体）。"
    )


async def cmd_note(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not (update.message and is_admin(update)):
        return
    if len(context.args) < 2:
        await update.message.reply_text("用法：/note <uid> <text>")
        return
    uid = int(context.args[0])
    note = " ".join(context.args[1:]).strip()
    st = load_state()
    st.setdefault("user_note", {})[str(uid)] = note
    save_state(st)
    await refresh_header(st, context, uid)
    await update.message.reply_text("已更新备注。")


async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not (update.message and is_admin(update)):
        return
    if not context.args:
        await update.message.reply_text("用法：/history <uid> [n]")
        return
    uid = int(context.args[0])
    n = 20
    if len(context.args) >= 2:
        try:
            n = max(1, min(100, int(context.args[1])))
        except Exception:
            n = 20

    if not LOG_FILE.exists():
        await update.message.reply_text("暂无历史记录。")
        return

    lines = LOG_FILE.read_text(encoding="utf-8").splitlines()
    recs = []
    for line in reversed(lines):
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if int(obj.get("user_id", 0)) == uid:
            recs.append(obj)
            if len(recs) >= n:
                break
    if not recs:
        await update.message.reply_text("该用户暂无记录。")
        return
    recs.reverse()

    out = [f"History {uid} (last {len(recs)})\n"]
    for r in recs:
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(r.get("ts", 0))))
        direction = "IN " if r.get("direction") == "in" else "OUT"
        typ = r.get("type", "msg")
        text = (r.get("text") or "").replace("\n", " ")
        if len(text) > 80:
            text = text[:80] + "..."
        out.append(f"{ts} {direction} {typ}  {text}")
    await update.message.reply_text("\n".join(out)[:3900])


async def cmd_close(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not (update.message and is_admin(update)):
        return
    if not context.args:
        await update.message.reply_text("用法：/close <uid>")
        return
    uid = int(context.args[0])
    st = load_state()
    t = (st.get("tickets") or {}).get(str(uid))
    if not t:
        await update.message.reply_text("该用户没有 ticket。")
        return
    t["status"] = "closed"
    save_state(st)
    await refresh_header(st, context, uid)
    await update.message.reply_text("已关闭。")


async def cmd_reopen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not (update.message and is_admin(update)):
        return
    if not context.args:
        await update.message.reply_text("用法：/reopen <uid>")
        return
    uid = int(context.args[0])
    st = load_state()
    t = (st.get("tickets") or {}).get(str(uid))
    if not t:
        await update.message.reply_text("该用户没有 ticket。")
        return
    t["status"] = "open"
    save_state(st)
    await refresh_header(st, context, uid)
    await update.message.reply_text("已重新打开。")


# ================== CALLBACKS ==================
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.callback_query:
        return
    q = update.callback_query
    data = q.data or ""
    await q.answer()

    # 用户语言菜单
    if data == "ulang_menu":
        if q.message:
            await q.message.reply_text("请选择语言 / Choose language:", reply_markup=language_keyboard())
        return

    if data.startswith("ulang|"):
        lang = data.split("|", 1)[1].strip()
        if not q.from_user:
            return
        st = load_state()
        st.setdefault("user_lang", {})[str(q.from_user.id)] = lang
        save_state(st)
        if q.message:
            await q.message.reply_text(f"已设置语言：{lang}")
        return

    # 管理员按钮
    if not is_admin(update):
        return

    parts = data.split("|")
    if len(parts) < 2:
        return
    action = parts[0]
    uid = int(parts[1])

    st = load_state()

    if action == "status" and len(parts) >= 3:
        val = parts[2]
        if val == "清空状态":
            st.setdefault("user_status", {}).pop(str(uid), None)
        else:
            st.setdefault("user_status", {})[str(uid)] = val
        save_state(st)
        await refresh_header(st, context, uid)
        return

    if action == "profile":
        try:
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=render_ticket_header(st, uid),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=admin_ticket_keyboard(uid),
            )
        except Exception:
            pass
        return


# ================== CORE: USER -> ADMIN ==================
async def handle_user_private(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat:
        return
    if update.effective_chat.type != ChatType.PRIVATE:
        return
    if is_admin(update):
        return

    user = update.effective_user
    uid = int(getattr(user, "id", 0) or 0)
    if uid <= 0:
        return

    st = load_state()

    # meta
    meta = st.setdefault("user_meta", {}).setdefault(str(uid), {})
    meta.setdefault("first_seen", _now_ts())
    meta["last_seen"] = _now_ts()
    meta["msg_count"] = int(meta.get("msg_count", 0) or 0) + 1
    meta["name"] = (getattr(user, "full_name", "") or "Unknown").strip()
    meta["username"] = getattr(user, "username", None)
    meta["language_code"] = getattr(user, "language_code", "")

    # ticket
    t = await ensure_ticket(st, context, uid)
    st["last_user"] = uid

    # 转发原消息（保留“转发自用户”格式）
    forwarded_msg_id = None
    try:
        fwd = await context.bot.forward_message(
            chat_id=ADMIN_ID,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id,
        )
        forwarded_msg_id = fwd.message_id
        remember_msg_index(st, fwd.message_id, uid)
    except Exception:
        copied = await context.bot.copy_message(
            chat_id=ADMIN_ID,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id,
        )
        forwarded_msg_id = copied.message_id
        remember_msg_index(st, copied.message_id, uid)

    if t.get("header_msg_id"):
        remember_msg_index(st, int(t["header_msg_id"]), uid)

    # 历史
    typ = message_type_name(update.message)
    preview = (update.message.text or update.message.caption or "")
    log_event("in", uid, {"type": typ, "text": preview[:1000]})

    # ✅ 用户 -> 管理员：文本自动翻译成中文（reply 到转发消息下面）
    if TRANSLATE_ENABLED:
        raw_text = (update.message.text or update.message.caption or "").strip()
        if raw_text:
            # 优先：用户选择的语言；否则：本地识别（解决德语/法语但language_code是en的问题）
            chosen = (st.get("user_lang") or {}).get(str(uid), "auto")
            if chosen != "auto":
                src_short = chosen
            else:
                src_short = detect_lang_local(raw_text)  # 关键修复点

            src = to_mymemory_lang(src_short)
            dst = ADMIN_TARGET_LANG
            zh = await translate_text(raw_text, src=src, dst=dst)

            # 避免翻译等于原文时刷屏
            if zh and zh.strip() and zh.strip() != raw_text.strip():
                try:
                    await context.bot.send_message(
                        chat_id=ADMIN_ID,
                        text=f"中文翻译（{src} → {dst}）：\n{zh}",
                        reply_to_message_id=forwarded_msg_id,
                    )
                except Exception:
                    pass

    # 自动回复（24h 冷却）
    last_ts = int((st.get("last_auto_reply") or {}).get(str(uid), 0) or 0)
    now_ts = _now_ts()
    if now_ts - last_ts >= AUTO_REPLY_COOLDOWN_SEC:
        # 自动回复语言：优先用户选择，否则使用 Telegram 的 language_code 粗略判断
        chosen = (st.get("user_lang") or {}).get(str(uid), "auto")
        if chosen != "auto":
            lang = chosen
        else:
            lang = normalize_lang(getattr(user, "language_code", "") or "")
        reply_text = auto_reply_text(lang if lang in {"zh", "en", "ja"} else "en")

        try:
            await update.message.reply_text(reply_text, reply_markup=start_keyboard())
        except Exception:
            pass
        st.setdefault("last_auto_reply", {})[str(uid)] = now_ts

    save_state(st)
    await refresh_header(st, context, uid)


# ================== CORE: ADMIN Reply -> USER (多媒体) ==================
async def handle_admin_private(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat:
        return
    if update.effective_chat.type != ChatType.PRIVATE:
        return
    if not is_admin(update):
        return

    if not update.message.reply_to_message:
        return

    st = load_state()
    rid = str(update.message.reply_to_message.message_id)
    to_user = None
    if rid in (st.get("msg_index") or {}):
        to_user = int(st["msg_index"][rid])

    if not to_user:
        await update.message.reply_text("没识别到用户ID。请 Reply 用户转发消息。")
        return

    try:
        await context.bot.copy_message(
            chat_id=to_user,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id,
        )

        typ = message_type_name(update.message)
        preview = (update.message.text or update.message.caption or "")
        log_event("out", to_user, {"type": typ, "text": preview[:1000]})

        st["last_user"] = to_user
        save_state(st)
        await update.message.reply_text("已发送。")
    except Exception as e:
        await update.message.reply_text(f"发送失败：{e}")


# ================== WEBHOOK SERVER ==================
async def run_webhook_server(tg_app: Application):
    if not PUBLIC_URL:
        raise RuntimeError("Missing PUBLIC_URL (or RENDER_EXTERNAL_URL).")
    if not WEBHOOK_SECRET:
        raise RuntimeError("Missing WEBHOOK_SECRET.")

    webhook_path = f"/{WEBHOOK_SECRET}"
    webhook_url = f"{PUBLIC_URL}{webhook_path}"

    await tg_app.initialize()
    await tg_app.start()
    await tg_app.bot.set_webhook(url=webhook_url, drop_pending_updates=True)

    aio = web.Application()

    async def health(_request):
        return web.Response(text="ok")

    async def handle_update(request: web.Request):
        try:
            data = await request.json()
        except Exception:
            return web.Response(status=400, text="bad json")
        update = Update.de_json(data, tg_app.bot)
        await tg_app.process_update(update)
        return web.Response(text="ok")

    aio.router.add_get(HEALTH_PATH, health)
    aio.router.add_post(webhook_path, handle_update)

    runner = web.AppRunner(aio)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=PORT)
    await site.start()

    print(f"[ok] webhook set: {webhook_url}")
    print(f"[ok] listening on 0.0.0.0:{PORT}, health: {HEALTH_PATH}")
    await asyncio.Event().wait()


def main():
    if not TOKEN:
        raise SystemExit("Missing TG_BOT_TOKEN")
    if ADMIN_ID <= 0:
        raise SystemExit("Missing TG_ADMIN_ID")

    tg_app = Application.builder().token(TOKEN).build()

    tg_app.add_handler(CommandHandler("start", cmd_start))
    tg_app.add_handler(CommandHandler("note", cmd_note))
    tg_app.add_handler(CommandHandler("history", cmd_history))
    tg_app.add_handler(CommandHandler("close", cmd_close))
    tg_app.add_handler(CommandHandler("reopen", cmd_reopen))

    tg_app.add_handler(CallbackQueryHandler(on_callback))

    tg_app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.User(user_id=ADMIN_ID), handle_user_private))
    tg_app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.User(user_id=ADMIN_ID), handle_admin_private))

    if PUBLIC_URL:
        asyncio.run(run_webhook_server(tg_app))
    else:
        tg_app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
