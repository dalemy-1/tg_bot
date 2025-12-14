import os
import re
import json
import time
import asyncio
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import aiohttp
from aiohttp import web

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

# ====== (Optional) language detection ======
# pip install langdetect
try:
    from langdetect import detect  # type: ignore
except Exception:
    detect = None  # fallback


# ================== ENV ==================
TOKEN = (os.getenv("TG_BOT_TOKEN") or "").strip()
ADMIN_ID = int(os.getenv("TG_ADMIN_ID", "0") or "0")

PUBLIC_URL = (os.getenv("RENDER_EXTERNAL_URL") or os.getenv("PUBLIC_URL") or "").strip().rstrip("/")
WEBHOOK_SECRET = (os.getenv("WEBHOOK_SECRET") or "").strip()
PORT = int(os.getenv("PORT", "10000"))
HEALTH_PATH = "/healthz"

# 用户端“一键联系管理员”
ADMIN_CONTACT_URL = (os.getenv("ADMIN_CONTACT_URL") or "https://t.me/Adalemy").strip()

# 24小时内只自动回复一次（默认 86400 秒）
AUTO_REPLY_COOLDOWN_SEC = int(os.getenv("AUTO_REPLY_COOLDOWN_SEC", "86400"))

AUTO_REPLY_ZH = (os.getenv("AUTO_REPLY_ZH") or "你好，已收到你的消息，我们会尽快回复。").strip()
AUTO_REPLY_EN = (os.getenv("AUTO_REPLY_EN") or "Hello, we received your message and will reply soon.").strip()
AUTO_REPLY_JA = (os.getenv("AUTO_REPLY_JA") or "メッセージを受け取りました。できるだけ早く返信します。").strip()
AUTO_REPLY_DEFAULT = (os.getenv("AUTO_REPLY_TEXT") or "已收到，请联系管理员。").strip()

# 翻译开关：1/0
TRANSLATE_ENABLED = (os.getenv("TRANSLATE_ENABLED") or "1").strip() == "1"
# 管理员侧希望看到的语言（通常中文）
ADMIN_TARGET_LANG = (os.getenv("ADMIN_TARGET_LANG") or "zh-CN").strip()

# 翻译提供方（免费）
# - mymemory: 免费、无需 key，但有限流/偶发失败
TRANSLATE_PROVIDER = (os.getenv("TRANSLATE_PROVIDER") or "mymemory").strip().lower()
MYMEMORY_EMAIL = (os.getenv("MYMEMORY_EMAIL") or "").strip()  # 可不填；填了通常更稳定一点

BASE_DIR = Path(__file__).resolve().parent
STATE_FILE = BASE_DIR / "support_state.json"
LOG_FILE = BASE_DIR / "history.jsonl"

MAX_MSG_INDEX = 12000  # admin_message_id -> user_id 的映射上限（避免文件无限长）

# 精简状态按钮：仅保留你要的
STATUS_BUTTONS = ["已下单", "退货退款", "已返款", "黑名单", "清空状态"]


# ================== STATE ==================
def _now_ts() -> int:
    return int(time.time())


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
        "last_user": 0,         # 最近一个用户
        "user_lang": {},        # user_id(str) -> auto|zh|en|ja|fr|de|es|it|pt|ru...
        "last_auto_reply": {},  # user_id(str) -> ts
        "user_meta": {},        # user_id(str) -> {name, username, language_code, first_seen, last_seen, msg_count, last_detected_lang}
        "user_status": {},      # user_id(str) -> 已下单/退货退款/已返款/黑名单/用户来信/...
        "user_note": {},        # user_id(str) -> "..."
    }


def save_state(state: Dict[str, Any]) -> None:
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


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


def is_admin(update: Update) -> bool:
    return bool(update.effective_user and update.effective_user.id == ADMIN_ID and ADMIN_ID > 0)


def fmt_time(ts: int) -> str:
    if not ts:
        return "-"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))


def contains_cjk(text: str) -> bool:
    # 粗略判断是否包含中文/日文汉字（用于判断管理员是否发中文）
    return bool(re.search(r"[\u4e00-\u9fff]", text or ""))


def normalize_lang(code: str) -> str:
    """把各种语言码统一成翻译接口常用格式"""
    c = (code or "").strip()
    if not c:
        return "auto"
    c = c.replace("_", "-")
    lc = c.lower()

    # Telegram language_code 常见：zh-hans / zh-hant / en / de / fr / es / it / ja / ru / pt-br ...
    if lc.startswith("zh"):
        return "zh-CN"
    if lc.startswith("ja"):
        return "ja"
    if lc.startswith("en"):
        return "en"
    if lc.startswith("de"):
        return "de"
    if lc.startswith("fr"):
        return "fr"
    if lc.startswith("es"):
        return "es"
    if lc.startswith("it"):
        return "it"
    if lc.startswith("pt"):
        return "pt"
    if lc.startswith("ru"):
        return "ru"

    # 其它语言直接返回前两位（更通用）
    if len(lc) >= 2:
        return lc[:2]
    return lc


def pick_user_target_lang(state: Dict[str, Any], user: Any) -> str:
    """用户希望收到的语言：优先 user_lang(用户自己选)，否则 telegram language_code，否则最近检测语言，否则 en"""
    uid = str(getattr(user, "id", 0) or 0)
    forced = (state.get("user_lang") or {}).get(uid, "auto")

    if forced and forced != "auto":
        return normalize_lang(forced)

    tg_code = normalize_lang(getattr(user, "language_code", "") or "")
    if tg_code != "auto":
        return tg_code

    meta = (state.get("user_meta") or {}).get(uid, {})
    last_det = normalize_lang(meta.get("last_detected_lang", "") or "")
    if last_det != "auto":
        return last_det

    return "en"


def auto_reply_text_for(lang: str) -> str:
    lang = normalize_lang(lang)
    if lang.startswith("zh"):
        return AUTO_REPLY_ZH
    if lang == "ja":
        return AUTO_REPLY_JA
    if lang == "en":
        return AUTO_REPLY_EN
    # 其它语言默认英文或通用
    return AUTO_REPLY_DEFAULT


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


# ================== TRANSLATION (free) ==================
async def translate_mymemory(text: str, src: str, dst: str) -> Optional[str]:
    if not text.strip():
        return ""
    q = text.strip()
    src = normalize_lang(src)
    dst = normalize_lang(dst)
    if src == "auto":
        # MyMemory 不支持 auto 源语言，我们尽量先 detect
        if detect:
            try:
                src = normalize_lang(detect(q))
            except Exception:
                src = "en"
        else:
            src = "en"

    params = {"q": q, "langpair": f"{src}|{dst}"}
    if MYMEMORY_EMAIL:
        params["de"] = MYMEMORY_EMAIL

    url = "https://api.mymemory.translated.net/get"
    timeout = aiohttp.ClientTimeout(total=12)

    try:
        async with aiohttp.ClientSession(timeout=timeout) as sess:
            async with sess.get(url, params=params) as r:
                if r.status != 200:
                    return None
                data = await r.json()
                out = ((data or {}).get("responseData") or {}).get("translatedText")
                if isinstance(out, str) and out.strip():
                    return out.strip()
                return None
    except Exception:
        return None


async def translate_text(text: str, src: str, dst: str) -> Optional[str]:
    if not TRANSLATE_ENABLED:
        return None
    if TRANSLATE_PROVIDER == "mymemory":
        return await translate_mymemory(text, src, dst)
    # 未来你要换其它免费服务，可在这里扩展
    return None


def detect_lang_best_effort(text: str, fallback: str = "en") -> str:
    if not text.strip():
        return fallback
    if detect:
        try:
            return normalize_lang(detect(text))
        except Exception:
            return normalize_lang(fallback)
    return normalize_lang(fallback)


# ================== UI: Buttons ==================
def user_contact_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("联系管理员", url=ADMIN_CONTACT_URL)]]
    )


def ticket_keyboard(uid: int) -> InlineKeyboardMarkup:
    # 两列按钮更紧凑
    rows = []
    for i in range(0, len(STATUS_BUTTONS), 2):
        chunk = STATUS_BUTTONS[i:i + 2]
        rows.append([InlineKeyboardButton(t, callback_data=f"status|{uid}|{t}") for t in chunk])

    rows.append([InlineKeyboardButton("Profile", callback_data=f"profile|{uid}|-")])
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
    user_link = f"tg://user?id={uid}"

    first_seen = fmt_time(int(meta.get("first_seen", 0) or 0))
    last_seen = fmt_time(int(meta.get("last_seen", 0) or 0))
    msg_count = int(meta.get("msg_count", 0) or 0)

    lines = [
        f"Ticket #{ticket_id}  Status: {t_status}",
        f"状态归类: {status}",
        f"Name: {name}",
    ]
    if username:
        lines.append(f"Username: @{username}")
    lines += [
        f"UserID: {uid}  Open: Click ({user_link})",
        f"First seen: {first_seen}",
        f"Last seen: {last_seen}  Msg count: {msg_count}",
    ]
    if note:
        lines.append(f"Note: {note}")

    lines.append("")
    lines.append("推荐：在管理员私聊里 Reply（回复）下面那条“转发自用户”的消息，即可回复对方（支持文字/图片/文件/贴纸/语音等）。")
    lines.append("备用命令：/reply <uid> <text> 或 /r <text>（回复最近用户）")
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

        header_text = render_ticket_header(state, uid)
        msg = await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=header_text,
            reply_markup=ticket_keyboard(uid),
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
            reply_markup=ticket_keyboard(uid),
        )
    except Exception:
        pass


# ================== COMMANDS (Admin) ==================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    if is_admin(update):
        await update.message.reply_text(
            "管理员模式已启用：私聊工单 + 精简状态按钮 + Reply 直接回复 + 多媒体转发 + 可选自动翻译。\n\n"
            "常用：直接 Reply 用户转发消息即可回复（支持媒体）。\n"
            "命令：\n"
            "/reply <uid> <text>\n"
            "/r <text>\n"
            "/note <uid> <text>\n"
            "/lang <uid> <auto|zh|en|ja|fr|de|es|it|pt|ru>\n"
            "/profile <uid>\n"
        )
    else:
        await update.message.reply_text(
            "你好，已连接客服。\n点击下方可一键联系管理员：",
            reply_markup=user_contact_keyboard(),
        )


async def cmd_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not is_admin(update):
        return
    if len(context.args) < 2:
        await update.message.reply_text("用法：/reply <user_id> <text>")
        return

    uid = int(context.args[0])
    raw_text = " ".join(context.args[1:])

    st = load_state()
    # 目标语言：用户选择/自动
    # 注意：这里没有 user 对象，只能用 state 里 meta 的 last_detected_lang/telegram language_code
    meta = (st.get("user_meta") or {}).get(str(uid), {})
    fake_user_lang = meta.get("language_code", "en")
    # 若 user_lang 强制指定，则优先
    forced = (st.get("user_lang") or {}).get(str(uid), "auto")
    target = normalize_lang(forced if forced != "auto" else fake_user_lang)
    if target == "auto":
        target = "en"

    send_text = raw_text
    if TRANSLATE_ENABLED and contains_cjk(raw_text):
        tr = await translate_text(raw_text, "zh-CN", target)
        if tr:
            send_text = tr

    await context.bot.send_message(chat_id=uid, text=send_text)
    st["last_user"] = uid
    save_state(st)
    log_event("out", uid, {"type": "text", "text": send_text[:1000]})
    await update.message.reply_text("已发送。")


async def cmd_r(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not is_admin(update):
        return
    if not context.args:
        await update.message.reply_text("用法：/r <text>")
        return

    st = load_state()
    uid = int(st.get("last_user", 0) or 0)
    if uid <= 0:
        await update.message.reply_text("没有最近用户。")
        return

    raw_text = " ".join(context.args)

    meta = (st.get("user_meta") or {}).get(str(uid), {})
    fake_user_lang = meta.get("language_code", "en")
    forced = (st.get("user_lang") or {}).get(str(uid), "auto")
    target = normalize_lang(forced if forced != "auto" else fake_user_lang)
    if target == "auto":
        target = "en"

    send_text = raw_text
    if TRANSLATE_ENABLED and contains_cjk(raw_text):
        tr = await translate_text(raw_text, "zh-CN", target)
        if tr:
            send_text = tr

    await context.bot.send_message(chat_id=uid, text=send_text)
    st["last_user"] = uid
    save_state(st)
    log_event("out", uid, {"type": "text", "text": send_text[:1000]})
    await update.message.reply_text("已发送。")


async def cmd_note(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not is_admin(update):
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


async def cmd_lang(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not is_admin(update):
        return
    if len(context.args) < 2:
        await update.message.reply_text("用法：/lang <uid> <auto|zh|en|ja|fr|de|es|it|pt|ru>")
        return
    uid = int(context.args[0])
    lang = context.args[1].strip().lower()
    allow = {"auto", "zh", "en", "ja", "fr", "de", "es", "it", "pt", "ru", "zh-cn"}
    if lang not in allow:
        await update.message.reply_text("不支持该语言码。")
        return
    st = load_state()
    st.setdefault("user_lang", {})[str(uid)] = lang
    save_state(st)
    await update.message.reply_text(f"已设置用户 {uid} 语言：{lang}")


async def cmd_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not is_admin(update):
        return
    if not context.args:
        await update.message.reply_text("用法：/profile <uid>")
        return
    uid = int(context.args[0])
    st = load_state()
    await update.message.reply_text(render_ticket_header(st, uid))


# ================== CALLBACK (Status buttons) ==================
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.callback_query:
        return
    q = update.callback_query
    await q.answer()

    if not is_admin(update):
        return

    data = q.data or ""
    parts = data.split("|")
    if len(parts) < 2:
        return

    action = parts[0]
    uid = int(parts[1])

    st = load_state()

    if action == "status" and len(parts) >= 3:
        status = parts[2]
        if status == "清空状态":
            st.setdefault("user_status", {})[str(uid)] = "用户来信"
        else:
            st.setdefault("user_status", {})[str(uid)] = status
        save_state(st)
        await refresh_header(st, context, uid)
        return

    if action == "profile":
        await context.bot.send_message(chat_id=ADMIN_ID, text=render_ticket_header(st, uid))
        return


# ================== USER -> ADMIN ==================
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

    # 默认状态：用户来信
    st.setdefault("user_status", {}).setdefault(str(uid), "用户来信")

    # ticket
    t = await ensure_ticket(st, context, uid)
    st["last_user"] = uid

    # 1) 转发原消息给管理员：保留“转发自用户”格式
    try:
        fwd = await context.bot.forward_message(
            chat_id=ADMIN_ID,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id,
        )
        remember_msg_index(st, fwd.message_id, uid)
        # 同时把 header 也记进索引（防止管理员误 Reply header）
        if t.get("header_msg_id"):
            remember_msg_index(st, int(t["header_msg_id"]), uid)
    except Exception:
        copied = await context.bot.copy_message(
            chat_id=ADMIN_ID,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id,
        )
        remember_msg_index(st, copied.message_id, uid)
        if t.get("header_msg_id"):
            remember_msg_index(st, int(t["header_msg_id"]), uid)

    # 2) 翻译：只对 text/caption 做（媒体本身无法“翻译”）
    typ = message_type_name(update.message)
    preview = (update.message.text or update.message.caption or "")
    log_event("in", uid, {"type": typ, "text": preview[:1000]})

    if TRANSLATE_ENABLED and preview.strip():
        src_guess = detect_lang_best_effort(preview, fallback=(meta.get("language_code") or "en"))
        meta["last_detected_lang"] = src_guess
        # 统一翻译成管理员目标语言（通常中文）
        if normalize_lang(src_guess) != normalize_lang(ADMIN_TARGET_LANG):
            tr = await translate_text(preview, src_guess, ADMIN_TARGET_LANG)
            if tr:
                # 给管理员发一条“翻译结果”，并 reply 到原转发消息（更清晰）
                try:
                    await context.bot.send_message(
                        chat_id=ADMIN_ID,
                        text=f"中文翻译（{src_guess} → {ADMIN_TARGET_LANG}）：\n{tr}",
                    )
                except Exception:
                    pass

    # 3) 24h 自动回复一次 + “联系管理员”按钮
    last_ts = int((st.get("last_auto_reply") or {}).get(str(uid), 0) or 0)
    now_ts = _now_ts()
    if now_ts - last_ts >= AUTO_REPLY_COOLDOWN_SEC:
        # 用用户目标语言回一条
        user_target = pick_user_target_lang(st, user)
        text = auto_reply_text_for(user_target)
        try:
            await update.message.reply_text(text, reply_markup=user_contact_keyboard())
        except Exception:
            pass
        st.setdefault("last_auto_reply", {})[str(uid)] = now_ts

    save_state(st)
    await refresh_header(st, context, uid)


# ================== ADMIN Reply -> USER ==================
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
        await update.message.reply_text("没识别到用户ID。请 Reply 用户转发消息，或用 /reply <uid> <text>。")
        return

    # 用户目标语言：强制 user_lang > telegram language_code > last_detected_lang > en
    meta = (st.get("user_meta") or {}).get(str(to_user), {})
    forced = (st.get("user_lang") or {}).get(str(to_user), "auto")
    target = normalize_lang(forced if forced != "auto" else (meta.get("language_code") or ""))
    if target == "auto":
        target = normalize_lang(meta.get("last_detected_lang", "") or "")
    if target == "auto":
        target = "en"

    try:
        # 1) 文字消息：检测管理员原文语言 -> 翻译到用户目标语言
        if update.message.text and update.message.text.strip():
            raw = update.message.text.strip()

            send_text = raw
            if TRANSLATE_ENABLED:
                src = detect_lang_best_effort(raw, fallback="zh-CN")  # 自动识别管理员消息语言
                # 只要 src != target，就翻译
                if normalize_lang(src) != normalize_lang(target):
                    tr = await translate_text(raw, src, target)
                    if tr and tr.strip():
                        send_text = tr.strip()
                    else:
                        # 翻译失败：在管理员侧提示一下原因（用户侧仍发原文，避免消息丢失）
                        await update.message.reply_text("翻译失败（可能限流/接口不稳定），已发送原文。")

            await context.bot.send_message(chat_id=to_user, text=send_text)

            st["last_user"] = to_user
            save_state(st)
            log_event("out", to_user, {"type": "text", "text": send_text[:1000]})
            await update.message.reply_text("已发送。")
            return

        # 2) 非文字（图片/文件/贴纸/语音等）：直接转发媒体
        await context.bot.copy_message(
            chat_id=to_user,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id,
        )

        # 3) 如果有 caption，额外再发一条“翻译后的 caption”（可选但实用）
        if update.message.caption and update.message.caption.strip() and TRANSLATE_ENABLED:
            cap = update.message.caption.strip()
            src = detect_lang_best_effort(cap, fallback="zh-CN")
            if normalize_lang(src) != normalize_lang(target):
                tr = await translate_text(cap, src, target)
                if tr and tr.strip():
                    await context.bot.send_message(chat_id=to_user, text=tr.strip())

        st["last_user"] = to_user
        save_state(st)
        typ = message_type_name(update.message)
        preview = (update.message.caption or "")
        log_event("out", to_user, {"type": typ, "text": preview[:1000]})
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
    tg_app.add_handler(CommandHandler("reply", cmd_reply))
    tg_app.add_handler(CommandHandler("r", cmd_r))
    tg_app.add_handler(CommandHandler("note", cmd_note))
    tg_app.add_handler(CommandHandler("lang", cmd_lang))
    tg_app.add_handler(CommandHandler("profile", cmd_profile))

    tg_app.add_handler(CallbackQueryHandler(on_callback))

    # user private
    tg_app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.User(user_id=ADMIN_ID), handle_user_private))
    # admin private
    tg_app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.User(user_id=ADMIN_ID), handle_admin_private))

    if PUBLIC_URL:
        asyncio.run(run_webhook_server(tg_app))
    else:
        tg_app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()

