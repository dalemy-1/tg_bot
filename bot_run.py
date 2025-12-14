import os
import re
import json
import time
import asyncio
from pathlib import Path
from typing import Any, Dict, Optional

from aiohttp import web, ClientSession
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

# ================== ENV ==================
TOKEN = (os.getenv("TG_BOT_TOKEN") or "").strip()
ADMIN_ID = int(os.getenv("TG_ADMIN_ID", "0") or "0")  # 必填：管理员 user_id
PUBLIC_URL = (os.getenv("RENDER_EXTERNAL_URL") or os.getenv("PUBLIC_URL") or "").strip().rstrip("/")
WEBHOOK_SECRET = (os.getenv("WEBHOOK_SECRET") or "").strip()
PORT = int(os.getenv("PORT", "10000"))
HEALTH_PATH = "/healthz"

# 用户端“一键联系管理员”按钮（你要保留）
ADMIN_CONTACT_URL = (os.getenv("ADMIN_CONTACT_URL") or "https://t.me/Adalemy").strip()

# 自动回复（24小时内一次）
AUTO_REPLY_TEXT_ZH = (os.getenv("AUTO_REPLY_TEXT_ZH") or "你好，已收到你的消息，我们会尽快回复。").strip()
AUTO_REPLY_COOLDOWN_SEC = int(os.getenv("AUTO_REPLY_COOLDOWN_SEC") or "86400")

# 翻译（免费：MyMemory；不稳定属正常，可在日志里看是否被限流）
TRANSLATE_ENABLED = (os.getenv("TRANSLATE_ENABLED") or "1").strip() == "1"
ADMIN_TARGET_LANG = (os.getenv("ADMIN_TARGET_LANG") or "zh-CN").strip()  # 管理员侧显示统一翻成中文
MYMEMORY_EMAIL = (os.getenv("MYMEMORY_EMAIL") or "").strip()  # 可留空

BASE_DIR = Path(__file__).resolve().parent
STATE_FILE = BASE_DIR / "support_state.json"
LOG_FILE = BASE_DIR / "history.jsonl"
MAX_MSG_INDEX = 8000

# 工单状态按钮（精简版）
STATUS_BUTTONS = ["已下单", "退货退款", "已返款", "黑名单"]
STATUS_CLEAR = "清空状态"

# 用户可选语言（用户端 /language）
LANG_OPTIONS = [
    ("auto", "Auto"),
    ("zh-CN", "中文"),
    ("en", "English"),
    ("ja", "日本語"),
    ("fr", "Français"),
    ("de", "Deutsch"),
    ("es", "Español"),
    ("it", "Italiano"),
    ("pt", "Português"),
    ("ru", "Русский"),
]

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
        "tickets": {},            # user_id(str)->{ticket_id,status,created_at,header_msg_id}
        "msg_index": {},          # admin_message_id(str)->user_id(int)
        "last_user": 0,
        "user_lang": {},          # user_id(str)-> auto|zh-CN|en|ja|...
        "last_auto_reply": {},    # user_id(str)->ts
        "user_meta": {},          # user_id(str)->{name,username,language_code,first_seen,last_seen,msg_count,last_detected_lang}
        "user_status": {},        # user_id(str)-> 已下单/退货退款/已返款/黑名单/用户来信
        "user_note": {},          # user_id(str)->"..."
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

def normalize_lang(code: str) -> str:
    c = (code or "").strip()
    if not c:
        return "auto"
    c = c.replace("_", "-")
    low = c.lower()
    if low in {"auto"}:
        return "auto"
    if low.startswith("zh"):
        return "zh-CN"
    if low.startswith("ja") or low == "jp":
        return "ja"
    if low.startswith("en"):
        return "en"
    if low.startswith("fr"):
        return "fr"
    if low.startswith("de"):
        return "de"
    if low.startswith("es"):
        return "es"
    if low.startswith("it"):
        return "it"
    if low.startswith("pt"):
        return "pt"
    if low.startswith("ru"):
        return "ru"
    # 兜底：取前两位
    if len(low) >= 2:
        return low[:2]
    return "auto"

def user_target_lang(st: Dict[str, Any], uid: int) -> str:
    forced = (st.get("user_lang") or {}).get(str(uid), "auto")
    forced = normalize_lang(forced)
    if forced != "auto":
        return forced
    meta = (st.get("user_meta") or {}).get(str(uid), {})
    # Telegram language_code 只是参考，用户若选了 /language 会覆盖
    tg_code = normalize_lang(meta.get("language_code") or "")
    if tg_code != "auto":
        return tg_code
    last_det = normalize_lang(meta.get("last_detected_lang") or "")
    if last_det != "auto":
        return last_det
    return "en"

# ================== Translation (Free: MyMemory) ==================
_http: Optional[ClientSession] = None

async def http_session() -> ClientSession:
    global _http
    if _http is None or _http.closed:
        _http = ClientSession(timeout=None)
    return _http

def detect_lang_best_effort(text: str, fallback: str = "en") -> str:
    t = (text or "").strip()
    if not t:
        return normalize_lang(fallback)

    # 优先用 langid（解决你“只有英文能翻译”的核心点）
    try:
        import langid  # type: ignore
        code, _score = langid.classify(t)
        return normalize_lang(code)
    except Exception:
        pass

    # 没有 langid 的兜底：非常粗糙
    if re.search(r"[\u3040-\u30ff]", t):
        return "ja"
    if re.search(r"[\u4e00-\u9fff]", t):
        return "zh-CN"
    if re.search(r"[\u0400-\u04FF]", t):
        return "ru"
    return normalize_lang(fallback)

async def translate_text(text: str, src: str, tgt: str) -> Optional[str]:
    if not TRANSLATE_ENABLED:
        return None
    q = (text or "").strip()
    if not q:
        return None

    src_n = normalize_lang(src)
    tgt_n = normalize_lang(tgt)
    if src_n == "auto":
        src_n = detect_lang_best_effort(q, fallback="en")
    if src_n == tgt_n:
        return q

    # MyMemory: GET /get?q=...&langpair=src|tgt
    url = "https://api.mymemory.translated.net/get"
    params = {"q": q, "langpair": f"{src_n}|{tgt_n}"}
    if MYMEMORY_EMAIL:
        params["de"] = MYMEMORY_EMAIL

    try:
        s = await http_session()
        async with s.get(url, params=params) as resp:
            data = await resp.json(content_type=None)
        tr = (((data or {}).get("responseData") or {}).get("translatedText") or "").strip()
        # MyMemory 有时会返回原文或空
        if tr:
            return tr
        return None
    except Exception:
        return None

# ================== UI ==================
def contact_admin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("联系管理员", url=ADMIN_CONTACT_URL)]])

def language_keyboard(uid: int) -> InlineKeyboardMarkup:
    rows = []
    row = []
    for code, label in LANG_OPTIONS:
        row.append(InlineKeyboardButton(label, callback_data=f"lang|{uid}|{code}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("返回 / Back", callback_data=f"lang|{uid}|back")])
    return InlineKeyboardMarkup(rows)

def status_keyboard(uid: int) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("已下单", callback_data=f"status|{uid}|已下单"),
            InlineKeyboardButton("退货退款", callback_data=f"status|{uid}|退货退款"),
        ],
        [
            InlineKeyboardButton("已返款", callback_data=f"status|{uid}|已返款"),
            InlineKeyboardButton("黑名单", callback_data=f"status|{uid}|黑名单"),
        ],
        [
            InlineKeyboardButton("清空状态", callback_data=f"status|{uid}|清空状态"),
            InlineKeyboardButton("Profile", callback_data=f"profile|{uid}|-"),
        ],
    ]
    return InlineKeyboardMarkup(rows)

def render_header(st: Dict[str, Any], uid: int) -> str:
    meta = (st.get("user_meta") or {}).get(str(uid), {})
    status = (st.get("user_status") or {}).get(str(uid), "用户来信")
    note = (st.get("user_note") or {}).get(str(uid), "-")
    lang = (st.get("user_lang") or {}).get(str(uid), "auto")

    name = meta.get("name", "Unknown")
    username = meta.get("username")
    user_link = f"tg://user?id={uid}"

    first_seen = fmt_time(int(meta.get("first_seen", 0) or 0))
    last_seen = fmt_time(int(meta.get("last_seen", 0) or 0))
    msg_count = int(meta.get("msg_count", 0) or 0)

    lines = [
        f"*Ticket*  *Status:* `{status}`",
        f"*Name:* {name}",
    ]
    if username:
        lines.append(f"*Username:* @{username}")
    lines += [
        f"*UserID:* `{uid}`   *Open:* [Click]({user_link})",
        f"*Lang:* `{lang}`   *Note:* {note}",
        f"*First seen:* `{first_seen}`",
        f"*Last seen:* `{last_seen}`   *Msg count:* `{msg_count}`",
        "",
        "推荐：在管理员私聊里 Reply（回复）下面那条“转发自用户”的消息，即可回复对方（支持文字/图片/文件/贴纸/语音等）。",
        "用户语言：让用户在机器人里输入 /language 选择。",
    ]
    return "\n".join(lines)

async def ensure_header(st: Dict[str, Any], context: ContextTypes.DEFAULT_TYPE, uid: int) -> int:
    tickets = st.setdefault("tickets", {})
    t = tickets.get(str(uid))
    if t and t.get("header_msg_id"):
        return int(t["header_msg_id"])

    st["ticket_seq"] = int(st.get("ticket_seq", 0)) + 1
    msg = await context.bot.send_message(
        chat_id=ADMIN_ID,
        text=render_header(st, uid),
        parse_mode=ParseMode.MARKDOWN,
        disable_web_page_preview=True,
        reply_markup=status_keyboard(uid),
    )
    tickets[str(uid)] = {"ticket_id": st["ticket_seq"], "created_at": _now_ts(), "header_msg_id": msg.message_id}
    return msg.message_id

async def refresh_header(st: Dict[str, Any], context: ContextTypes.DEFAULT_TYPE, uid: int) -> None:
    t = (st.get("tickets") or {}).get(str(uid))
    if not t or not t.get("header_msg_id"):
        return
    try:
        await context.bot.edit_message_text(
            chat_id=ADMIN_ID,
            message_id=int(t["header_msg_id"]),
            text=render_header(st, uid),
            parse_mode=ParseMode.MARKDOWN,
            disable_web_page_preview=True,
            reply_markup=status_keyboard(uid),
        )
    except Exception:
        pass

# ================== Commands ==================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    if is_admin(update):
        await update.message.reply_text("管理员端已启用：工单+状态按钮+翻译+用户语言选择。")
    else:
        await update.message.reply_text(
            AUTO_REPLY_TEXT_ZH,
            reply_markup=contact_admin_keyboard()
        )

async def cmd_language(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_user:
        return
    # 仅用户端（非管理员）展示选择
    if is_admin(update):
        await update.message.reply_text("该命令给用户使用。管理员如需指定语言，请让用户在机器人里输入 /language 选择。")
        return
    uid = int(update.effective_user.id)
    await update.message.reply_text(
        "请选择语言 / Choose language:",
        reply_markup=language_keyboard(uid),
    )

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

async def cmd_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not is_admin(update):
        return
    if not context.args:
        await update.message.reply_text("用法：/profile <uid>")
        return
    uid = int(context.args[0])
    st = load_state()
    await update.message.reply_text(render_header(st, uid), parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)

# ================== Callbacks ==================
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.callback_query:
        return
    q = update.callback_query
    await q.answer()

    data = q.data or ""
    parts = data.split("|")
    if len(parts) < 2:
        return

    action = parts[0]
    uid = int(parts[1])

    st = load_state()

    # 用户选语言
    if action == "lang":
        if len(parts) >= 3 and parts[2] == "back":
            await q.edit_message_text("已返回 / Back.")
            return
        if len(parts) < 3:
            return
        code = normalize_lang(parts[2])
        st.setdefault("user_lang", {})[str(uid)] = code
        save_state(st)
        try:
            await q.edit_message_text(f"已设置语言：{code}")
        except Exception:
            pass
        return

    # 管理员点状态按钮
    if action == "status":
        if not (update.effective_user and update.effective_user.id == ADMIN_ID):
            return
        if len(parts) < 3:
            return
        val = parts[2]
        if val == STATUS_CLEAR:
            st.setdefault("user_status", {}).pop(str(uid), None)
        else:
            st.setdefault("user_status", {})[str(uid)] = val
        save_state(st)
        await refresh_header(st, context, uid)
        return

    if action == "profile":
        if not (update.effective_user and update.effective_user.id == ADMIN_ID):
            return
        try:
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=render_header(st, uid),
                parse_mode=ParseMode.MARKDOWN,
                disable_web_page_preview=True,
            )
        except Exception:
            pass
        return

# ================== Core: USER -> ADMIN ==================
async def handle_user_private(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    if update.effective_chat.type != ChatType.PRIVATE:
        return
    if is_admin(update):
        return

    uid = int(update.effective_user.id)
    st = load_state()

    meta = st.setdefault("user_meta", {}).setdefault(str(uid), {})
    meta.setdefault("first_seen", _now_ts())
    meta["last_seen"] = _now_ts()
    meta["msg_count"] = int(meta.get("msg_count", 0) or 0) + 1
    meta["name"] = (update.effective_user.full_name or "Unknown").strip()
    meta["username"] = update.effective_user.username
    meta["language_code"] = update.effective_user.language_code or ""

    # 默认状态：用户来信
    st.setdefault("user_status", {}).setdefault(str(uid), "用户来信")

    header_id = await ensure_header(st, context, uid)
    remember_msg_index(st, header_id, uid)

    # 1) 转发原消息给管理员（保留“转发自用户”格式）
    try:
        fwd = await context.bot.forward_message(
            chat_id=ADMIN_ID,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id,
        )
        remember_msg_index(st, fwd.message_id, uid)
        forwarded_msg_id = fwd.message_id
    except Exception:
        copied = await context.bot.copy_message(
            chat_id=ADMIN_ID,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id,
        )
        remember_msg_index(st, copied.message_id, uid)
        forwarded_msg_id = copied.message_id

    # 2) 如果是文本/带caption：自动翻译成中文，回复在转发消息下面（管理员更好看）
    preview = (update.message.text or update.message.caption or "").strip()
    if preview and TRANSLATE_ENABLED:
        src = detect_lang_best_effort(preview, fallback=meta.get("language_code") or "en")
        meta["last_detected_lang"] = src
        tr = await translate_text(preview, src, ADMIN_TARGET_LANG)
        if tr and tr.strip() and normalize_lang(src) != normalize_lang(ADMIN_TARGET_LANG):
            try:
                await context.bot.send_message(
                    chat_id=ADMIN_ID,
                    text=f"中文翻译（{src} → {ADMIN_TARGET_LANG}）：\n{tr}",
                    reply_to_message_id=forwarded_msg_id,
                )
            except Exception:
                pass

    # 写历史
    log_event("in", uid, {"type": "msg", "text": preview[:1000]})

    # 3) 自动回复（24h冷却）+ 联系管理员按钮
    last_ts = int((st.get("last_auto_reply") or {}).get(str(uid), 0) or 0)
    now_ts = _now_ts()
    if now_ts - last_ts >= AUTO_REPLY_COOLDOWN_SEC:
        try:
            await update.message.reply_text(
                AUTO_REPLY_TEXT_ZH,
                reply_markup=contact_admin_keyboard(),
            )
        except Exception:
            pass
        st.setdefault("last_auto_reply", {})[str(uid)] = now_ts

    st["last_user"] = uid
    save_state(st)
    await refresh_header(st, context, uid)

# ================== Core: ADMIN Reply -> USER ==================
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
        await update.message.reply_text("没识别到用户ID。请 Reply 用户转发消息再发。")
        return

    target = user_target_lang(st, to_user)

    # 文字：先检测语言 -> 翻译到用户目标语言 -> 只发翻译结果
    if update.message.text and update.message.text.strip():
        raw = update.message.text.strip()
        send_text = raw

        if TRANSLATE_ENABLED:
            src = detect_lang_best_effort(raw, fallback="zh-CN")
            if normalize_lang(src) != normalize_lang(target):
                tr = await translate_text(raw, src, target)
                if tr and tr.strip():
                    send_text = tr.strip()
                else:
                    await update.message.reply_text("翻译失败（免费接口可能限流/不稳定），已发送原文。")

        try:
            await context.bot.send_message(chat_id=to_user, text=send_text)
            st["last_user"] = to_user
            save_state(st)
            log_event("out", to_user, {"type": "text", "text": send_text[:1000], "target": target})
            await update.message.reply_text("已发送。")
        except Exception as e:
            await update.message.reply_text(f"发送失败：{e}")
        return

    # 媒体：先转发媒体；若有caption，再额外发送“翻译后的caption”
    try:
        await context.bot.copy_message(
            chat_id=to_user,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id,
        )

        cap = (update.message.caption or "").strip()
        if cap and TRANSLATE_ENABLED:
            src = detect_lang_best_effort(cap, fallback="zh-CN")
            if normalize_lang(src) != normalize_lang(target):
                tr = await translate_text(cap, src, target)
                if tr and tr.strip():
                    await context.bot.send_message(chat_id=to_user, text=tr.strip())

        st["last_user"] = to_user
        save_state(st)
        log_event("out", to_user, {"type": "media", "text": cap[:1000], "target": target})
        await update.message.reply_text("已发送。")
    except Exception as e:
        await update.message.reply_text(f"发送失败：{e}")

# ================== Webhook ==================
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

    # commands
    tg_app.add_handler(CommandHandler("start", cmd_start))
    tg_app.add_handler(CommandHandler("language", cmd_language))
    tg_app.add_handler(CommandHandler("note", cmd_note))
    tg_app.add_handler(CommandHandler("profile", cmd_profile))

    # callbacks
    tg_app.add_handler(CallbackQueryHandler(on_callback))

    # private handlers
    tg_app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.User(user_id=ADMIN_ID), handle_user_private))
    tg_app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.User(user_id=ADMIN_ID), handle_admin_private))

    if PUBLIC_URL:
        asyncio.run(run_webhook_server(tg_app))
    else:
        tg_app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
