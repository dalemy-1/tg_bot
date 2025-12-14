import os
import re
import json
import time
import asyncio
from pathlib import Path
from typing import Any, Dict, Optional

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

# ================== ENV ==================
TOKEN = (os.getenv("TG_BOT_TOKEN") or "").strip()
ADMIN_ID = int(os.getenv("TG_ADMIN_ID", "0") or "0")

PUBLIC_URL = (os.getenv("RENDER_EXTERNAL_URL") or os.getenv("PUBLIC_URL") or "").strip().rstrip("/")
WEBHOOK_SECRET = (os.getenv("WEBHOOK_SECRET") or "").strip()
PORT = int(os.getenv("PORT", "10000"))
HEALTH_PATH = "/healthz"

ADMIN_USERNAME = (os.getenv("ADMIN_USERNAME") or "Adalemy").strip().lstrip("@")

AUTO_REPLY_COOLDOWN_SEC = int(os.getenv("AUTO_REPLY_COOLDOWN_SEC", "86400"))  # 24h

BASE_DIR = Path(__file__).resolve().parent
STATE_FILE = BASE_DIR / "support_state.json"
LOG_FILE = BASE_DIR / "history.jsonl"

MAX_MSG_INDEX = 8000

# ================== LANGUAGE ==================
SUPPORTED_LANGS = ["auto", "zh", "en", "ja", "fr", "de", "es", "it"]
LANG_LABEL = {
    "auto": "Auto",
    "zh": "中文",
    "en": "English",
    "ja": "日本語",
    "fr": "Français",
    "de": "Deutsch",
    "es": "Español",
    "it": "Italiano",
}

AUTO_REPLY_TEXT = {
    "zh": "你好，已收到你的消息，我们会尽快回复。\n\n如需更快处理，可点击“一键联系管理员”。",
    "en": "Hello! We received your message and will reply as soon as possible.\n\nFor faster assistance, tap “Contact Admin”.",
    "ja": "メッセージを受け取りました。できるだけ早く返信します。\n\nお急ぎの場合は「管理者に連絡」を押してください。",
    "fr": "Bonjour, nous avons bien reçu votre message et nous vous répondrons dès que possible.\n\nPour une aide plus rapide, appuyez sur « Contacter l’admin ».",
    "de": "Hallo! Wir haben deine Nachricht erhalten und antworten so schnell wie möglich.\n\nFür schnellere Hilfe tippe auf „Admin kontaktieren“.",
    "es": "Hola, hemos recibido tu mensaje y responderemos lo antes posible.\n\nPara una atención más rápida, pulsa “Contactar al admin”.",
    "it": "Ciao! Abbiamo ricevuto il tuo messaggio e risponderemo il prima possibile.\n\nPer assistenza più rapida, premi “Contatta l’admin”.",
}

def _now_ts() -> int:
    return int(time.time())

def html_escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def is_admin(update: Update) -> bool:
    return bool(update.effective_user and update.effective_user.id == ADMIN_ID and ADMIN_ID > 0)

def load_state() -> Dict[str, Any]:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {
        "ticket_seq": 0,
        "tickets": {},          # uid(str) -> {ticket_id, status, created_at, header_msg_id}
        "msg_index": {},        # admin_message_id(str) -> uid(int)
        "last_user": 0,
        "user_lang": {},        # uid(str) -> auto|zh|en|...
        "last_auto_reply": {},  # uid(str) -> ts
        "user_meta": {},        # uid(str) -> {name, username, language_code, first_seen, last_seen, msg_count}
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

def pick_lang(state: Dict[str, Any], user: Any) -> str:
    uid = str(getattr(user, "id", 0) or 0)
    forced = (state.get("user_lang") or {}).get(uid, "auto")
    if forced in SUPPORTED_LANGS and forced != "auto":
        return forced

    code = (getattr(user, "language_code", "") or "").lower()
    if code.startswith("zh"):
        return "zh"
    if code.startswith("ja"):
        return "ja"
    if code.startswith("fr"):
        return "fr"
    if code.startswith("de"):
        return "de"
    if code.startswith("es"):
        return "es"
    if code.startswith("it"):
        return "it"
    return "en"

def contact_admin_keyboard() -> InlineKeyboardMarkup:
    url = f"https://t.me/{ADMIN_USERNAME}" if ADMIN_USERNAME else "https://t.me/"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("一键联系管理员 / Contact Admin", url=url)],
        [InlineKeyboardButton("选择语言 / Choose Language", callback_data="langmenu")],
    ])

def language_keyboard(current: str) -> InlineKeyboardMarkup:
    rows = []
    # 每行 2 个
    keys = ["auto", "zh", "en", "ja", "fr", "de", "es", "it"]
    for i in range(0, len(keys), 2):
        row = []
        for k in keys[i:i+2]:
            label = LANG_LABEL.get(k, k)
            if k == current:
                label = f"✅ {label}"
            row.append(InlineKeyboardButton(label, callback_data=f"setlang|{k}"))
        rows.append(row)
    rows.append([InlineKeyboardButton("返回 / Back", callback_data="backmain")])
    return InlineKeyboardMarkup(rows)

# ================== STATUS / TICKET ==================
STATUS_LABEL = {
    "new": "用户来信",
    "ordered": "已下单",
    "refund": "退货退款",
    "paid": "已返款",
    "blacklist": "黑名单",
}

def ticket_keyboard(uid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("已下单", callback_data=f"status|{uid}|ordered"),
            InlineKeyboardButton("退货退款", callback_data=f"status|{uid}|refund"),
        ],
        [
            InlineKeyboardButton("已返款", callback_data=f"status|{uid}|paid"),
            InlineKeyboardButton("黑名单", callback_data=f"status|{uid}|blacklist"),
        ],
        [
            InlineKeyboardButton("清空状态", callback_data=f"status|{uid}|new"),
            InlineKeyboardButton("Profile", callback_data=f"profile|{uid}"),
        ],
    ])

def fmt_time(ts: int) -> str:
    if not ts:
        return "-"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))

def render_ticket_header(state: Dict[str, Any], uid: int) -> str:
    uid_key = str(uid)
    t = (state.get("tickets") or {}).get(uid_key, {})
    meta = (state.get("user_meta") or {}).get(uid_key, {})

    ticket_id = t.get("ticket_id", "-")
    status_key = t.get("status", "new")
    status = STATUS_LABEL.get(status_key, status_key)

    name = meta.get("name", "Unknown")
    username = meta.get("username")
    user_link = f"tg://user?id={uid}"

    first_seen = int(meta.get("first_seen", 0) or 0)
    last_seen = int(meta.get("last_seen", 0) or 0)
    msg_count = int(meta.get("msg_count", 0) or 0)

    lines = []
    lines.append(f"<b>Ticket #{ticket_id}</b>  <b>Status:</b> <code>{html_escape(status)}</code>")
    lines.append(f"<b>Name:</b> {html_escape(name)}")
    if username:
        lines.append(f"<b>Username:</b> @{html_escape(username)}")
    lines.append(f"<b>UserID:</b> <code>{uid}</code>   <b>Open:</b> <a href=\"{user_link}\">Click</a>")
    lines.append(f"<b>First seen:</b> <code>{fmt_time(first_seen)}</code>")
    lines.append(f"<b>Last seen:</b> <code>{fmt_time(last_seen)}</code>   <b>Msg count:</b> <code>{msg_count}</code>")
    lines.append("")
    lines.append("推荐：在管理员私聊里 <b>Reply（回复）</b> 下面那条“转发自用户”的消息，即可回复对方（支持文字/图片/文件/贴纸/语音等）。")
    lines.append(f"备用命令：<code>/reply {uid} 你的回复内容</code>   或   <code>/r 你的回复内容</code>（回复最近用户）")
    return "\n".join(lines)

async def ensure_ticket(state: Dict[str, Any], context: ContextTypes.DEFAULT_TYPE, uid: int) -> Dict[str, Any]:
    tickets = state.setdefault("tickets", {})
    uid_key = str(uid)
    t = tickets.get(uid_key)

    if not t or not t.get("header_msg_id"):
        state["ticket_seq"] = int(state.get("ticket_seq", 0)) + 1
        ticket_id = state["ticket_seq"]
        header_text = render_ticket_header(state, uid)
        msg = await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=header_text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=ticket_keyboard(uid),
        )
        tickets[uid_key] = {
            "ticket_id": ticket_id,
            "status": "new",
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
            reply_markup=ticket_keyboard(uid),
        )
    except Exception:
        pass

# ================== BASIC COMMANDS ==================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # 用户侧 /start
    if update.effective_chat and update.effective_chat.type == ChatType.PRIVATE and not is_admin(update):
        st = load_state()
        lang = pick_lang(st, update.effective_user)
        text = (
            "欢迎，请直接发送消息给我，我会把你的消息转发给管理员。\n"
            "Welcome. Send me a message and I will forward it to the admin.\n\n"
            f"当前语言 / Current: <b>{html_escape(LANG_LABEL.get((st.get('user_lang') or {}).get(str(update.effective_user.id), 'auto'), 'Auto'))}</b>"
        )
        await update.message.reply_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=contact_admin_keyboard(),
            disable_web_page_preview=True,
        )
        return

    # 管理员侧 /start
    if is_admin(update):
        await update.message.reply_text(
            "管理员模式已启用。\n\n"
            "使用方法：\n"
            "1) 用户私聊机器人发来任何消息 → 会“转发自用户”到你的私聊\n"
            "2) 你直接 Reply（回复）那条转发消息 → 机器人会把你的回复原样发回用户（支持任何媒体）\n\n"
            "备用命令：\n"
            "/reply <uid> <text>  指定用户发送文字\n"
            "/r <text>  回复最近一个用户\n"
        )

async def cmd_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    if len(context.args) < 2:
        await update.message.reply_text("用法：/reply <user_id> <text>")
        return
    uid = int(context.args[0])
    text = " ".join(context.args[1:])
    await context.bot.send_message(chat_id=uid, text=text)
    st = load_state()
    st["last_user"] = uid
    save_state(st)
    log_event("out", uid, {"type": "text", "text": text[:1000]})
    await update.message.reply_text("已发送。")

async def cmd_r(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    if not context.args:
        await update.message.reply_text("用法：/r <text>")
        return
    st = load_state()
    uid = int(st.get("last_user", 0) or 0)
    if uid <= 0:
        await update.message.reply_text("没有最近用户。请先 Reply 某个用户消息或用 /reply。")
        return
    text = " ".join(context.args)
    await context.bot.send_message(chat_id=uid, text=text)
    log_event("out", uid, {"type": "text", "text": text[:1000]})
    await update.message.reply_text("已发送。")

async def cmd_lang(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # 用户用 /lang 手动弹出语言选择
    if not update.message or not update.effective_user:
        return
    st = load_state()
    cur = (st.get("user_lang") or {}).get(str(update.effective_user.id), "auto")
    await update.message.reply_text(
        "请选择语言 / Choose language:",
        reply_markup=language_keyboard(cur),
    )

# ================== CALLBACKS ==================
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.callback_query:
        return
    q = update.callback_query
    await q.answer()

    data = (q.data or "").strip()

    # 用户侧语言菜单
    if data == "langmenu":
        st = load_state()
        uid = str(update.effective_user.id) if update.effective_user else "0"
        cur = (st.get("user_lang") or {}).get(uid, "auto")
        await q.message.edit_text("请选择语言 / Choose language:", reply_markup=language_keyboard(cur))
        return

    if data == "backmain":
        if not update.effective_user:
            return
        st = load_state()
        uid = str(update.effective_user.id)
        cur_label = LANG_LABEL.get((st.get("user_lang") or {}).get(uid, "auto"), "Auto")
        text = (
            "你可以直接发送消息给我，我会转发给管理员。\n"
            "You can send me a message, I will forward it to the admin.\n\n"
            f"当前语言 / Current: <b>{html_escape(cur_label)}</b>"
        )
        await q.message.edit_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=contact_admin_keyboard(),
            disable_web_page_preview=True,
        )
        return

    if data.startswith("setlang|"):
        lang = data.split("|", 1)[1].strip()
        if lang not in SUPPORTED_LANGS:
            return
        if not update.effective_user:
            return
        st = load_state()
        st.setdefault("user_lang", {})[str(update.effective_user.id)] = lang
        save_state(st)
        await q.message.edit_reply_markup(reply_markup=language_keyboard(lang))
        return

    # 管理员侧：状态按钮/资料
    if not is_admin(update):
        return

    if data.startswith("status|"):
        # status|uid|key
        parts = data.split("|")
        if len(parts) != 3:
            return
        uid = int(parts[1])
        status_key = parts[2]
        if status_key not in STATUS_LABEL:
            return
        st = load_state()
        t = (st.get("tickets") or {}).get(str(uid))
        if not t:
            # 没有 ticket 也允许写入
            st.setdefault("tickets", {})[str(uid)] = {
                "ticket_id": int(st.get("ticket_seq", 0)) + 1,
                "status": status_key,
                "created_at": _now_ts(),
                "header_msg_id": q.message.message_id,
            }
            st["ticket_seq"] = int(st.get("ticket_seq", 0)) + 1
        else:
            t["status"] = status_key
        save_state(st)
        await refresh_header(st, context, uid)
        return

    if data.startswith("profile|"):
        uid = int(data.split("|")[1])
        st = load_state()
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=render_ticket_header(st, uid),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=ticket_keyboard(uid),
        )
        return

# ================== MESSAGE TYPES ==================
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

# ================== USER -> ADMIN (forward all) ==================
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

    # ensure ticket
    t = await ensure_ticket(st, context, uid)
    st["last_user"] = uid

    # forward to admin to keep "转发自用户"
    try:
        fwd = await context.bot.forward_message(
            chat_id=ADMIN_ID,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id,
        )
        remember_msg_index(st, fwd.message_id, uid)
    except Exception:
        copied = await context.bot.copy_message(
            chat_id=ADMIN_ID,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id,
        )
        remember_msg_index(st, copied.message_id, uid)

    # also map header msg id -> uid (so admin can reply header by mistake)
    if t.get("header_msg_id"):
        remember_msg_index(st, int(t["header_msg_id"]), uid)

    # log in
    typ = message_type_name(update.message)
    preview = (update.message.text or update.message.caption or "")
    log_event("in", uid, {"type": typ, "text": preview[:1000]})

    # auto reply (24h cooldown)
    last_ts = int((st.get("last_auto_reply") or {}).get(str(uid), 0) or 0)
    now_ts = _now_ts()
    if now_ts - last_ts >= AUTO_REPLY_COOLDOWN_SEC:
        lang = pick_lang(st, user)
        txt = AUTO_REPLY_TEXT.get(lang, AUTO_REPLY_TEXT["en"])
        try:
            await update.message.reply_text(
                txt,
                reply_markup=contact_admin_keyboard(),
                disable_web_page_preview=True,
            )
        except Exception:
            pass
        st.setdefault("last_auto_reply", {})[str(uid)] = now_ts

    save_state(st)
    await refresh_header(st, context, uid)

# ================== ADMIN Reply -> USER (copy all media) ==================
async def handle_admin_private(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat:
        return
    if update.effective_chat.type != ChatType.PRIVATE:
        return
    if not is_admin(update):
        return

    # must be a reply
    if not update.message.reply_to_message:
        return

    st = load_state()
    reply_to_id = str(update.message.reply_to_message.message_id)
    to_user = None

    if reply_to_id in (st.get("msg_index") or {}):
        to_user = int(st["msg_index"][reply_to_id])

    if not to_user:
        # fallback: parse UserID from header text if admin replied header
        txt = update.message.reply_to_message.text or update.message.reply_to_message.caption or ""
        m = re.search(r"UserID:\s*(\d+)", txt)
        if m:
            to_user = int(m.group(1))

    if not to_user:
        await update.message.reply_text("没识别到用户ID。请 Reply 那条“转发自用户”的消息，或使用 /reply <uid> <text>。")
        return

    try:
        # copy admin message to user (supports media)
        await context.bot.copy_message(
            chat_id=to_user,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id,
        )

        st["last_user"] = to_user
        save_state(st)

        typ = message_type_name(update.message)
        preview = (update.message.text or update.message.caption or "")
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

    # commands
    tg_app.add_handler(CommandHandler("start", cmd_start))
    tg_app.add_handler(CommandHandler("lang", cmd_lang))
    tg_app.add_handler(CommandHandler("reply", cmd_reply))
    tg_app.add_handler(CommandHandler("r", cmd_r))

    # callbacks
    tg_app.add_handler(CallbackQueryHandler(on_callback))

    # messages
    # user private (not admin)
    tg_app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.User(user_id=ADMIN_ID), handle_user_private))
    # admin private
    tg_app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.User(user_id=ADMIN_ID), handle_admin_private))

    if PUBLIC_URL:
        asyncio.run(run_webhook_server(tg_app))
    else:
        tg_app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
