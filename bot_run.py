import os
import re
import json
import time
import asyncio
from pathlib import Path
from typing import Any, Dict

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
ADMIN_ID = int(os.getenv("TG_ADMIN_ID", "0") or "0")  # 必填：管理员 Telegram user_id
PUBLIC_URL = (os.getenv("RENDER_EXTERNAL_URL") or os.getenv("PUBLIC_URL") or "").strip().rstrip("/")
WEBHOOK_SECRET = (os.getenv("WEBHOOK_SECRET") or "").strip()
PORT = int(os.getenv("PORT", "10000"))

# 用户自动回复（24小时只回一次）
AUTO_REPLY_TEXT = (os.getenv("AUTO_REPLY_TEXT") or "Hello, thank you for contacting us.\nPlease contact the administrator.: @Adalemy").strip()
AUTO_REPLY_COOLDOWN_SEC = int(os.getenv("AUTO_REPLY_COOLDOWN_SEC", "86400"))  # 默认 24h

# 给用户的“一键联系管理员”按钮
ADMIN_CONTACT_URL = (os.getenv("ADMIN_CONTACT_URL") or "https://t.me/Adalemy").strip()

HEALTH_PATH = "/healthz"

BASE_DIR = Path(__file__).resolve().parent
STATE_FILE = BASE_DIR / "support_state.json"
LOG_FILE = BASE_DIR / "history.jsonl"

MAX_MSG_INDEX = 8000  # admin_message_id -> user_id 映射上限


# ================== UTIL ==================
def _now_ts() -> int:
    return int(time.time())


def _html_escape(s: str) -> str:
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
        "last_user": 0,         # 最近一个用户
        "last_auto_reply": {},  # user_id(str) -> ts
        "user_meta": {},        # user_id(str) -> {name, username, language_code, first_seen, last_seen, msg_count}
        "user_status": {},      # user_id(str) -> "已下单/退货退款/已返款/用户来信/..."
        "blocked": {},          # user_id(str) -> 0/1
    }


def save_state(state: Dict[str, Any]) -> None:
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def is_admin(update: Update) -> bool:
    return bool(update.effective_user and update.effective_user.id == ADMIN_ID and ADMIN_ID > 0)


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
    if getattr(msg, "photo", None):
        return "photo"
    if getattr(msg, "sticker", None):
        return "sticker"
    if getattr(msg, "voice", None):
        return "voice"
    if getattr(msg, "video", None):
        return "video"
    if getattr(msg, "document", None):
        return "document"
    if getattr(msg, "animation", None):
        return "animation"
    if getattr(msg, "audio", None):
        return "audio"
    if getattr(msg, "video_note", None):
        return "video_note"
    if getattr(msg, "contact", None):
        return "contact"
    if getattr(msg, "location", None):
        return "location"
    if getattr(msg, "poll", None):
        return "poll"
    if getattr(msg, "text", None):
        return "text"
    return "unknown"


def fmt_time(ts: int) -> str:
    if not ts:
        return "-"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))


# ================== UI (Admin Ticket) ==================
def ticket_keyboard(uid: int) -> InlineKeyboardMarkup:
    # 精简按钮：用户来信/已下单/退货退款/已返款/黑名单/清空状态
    rows = [
        [
            InlineKeyboardButton("用户来信", callback_data=f"status|{uid}|用户来信"),
            InlineKeyboardButton("已下单", callback_data=f"status|{uid}|已下单"),
            InlineKeyboardButton("退货退款", callback_data=f"status|{uid}|退货退款"),
        ],
        [
            InlineKeyboardButton("已返款", callback_data=f"status|{uid}|已返款"),
            InlineKeyboardButton("黑名单", callback_data=f"block|{uid}|toggle"),
            InlineKeyboardButton("清空状态", callback_data=f"status|{uid}|-"),
        ],
    ]
    return InlineKeyboardMarkup(rows)


def render_ticket_header(state: Dict[str, Any], uid: int) -> str:
    uid_key = str(uid)
    t = (state.get("tickets") or {}).get(uid_key, {})
    meta = (state.get("user_meta") or {}).get(uid_key, {})
    user_status = (state.get("user_status") or {}).get(uid_key, "-")
    blocked = int((state.get("blocked") or {}).get(uid_key, 0) or 0)

    ticket_id = t.get("ticket_id", "-")
    support_status = t.get("status", "open")

    name = _html_escape(meta.get("name", "Unknown"))
    username = meta.get("username") or ""
    first_seen = int(meta.get("first_seen", 0) or 0)
    last_seen = int(meta.get("last_seen", 0) or 0)
    msg_count = int(meta.get("msg_count", 0) or 0)

    user_link = f"tg://user?id={uid}"
    blocked_str = "YES" if blocked == 1 else "NO"

    lines = []
    lines.append(f"<b>Ticket #{ticket_id}</b>    <b>Support:</b> <code>{_html_escape(str(support_status))}</code>")
    lines.append(f"<b>Name:</b> {name}")
    if username:
        lines.append(f"<b>Username:</b> @{_html_escape(username)}")
    lines.append(f"<b>UserID:</b> <code>{uid}</code>    <b>Open:</b> <a href=\"{user_link}\">Click</a>")
    lines.append(f"<b>Biz Status:</b> <code>{blocked_str}</code>")
    lines.append(f"<b>Order Status:</b> <code>{_html_escape(str(user_status))}</code>")
    lines.append(f"<b>First seen:</b> <code>{_html_escape(fmt_time(first_seen))}</code>")
    lines.append(f"<b>Last seen:</b> <code>{_html_escape(fmt_time(last_seen))}</code>    <b>Msg count:</b> <code>{msg_count}</code>")
    lines.append("")
    lines.append("<b>推荐：</b>在管理员私聊里 <b>Reply</b> 用户转发消息即可回复（支持文字/图片/文件/贴纸/语音等）。")
    lines.append("<b>命令：</b>/reply &lt;uid&gt; &lt;text&gt;   /r &lt;text&gt;   /history &lt;uid&gt; 20   /block &lt;uid&gt;   /unblock &lt;uid&gt;")
    return "\n".join(lines)


async def ensure_ticket(state: Dict[str, Any], context: ContextTypes.DEFAULT_TYPE, uid: int) -> Dict[str, Any]:
    tickets = state.setdefault("tickets", {})
    uid_key = str(uid)
    t = tickets.get(uid_key)

    # 新用户 or header 丢失：创建；若已关闭且用户又来消息：自动 reopen
    need_new = False
    if not t or not t.get("header_msg_id"):
        need_new = True
    if t and t.get("status") == "closed":
        t["status"] = "open"

    if need_new:
        state["ticket_seq"] = int(state.get("ticket_seq", 0)) + 1
        ticket_id = state["ticket_seq"]

        msg = await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=render_ticket_header(state, uid),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
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
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=ticket_keyboard(uid),
        )
    except Exception:
        pass


# ================== COMMANDS ==================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    if is_admin(update):
        await update.message.reply_text(
            "已启用：私聊工单（管理员私聊）+ 多媒体转发 + Reply 直接回复。\n\n"
            "用法：\n"
            "1) 用户私聊机器人 → 自动转发到管理员私聊（保留“转发自用户”）。\n"
            "2) 管理员在私聊里 Reply 用户转发消息 → 机器人把你的这条消息发给用户。\n\n"
            "命令：\n"
            "/reply <uid> <text>\n"
            "/r <text>  (回复最近一个用户)\n"
            "/history <uid> [n]\n"
            "/status <uid> <用户来信|已下单|退货退款|已返款|->\n"
            "/block <uid>  /unblock <uid>\n",
        )
    else:
        kb = None
        if ADMIN_CONTACT_URL:
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("联系管理员", url=ADMIN_CONTACT_URL)]])
        await update.message.reply_text(AUTO_REPLY_TEXT, reply_markup=kb)


async def cmd_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update) or not update.message:
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
    if not is_admin(update) or not update.message:
        return
    if not context.args:
        await update.message.reply_text("用法：/r <text>")
        return
    st = load_state()
    uid = int(st.get("last_user", 0) or 0)
    if uid <= 0:
        await update.message.reply_text("没有最近用户。")
        return
    text = " ".join(context.args)
    await context.bot.send_message(chat_id=uid, text=text)

    log_event("out", uid, {"type": "text", "text": text[:1000]})
    await update.message.reply_text("已发送。")


async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update) or not update.message:
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
        out.append(f"{ts}  {direction}  {typ}  {text}")
    await update.message.reply_text("\n".join(out)[:3500])


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update) or not update.message:
        return
    if len(context.args) < 2:
        await update.message.reply_text("用法：/status <uid> <用户来信|已下单|退货退款|已返款|->")
        return
    uid = int(context.args[0])
    val = " ".join(context.args[1:]).strip()
    st = load_state()
    if val == "-" or val.lower() == "clear":
        st.setdefault("user_status", {}).pop(str(uid), None)
    else:
        st.setdefault("user_status", {})[str(uid)] = val
    save_state(st)
    await refresh_header(st, context, uid)
    await update.message.reply_text("已更新状态。")


async def cmd_block(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update) or not update.message:
        return
    if not context.args:
        await update.message.reply_text("用法：/block <uid>")
        return
    uid = int(context.args[0])
    st = load_state()
    st.setdefault("blocked", {})[str(uid)] = 1
    save_state(st)
    await refresh_header(st, context, uid)
    await update.message.reply_text("已拉黑。")


async def cmd_unblock(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update) or not update.message:
        return
    if not context.args:
        await update.message.reply_text("用法：/unblock <uid>")
        return
    uid = int(context.args[0])
    st = load_state()
    st.setdefault("blocked", {})[str(uid)] = 0
    save_state(st)
    await refresh_header(st, context, uid)
    await update.message.reply_text("已解封。")


# ================== CALLBACK ==================
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.callback_query:
        return
    q = update.callback_query
    await q.answer()

    if not is_admin(update):
        return

    data = q.data or ""
    parts = data.split("|")
    if len(parts) < 3:
        return

    action = parts[0]
    uid = int(parts[1])
    val = parts[2]

    st = load_state()

    if action == "status":
        if val == "-" or val.lower() == "clear":
            st.setdefault("user_status", {}).pop(str(uid), None)
        else:
            st.setdefault("user_status", {})[str(uid)] = val
        save_state(st)
        await refresh_header(st, context, uid)
        return

    if action == "block":
        # toggle 黑名单
        cur = int((st.get("blocked") or {}).get(str(uid), 0) or 0)
        st.setdefault("blocked", {})[str(uid)] = 0 if cur == 1 else 1
        save_state(st)
        await refresh_header(st, context, uid)
        return


# ================== USER -> ADMIN (forward, all media) ==================
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

    # forward to admin (keep “转发自用户”)
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

    # header message_id 也做映射（避免管理员误 Reply header）
    if t.get("header_msg_id"):
        remember_msg_index(st, int(t["header_msg_id"]), uid)

    # history
    typ = message_type_name(update.message)
    preview = (update.message.text or update.message.caption or "")
    log_event("in", uid, {"type": typ, "text": preview[:1000]})

    # auto reply 24h cooldown
    last_ts = int((st.get("last_auto_reply") or {}).get(str(uid), 0) or 0)
    now_ts = _now_ts()
    if now_ts - last_ts >= AUTO_REPLY_COOLDOWN_SEC:
        kb = None
        if ADMIN_CONTACT_URL:
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("联系管理员", url=ADMIN_CONTACT_URL)]])
        try:
            await update.message.reply_text(AUTO_REPLY_TEXT, reply_markup=kb)
        except Exception:
            pass
        st.setdefault("last_auto_reply", {})[str(uid)] = now_ts

    save_state(st)
    await refresh_header(st, context, uid)


# ================== ADMIN Reply -> USER (copy, all media) ==================
async def handle_admin_private(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat:
        return
    if update.effective_chat.type != ChatType.PRIVATE:
        return
    if not is_admin(update):
        return

    # 必须 Reply（避免误发）
    if not update.message.reply_to_message:
        return

    st = load_state()

    # 识别被 Reply 的消息属于哪个用户
    rid = str(update.message.reply_to_message.message_id)
    to_user = None
    if rid in (st.get("msg_index") or {}):
        to_user = int(st["msg_index"][rid])

    if not to_user:
        # 兜底：从 header 里解析 UserID
        txt = update.message.reply_to_message.text or update.message.reply_to_message.caption or ""
        m = re.search(r"UserID:\s*(\d+)", txt)
        if m:
            to_user = int(m.group(1))

    if not to_user:
        await update.message.reply_text("没识别到用户ID。请 Reply 用户转发消息，或用 /reply <uid> <text>。")
        return

    # 如果用户在黑名单：不发送（只提示管理员）
    blocked = int((st.get("blocked") or {}).get(str(to_user), 0) or 0)
    if blocked == 1:
        await update.message.reply_text("该用户处于黑名单，已阻止发送。需要发请先解封（按钮黑名单再点一次或 /unblock）。")
        return

    # copy 管理员这条消息给用户（支持多媒体）
    try:
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

    # Commands
    tg_app.add_handler(CommandHandler("start", cmd_start))
    tg_app.add_handler(CommandHandler("reply", cmd_reply))
    tg_app.add_handler(CommandHandler("r", cmd_r))
    tg_app.add_handler(CommandHandler("history", cmd_history))
    tg_app.add_handler(CommandHandler("status", cmd_status))
    tg_app.add_handler(CommandHandler("block", cmd_block))
    tg_app.add_handler(CommandHandler("unblock", cmd_unblock))

    # Buttons
    tg_app.add_handler(CallbackQueryHandler(on_callback))

    # Private chat handlers
    tg_app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.User(user_id=ADMIN_ID), handle_user_private))
    tg_app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.User(user_id=ADMIN_ID), handle_admin_private))

    if PUBLIC_URL:
        asyncio.run(run_webhook_server(tg_app))
    else:
        tg_app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
