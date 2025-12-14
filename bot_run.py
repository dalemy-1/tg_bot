import os
import re
import json
import time
import asyncio
import html
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
ADMIN_ID = int(os.getenv("TG_ADMIN_ID", "0") or "0")
PUBLIC_URL = (os.getenv("RENDER_EXTERNAL_URL") or os.getenv("PUBLIC_URL") or "").strip().rstrip("/")
WEBHOOK_SECRET = (os.getenv("WEBHOOK_SECRET") or "").strip()
PORT = int(os.getenv("PORT", "10000"))

HEALTH_PATH = "/healthz"

# 多语言自动回复模板（可在 Render 环境变量覆盖）
AUTO_REPLY_ZH = (os.getenv("AUTO_REPLY_ZH") or "你好，已收到你的消息，我们会尽快回复。").strip()
AUTO_REPLY_EN = (os.getenv("AUTO_REPLY_EN") or "Hello, we received your message and will reply soon.").strip()
AUTO_REPLY_JA = (os.getenv("AUTO_REPLY_JA") or "メッセージを受け取りました。できるだけ早く返信します。").strip()
AUTO_REPLY_DEFAULT = (os.getenv("AUTO_REPLY_TEXT") or "已收到，请联系 @Dalemy").strip()

# 关键：默认 24 小时内自动回复一次
AUTO_REPLY_COOLDOWN_SEC = int(os.getenv("AUTO_REPLY_COOLDOWN_SEC", "86400"))

# 新加坡时区：UTC+8（用于“当天”判断）
TZ_OFFSET_SEC = int(os.getenv("TZ_OFFSET_SEC", "28800"))

# 黑名单用户自动回复（可选）
BLOCKED_REPLY_TEXT = (os.getenv("BLOCKED_REPLY_TEXT") or "该账号已被限制联系。").strip()

BASE_DIR = Path(__file__).resolve().parent
STATE_FILE = BASE_DIR / "support_state.json"
LOG_FILE = BASE_DIR / "history.jsonl"

# 标签按钮（可自定义）
DEFAULT_TAGS = (os.getenv("DEFAULT_TAGS") or "VIP,售后,咨询,广告,其他").split(",")
DEFAULT_TAGS = [t.strip() for t in DEFAULT_TAGS if t.strip()][:8]

MAX_MSG_INDEX = 8000

# 业务状态
STATUS_CHOICES = ["已下单", "退货退款", "已返款"]
STATUS_KEYWORDS = {
    "已下单": ["已下单", "下单了", "已购买", "已付款", "订单号", "order", "paid"],
    "退货退款": ["退货", "退款", "退货退款", "return", "refund"],
    "已返款": ["已返款", "返款", "已打款", "已到账", "paid back", "refunded", "已退回"],
}

# ================== STATE ==================
def _now_ts() -> int:
    return int(time.time())


def local_day(ts: int) -> str:
    """按 TZ_OFFSET_SEC 计算本地日期（YYYY-MM-DD），用于“当天管理员已回复”判断。"""
    return time.strftime("%Y-%m-%d", time.gmtime(int(ts) + TZ_OFFSET_SEC))


def load_state() -> Dict[str, Any]:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {
        "ticket_seq": 0,
        "tickets": {},           # user_id(str) -> {ticket_id, status, created_at, header_msg_id}
        "msg_index": {},         # admin_message_id(str) -> user_id(int)
        "last_user": 0,
        "user_lang": {},         # user_id(str) -> auto|zh|en|ja
        "last_auto_reply": {},   # user_id(str) -> ts
        "last_admin_reply": {},  # user_id(str) -> ts  (新增：管理员最后一次回复用户时间)
        "user_meta": {},         # user_id(str) -> {name, username, language_code, first_seen, last_seen, msg_count}
        "user_tags": {},         # user_id(str) -> [tag, ...]
        "user_note": {},         # user_id(str) -> "..."
        "user_status": {},       # user_id(str) -> 已下单|退货退款|已返款
        "blocked": {},           # user_id(str) -> 1/0
    }


def save_state(state: Dict[str, Any]) -> None:
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def is_admin(update: Update) -> bool:
    return bool(update.effective_user and update.effective_user.id == ADMIN_ID and ADMIN_ID > 0)


def pick_lang(state: Dict[str, Any], user: Any) -> str:
    uid = str(getattr(user, "id", 0) or 0)
    forced = (state.get("user_lang") or {}).get(uid, "auto")
    if forced in {"zh", "en", "ja"}:
        return forced

    code = (getattr(user, "language_code", "") or "").lower()
    if code.startswith("zh"):
        return "zh"
    if code.startswith("ja"):
        return "ja"
    return "en"


def auto_reply_text(lang: str) -> str:
    if lang == "zh":
        return AUTO_REPLY_ZH
    if lang == "ja":
        return AUTO_REPLY_JA
    if lang == "en":
        return AUTO_REPLY_EN
    return AUTO_REPLY_DEFAULT


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


def fmt_time(ts: int) -> str:
    if not ts:
        return "-"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))


def render_ticket_header(state: Dict[str, Any], uid: int) -> str:
    uid_key = str(uid)
    t = (state.get("tickets") or {}).get(uid_key, {})
    meta = (state.get("user_meta") or {}).get(uid_key, {})
    tags = (state.get("user_tags") or {}).get(uid_key, [])
    note = (state.get("user_note") or {}).get(uid_key, "")

    biz_status = (state.get("user_status") or {}).get(uid_key, "-") or "-"
    is_blocked = int((state.get("blocked") or {}).get(uid_key, 0) or 0)
    blocked_str = "YES" if is_blocked else "NO"

    ticket_id = t.get("ticket_id", "-")
    status = t.get("status", "open")

    name = html.escape((meta.get("name") or "Unknown").strip())
    username = meta.get("username")
    username = html.escape(username) if username else None
    user_link = f"tg://user?id={uid}"

    tags_str = html.escape(", ".join(tags) if tags else "-")
    note_str = html.escape(note if note else "-")
    biz_status = html.escape(biz_status)

    first_seen = fmt_time(int(meta.get("first_seen", 0) or 0))
    last_seen = fmt_time(int(meta.get("last_seen", 0) or 0))
    msg_count = int(meta.get("msg_count", 0) or 0)

    lines = []
    lines.append(f"🧾 <b>Ticket #{ticket_id}</b> &nbsp; <b>Status:</b> <code>{html.escape(str(status))}</code>")
    lines.append(f"<b>Name:</b> {name}")
    if username:
        lines.append(f"<b>Username:</b> @{username}")
    lines.append(f"<b>UserID:</b> <code>{uid}</code> &nbsp; <b>Open:</b> <a href=\"{user_link}\">Click</a>")
    lines.append(f"<b>Biz Status:</b> <code>{biz_status}</code> &nbsp; <b>Blocked:</b> <code>{blocked_str}</code>")
    lines.append(f"<b>Tags:</b> <code>{tags_str}</code>")
    lines.append(f"<b>Note:</b> {note_str}")
    lines.append(f"<b>First seen:</b> <code>{html.escape(first_seen)}</code>")
    lines.append(f"<b>Last seen:</b> <code>{html.escape(last_seen)}</code> &nbsp; <b>Msg count:</b> <code>{msg_count}</code>")
    lines.append("")
    lines.append("<b>推荐：</b>在管理员私聊里 <b>Reply</b> 用户转发消息即可回复（支持文字/图片/文件/贴纸/语音等）。")
    lines.append(f"<b>历史：</b><code>/history {uid} 20</code> &nbsp; <b>备注：</b><code>/note {uid} ...</code> &nbsp; <b>状态：</b><code>/status {uid} 已下单</code>")
    lines.append(f"<b>关闭：</b><code>/close {uid}</code> &nbsp; <b>黑名单：</b><code>/block {uid}</code> &nbsp; <code>/unblock {uid}</code>")
    return "\n".join(lines)


def ticket_keyboard(uid: int) -> InlineKeyboardMarkup:
    # 标签按钮（每行最多 3）
    tag_buttons = [InlineKeyboardButton(f"Tag:{t}", callback_data=f"tag|{uid}|{t}") for t in DEFAULT_TAGS]
    rows = []
    for i in range(0, len(tag_buttons), 3):
        rows.append(tag_buttons[i:i+3])

    # 业务状态
    rows.append([
        InlineKeyboardButton("已下单", callback_data=f"status|{uid}|已下单"),
        InlineKeyboardButton("退货退款", callback_data=f"status|{uid}|退货退款"),
        InlineKeyboardButton("已返款", callback_data=f"status|{uid}|已返款"),
    ])
    rows.append([
        InlineKeyboardButton("黑名单", callback_data=f"block|{uid}|1"),
        InlineKeyboardButton("解封", callback_data=f"block|{uid}|0"),
        InlineKeyboardButton("清空状态", callback_data=f"status|{uid}|-"),
    ])

    rows.append([
        InlineKeyboardButton("Clear Tags", callback_data=f"cleartags|{uid}|-"),
        InlineKeyboardButton("Profile", callback_data=f"profile|{uid}|-"),
    ])
    rows.append([
        InlineKeyboardButton("Close", callback_data=f"close|{uid}|-"),
        InlineKeyboardButton("Reopen", callback_data=f"reopen|{uid}|-"),
    ])
    return InlineKeyboardMarkup(rows)


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


# ================== COMMANDS (Admin) ==================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "已启用：私聊工单 + 标签 + 状态（已下单/退货退款/已返款）+ 黑名单 + 多媒体。\n"
        "自动回复规则：每用户 24h 一次；若管理员当天已回复该用户，则当天不再自动回复。\n\n"
        "管理员命令：\n"
        "/open [tag]\n"
        "/profile <uid>\n"
        "/note <uid> <text>\n"
        "/status <uid> <已下单|退货退款|已返款|clear>\n"
        "/block <uid>   /unblock <uid>\n"
        "/setlang <uid> <auto|zh|en|ja>\n"
        "/history <uid> [n]\n"
        "/close <uid>   /reopen <uid>\n"
        "/reply <uid> <text>   /r <text>\n\n"
        "最推荐：直接 Reply 用户转发消息回复（支持媒体）。"
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
    st.setdefault("last_admin_reply", {})[str(uid)] = _now_ts()  # 记录管理员回复
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
        await update.message.reply_text("没有最近用户。")
        return
    text = " ".join(context.args)
    await context.bot.send_message(chat_id=uid, text=text)

    st.setdefault("last_admin_reply", {})[str(uid)] = _now_ts()  # 记录管理员回复
    save_state(st)

    log_event("out", uid, {"type": "text", "text": text[:1000]})
    await update.message.reply_text("已发送。")


async def cmd_note(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
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


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    if len(context.args) < 2:
        await update.message.reply_text("用法：/status <uid> <已下单|退货退款|已返款|clear>")
        return
    uid = int(context.args[0])
    val = context.args[1].strip()

    st = load_state()
    if val in {"clear", "0", "-", "清空"}:
        st.setdefault("user_status", {}).pop(str(uid), None)
    elif val in STATUS_CHOICES:
        st.setdefault("user_status", {})[str(uid)] = val
    else:
        await update.message.reply_text("状态仅支持：已下单 / 退货退款 / 已返款 / clear")
        return

    save_state(st)
    await refresh_header(st, context, uid)
    await update.message.reply_text("已更新状态。")


async def cmd_block(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    if not context.args:
        await update.message.reply_text("用法：/block <uid>")
        return
    uid = int(context.args[0])

    st = load_state()
    st.setdefault("blocked", {})[str(uid)] = 1
    save_state(st)
    await refresh_header(st, context, uid)
    await update.message.reply_text("已加入黑名单（后续不再转发此用户消息）。")


async def cmd_unblock(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
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


async def cmd_setlang(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    if len(context.args) < 2:
        await update.message.reply_text("用法：/setlang <uid> <auto|zh|en|ja>")
        return
    uid = int(context.args[0])
    lang = context.args[1].lower().strip()
    if lang not in {"auto", "zh", "en", "ja"}:
        await update.message.reply_text("lang 仅支持：auto|zh|en|ja")
        return

    st = load_state()
    st.setdefault("user_lang", {})[str(uid)] = lang
    save_state(st)
    await refresh_header(st, context, uid)
    await update.message.reply_text("已设置语言。")


async def cmd_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    if not context.args:
        await update.message.reply_text("用法：/profile <uid>")
        return
    uid = int(context.args[0])

    st = load_state()
    await context.bot.send_message(
        chat_id=ADMIN_ID,
        text=render_ticket_header(st, uid),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
        reply_markup=ticket_keyboard(uid),
    )


async def cmd_close(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
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
    if not is_admin(update):
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


async def cmd_open(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    tag_filter = (context.args[0].strip() if context.args else "")

    st = load_state()
    tickets = st.get("tickets") or {}
    user_tags = st.get("user_tags") or {}
    user_meta = st.get("user_meta") or {}
    user_status = st.get("user_status") or {}
    blocked = st.get("blocked") or {}

    rows = []
    for uid_key, t in tickets.items():
        if t.get("status") != "open":
            continue

        tags = user_tags.get(uid_key, [])
        if tag_filter and tag_filter not in tags:
            continue

        uid = int(uid_key)
        meta = user_meta.get(uid_key, {})
        name = meta.get("name", "Unknown")
        last_seen = fmt_time(int(meta.get("last_seen", 0) or 0))
        tid = t.get("ticket_id", "-")
        tags_str = ",".join(tags) if tags else "-"
        stt = user_status.get(uid_key, "-") or "-"
        blk = "Y" if int(blocked.get(uid_key, 0) or 0) == 1 else "N"

        rows.append(f"#{tid} {uid} {name}  status:{stt} blk:{blk} tags:{tags_str} last:{last_seen}")

    if not rows:
        await update.message.reply_text("暂无未关闭工单。")
        return

    header = "Open Tickets"
    if tag_filter:
        header += f" (tag={tag_filter})"
    msg = header + "\n" + "\n".join(rows)
    await update.message.reply_text(msg[:3500])


async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
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
    await update.message.reply_text("\n".join(out)[:3500])


# ================== CALLBACK (Admin buttons) ==================
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

    if action == "tag" and len(parts) >= 3:
        tag = parts[2]
        tags = st.setdefault("user_tags", {}).setdefault(str(uid), [])
        if tag not in tags:
            tags.append(tag)
        save_state(st)
        await refresh_header(st, context, uid)
        return

    if action == "cleartags":
        st.setdefault("user_tags", {})[str(uid)] = []
        save_state(st)
        await refresh_header(st, context, uid)
        return

    if action == "status" and len(parts) >= 3:
        val = parts[2]
        if val == "-":
            st.setdefault("user_status", {}).pop(str(uid), None)
        else:
            st.setdefault("user_status", {})[str(uid)] = val
        save_state(st)
        await refresh_header(st, context, uid)
        return

    if action == "block" and len(parts) >= 3:
        v = parts[2]
        st.setdefault("blocked", {})[str(uid)] = 1 if v == "1" else 0
        save_state(st)
        await refresh_header(st, context, uid)
        return

    if action == "close":
        t = (st.get("tickets") or {}).get(str(uid))
        if t:
            t["status"] = "closed"
            save_state(st)
            await refresh_header(st, context, uid)
        return

    if action == "reopen":
        t = (st.get("tickets") or {}).get(str(uid))
        if t:
            t["status"] = "open"
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
                reply_markup=ticket_keyboard(uid),
            )
        except Exception:
            pass
        return


# ================== CORE: USER -> ADMIN (All media) ==================
async def handle_user_private(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat:
        return
    if update.effective_chat.type != ChatType.PRIVATE:
        return

    # 管理员自己不走这里
    if is_admin(update):
        return

    user = update.effective_user
    uid = int(getattr(user, "id", 0) or 0)
    if uid <= 0:
        return

    st = load_state()

    # 黑名单：不转发给管理员
    if int((st.get("blocked") or {}).get(str(uid), 0) or 0) == 1:
        preview = (update.message.text or update.message.caption or "")
        log_event("in", uid, {"type": "blocked", "text": preview[:500]})
        try:
            if BLOCKED_REPLY_TEXT:
                await update.message.reply_text(BLOCKED_REPLY_TEXT)
        except Exception:
            pass
        save_state(st)
        return

    # 更新 meta
    meta = st.setdefault("user_meta", {}).setdefault(str(uid), {})
    meta.setdefault("first_seen", _now_ts())
    meta["last_seen"] = _now_ts()
    meta["msg_count"] = int(meta.get("msg_count", 0) or 0) + 1
    meta["name"] = (getattr(user, "full_name", "") or "Unknown").strip()
    meta["username"] = getattr(user, "username", None)
    meta["language_code"] = getattr(user, "language_code", "")

    # 关键词自动识别状态
    text_all = ((update.message.text or "") + "\n" + (update.message.caption or "")).strip()
    text_low = text_all.lower()
    for status_name, keys in STATUS_KEYWORDS.items():
        if any((k.lower() in text_low) for k in keys):
            st.setdefault("user_status", {})[str(uid)] = status_name
            break

    # 确保 ticket
    t = await ensure_ticket(st, context, uid)
    st["last_user"] = uid

    # 转发原消息给管理员（保留“转发自用户”格式）
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

    # header 也绑定到 msg_index（管理员误 Reply header 时也能回）
    if t.get("header_msg_id"):
        remember_msg_index(st, int(t["header_msg_id"]), uid)

    # 写历史
    typ = message_type_name(update.message)
    preview = (update.message.text or update.message.caption or "")
    log_event("in", uid, {"type": typ, "text": preview[:1000]})

    # 自动回复（24h一次 + 管理员当天回复后抑制）
    now_ts = _now_ts()
    last_admin_ts = int((st.get("last_admin_reply") or {}).get(str(uid), 0) or 0)
    admin_replied_today = (last_admin_ts > 0 and local_day(last_admin_ts) == local_day(now_ts))

    if not admin_replied_today:
        last_ts = int((st.get("last_auto_reply") or {}).get(str(uid), 0) or 0)
        if now_ts - last_ts >= AUTO_REPLY_COOLDOWN_SEC:
            lang = pick_lang(st, user)
            reply_text = auto_reply_text(lang)
            try:
                await update.message.reply_text(reply_text)
            except Exception:
                pass
            st.setdefault("last_auto_reply", {})[str(uid)] = now_ts

    save_state(st)
    await refresh_header(st, context, uid)


# ================== CORE: ADMIN Reply -> USER (All media) ==================
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
    rid = str(update.message.reply_to_message.message_id)
    to_user = None
    if rid in (st.get("msg_index") or {}):
        to_user = int(st["msg_index"][rid])

    if not to_user:
        # 兜底：从被回复消息里解析 UserID
        txt = update.message.reply_to_message.text or update.message.reply_to_message.caption or ""
        m = re.search(r"UserID:\s*`?(\d+)`?", txt)
        if m:
            to_user = int(m.group(1))

    if not to_user:
        await update.message.reply_text("没识别到用户ID。请 Reply 用户转发消息，或用 /reply <uid> <text>。")
        return

    # 若用户在黑名单，默认阻止发送（避免误操作）
    if int((st.get("blocked") or {}).get(str(to_user), 0) or 0) == 1:
        await update.message.reply_text("该用户在黑名单中，已阻止发送。如需发送请先解封。")
        return

    try:
        await context.bot.copy_message(
            chat_id=to_user,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id,
        )

        # 记录：管理员已回复（用于“当天不再自动回复”）
        st.setdefault("last_admin_reply", {})[str(to_user)] = _now_ts()
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
    tg_app.add_handler(CommandHandler("note", cmd_note))
    tg_app.add_handler(CommandHandler("status", cmd_status))
    tg_app.add_handler(CommandHandler("block", cmd_block))
    tg_app.add_handler(CommandHandler("unblock", cmd_unblock))
    tg_app.add_handler(CommandHandler("setlang", cmd_setlang))
    tg_app.add_handler(CommandHandler("profile", cmd_profile))
    tg_app.add_handler(CommandHandler("close", cmd_close))
    tg_app.add_handler(CommandHandler("reopen", cmd_reopen))
    tg_app.add_handler(CommandHandler("open", cmd_open))
    tg_app.add_handler(CommandHandler("history", cmd_history))

    # Buttons
    tg_app.add_handler(CallbackQueryHandler(on_callback))

    # Handlers (private chat only)
    tg_app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.User(user_id=ADMIN_ID), handle_user_private))
    tg_app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.User(user_id=ADMIN_ID), handle_admin_private))

    if PUBLIC_URL:
        asyncio.run(run_webhook_server(tg_app))
    else:
        tg_app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
