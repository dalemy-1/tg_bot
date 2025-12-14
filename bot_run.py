import os
import re
import json
import time
import asyncio
import html
from pathlib import Path
from typing import Any, Dict, Optional, List, Tuple

from aiohttp import web
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ChatType
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

# ===== GCP Translate (v2 wrapper in google-cloud-translate) =====
from google.cloud import translate_v2 as translate  # type: ignore


# ================== ENV ==================
TOKEN = (os.getenv("TG_BOT_TOKEN") or "").strip()
ADMIN_ID = int(os.getenv("TG_ADMIN_ID", "0") or "0")  # 必填：管理员 user_id（数字）

PUBLIC_URL = (os.getenv("RENDER_EXTERNAL_URL") or os.getenv("PUBLIC_URL") or "").strip().rstrip("/")
WEBHOOK_SECRET = (os.getenv("WEBHOOK_SECRET") or "").strip()
PORT = int(os.getenv("PORT", "10000"))

HEALTH_PATH = "/healthz"

# 管理员用户名，用于给用户“一键联系管理员”按钮（如 https://t.me/Adalemy）
ADMIN_USERNAME = (os.getenv("ADMIN_USERNAME") or "Adalemy").strip().lstrip("@")

# 自动回复（给用户）
AUTO_REPLY_TEXT = (os.getenv("AUTO_REPLY_TEXT") or "Hello, we received your message and will reply soon.").strip()
AUTO_REPLY_COOLDOWN_SEC = int(os.getenv("AUTO_REPLY_COOLDOWN_SEC", "86400"))  # 24h 默认

BASE_DIR = Path(__file__).resolve().parent
STATE_FILE = BASE_DIR / "support_state.json"
LOG_FILE = BASE_DIR / "history.jsonl"

MAX_MSG_INDEX = 8000  # admin_message_id -> user_id 映射上限


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
        "last_user": 0,
        "last_auto_reply": {},  # user_id(str) -> ts
        "user_meta": {},        # user_id(str) -> {name, username, language_code, first_seen, last_seen, msg_count}
        "user_status": {},      # user_id(str) -> 已下单/退货退款/已返款/黑名单/""（空表示未设置）
        "user_note": {},        # user_id(str) -> "..."
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


def fmt_time(ts: int) -> str:
    if not ts:
        return "-"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))


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


# ================== TRANSLATION ==================
_translate_client: Optional[translate.Client] = None


def get_translate_client() -> translate.Client:
    global _translate_client
    if _translate_client is None:
        # GOOGLE_APPLICATION_CREDENTIALS 必须在 Render 环境里指向 Secret File
        _translate_client = translate.Client()
    return _translate_client


def safe_strip_prefix(text: str, prefix: str) -> str:
    if text.startswith(prefix):
        return text[len(prefix):].lstrip()
    return text


def translate_text(text: str, target_lang: str) -> Tuple[str, str]:
    """
    return: (translated_text, detected_source_lang)
    """
    text = (text or "").strip()
    if not text:
        return "", ""
    client = get_translate_client()
    result = client.translate(text, target_language=target_lang)
    translated = result.get("translatedText", "") or ""
    detected = result.get("detectedSourceLanguage", "") or ""
    # translatedText 可能含 HTML entity
    translated = html.unescape(translated)
    return translated, detected


# ================== UI: ticket header & buttons ==================
STATUS_BUTTONS = ["已下单", "退货退款", "已返款", "黑名单"]
CLEAR_STATUS = "清空状态"


def ticket_keyboard(uid: int) -> InlineKeyboardMarkup:
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
            InlineKeyboardButton("清空状态", callback_data=f"status|{uid}|"),
            InlineKeyboardButton("Profile", callback_data=f"profile|{uid}|-"),
        ],
    ]
    return InlineKeyboardMarkup(rows)


def render_ticket_header_html(state: Dict[str, Any], uid: int) -> str:
    uid_key = str(uid)
    t = (state.get("tickets") or {}).get(uid_key, {})
    meta = (state.get("user_meta") or {}).get(uid_key, {})
    status = (state.get("user_status") or {}).get(uid_key, "") or "-"
    note = (state.get("user_note") or {}).get(uid_key, "") or "-"

    ticket_id = t.get("ticket_id", "-")
    ticket_status = t.get("status", "open")

    name = html.escape(meta.get("name", "Unknown") or "Unknown")
    username = meta.get("username")
    username = html.escape(username) if username else ""
    lang_code = html.escape(meta.get("language_code", "") or "")
    first_seen = fmt_time(int(meta.get("first_seen", 0) or 0))
    last_seen = fmt_time(int(meta.get("last_seen", 0) or 0))
    msg_count = int(meta.get("msg_count", 0) or 0)

    user_link = f"tg://user?id={uid}"

    lines = []
    lines.append(f"🧾 <b>Ticket #{ticket_id}</b>   <b>Status:</b> <code>{html.escape(str(ticket_status))}</code>")
    lines.append(f"<b>Name:</b> {name}")
    if username:
        lines.append(f"<b>Username:</b> @{username}")
    lines.append(f"<b>UserID:</b> <code>{uid}</code>   <b>Open:</b> <a href=\"{user_link}\">Click</a>")
    lines.append(f"<b>Lang:</b> <code>{lang_code or '-'}</code>")
    lines.append(f"<b>状态:</b> <code>{html.escape(status)}</code>")
    lines.append(f"<b>备注:</b> {html.escape(note)}")
    lines.append(f"<b>First seen:</b> <code>{html.escape(first_seen)}</code>")
    lines.append(f"<b>Last seen:</b> <code>{html.escape(last_seen)}</code>   <b>Msg count:</b> <code>{msg_count}</code>")
    lines.append("")
    lines.append("<b>推荐：</b>在管理员私聊里 <b>Reply</b> 用户“转发自用户”的消息即可回复（支持文字/图片/文件/贴纸等）。")
    lines.append(f"<b>历史：</b><code>/history {uid} 20</code>   <b>备注：</b><code>/note {uid} ...</code>")
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
            text=render_ticket_header_html(state, uid),
            parse_mode="HTML",
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
            text=render_ticket_header_html(state, uid),
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=ticket_keyboard(uid),
        )
    except Exception:
        pass


# ================== COMMANDS ==================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat and update.effective_chat.type == ChatType.PRIVATE:
        if is_admin(update):
            await update.message.reply_text(
                "管理员模式已启用：\n"
                "- 用户来信：原消息会“转发自用户”给你，同时额外发一条【中文翻译】\n"
                "- 你回复用户：只需 Reply 用户转发消息（你发中文会自动翻译成英文给用户）\n\n"
                "可用命令：\n"
                "/note <uid> <text>\n"
                "/history <uid> [n]\n"
                "/reply <uid> <text>\n"
                "/r <text>（回复最近用户）\n"
            )
        else:
            kb = InlineKeyboardMarkup(
                [[InlineKeyboardButton("一键联系管理员", url=f"https://t.me/{ADMIN_USERNAME}")]]
            )
            await update.message.reply_text(
                "你好，欢迎联系。\n"
                "请直接发送你的问题（支持文字/图片/文件/语音）。\n"
                "我们收到后会尽快回复。",
                reply_markup=kb
            )


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
    await update.message.reply_text("\n".join(out)[:3900])


async def cmd_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # 仍保留命令方式（文本会自动翻译成英文发给用户）
    if not is_admin(update):
        return
    if len(context.args) < 2:
        await update.message.reply_text("用法：/reply <user_id> <text>")
        return
    uid = int(context.args[0])
    text = " ".join(context.args[1:])
    await send_to_user_with_translation(context, uid, text=text, force_translate=True)
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
        await update.message.reply_text("没有最近用户。")
        return
    text = " ".join(context.args)
    await send_to_user_with_translation(context, uid, text=text, force_translate=True)
    log_event("out", uid, {"type": "text", "text": text[:1000]})
    await update.message.reply_text("已发送。")


# ================== CALLBACK: status buttons ==================
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

    if action == "status":
        value = parts[2] if len(parts) >= 3 else ""
        st.setdefault("user_status", {})[str(uid)] = value.strip()
        save_state(st)
        await refresh_header(st, context, uid)
        return

    if action == "profile":
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=render_ticket_header_html(st, uid),
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
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

    # 更新 meta
    meta = st.setdefault("user_meta", {}).setdefault(str(uid), {})
    meta.setdefault("first_seen", _now_ts())
    meta["last_seen"] = _now_ts()
    meta["msg_count"] = int(meta.get("msg_count", 0) or 0) + 1
    meta["name"] = (getattr(user, "full_name", "") or "Unknown").strip()
    meta["username"] = getattr(user, "username", None)
    meta["language_code"] = getattr(user, "language_code", "") or ""

    # 确保 ticket
    t = await ensure_ticket(st, context, uid)
    st["last_user"] = uid

    # 1) 先“转发自用户”给管理员（保留原格式）
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

    # 2) 再发“中文翻译”给管理员（仅对 text/caption）
    original = (update.message.text or update.message.caption or "").strip()
    if original:
        try:
            zh, detected = translate_text(original, target_lang="zh-CN")
            detected = detected or meta.get("language_code", "") or "-"
            msg_text = (
                f"【中文翻译】\n"
                f"UserID: {uid}\n"
                f"Detected: {detected}\n"
                f"——\n{zh}"
            )
            tr_msg = await context.bot.send_message(chat_id=ADMIN_ID, text=msg_text[:3900])
            remember_msg_index(st, tr_msg.message_id, uid)
        except Exception as e:
            # 翻译失败不影响转发
            err_msg = await context.bot.send_message(chat_id=ADMIN_ID, text=f"（翻译失败：{e}）")
            remember_msg_index(st, err_msg.message_id, uid)

    # 让管理员误 Reply header 也能回
    if t.get("header_msg_id"):
        remember_msg_index(st, int(t["header_msg_id"]), uid)

    # 历史记录
    typ = message_type_name(update.message)
    preview = (update.message.text or update.message.caption or "")
    log_event("in", uid, {"type": typ, "text": preview[:1000]})

    # 自动回复（24h 冷却）
    last_ts = int((st.get("last_auto_reply") or {}).get(str(uid), 0) or 0)
    now_ts = _now_ts()
    if now_ts - last_ts >= AUTO_REPLY_COOLDOWN_SEC:
        try:
            kb = InlineKeyboardMarkup(
                [[InlineKeyboardButton("一键联系管理员", url=f"https://t.me/{ADMIN_USERNAME}")]]
            )
            await update.message.reply_text(AUTO_REPLY_TEXT, reply_markup=kb)
        except Exception:
            pass
        st.setdefault("last_auto_reply", {})[str(uid)] = now_ts

    save_state(st)
    await refresh_header(st, context, uid)


# ================== ADMIN -> USER (Reply) ==================
def extract_target_user_id_from_reply(state: Dict[str, Any], reply_msg) -> Optional[int]:
    rid = str(reply_msg.message_id)
    mi = state.get("msg_index") or {}
    if rid in mi:
        return int(mi[rid])

    # 兜底：从翻译提示里抓 UserID
    txt = (reply_msg.text or reply_msg.caption or "") or ""
    m = re.search(r"UserID:\s*(\d+)", txt)
    if m:
        return int(m.group(1))
    return None


async def send_to_user_with_translation(
    context: ContextTypes.DEFAULT_TYPE,
    to_user: int,
    text: Optional[str] = None,
    force_translate: bool = True,
    message: Optional[Any] = None,
):
    """
    发送给用户：
    - 若是文本：中文 -> 英文（默认）
    - 若是媒体且带 caption：翻译 caption 后重新发送媒体
    - 其他：直接 copy
    特殊规则：
    - 以 "!!" 开头：跳过翻译，原样发送
    """
    # 只固定翻成英文（按你的需求）
    target_lang = "en"

    if text is not None:
        raw = text.strip()
        if raw.startswith("!!"):
            raw = safe_strip_prefix(raw, "!!")
            await context.bot.send_message(chat_id=to_user, text=raw)
            return

        # 翻译
        translated, detected = translate_text(raw, target_lang=target_lang)
        # 如果检测到本来就是英文，可以直接发原文或发 translated 都行；这里发 translated 更统一
        await context.bot.send_message(chat_id=to_user, text=translated or raw)
        return

    if message is None:
        return

    # 管理员发的是媒体/贴纸等：尽量支持 caption 翻译
    caption = (message.caption or "").strip()
    skip = False
    if caption.startswith("!!"):
        caption = safe_strip_prefix(caption, "!!")
        skip = True

    # 没 caption，直接 copy
    if not caption:
        await context.bot.copy_message(chat_id=to_user, from_chat_id=message.chat_id, message_id=message.message_id)
        return

    # 有 caption：翻译后用对应 send_* 重发
    out_caption = caption
    if not skip and force_translate:
        out_caption, _ = translate_text(caption, target_lang=target_lang)
        out_caption = out_caption or caption

    # photo
    if message.photo:
        file_id = message.photo[-1].file_id
        await context.bot.send_photo(chat_id=to_user, photo=file_id, caption=out_caption)
        return

    # video
    if message.video:
        await context.bot.send_video(chat_id=to_user, video=message.video.file_id, caption=out_caption)
        return

    # document
    if message.document:
        await context.bot.send_document(chat_id=to_user, document=message.document.file_id, caption=out_caption)
        return

    # animation (gif)
    if message.animation:
        await context.bot.send_animation(chat_id=to_user, animation=message.animation.file_id, caption=out_caption)
        return

    # audio
    if message.audio:
        await context.bot.send_audio(chat_id=to_user, audio=message.audio.file_id, caption=out_caption)
        return

    # 其他类型：copy（caption 无法改）
    await context.bot.copy_message(chat_id=to_user, from_chat_id=message.chat_id, message_id=message.message_id)


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
    to_user = extract_target_user_id_from_reply(st, update.message.reply_to_message)
    if not to_user:
        await update.message.reply_text("没识别到用户。请 Reply 用户“转发自用户”的消息（或中文翻译提示），或用 /reply <uid> <text>。")
        return

    # 文本消息：翻译后发送
    if update.message.text and not update.message.text.startswith("/"):
        await send_to_user_with_translation(context, to_user, text=update.message.text, force_translate=True)
        st["last_user"] = to_user
        save_state(st)
        log_event("out", to_user, {"type": "text", "text": update.message.text[:1000]})
        await update.message.reply_text("已发送。")
        return

    # 媒体/贴纸等：尽量翻译 caption 后重发，否则 copy
    try:
        await send_to_user_with_translation(context, to_user, message=update.message, force_translate=True)
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
    tg_app.add_handler(CommandHandler("note", cmd_note))
    tg_app.add_handler(CommandHandler("history", cmd_history))
    tg_app.add_handler(CommandHandler("reply", cmd_reply))
    tg_app.add_handler(CommandHandler("r", cmd_r))

    # Buttons
    tg_app.add_handler(CallbackQueryHandler(on_callback))

    # Private chat routing
    tg_app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.User(user_id=ADMIN_ID), handle_user_private))
    tg_app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.User(user_id=ADMIN_ID), handle_admin_private))

    if PUBLIC_URL:
        asyncio.run(run_webhook_server(tg_app))
    else:
        tg_app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
