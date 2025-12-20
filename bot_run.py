import asyncio
import os
import re
import json
import time
import asyncio
from pathlib import Path
from typing import Any, Dict, Optional

from aiohttp import web, ClientSession, ClientTimeout

import langid
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
print("[boot] TG_BOT_TOKEN prefix:", (TOKEN or "")[:10], "len:", len(TOKEN or ""), "tail:", (TOKEN or "")[-4:])
print("[boot] RENDER_EXTERNAL_URL:", (os.getenv("RENDER_EXTERNAL_URL") or "")[:80])
print("[boot] PUBLIC_URL:", (os.getenv("PUBLIC_URL") or "")[:80])



PUBLIC_URL = (os.getenv("RENDER_EXTERNAL_URL") or os.getenv("PUBLIC_URL") or "").strip().rstrip("/")
WEBHOOK_SECRET = (os.getenv("WEBHOOK_SECRET") or "").strip()
PORT = int(os.getenv("PORT", "10000"))
HEALTH_PATH = "/healthz"

ADMIN_USERNAME = (os.getenv("ADMIN_USERNAME") or "Adalemy").strip().lstrip("@")

AUTO_REPLY_TEXT = (os.getenv("AUTO_REPLY_TEXT") or "你好，已收到你的消息，我们会尽快回复。").strip()
AUTO_REPLY_COOLDOWN_SEC = int(os.getenv("AUTO_REPLY_COOLDOWN_SEC", "86400"))  # 24h 默认

TRANSLATE_ENABLED = (os.getenv("TRANSLATE_ENABLED") or "1").strip() == "1"
ADMIN_LANG = "zh-CN"  # 管理员侧统一中文

# 免费翻译后端：可选 LibreTranslate + 兜底 MyMemory
LIBRETRANSLATE_URL = (os.getenv("LIBRETRANSLATE_URL") or "").strip().rstrip("/")
LIBRETRANSLATE_API_KEY = (os.getenv("LIBRETRANSLATE_API_KEY") or "").strip()
MYMEMORY_EMAIL = (os.getenv("MYMEMORY_EMAIL") or "").strip()

BASE_DIR = Path(__file__).resolve().parent
STATE_FILE = BASE_DIR / "support_state.json"

MAX_MSG_INDEX = 8000

STATUS_OPTIONS = ["已下单", "退货退款", "已返款", "黑名单"]
DEFAULT_STATUS = "用户来信"


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
        "user_meta": {},        # user_id(str) -> {name, username, language_code, first_seen, last_seen, msg_count, last_detected_lang}
        "user_status": {},      # user_id(str) -> status
    }


def save_state(state: Dict[str, Any]) -> None:
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def is_admin(update: Update) -> bool:
    return bool(update.effective_user and update.effective_user.id == ADMIN_ID and ADMIN_ID > 0)


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


def _safe(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# ================== TRANSLATION (strict mutual) ==================
_http: Optional[ClientSession] = None


def _norm_lang(code: str) -> str:
    c = (code or "").strip().replace("_", "-")
    low = c.lower()
    if not low:
        return "auto"
    if low.startswith("zh"):
        return "zh-CN"
    if low.startswith("ja"):
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
    return low[:2]


def _is_chinese(text: str) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", text or ""))


def detect_lang(text: str) -> str:
    t = (text or "").strip()
    if not t:
        return "auto"
    if _is_chinese(t):
        return "zh-CN"
    try:
        code, _score = langid.classify(t)
        return _norm_lang(code)
    except Exception:
        return "auto"


async def _session() -> ClientSession:
    global _http
    if _http is None or _http.closed:
        _http = ClientSession(timeout=ClientTimeout(total=12))
    return _http


async def _translate_libre(text: str, src: str, tgt: str) -> Optional[str]:
    if not LIBRETRANSLATE_URL:
        return None
    url = f"{LIBRETRANSLATE_URL}/translate"
    payload = {"q": text, "source": src, "target": tgt, "format": "text"}
    if LIBRETRANSLATE_API_KEY:
        payload["api_key"] = LIBRETRANSLATE_API_KEY
    try:
        s = await _session()
        async with s.post(url, json=payload) as resp:
            data = await resp.json(content_type=None)
        tr = (data or {}).get("translatedText")
        if tr and tr.strip():
            return tr.strip()
        return None
    except Exception:
        return None


async def _translate_mymemory(text: str, src: str, tgt: str) -> Optional[str]:
    url = "https://api.mymemory.translated.net/get"
    params = {"q": text, "langpair": f"{src}|{tgt}"}
    if MYMEMORY_EMAIL:
        params["de"] = MYMEMORY_EMAIL
    try:
        s = await _session()
        async with s.get(url, params=params) as resp:
            data = await resp.json(content_type=None)
        tr = (((data or {}).get("responseData") or {}).get("translatedText") or "").strip()
        if tr:
            return tr
        return None
    except Exception:
        return None


async def translate(text: str, src: str, tgt: str) -> Optional[str]:
    """失败返回 None；严格互译：中文<->其它语言"""
    if not TRANSLATE_ENABLED:
        return None
    q = (text or "").strip()
    if not q:
        return None

    src = _norm_lang(src)
    tgt = _norm_lang(tgt)

    if src == "auto":
        src = detect_lang(q)
        if src == "auto":
            # 兜底：如果目标是中文，就当英文；否则当中文
            src = "en" if tgt == "zh-CN" else "zh-CN"

    if src == tgt:
        return q

    tr = await _translate_libre(q, src, tgt)
    if tr:
        return tr

    tr = await _translate_mymemory(q, src, tgt)
    if tr:
        return tr

    return None


# ================== UI ==================
def contact_admin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("联系管理员", url=f"https://t.me/{ADMIN_USERNAME}")]
    ])


def status_keyboard(uid: int) -> InlineKeyboardMarkup:
    # 只保留：已下单 / 退货退款 / 已返款 / 黑名单 / 清空状态 + Profile(可选保留)
    row1 = [
        InlineKeyboardButton("已下单", callback_data=f"status|{uid}|已下单"),
        InlineKeyboardButton("退货退款", callback_data=f"status|{uid}|退货退款"),
    ]
    row2 = [
        InlineKeyboardButton("已返款", callback_data=f"status|{uid}|已返款"),
        InlineKeyboardButton("黑名单", callback_data=f"status|{uid}|黑名单"),
    ]
    row3 = [
        InlineKeyboardButton("清空状态", callback_data=f"clear|{uid}|-"),
        InlineKeyboardButton("Profile", callback_data=f"profile|{uid}|-"),
    ]
    return InlineKeyboardMarkup([row1, row2, row3])


def render_header(state: Dict[str, Any], uid: int) -> str:
    uid_key = str(uid)
    t = (state.get("tickets") or {}).get(uid_key, {})
    meta = (state.get("user_meta") or {}).get(uid_key, {})
    status = (state.get("user_status") or {}).get(uid_key, DEFAULT_STATUS)

    ticket_id = t.get("ticket_id", "-")
    name = meta.get("name", "Unknown")
    username = meta.get("username")
    user_link = f"tg://user?id={uid}"

    first_seen = int(meta.get("first_seen", 0) or 0)
    last_seen = int(meta.get("last_seen", 0) or 0)
    msg_count = int(meta.get("msg_count", 0) or 0)
    last_lang = _norm_lang(meta.get("last_detected_lang", "auto"))

    lines = [
        f"🧾 <b>Ticket #{ticket_id}</b>   <b>Status:</b> <code>{_safe(status)}</code>",
        f"<b>Name:</b> {_safe(name)}",
    ]
    if username:
        lines.append(f"<b>Username:</b> @{_safe(username)}")
    lines += [
        f"<b>UserID:</b> <code>{uid}</code>   <b>Open:</b> <a href=\"{user_link}\">Click</a>",
        f"<b>Last lang:</b> <code>{_safe(last_lang)}</code>",
        f"<b>First seen:</b> <code>{fmt_time(first_seen)}</code>",
        f"<b>Last seen:</b> <code>{fmt_time(last_seen)}</code>   <b>Msg count:</b> <code>{msg_count}</code>",
        "",
        "<b>推荐：</b>在管理员私聊里 <b>Reply（回复）</b>下面那条“转发自用户”的消息，即可回复对方（支持文字/图片/文件/贴纸/语音等）。",
        f"<b>备用：</b>直接在此用户聊天窗口私聊对方：<a href=\"{user_link}\">打开对话</a>",
    ]
    return "\n".join(lines)


async def ensure_ticket(state: Dict[str, Any], context: ContextTypes.DEFAULT_TYPE, uid: int) -> Dict[str, Any]:
    tickets = state.setdefault("tickets", {})
    uid_key = str(uid)
    t = tickets.get(uid_key)

    need_new = True
    if t and t.get("header_msg_id"):
        need_new = False

    if need_new:
        state["ticket_seq"] = int(state.get("ticket_seq", 0)) + 1
        ticket_id = state["ticket_seq"]

        # 初始化状态
        state.setdefault("user_status", {}).setdefault(uid_key, DEFAULT_STATUS)

        msg = await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=render_header(state, uid),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=status_keyboard(uid),
        )

        tickets[uid_key] = {
            "ticket_id": ticket_id,
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
            text=render_header(state, uid),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=status_keyboard(uid),
        )
    except Exception:
        pass


# ================== COMMANDS (keep minimal) ==================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if is_admin(update):
        await update.message.reply_text(
            "机器人已上线。\n\n"
            "管理员用法：\n"
            "1) 用户给机器人发消息 -> 你会收到“转发自用户”的消息。\n"
            "2) 你只需要 Reply 那条“转发自用户”的消息（可发文字/图片/文件等），机器人会转发给用户。\n"
            "3) 支持严格互译：用户非中文 -> 自动翻译成中文发给你；你发中文 -> 自动翻译成用户语言发给用户。\n"
        )
    else:
        await update.message.reply_text(
            "你好，欢迎联系。\n"
            "请直接发送你的消息（文字/图片/文件等）。我们收到后会尽快回复。\n",
            reply_markup=contact_admin_keyboard()
        )


# ================== CALLBACKS (admin buttons) ==================
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
        if status in STATUS_OPTIONS:
            st.setdefault("user_status", {})[str(uid)] = status
            save_state(st)
            await refresh_header(st, context, uid)
        return

    if action == "clear":
        st.setdefault("user_status", {})[str(uid)] = DEFAULT_STATUS
        save_state(st)
        await refresh_header(st, context, uid)
        return

    if action == "profile":
        # 只复发一份 header 作为 profile（无需命令）
        try:
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=render_header(st, uid),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=status_keyboard(uid),
            )
        except Exception:
            pass
        return


# ================== USER -> ADMIN (forward + translate to zh) ==================
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

    # ticket/header
    t = await ensure_ticket(st, context, uid)

    # 如果状态为空/清空后，保持“用户来信”
    st.setdefault("user_status", {}).setdefault(str(uid), DEFAULT_STATUS)

    # 转发给管理员（保留“转发自用户”）
    forwarded_id = None
    try:
        fwd = await context.bot.forward_message(
            chat_id=ADMIN_ID,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id,
        )
        forwarded_id = fwd.message_id
        remember_msg_index(st, fwd.message_id, uid)
    except Exception:
        copied = await context.bot.copy_message(
            chat_id=ADMIN_ID,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id,
        )
        forwarded_id = copied.message_id
        remember_msg_index(st, copied.message_id, uid)

    # 也把 header 记入 index（防止管理员误 Reply header）
    if t.get("header_msg_id"):
        remember_msg_index(st, int(t["header_msg_id"]), uid)

    # 检测语言（以用户消息为准）
    txt = (update.message.text or update.message.caption or "").strip()
    if txt:
        src = detect_lang(txt)
        meta["last_detected_lang"] = src

        # 严格：非中文 -> 翻译成中文发给管理员（贴在转发下面）
        if TRANSLATE_ENABLED and _norm_lang(src) != "zh-CN" and forwarded_id:
            zh = await translate(txt, src, "zh-CN")
            if zh and zh.strip() and zh.strip() != txt.strip():
                try:
                    await context.bot.send_message(
                        chat_id=ADMIN_ID,
                        text=f"中文翻译（{_safe(src)} → zh-CN）：\n{_safe(zh)}",
                        reply_to_message_id=forwarded_id,
                    )
                except Exception:
                    pass

    # 自动回复（24小时一次）
    last_ts = int((st.get("last_auto_reply") or {}).get(str(uid), 0) or 0)
    now_ts = _now_ts()
    if now_ts - last_ts >= AUTO_REPLY_COOLDOWN_SEC:
        try:
            await update.message.reply_text(AUTO_REPLY_TEXT, reply_markup=contact_admin_keyboard())
        except Exception:
            pass
        st.setdefault("last_auto_reply", {})[str(uid)] = now_ts

    st["last_user"] = uid
    save_state(st)
    await refresh_header(st, context, uid)


# ================== ADMIN Reply -> USER (support media + zh->user lang) ==================
async def handle_admin_private(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat:
        return
    if update.effective_chat.type != ChatType.PRIVATE:
        return
    if not is_admin(update):
        return

    # 必须 Reply 才转发（避免误发）
    if not update.message.reply_to_message:
        return

    st = load_state()

    rid = str(update.message.reply_to_message.message_id)
    to_user = None
    if rid in (st.get("msg_index") or {}):
        to_user = int(st["msg_index"][rid])

    if not to_user:
        # 没识别到就不发
        try:
            await update.message.reply_text("没识别到用户ID：请 Reply 用户的“转发自用户”消息。")
        except Exception:
            pass
        return

    # 目标语言：以用户最后检测语言为准（严格互译的关键）
    user_meta = (st.get("user_meta") or {}).get(str(to_user), {})
    user_lang = _norm_lang(user_meta.get("last_detected_lang", "en"))
    if user_lang == "auto":
        user_lang = "en"

    # 1) 先把管理员消息 copy 给用户（保证媒体可达）
    # 2) 如需翻译，则额外再发一条“翻译后的文本”（文本消息则直接发翻译）
    try:
        admin_text = (update.message.text or "").strip()
        admin_caption = (update.message.caption or "").strip()

        # 文本消息：直接发翻译后的文本
        if admin_text:
            send_text = admin_text
            if TRANSLATE_ENABLED and _is_chinese(admin_text) and user_lang != "zh-CN":
                tr = await translate(admin_text, "zh-CN", user_lang)
                if tr and tr.strip():
                    send_text = tr.strip()
            await context.bot.send_message(chat_id=to_user, text=send_text)

        else:
            # 媒体/文件/贴纸等：先 copy 原消息
            await context.bot.copy_message(
                chat_id=to_user,
                from_chat_id=update.effective_chat.id,
                message_id=update.message.message_id,
            )

            # 如果有 caption 且为中文，则再补发翻译文本
            if admin_caption and TRANSLATE_ENABLED and _is_chinese(admin_caption) and user_lang != "zh-CN":
                tr = await translate(admin_caption, "zh-CN", user_lang)
                if tr and tr.strip():
                    await context.bot.send_message(chat_id=to_user, text=tr.strip())

        st["last_user"] = to_user
        save_state(st)

        try:
            await update.message.reply_text("已发送。")
        except Exception:
            pass

    except Exception as e:
        try:
            await update.message.reply_text(f"发送失败：{e}")
        except Exception:
            pass


# ================== WEBHOOK SERVER ==================
import asyncio  # 建议放文件顶部（如果你顶部已经 import 过，就不要重复）

async def run_webhook_server(tg_app: Application):
    if not PUBLIC_URL:
        raise RuntimeError("Missing PUBLIC_URL (or RENDER_EXTERNAL_URL).")
    if not WEBHOOK_SECRET:
        raise RuntimeError("Missing WEBHOOK_SECRET.")
    if ADMIN_ID <= 0:
        raise RuntimeError("Missing TG_ADMIN_ID.")

    webhook_path = f"/{WEBHOOK_SECRET}"
    webhook_url = f"{PUBLIC_URL}{webhook_path}"

    # 先启动 PTB
    await tg_app.initialize()
    await tg_app.start()

    # 再设置 webhook
    await tg_app.bot.set_webhook(url=webhook_url, drop_pending_updates=True)

    aio = web.Application()

    async def health(_request):
        return web.Response(text="ok")

    async def handle_update(request: web.Request):
        # 只做最轻量的事情：读 json + 立刻回 ok
        try:
            data = await request.json()
        except Exception:
            return web.Response(status=400, text="bad json")

        resp = web.Response(text="ok")  # 立刻响应 Telegram，避免 Read timeout expired

        async def _process():
            try:
                update = Update.de_json(data, tg_app.bot)
                await tg_app.process_update(update)
            except Exception as e:
                print("process_update error:", repr(e))

        asyncio.create_task(_process())
        return resp

    # 路由注册必须在这里（不能缩进到 handle_update 里）
    aio.router.add_get(HEALTH_PATH, health)
    aio.router.add_post(webhook_path, handle_update)

    runner = web.AppRunner(aio)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=PORT)
    await site.start()

    print(f"[ok] webhook set: {webhook_url}")
    print(f"[ok] listening on 0.0.0.0:{PORT}, health: {HEALTH_PATH}")

    # 常驻不退出
    await asyncio.Event().wait()




def main():
    if not TOKEN:
        raise SystemExit("Missing TG_BOT_TOKEN")
    if ADMIN_ID <= 0:
        raise SystemExit("Missing TG_ADMIN_ID")

    tg_app = Application.builder().token(TOKEN).build()

    # Minimal command
    tg_app.add_handler(CommandHandler("start", cmd_start))

    # Buttons (admin)
    tg_app.add_handler(CallbackQueryHandler(on_callback))

    # Private handlers
    tg_app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.User(user_id=ADMIN_ID), handle_user_private))
    tg_app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.User(user_id=ADMIN_ID), handle_admin_private))

    if PUBLIC_URL:
        asyncio.run(run_webhook_server(tg_app))
    else:
        tg_app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()







