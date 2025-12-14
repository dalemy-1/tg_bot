import os
import re
import json
import time
import asyncio
from pathlib import Path
from typing import Any, Dict, Optional

from aiohttp import web
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# ================== ENV ==================
TOKEN = (os.getenv("TG_BOT_TOKEN") or "").strip()
ADMIN_ID = int(os.getenv("TG_ADMIN_ID", "0") or "0")  # 管理员 Telegram user_id（必须填）
PUBLIC_URL = (os.getenv("RENDER_EXTERNAL_URL") or os.getenv("PUBLIC_URL") or "").strip().rstrip("/")
WEBHOOK_SECRET = (os.getenv("WEBHOOK_SECRET") or "").strip()
PORT = int(os.getenv("PORT", "10000"))

HEALTH_PATH = "/healthz"

# 自动回复：多语言模板（可用环境变量覆盖）
AUTO_REPLY_ZH = (os.getenv("AUTO_REPLY_ZH") or "你好，已收到你的消息，我们会尽快回复。").strip()
AUTO_REPLY_EN = (os.getenv("AUTO_REPLY_EN") or "Hello, we received your message and will reply soon.").strip()
AUTO_REPLY_JA = (os.getenv("AUTO_REPLY_JA") or "メッセージを受け取りました。できるだけ早く返信します。").strip()
AUTO_REPLY_DEFAULT = (os.getenv("AUTO_REPLY_TEXT") or "已收到，请联系 @Dalemy").strip()

# 自动回复冷却（避免用户连发时每条都自动回复）
AUTO_REPLY_COOLDOWN_SEC = int(os.getenv("AUTO_REPLY_COOLDOWN_SEC", "300"))

# 是否允许管理员“直接发一句话”默认回复最近用户（不建议开，防误发）
ALLOW_PLAIN_TO_LAST = (os.getenv("ALLOW_PLAIN_TO_LAST", "0").strip() == "1")

BASE_DIR = Path(__file__).resolve().parent
STATE_FILE = BASE_DIR / "support_state.json"
LOG_FILE = BASE_DIR / "history.jsonl"


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
        "tickets": {},          # user_id -> {ticket_id, status, created_at, header_msg_id}
        "msg_index": {},        # admin_message_id -> user_id（用于管理员 Reply 回用户）
        "last_user": 0,         # 最近一个发消息的用户
        "user_lang": {},        # user_id -> auto|zh|en|ja
        "last_auto_reply": {},  # user_id -> ts
    }


def save_state(state: Dict[str, Any]) -> None:
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def is_admin_user(update: Update) -> bool:
    return bool(update.effective_user and update.effective_user.id == ADMIN_ID and ADMIN_ID > 0)


def pick_lang(state: Dict[str, Any], user: Any) -> str:
    """返回 auto|zh|en|ja 的最终选择。"""
    uid = str(getattr(user, "id", 0) or 0)
    forced = (state.get("user_lang") or {}).get(uid, "auto")
    if forced in {"zh", "en", "ja"}:
        return forced

    # auto：用 telegram language_code 推断
    code = (getattr(user, "language_code", "") or "").lower()
    if code.startswith("zh"):
        return "zh"
    if code.startswith("ja"):
        return "ja"
    return "en"  # 默认英文更稳


def auto_reply_text_by_lang(lang: str) -> str:
    if lang == "zh":
        return AUTO_REPLY_ZH
    if lang == "ja":
        return AUTO_REPLY_JA
    if lang == "en":
        return AUTO_REPLY_EN
    return AUTO_REPLY_DEFAULT


def log_event(direction: str, user_id: int, payload: Dict[str, Any]) -> None:
    rec = {
        "ts": _now_ts(),
        "direction": direction,  # "in" 用户->管理员 / "out" 管理员->用户
        "user_id": user_id,
        **payload,
    }
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def _fmt_user_line(u: Any) -> str:
    name = (getattr(u, "full_name", "") or "").strip() or (getattr(u, "first_name", "") or "").strip() or "Unknown"
    username = getattr(u, "username", None)
    uid = getattr(u, "id", 0) or 0
    user_link = f"tg://user?id={uid}"
    if username:
        return f"*Name:* {name}\n*Username:* @{username}\n*UserID:* `{uid}`\n*Open:* [Click]({user_link})"
    return f"*Name:* {name}\n*UserID:* `{uid}`\n*Open:* [Click]({user_link})"


def _extract_user_id_from_text(text: str) -> Optional[int]:
    if not text:
        return None
    m = re.search(r"UserID:\s*`?(\d+)`?", text)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    return None


async def ensure_ticket_and_header(update: Update, context: ContextTypes.DEFAULT_TYPE, state: Dict[str, Any]) -> Dict[str, Any]:
    """为某个用户确保有 open ticket，并在管理员私聊发一条 header（工单卡片）。"""
    u = update.effective_user
    uid = getattr(u, "id", 0) or 0
    uid_key = str(uid)

    tickets = state.setdefault("tickets", {})
    t = tickets.get(uid_key)

    need_new = True
    if t and t.get("status") == "open" and t.get("header_msg_id"):
        need_new = False

    if need_new:
        state["ticket_seq"] = int(state.get("ticket_seq", 0)) + 1
        ticket_id = state["ticket_seq"]

        header = (
            f"🧾 *New DM (Ticket #{ticket_id})*\n"
            f"{_fmt_user_line(u)}\n\n"
            f"*用法：*\n"
            f"`/reply {uid} 你的回复内容`\n"
            f"`/r 你的回复内容`（回复最近一个用户）\n\n"
            f"*最推荐：* 直接 *Reply*（回复）下面用户消息，输入文字/图片/文件发送即可。\n"
            f"*历史：* `/history {uid} 20`\n"
            f"*关闭：* `/close {uid}`\n"
        )

        msg = await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=header,
            parse_mode=ParseMode.MARKDOWN,
            disable_web_page_preview=True,
        )

        tickets[uid_key] = {
            "ticket_id": ticket_id,
            "status": "open",
            "created_at": _now_ts(),
            "header_msg_id": msg.message_id,
        }
        save_state(state)

    return tickets[uid_key]


# ================== COMMANDS ==================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat and update.effective_chat.type == "private":
        await update.message.reply_text(
            "Bot is online.\n\n"
            "管理员：\n"
            "1) 用户私聊会自动生成 Ticket 卡片\n"
            "2) 直接 Reply 用户消息即可回复（支持文字/图片/文件/表情等）\n"
            "3) /reply <user_id> <text>\n"
            "4) /r <text>（回复最近用户）\n"
            "5) /history <user_id> [n]\n"
            "6) /close <user_id>\n"
            "7) /setlang <user_id> <auto|zh|en|ja>\n"
        )


async def cmd_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin_user(update):
        await update.message.reply_text("无权限。")
        return

    if len(context.args) < 2:
        await update.message.reply_text("用法：/reply <user_id> <text>")
        return

    try:
        to_user = int(context.args[0])
    except Exception:
        await update.message.reply_text("user_id 必须是数字。")
        return

    text = " ".join(context.args[1:])
    await context.bot.send_message(chat_id=to_user, text=text)

    state = load_state()
    state["last_user"] = to_user
    save_state(state)
    log_event("out", to_user, {"type": "text", "text": text[:1000]})

    await update.message.reply_text("已发送。")


async def cmd_r(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin_user(update):
        await update.message.reply_text("无权限。")
        return
    if not context.args:
        await update.message.reply_text("用法：/r <text>")
        return

    state = load_state()
    to_user = int(state.get("last_user", 0) or 0)
    if to_user <= 0:
        await update.message.reply_text("没有“最近用户”。请先等用户发来消息，或用 /reply <user_id> <text>。")
        return

    text = " ".join(context.args)
    await context.bot.send_message(chat_id=to_user, text=text)
    log_event("out", to_user, {"type": "text", "text": text[:1000]})
    await update.message.reply_text(f"已发送给 {to_user}。")


async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin_user(update):
        await update.message.reply_text("无权限。")
        return
    if not context.args:
        await update.message.reply_text("用法：/history <user_id> [n]")
        return

    try:
        uid = int(context.args[0])
    except Exception:
        await update.message.reply_text("user_id 必须是数字。")
        return

    n = 20
    if len(context.args) >= 2:
        try:
            n = max(1, min(100, int(context.args[1])))
        except Exception:
            n = 20

    if not LOG_FILE.exists():
        await update.message.reply_text("暂无历史记录。")
        return

    # 读取末尾 n 条（简单实现：全读再筛，数据量大时再优化）
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
    out = [f"*History for* `{uid}` (last {len(recs)})\n"]
    for r in recs:
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(r.get("ts", 0))))
        direction = "IN " if r.get("direction") == "in" else "OUT"
        typ = r.get("type", "msg")
        text = (r.get("text") or "").replace("\n", " ")
        if len(text) > 60:
            text = text[:60] + "..."
        out.append(f"`{ts}` *{direction}* _{typ}_  {text}")

    msg = "\n".join(out)
    await update.message.reply_text(msg[:3500], parse_mode=ParseMode.MARKDOWN)


async def cmd_close(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin_user(update):
        await update.message.reply_text("无权限。")
        return
    if not context.args:
        await update.message.reply_text("用法：/close <user_id>")
        return
    try:
        uid = int(context.args[0])
    except Exception:
        await update.message.reply_text("user_id 必须是数字。")
        return

    state = load_state()
    t = (state.get("tickets") or {}).get(str(uid))
    if not t:
        await update.message.reply_text("该用户没有 ticket。")
        return
    t["status"] = "closed"
    save_state(state)
    await update.message.reply_text(f"已关闭 ticket：{uid}")


async def cmd_setlang(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin_user(update):
        await update.message.reply_text("无权限。")
        return
    if len(context.args) < 2:
        await update.message.reply_text("用法：/setlang <user_id> <auto|zh|en|ja>")
        return
    try:
        uid = int(context.args[0])
    except Exception:
        await update.message.reply_text("user_id 必须是数字。")
        return

    lang = context.args[1].lower().strip()
    if lang not in {"auto", "zh", "en", "ja"}:
        await update.message.reply_text("lang 仅支持：auto|zh|en|ja")
        return

    state = load_state()
    state.setdefault("user_lang", {})[str(uid)] = lang
    save_state(state)
    await update.message.reply_text(f"已设置 {uid} language={lang}")


# ================== CORE HANDLERS ==================
async def handle_user_private(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """用户私聊 -> 管理员私聊（工单）"""
    if not update.message or not update.effective_chat:
        return
    if update.effective_chat.type != "private":
        return

    # 管理员自己的私聊消息不走这里
    if is_admin_user(update):
        return

    if ADMIN_ID <= 0:
        # 没设置管理员时只自动回复
        await update.message.reply_text(AUTO_REPLY_DEFAULT)
        return

    state = load_state()

    # 1) 确保 ticket + header
    t = await ensure_ticket_and_header(update, context, state)
    header_msg_id = int(t.get("header_msg_id") or 0)

    user = update.effective_user
    user_id = getattr(user, "id", 0) or 0
    state["last_user"] = user_id

    # 2) 把用户消息转发到管理员（保持“转发自用户”样式）
    try:
        fwd = await context.bot.forward_message(
            chat_id=ADMIN_ID,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id,
        )
        # 用 reply_to_message_id 把消息挂在 header 下，更像“线程”
        # 说明：forward_message 自身不能指定 reply_to，所以我们再补发一个小锚点（可选）
        # 为了不污染对话，这里用 msg_index 来支持管理员 Reply。
        state.setdefault("msg_index", {})[str(fwd.message_id)] = user_id
    except Exception:
        # forward 失败则退化为 copy（仍然支持多媒体）
        copied = await context.bot.copy_message(
            chat_id=ADMIN_ID,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id,
        )
        state.setdefault("msg_index", {})[str(copied.message_id)] = user_id

    # 3) 发送一个“锚点”到管理员（reply 到 header），让视觉更像“同一工单串”
    # 这条非常短，不会影响你阅读，但能让你在管理员私聊里快速定位 ticket
    try:
        anchor_text = f"↳ Ticket #{t.get('ticket_id')}  UserID: `{user_id}`"
        anchor = await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=anchor_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_to_message_id=header_msg_id if header_msg_id else None,
        )
        state.setdefault("msg_index", {})[str(anchor.message_id)] = user_id
    except Exception:
        pass

    # 4) 记录历史
    msg_type = "text" if update.message.text else ("caption" if update.message.caption else "media")
    text_preview = (update.message.text or update.message.caption or "")
    log_event("in", user_id, {"type": msg_type, "text": text_preview[:1000]})

    # 5) 自动回复（带冷却）
    last_ts = int((state.get("last_auto_reply") or {}).get(str(user_id), 0) or 0)
    now_ts = _now_ts()
    if now_ts - last_ts >= AUTO_REPLY_COOLDOWN_SEC:
        lang = pick_lang(state, user)
        reply_text = auto_reply_text_by_lang(lang)
        try:
            await update.message.reply_text(reply_text)
        except Exception:
            pass
        state.setdefault("last_auto_reply", {})[str(user_id)] = now_ts

    save_state(state)


async def handle_admin_private(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """管理员私聊里：直接 Reply 用户消息 -> 自动回给用户（支持文字/图片/文件/表情等）"""
    if not update.message or not update.effective_chat:
        return
    if update.effective_chat.type != "private":
        return
    if not is_admin_user(update):
        return

    state = load_state()

    # 1) 如果管理员在回复某条消息，优先从 msg_index 取 user_id
    to_user: Optional[int] = None
    if update.message.reply_to_message:
        rid = str(update.message.reply_to_message.message_id)
        if rid in (state.get("msg_index") or {}):
            to_user = int(state["msg_index"][rid])
        else:
            # 再从被回复的文本里兜底解析 UserID
            text = update.message.reply_to_message.text or update.message.reply_to_message.caption or ""
            parsed = _extract_user_id_from_text(text)
            if parsed:
                to_user = parsed

    # 2) 如果没 reply，且允许发给最近用户
    if to_user is None and ALLOW_PLAIN_TO_LAST:
        lu = int(state.get("last_user", 0) or 0)
        if lu > 0:
            to_user = lu

    if not to_user:
        # 不用报错太长，避免刷屏
        await update.message.reply_text("我没能识别用户ID。请 Reply 用户消息，或用 /reply <user_id> <text> / /r <text>。")
        return

    # 3) 复制管理员消息给用户（copy_message 支持所有类型：文字/图片/文件/表情/语音等）
    try:
        await context.bot.copy_message(
            chat_id=to_user,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id,
        )
        state["last_user"] = to_user
        save_state(state)

        text_preview = (update.message.text or update.message.caption or "")
        out_type = "text" if update.message.text else ("caption" if update.message.caption else "media")
        log_event("out", to_user, {"type": out_type, "text": text_preview[:1000]})

        await update.message.reply_text("已发送。")
    except Exception as e:
        await update.message.reply_text(f"发送失败：{e}")


# ================== WEBHOOK SERVER ==================
async def run_webhook_server(tg_app: Application):
    if not PUBLIC_URL:
        raise RuntimeError("Missing PUBLIC_URL (or RENDER_EXTERNAL_URL). Please set PUBLIC_URL in Render env.")
    if not WEBHOOK_SECRET:
        raise RuntimeError("Missing WEBHOOK_SECRET. Please set WEBHOOK_SECRET in Render env (random string).")

    webhook_path = f"/{WEBHOOK_SECRET}"
    webhook_url = f"{PUBLIC_URL}{webhook_path}"

    await tg_app.initialize()
    await tg_app.start()
    await tg_app.bot.set_webhook(url=webhook_url)

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
        raise SystemExit("Missing TG_ADMIN_ID (admin user_id).")

    tg_app = Application.builder().token(TOKEN).build()

    # commands
    tg_app.add_handler(CommandHandler("start", cmd_start))
    tg_app.add_handler(CommandHandler("reply", cmd_reply))
    tg_app.add_handler(CommandHandler("r", cmd_r))
    tg_app.add_handler(CommandHandler("history", cmd_history))
    tg_app.add_handler(CommandHandler("close", cmd_close))
    tg_app.add_handler(CommandHandler("setlang", cmd_setlang))

    # user private: ALL messages (包含图片/表情/文件/语音等)
    tg_app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.User(user_id=ADMIN_ID), handle_user_private))

    # admin private: ALL messages（管理员在私聊里 Reply 任意一条用户消息即可回复）
    tg_app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.User(user_id=ADMIN_ID), handle_admin_private))

    # Render 用 webhook；本地无 PUBLIC_URL 则 polling
    if PUBLIC_URL:
        asyncio.run(run_webhook_server(tg_app))
    else:
        tg_app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
