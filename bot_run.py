import os
import json
import asyncio
import re
from pathlib import Path
from typing import Optional, Dict, Any

from aiohttp import web
from telegram import Update
from telegram.constants import ChatType
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# =========================
# 环境变量
# =========================
TOKEN = (os.getenv("TG_BOT_TOKEN") or "").strip()
ADMIN_ID = int(os.getenv("TG_ADMIN_ID", "0") or "0")
AUTO_REPLY_TEXT = (os.getenv("AUTO_REPLY_TEXT") or "已收到，请联系 @Dalemy").strip()

PORT = int(os.getenv("PORT", "10000"))
PUBLIC_URL = (os.getenv("RENDER_EXTERNAL_URL") or os.getenv("PUBLIC_URL") or "").strip().rstrip("/")
WEBHOOK_SECRET = (os.getenv("WEBHOOK_SECRET") or "").strip()
HEALTH_PATH = "/healthz"

BASE_DIR = Path(__file__).resolve().parent
STATE_FILE = BASE_DIR / "state.json"         # 存 last_uid / thread_map
VALID_MARKETS = {"US", "UK", "DE", "FR", "IT", "ES", "CA", "JP"}


# =========================
# 简单持久化
# =========================
def load_state() -> Dict[str, Any]:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_state(s: Dict[str, Any]) -> None:
    STATE_FILE.write_text(json.dumps(s, ensure_ascii=False, indent=2), encoding="utf-8")

def is_admin(uid: int) -> bool:
    return ADMIN_ID > 0 and uid == ADMIN_ID


# =========================
# 管理命令
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Bot is online.\n\n"
        "群内话题绑定：/bind <US|UK|DE|FR|IT|ES|CA|JP>\n"
        "查看映射：/map\n\n"
        "管理员回复：\n"
        "  /reply <user_id> <text>\n"
        "  /r <text>  （回复最近一个私聊用户）\n\n"
        "也支持：直接 Reply 那条“转发自用户”的消息 -> 发送文字，即可自动回对方。\n"
    )

async def bind(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("用法：/bind US")
        return

    market = context.args[0].upper().strip()
    if market not in VALID_MARKETS:
        await update.message.reply_text("国家代码仅支持：US UK DE FR IT ES CA JP")
        return

    chat_id = update.effective_chat.id
    thread_id = getattr(update.message, "message_thread_id", None)
    if thread_id is None:
        await update.message.reply_text("请在对应国家的话题里发送 /bind，例如在 US 话题里发送 /bind US")
        return

    s = load_state()
    s.setdefault("thread_map", {})
    s["thread_map"].setdefault(str(chat_id), {})
    s["thread_map"][str(chat_id)][market] = int(thread_id)
    save_state(s)

    await update.message.reply_text(f"已绑定：{market} -> thread_id={thread_id}\nchat_id={chat_id}")

async def show_map(update: Update, context: ContextTypes.DEFAULT_TYPE):
    s = load_state()
    m = s.get("thread_map", {})
    await update.message.reply_text(json.dumps(m, ensure_ascii=False, indent=2)[:3500])


# =========================
# 发送消息的安全封装
# =========================
async def safe_send(bot, chat_id: int, text: str) -> bool:
    try:
        await bot.send_message(chat_id=chat_id, text=text)
        return True
    except Exception:
        return False


# =========================
# 用户私聊 -> 转发给管理员
# =========================
async def forward_private_to_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat:
        return

    # 只处理“用户私聊机器人”
    if update.effective_chat.type != ChatType.PRIVATE:
        return

    user = update.effective_user
    uid = user.id if user else 0
    uname = f"@{user.username}" if user and user.username else "-"
    name = (user.full_name if user else "-").strip()

    # 没设置管理员，就只自动回复
    if ADMIN_ID <= 0:
        await update.message.reply_text(AUTO_REPLY_TEXT)
        return

    # 1) 先用 Telegram 原生 forward（你要的“转发自用户”样式就在这里）
    try:
        await context.bot.forward_message(
            chat_id=ADMIN_ID,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id,
        )
    except Exception:
        # forward 失败也继续走
        pass

    # 2) 再给管理员发一张“信息卡片”（确保你永远拿得到 user_id，用于 /reply）
    card = (
        "New DM\n"
        f"Name: {name}\n"
        f"Username: {uname}\n"
        f"UserID: {uid}\n\n"
        f"用法：/reply {uid} 你的回复内容\n"
        f"快捷：/r 你的回复内容（回复最近一个用户）\n"
        f"或：直接 Reply（回复）上面那条“转发自用户”的消息 -> 输入文字发送\n"
    )
    await context.bot.send_message(chat_id=ADMIN_ID, text=card)

    # 3) 记录“最近一个用户”，支持 /r
    s = load_state()
    s["last_uid"] = uid
    save_state(s)

    # 4) 对用户自动回复
    await update.message.reply_text(AUTO_REPLY_TEXT)


# =========================
# 管理员命令：/reply /r
# =========================
async def reply_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    uid = update.effective_user.id if update.effective_user else 0
    if not is_admin(uid):
        await update.message.reply_text("无权限。")
        return

    if len(context.args) < 2:
        await update.message.reply_text("用法：/reply <user_id> <text>")
        return

    to_user = int(context.args[0])
    text = " ".join(context.args[1:])
    ok = await safe_send(context.bot, to_user, text)
    await update.message.reply_text("已发送。" if ok else "发送失败：对方可能未 /start 机器人或已屏蔽机器人。")

async def reply_last_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    uid = update.effective_user.id if update.effective_user else 0
    if not is_admin(uid):
        await update.message.reply_text("无权限。")
        return

    s = load_state()
    last_uid = int(s.get("last_uid", 0) or 0)
    if last_uid <= 0:
        await update.message.reply_text("没有最近用户。请用 /reply <user_id> <text>。")
        return

    if len(context.args) < 1:
        await update.message.reply_text("用法：/r <text>")
        return

    text = " ".join(context.args)
    ok = await safe_send(context.bot, last_uid, text)
    await update.message.reply_text("已发送。" if ok else "发送失败：对方可能未 /start 机器人或已屏蔽机器人。")


# =========================
# 管理员：直接 Reply “转发自用户”消息即可回复
# =========================
def extract_uid_from_replied_message(replied_msg) -> Optional[int]:
    """
    你 Reply 的那条消息可能是：
    A) Telegram 原生转发消息（带 forward_origin / forward_from）
    B) 机器人发的 New DM 卡片（文本里有 UserID: xxxx）
    """
    # A1: 新版字段 forward_origin.sender_user.id
    try:
        origin = getattr(replied_msg, "forward_origin", None)
        sender_user = getattr(origin, "sender_user", None) if origin else None
        if sender_user and getattr(sender_user, "id", None):
            return int(sender_user.id)
    except Exception:
        pass

    # A2: 旧字段 forward_from.id
    try:
        fwd = getattr(replied_msg, "forward_from", None)
        if fwd and getattr(fwd, "id", None):
            return int(fwd.id)
    except Exception:
        pass

    # B: 从卡片文字解析 UserID
    try:
        body = (replied_msg.text or "") + "\n" + (replied_msg.caption or "")
        m = re.search(r"UserID:\s*(\d+)", body)
        if m:
            return int(m.group(1))
    except Exception:
        pass

    return None

async def admin_reply_by_replying(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    管理员在私聊里：
    - Reply（回复）“转发自用户”的那条消息
    - 输入文字
    机器人自动识别 user_id 并发回给对方
    """
    if not update.message or not update.effective_chat:
        return
    if update.effective_chat.type != ChatType.PRIVATE:
        return

    uid = update.effective_user.id if update.effective_user else 0
    if not is_admin(uid):
        return

    text = (update.message.text or "").strip()
    if not text or text.startswith("/"):
        return

    replied = update.message.reply_to_message
    if not replied:
        return

    to_user = extract_uid_from_replied_message(replied)
    if not to_user:
        await update.message.reply_text("我没能识别原用户ID。请用 /reply <user_id> <text> 或 /r <text>。")
        return

    ok = await safe_send(context.bot, to_user, text)
    await update.message.reply_text("已发送。" if ok else "发送失败：对方可能未 /start 机器人或已屏蔽机器人。")


# =========================
# Webhook Server（Render）
# =========================
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

    tg_app = Application.builder().token(TOKEN).build()

    # commands
    tg_app.add_handler(CommandHandler("start", start))
    tg_app.add_handler(CommandHandler("bind", bind))
    tg_app.add_handler(CommandHandler("map", show_map))
    tg_app.add_handler(CommandHandler("reply", reply_cmd))
    tg_app.add_handler(CommandHandler("r", reply_last_cmd))

    # handlers
    # 1) 管理员“Reply 一条消息 -> 自动回复”
    tg_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, admin_reply_by_replying), group=0)
    # 2) 用户私聊消息 -> 转发给管理员
    tg_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, forward_private_to_admin), group=1)

    # Render 用 webhook，本地没 PUBLIC_URL 用 polling
    if PUBLIC_URL:
        asyncio.run(run_webhook_server(tg_app))
    else:
        tg_app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
