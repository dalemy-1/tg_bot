import os
import json
import asyncio
from pathlib import Path

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

# ====== ENV ======
TOKEN = (os.getenv("TG_BOT_TOKEN") or "").strip()
ADMIN_ID = int(os.getenv("TG_ADMIN_ID", "0") or "0")
AUTO_REPLY_TEXT = os.getenv("AUTO_REPLY_TEXT", "已收到，请联系 @Dalemy").strip()

BASE_DIR = Path(__file__).resolve().parent
MAP_FILE = BASE_DIR / "thread_map.json"

VALID_MARKETS = {"US", "UK", "DE", "FR", "IT", "ES", "CA", "JP"}

PORT = int(os.getenv("PORT", "10000"))
PUBLIC_URL = (os.getenv("RENDER_EXTERNAL_URL") or os.getenv("PUBLIC_URL") or "").strip().rstrip("/")
WEBHOOK_SECRET = (os.getenv("WEBHOOK_SECRET") or "").strip()
HEALTH_PATH = "/healthz"


def load_map() -> dict:
    if MAP_FILE.exists():
        try:
            return json.loads(MAP_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def save_map(m: dict) -> None:
    MAP_FILE.write_text(json.dumps(m, ensure_ascii=False, indent=2), encoding="utf-8")


def is_admin(uid: int) -> bool:
    return ADMIN_ID > 0 and uid == ADMIN_ID


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Bot is online.\n\n"
        "群内话题绑定：/bind <US|UK|DE|FR|IT|ES|CA|JP>\n"
        "查看映射：/map\n"
        "管理员命令回复：/reply <user_id> <text>\n"
        "管理员也可以：直接【回复】转发来的消息，机器人会回给原用户。\n"
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

    m = load_map()
    m.setdefault(str(chat_id), {})
    m[str(chat_id)][market] = int(thread_id)
    save_map(m)

    await update.message.reply_text(f"已绑定：{market} -> thread_id={thread_id}\nchat_id={chat_id}")


async def show_map(update: Update, context: ContextTypes.DEFAULT_TYPE):
    m = load_map()
    await update.message.reply_text(json.dumps(m, ensure_ascii=False, indent=2)[:3500])


def _extract_forwarded_user_id(msg) -> int | None:
    """
    兼容 Telegram 不同转发结构：
    - 旧：msg.forward_from
    - 新：msg.forward_origin.sender_user
    """
    try:
        ff = getattr(msg, "forward_from", None)
        if ff:
            return int(ff.id)
    except Exception:
        pass

    try:
        fo = getattr(msg, "forward_origin", None)
        if fo:
            sender_user = getattr(fo, "sender_user", None)
            if sender_user:
                return int(sender_user.id)
    except Exception:
        pass

    return None


async def admin_reply_to_forwarded(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    管理员在私聊里对“转发自用户”的消息点【回复】：
    机器人把这条回复发回给原用户。
    """
    if not update.message:
        return

    # 必须是管理员、必须在私聊
    if update.effective_chat.type != ChatType.PRIVATE:
        return
    uid = update.effective_user.id if update.effective_user else 0
    if not is_admin(uid):
        return

    # 必须是“回复某条消息”
    replied = update.message.reply_to_message
    if not replied:
        return

    target_id = _extract_forwarded_user_id(replied)
    if not target_id:
        await update.message.reply_text("这条被回复的消息不是从用户转发来的，无法自动识别对方ID。")
        return

    text = (update.message.text or "").strip()
    if not text:
        await update.message.reply_text("请输入要回复的文字。")
        return

    try:
        await context.bot.send_message(chat_id=target_id, text=text)
        await update.message.reply_text("已回复给对方。")
    except Exception as e:
        await update.message.reply_text(f"发送失败：{e!s}")


async def forward_private_to_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    普通用户私聊机器人 -> 转发给管理员，并自动回复用户
    """
    if not update.message:
        return

    # 只处理私聊
    if not (update.effective_chat and update.effective_chat.type == "private"):
        return

    # 没设置管理员：只自动回复
    if ADMIN_ID <= 0:
        await update.message.reply_text(AUTO_REPLY_TEXT)
        return

    # 管理员自己发的消息不要再转发给自己
    from_uid = update.effective_user.id if update.effective_user else 0
    if is_admin(from_uid):
        return

    try:
        await context.bot.forward_message(
            chat_id=ADMIN_ID,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id,
        )
    except Exception:
        pass

    await update.message.reply_text(AUTO_REPLY_TEXT)


async def reply_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /reply <user_id> <text>
    """
    if not update.message:
        return

    uid = update.effective_user.id if update.effective_user else 0
    if not is_admin(uid):
        await update.message.reply_text("无权限。")
        return

    if len(context.args) < 2:
        await update.message.reply_text("用法：/reply <user_id> <text>")
        return

    try:
        to_user = int(context.args[0])
    except Exception:
        await update.message.reply_text("用法：/reply <user_id> <text>（user_id 必须是数字）")
        return

    text = " ".join(context.args[1:]).strip()
    if not text:
        await update.message.reply_text("请输入回复内容。")
        return

    try:
        await context.bot.send_message(chat_id=to_user, text=text)
        await update.message.reply_text("已发送。")
    except Exception as e:
        await update.message.reply_text(f"发送失败：{e!s}")


async def run_webhook_server(tg_app: Application):
    """
    Render Web Service:
    - GET /healthz 健康检查
    - POST /<WEBHOOK_SECRET> 接收 Telegram webhook
    """
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

    tg_app.add_handler(CommandHandler("start", start))
    tg_app.add_handler(CommandHandler("bind", bind))
    tg_app.add_handler(CommandHandler("map", show_map))
    tg_app.add_handler(CommandHandler("reply", reply_cmd))

    # 顺序很关键：先处理“管理员回复转发消息”，再处理“普通用户私聊转发”
    tg_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, admin_reply_to_forwarded), group=0)
    tg_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, forward_private_to_admin), group=1)

    if PUBLIC_URL:
        asyncio.run(run_webhook_server(tg_app))
    else:
        tg_app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
