import os
import json
import asyncio
from pathlib import Path

from aiohttp import web
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# ====== ENV ======
TOKEN = (os.getenv("TG_BOT_TOKEN") or "").strip()
ADMIN_ID = int(os.getenv("TG_ADMIN_ID", "0") or "0")  # 需要的话在 Render 里加
AUTO_REPLY_TEXT = os.getenv("AUTO_REPLY_TEXT", "已收到，请联系 @Dalemy").strip()

BASE_DIR = Path(__file__).resolve().parent
MAP_FILE = BASE_DIR / "thread_map.json"

VALID_MARKETS = {"US", "UK", "DE", "FR", "IT", "ES", "CA", "JP"}

# Render 会提供 PORT；PUBLIC_URL 你需要在 Render 环境变量里手动填
PORT = int(os.getenv("PORT", "10000"))
PUBLIC_URL = (os.getenv("RENDER_EXTERNAL_URL") or os.getenv("PUBLIC_URL") or "").strip().rstrip("/")

# 建议你在 Render 环境变量里设置一个随机串（不要用 TG token）
WEBHOOK_SECRET = (os.getenv("WEBHOOK_SECRET") or "").strip()
HEALTH_PATH = "/healthz"


def load_map() -> dict:
    if MAP_FILE.exists():
        return json.loads(MAP_FILE.read_text(encoding="utf-8"))
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
        "（可选）管理员私聊回复：/reply <user_id> <text>\n"
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


async def forward_private_to_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    if ADMIN_ID <= 0:
        # 没设置管理员就只做自动回复
        await update.message.reply_text(AUTO_REPLY_TEXT)
        return

    # 私聊用户消息 -> 转发给管理员
    if update.effective_chat and update.effective_chat.type == "private":
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
    await context.bot.send_message(chat_id=to_user, text=text)
    await update.message.reply_text("已发送。")


async def run_webhook_server(tg_app: Application):
    """
    Render Web Service:
    - 提供 GET /healthz 给 Render 做健康检查
    - 提供 POST /<WEBHOOK_SECRET> 接收 Telegram webhook
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

    # 永久等待
    await asyncio.Event().wait()


def main():
    if not TOKEN:
        raise SystemExit("Missing TG_BOT_TOKEN")

    tg_app = Application.builder().token(TOKEN).build()

    tg_app.add_handler(CommandHandler("start", start))
    tg_app.add_handler(CommandHandler("bind", bind))
    tg_app.add_handler(CommandHandler("map", show_map))
    tg_app.add_handler(CommandHandler("reply", reply_cmd))
    tg_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, forward_private_to_admin))

    # Render 上用 webhook；本地没 PUBLIC_URL 就用 polling
    if PUBLIC_URL:
        asyncio.run(run_webhook_server(tg_app))
    else:
        tg_app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
