import os
import json
from pathlib import Path
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

TOKEN = (os.getenv("TG_BOT_TOKEN") or "").strip()
ADMIN_ID = int(os.getenv("ADMIN_ID", "0") or "0")

# Render 上你需要设置：
# WEBHOOK_BASE_URL = https://xxx.onrender.com
# WEBHOOK_PATH = hook-xxxx（自定义）
WEBHOOK_BASE_URL = (os.getenv("WEBHOOK_BASE_URL") or "").strip().rstrip("/")
WEBHOOK_PATH = (os.getenv("WEBHOOK_PATH") or "").strip().lstrip("/") or "webhook"

PORT = int(os.getenv("PORT", "10000"))

BASE_DIR = Path(__file__).resolve().parent
MAP_FILE = BASE_DIR / "thread_map.json"

VALID_MARKETS = {"US", "UK", "DE", "FR", "IT", "ES", "CA", "JP"}

AUTO_REPLY_TEXT = "已收到，请联系管理员。"

def _is_admin(user_id: int) -> bool:
    return ADMIN_ID > 0 and user_id == ADMIN_ID

def load_map() -> dict:
    if MAP_FILE.exists():
        return json.loads(MAP_FILE.read_text(encoding="utf-8"))
    return {}

def save_map(m: dict) -> None:
    # 注意：Render Free 文件系统不保证持久化；重启/重新部署后可能丢失。
    MAP_FILE.write_text(json.dumps(m, ensure_ascii=False, indent=2), encoding="utf-8")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Bot is online.\n"
        "群内绑定：/bind <US|UK|DE|FR|IT|ES|CA|JP>\n"
        "查看映射：/map\n"
    )

async def bind(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    # 建议只允许管理员绑定，避免群里被乱改
    uid = update.effective_user.id if update.effective_user else 0
    if ADMIN_ID and not _is_admin(uid):
        await update.message.reply_text("无权限。")
        return

    if not context.args:
        await update.message.reply_text("用法：/bind US")
        return

    market = (context.args[0] or "").upper().strip()
    if market not in VALID_MARKETS:
        await update.message.reply_text(f"不支持的市场：{market}")
        return

    chat_id = update.effective_chat.id
    thread_id = getattr(update.message, "message_thread_id", None)
    if thread_id is None:
        await update.message.reply_text("请在对应话题(Topic)里发送 /bind，例如在 US 话题里 /bind US")
        return

    m = load_map()
    m.setdefault(str(chat_id), {})
    m[str(chat_id)][market] = int(thread_id)
    save_map(m)

    await update.message.reply_text(f"已绑定：chat_id={chat_id} market={market} -> thread_id={thread_id}")

async def show_map(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    uid = update.effective_user.id if update.effective_user else 0
    if ADMIN_ID and not _is_admin(uid):
        await update.message.reply_text("无权限。")
        return
    m = load_map()
    await update.message.reply_text(json.dumps(m, ensure_ascii=False, indent=2)[:3500])

async def forward_private_to_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # 私聊消息转发给管理员（可选）
    if not update.message:
        return
    if ADMIN_ID <= 0:
        return
    if update.effective_chat and update.effective_chat.type == "private":
        try:
            await context.bot.forward_message(
                chat_id=ADMIN_ID,
                from_chat_id=update.effective_chat.id,
                message_id=update.message.message_id
            )
        except Exception:
            pass
        await update.message.reply_text(AUTO_REPLY_TEXT)

def main():
    if not TOKEN:
        raise SystemExit("Missing TG_BOT_TOKEN")

    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("bind", bind))
    app.add_handler(CommandHandler("map", show_map))
    app.add_handler(MessageHandler(filters.ALL, forward_private_to_admin))

    # Render 上用 webhook（必须监听 PORT）
    if WEBHOOK_BASE_URL:
        webhook_url = f"{WEBHOOK_BASE_URL}/{WEBHOOK_PATH}"
        print("Starting webhook on port", PORT, "url_path=/" + WEBHOOK_PATH)
        app.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=WEBHOOK_PATH,
            webhook_url=webhook_url,
            drop_pending_updates=True,
        )
    else:
        # 本地调试用轮询
        print("Starting polling (local dev)")
        app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
