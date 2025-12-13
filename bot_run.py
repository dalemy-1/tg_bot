import os
import json
from pathlib import Path
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

TOKEN = (os.getenv("TG_BOT_TOKEN") or "").strip()
ADMIN_ID = int(os.getenv("TG_ADMIN_ID", "0"))

MAP_FILE = Path("thread_map.json")
VALID_MARKETS = {"US", "UK", "DE", "FR", "IT", "ES", "CA", "JP"}

AUTO_REPLY_TEXT = "已收到。请联系 @Adalemy"

def load_map() -> dict:
    if MAP_FILE.exists():
        return json.loads(MAP_FILE.read_text(encoding="utf-8"))
    return {}

def save_map(m: dict) -> None:
    MAP_FILE.write_text(json.dumps(m, ensure_ascii=False, indent=2), encoding="utf-8")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Bot is online ✅\n"
        "群内命令：/bind <US|UK|DE|FR|IT|ES|CA|JP>  /map\n"
        "管理员私聊命令：/reply <user_id> <text>  或 /r <text>（回复最近用户）\n"
        "用户私聊会自动回复并转发给管理员。"
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
        await update.message.reply_text("请在【对应国家的话题】里发送 /bind，例如在 US 话题里发送 /bind US")
        return

    m = load_map()
    m.setdefault(str(chat_id), {})
    m[str(chat_id)][market] = int(thread_id)
    save_map(m)

    await update.message.reply_text(f"已绑定 ✅  {market} -> thread_id={thread_id}\nchat_id={chat_id}")

async def show_map(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    m = load_map().get(str(chat_id), {})
    if not m:
        await update.message.reply_text("当前群还未绑定任何国家话题。请在各话题内发送：/bind US 等。")
        return
    lines = [f"{k} -> {v}" for k, v in sorted(m.items())]
    await update.message.reply_text("当前绑定：\n" + "\n".join(lines))

async def forward_private_to_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # 仅处理用户私聊机器人
    if update.effective_chat.type != "private":
        return

    # 自动回复给用户
    await update.message.reply_text(AUTO_REPLY_TEXT)

    # 没配置管理员就到此为止（但用户仍会收到自动回复）
    if not ADMIN_ID:
        return

    user = update.effective_user

    # 转发原消息（保留媒体/原文）
    try:
        await update.message.forward(chat_id=ADMIN_ID)
    except Exception as e:
        await context.bot.send_message(chat_id=ADMIN_ID, text=f"Forward failed: {e}")

    meta = (
        f"📩 New DM\n"
        f"Name: {user.full_name}\n"
        f"Username: @{user.username}\n"
        f"UserID: {user.id}\n"
        f"Time: {update.message.date}\n\n"
        f"用法：/reply {user.id} 你的回复内容\n"
        f"快捷：/r 你的回复内容（回复最近一个用户）"
    )
    await context.bot.send_message(chat_id=ADMIN_ID, text=meta)

    # 记录最近用户
    context.bot_data["last_user_id"] = user.id

async def reply_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    if len(context.args) < 2:
        await update.message.reply_text("用法：/reply <user_id> <text>")
        return

    user_id = int(context.args[0])
    text = " ".join(context.args[1:])
    await context.bot.send_message(chat_id=user_id, text=text)
    await update.message.reply_text("已发送。")

async def reply_last_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    last_user_id = context.bot_data.get("last_user_id")
    if not last_user_id:
        await update.message.reply_text("暂无最近私聊用户。")
        return

    text = " ".join(context.args).strip()
    if not text:
        await update.message.reply_text("用法：/r <text>")
        return

    await context.bot.send_message(chat_id=int(last_user_id), text=text)
    await update.message.reply_text("已发送（reply last）。")

def main():
    if not TOKEN:
        raise SystemExit("Missing TG_BOT_TOKEN env var.")

    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("bind", bind))
    app.add_handler(CommandHandler("map", show_map))

    # 客服转发与回复
    app.add_handler(CommandHandler("reply", reply_cmd))
    app.add_handler(CommandHandler("r", reply_last_cmd))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.ALL, forward_private_to_admin))

    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
