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

import os
import hashlib

def _pick_webhook_secret(token: str) -> str:
    # 固定且不太长，作为 url_path
    return os.getenv("WEBHOOK_SECRET", hashlib.sha1(token.encode("utf-8")).hexdigest()[:20])

def main():
    # ...你原来的 Application 构建、handler 注册逻辑保持不变...
    # application = Application.builder().token(TOKEN).build()
    # application.add_handler(...)

    token = (os.getenv("TG_BOT_TOKEN") or "").strip()  # 或你原来的 TOKEN 变量
    if not token:
        raise SystemExit("Missing TG_BOT_TOKEN")

    # Render 会自动注入这些变量：RENDER / RENDER_EXTERNAL_URL / PORT
    render_external_url = (os.getenv("RENDER_EXTERNAL_URL") or "").rstrip("/")
    port = int(os.getenv("PORT", "10000"))

    # 你也可以手动设置 WEBHOOK_URL（优先级更高）
    webhook_base = (os.getenv("WEBHOOK_URL") or render_external_url).rstrip("/")
    use_webhook = bool(webhook_base)

    if use_webhook:
        secret = _pick_webhook_secret(token)
        webhook_url = f"{webhook_base}/{secret}"
        print("[ok] run webhook:", webhook_url)

        application.run_webhook(
            listen="0.0.0.0",
            port=port,
            url_path=secret,          # 注意：这里不要加前导 /
            webhook_url=webhook_url,  # 必须是完整可访问 URL
            drop_pending_updates=True
        )
    else:
        print("[ok] run polling")
        application.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()


