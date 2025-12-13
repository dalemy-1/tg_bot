import os
import json
from pathlib import Path

from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters


# =========================
# 环境变量配置
# =========================
TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()
ADMIN_ID = int(os.getenv("TG_ADMIN_ID", "0"))  # 你的数字ID（用 @userinfobot 获取）

BASE_DIR = Path(__file__).resolve().parent
MAP_FILE = BASE_DIR / "thread_map.json"

VALID_MARKETS = {"US", "UK", "DE", "FR", "IT", "ES", "CA", "JP"}


# =========================
# 自动回复设置（方案 C）
# =========================
AUTO_REPLY_TEXT = "Hello, thank you for contacting us.。\nPlease contact the administrator.：@Adalemy"

# True：同一个用户只自动回复一次（推荐，避免刷屏）
# False：用户每发一条都自动回复一次
AUTO_REPLY_ONCE_PER_USER = False

# 用于记录已自动回复过的用户（内存级，重启会清空；如需持久化可再升级为文件）
_replied_users = set()


# =========================
# 工具函数（群话题映射）
# =========================
def load_map() -> dict:
    if MAP_FILE.exists():
        return json.loads(MAP_FILE.read_text(encoding="utf-8"))
    return {}

def save_map(m: dict) -> None:
    MAP_FILE.write_text(json.dumps(m, ensure_ascii=False, indent=2), encoding="utf-8")


# =========================
# 基础命令
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Bot is online ✅\n"
        "群内命令：/bind <US|UK|DE|FR|IT|ES|CA|JP>  /map  /send <market> <text>\n"
        "私聊：直接发消息，我会自动回复并转发给管理员。"
    )

async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("pong")

async def whoami(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await update.message.reply_text(
        f"Your ID: {user.id}\nUsername: @{user.username}\nName: {user.full_name}"
    )


# =========================
# 群内：绑定话题 / 查看映射 / 指定话题发送
# =========================
async def bind(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("用法：/bind US （请在对应国家的话题里发送）")
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

async def send_to_market(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2:
        await update.message.reply_text("用法：/send US 你好")
        return

    market = context.args[0].upper().strip()
    if market not in VALID_MARKETS:
        await update.message.reply_text("国家代码仅支持：US UK DE FR IT ES CA JP")
        return

    text = " ".join(context.args[1:]).strip()

    chat_id = update.effective_chat.id
    m = load_map().get(str(chat_id), {})
    thread_id = m.get(market)

    if not thread_id:
        await update.message.reply_text(f"{market} 尚未绑定。请在 {market} 话题里发送 /bind {market}")
        return

    await context.bot.send_message(chat_id=chat_id, message_thread_id=int(thread_id), text=text)
    await update.message.reply_text(f"已发送到 {market} 话题 ✅")


# =========================
# 私聊：自动回复 + 转发给管理员（方案 C）
# =========================
async def forward_private_to_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # 只处理私聊
    if update.effective_chat.type != "private":
        return

    user = update.effective_user
    uid = user.id

    # 1) 自动回复（一次/每次 可配置）
    if (not AUTO_REPLY_ONCE_PER_USER) or (uid not in _replied_users):
        try:
            await update.message.reply_text(AUTO_REPLY_TEXT)
        except Exception:
            pass
        _replied_users.add(uid)

    # 2) 转发给管理员（可选但你选了 C，建议必须配置）
    if not ADMIN_ID:
        # 管理员ID没配就无法转发，但自动回复仍然有效
        return

    # 2.1 转发原消息（保留媒体）
    try:
        await update.message.forward(chat_id=ADMIN_ID)
    except Exception as e:
        await context.bot.send_message(chat_id=ADMIN_ID, text=f"Forward failed: {e}")

    # 2.2 发送用户信息，便于管理员 /reply
    meta = (
        f"📩 New DM\n"
        f"Name: {user.full_name}\n"
        f"Username: @{user.username}\n"
        f"UserID: {uid}\n"
        f"\n用法：/reply {uid} 你的回复内容\n"
        f"快捷：/r 你的回复内容（回复最近一个用户）"
    )
    await context.bot.send_message(chat_id=ADMIN_ID, text=meta)

    # 记录最近一个私聊用户（用于 /r）
    context.bot_data["last_user_id"] = uid


# =========================
# 管理员：回复用户
# =========================
async def reply_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    if len(context.args) < 2:
        await update.message.reply_text("用法：/reply <user_id> <text>")
        return

    user_id = int(context.args[0])
    text = " ".join(context.args[1:]).strip()

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

    # 命令
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("ping", ping))
    app.add_handler(CommandHandler("whoami", whoami))

    app.add_handler(CommandHandler("bind", bind))
    app.add_handler(CommandHandler("map", show_map))
    app.add_handler(CommandHandler("send", send_to_market))

    app.add_handler(CommandHandler("reply", reply_cmd))
    app.add_handler(CommandHandler("r", reply_last_cmd))

    # 私聊消息：自动回复 + 转发给管理员（放最后避免影响命令）
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.ALL, forward_private_to_admin))

    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
