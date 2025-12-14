import os
import json
import asyncio
from pathlib import Path
from typing import Optional

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
ADMIN_ID = int(os.getenv("TG_ADMIN_ID", "0") or "0")  # 你的个人 Telegram 数字ID
AUTO_REPLY_TEXT = os.getenv("AUTO_REPLY_TEXT", "已收到，请联系 @Dalemy").strip()

PORT = int(os.getenv("PORT", "10000"))
PUBLIC_URL = (os.getenv("RENDER_EXTERNAL_URL") or os.getenv("PUBLIC_URL") or "").strip().rstrip("/")
WEBHOOK_SECRET = (os.getenv("WEBHOOK_SECRET") or "").strip()
HEALTH_PATH = "/healthz"

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

MAP_FILE = DATA_DIR / "thread_map.json"
STATE_FILE = DATA_DIR / "state.json"

VALID_MARKETS = {"US", "UK", "DE", "FR", "IT", "ES", "CA", "JP"}


# ====== persistence ======
def _read_json(path: Path, default):
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return default


def _write_json(path: Path, obj) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def load_map() -> dict:
    return _read_json(MAP_FILE, {})


def save_map(m: dict) -> None:
    _write_json(MAP_FILE, m)


def load_state() -> dict:
    # state 示例：{"last_private_user_id": 123456789}
    return _read_json(STATE_FILE, {"last_private_user_id": 0})


def save_state(s: dict) -> None:
    _write_json(STATE_FILE, s)


def is_admin(uid: int) -> bool:
    return ADMIN_ID > 0 and uid == ADMIN_ID


# ====== helpers ======
def extract_forwarded_user_id(msg) -> Optional[int]:
    """
    从“被转发到管理员”的那条消息中，尽量解析原始用户ID
    兼容不同 Telegram/库版本字段
    """
    if not msg:
        return None

    # 老字段（部分情况下可用）
    forward_from = getattr(msg, "forward_from", None)
    if forward_from and getattr(forward_from, "id", None):
        return int(forward_from.id)

    # 新字段：forward_origin（更常见）
    forward_origin = getattr(msg, "forward_origin", None)
    if forward_origin:
        sender_user = getattr(forward_origin, "sender_user", None)
        if sender_user and getattr(sender_user, "id", None):
            return int(sender_user.id)

    return None


async def safe_send(bot, chat_id: int, text: str, reply_to_message_id: Optional[int] = None) -> bool:
    try:
        await bot.send_message(chat_id=chat_id, text=text, reply_to_message_id=reply_to_message_id)
        return True
    except Exception:
        return False


# ====== commands ======
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Bot is online.\n\n"
        "群内话题绑定：/bind <US|UK|DE|FR|IT|ES|CA|JP>\n"
        "查看映射：/map\n\n"
        "管理员回复方式：\n"
        "1) 直接回复(Reply)管理员私聊里那条转发消息（推荐）\n"
        "2) /reply <user_id> <text>\n"
        "3) /r <text>  (回复最近一个私聊用户)\n"
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
    """
    /r <text> 回复最近一个私聊用户
    """
    if not update.message:
        return

    uid = update.effective_user.id if update.effective_user else 0
    if not is_admin(uid):
        await update.message.reply_text("无权限。")
        return

    if len(context.args) < 1:
        await update.message.reply_text("用法：/r <text>  （回复最近一个私聊用户）")
        return

    s = load_state()
    to_user = int(s.get("last_private_user_id", 0) or 0)
    if to_user <= 0:
        await update.message.reply_text("暂无“最近私聊用户”。先让用户给机器人发一条私聊消息。")
        return

    text = " ".join(context.args)
    ok = await safe_send(context.bot, to_user, text)
    await update.message.reply_text("已发送。" if ok else "发送失败：对方可能未 /start 机器人或已屏蔽机器人。")


# ====== core logic ======
async def forward_private_to_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    普通用户私聊机器人：
    - 转发到管理员
    - 记录 last_private_user_id，方便 /r
    - 自动回复用户 AUTO_REPLY_TEXT
    """
    if not update.message or not update.effective_chat:
        return

    if update.effective_chat.type != "private":
        return

    from_user = update.effective_user
    if not from_user:
        return

    # 管理员自己私聊机器人：不走转发逻辑
    if is_admin(from_user.id):
        return

    if ADMIN_ID <= 0:
        await update.message.reply_text(AUTO_REPLY_TEXT)
        return

    # 记录最近私聊用户
    s = load_state()
    s["last_private_user_id"] = int(from_user.id)
    save_state(s)

    # 转发给管理员
    try:
        await context.bot.forward_message(
            chat_id=ADMIN_ID,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id,
        )
    except Exception:
        pass

    # 给管理员发一条提示（包含 user_id）
    hint = (
        "New DM\n"
        f"Name: {from_user.full_name}\n"
        f"Username: @{from_user.username}\n" if from_user.username else f"Name: {from_user.full_name}\n"
    )
    hint += f"UserID: {from_user.id}\n\n"
    hint += f"用法：/reply {from_user.id} 你的回复内容\n"
    hint += f"快捷：/r 你的回复内容（回复最近一个用户）\n"
    hint += "或：直接 Reply（回复）上面那条转发消息，我会自动回给对方。"

    await safe_send(context.bot, ADMIN_ID, hint)

    # 回复用户
    await update.message.reply_text(AUTO_REPLY_TEXT)


async def admin_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    管理员私聊机器人时：
    - 如果管理员“回复(Reply)了一条转发消息”，自动回给原用户
    """
    if not update.message or not update.effective_chat:
        return

    if update.effective_chat.type != "private":
        return

    uid = update.effective_user.id if update.effective_user else 0
    if not is_admin(uid):
        return

    # 只处理“普通文字”（命令交给 CommandHandler）
    text = (update.message.text or "").strip()
    if not text or text.startswith("/"):
        return

    # 必须是“回复(Reply)某条消息”
    replied = update.message.reply_to_message
    if not replied:
        return

    to_user = extract_forwarded_user_id(replied)
    if not to_user:
        await update.message.reply_text("我没能识别原用户ID。请用 /reply <user_id> <text> 或 /r <text>。")
        return

    ok = await safe_send(context.bot, to_user, text)
    await update.message.reply_text("已发送。" if ok else "发送失败：对方可能未 /start 机器人或已屏蔽机器人。")


async def run_webhook_server(tg_app: Application):
    """
    Render Web Service:
    - GET /healthz 健康检查
    - POST /<WEBHOOK_SECRET> Telegram webhook
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
    if ADMIN_ID <= 0:
        print("[warn] TG_ADMIN_ID not set; reply feature will be disabled.")

    tg_app = Application.builder().token(TOKEN).build()

    tg_app.add_handler(CommandHandler("start", start))
    tg_app.add_handler(CommandHandler("bind", bind))
    tg_app.add_handler(CommandHandler("map", show_map))
    tg_app.add_handler(CommandHandler("reply", reply_cmd))
    tg_app.add_handler(CommandHandler("r", reply_last_cmd))

    # 用户私聊 -> 转发给管理员
    tg_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, forward_private_to_admin))

    # 管理员私聊里“回复转发消息” -> 自动回给原用户
    tg_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, admin_text_handler), group=1)

    # Render 上用 webhook；本地没 PUBLIC_URL 就用 polling
    if PUBLIC_URL:
        asyncio.run(run_webhook_server(tg_app))
    else:
        tg_app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
