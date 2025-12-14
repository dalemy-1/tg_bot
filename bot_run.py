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
# ENV
# =========================
TOKEN = (os.getenv("TG_BOT_TOKEN") or "").strip()
ADMIN_ID = int(os.getenv("TG_ADMIN_ID", "0") or "0")
AUTO_REPLY_TEXT = (os.getenv("AUTO_REPLY_TEXT") or "已收到，请联系 @Dalemy").strip()

PORT = int(os.getenv("PORT", "10000"))
PUBLIC_URL = (os.getenv("RENDER_EXTERNAL_URL") or os.getenv("PUBLIC_URL") or "").strip().rstrip("/")
WEBHOOK_SECRET = (os.getenv("WEBHOOK_SECRET") or "").strip()
HEALTH_PATH = "/healthz"

BASE_DIR = Path(__file__).resolve().parent
STATE_FILE = BASE_DIR / "state.json"

VALID_MARKETS = {"US", "UK", "DE", "FR", "IT", "ES", "CA", "JP"}

# fwd message_id -> user_id 记录上限
FWD_MAP_LIMIT = 3000


# =========================
# STATE
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
# UTIL
# =========================
async def safe_send(bot, chat_id: int, text: str) -> bool:
    try:
        await bot.send_message(chat_id=chat_id, text=text)
        return True
    except Exception:
        return False

def remember_forward(admin_fwd_msg_id: int, user_id: int) -> None:
    s = load_state()
    fm = s.get("fwd_map", {})
    fm[str(admin_fwd_msg_id)] = int(user_id)

    # 控制大小：超过就删最早的一批（这里用简单做法：按 key 插入顺序截断）
    if len(fm) > FWD_MAP_LIMIT:
        # 保留后 2500 条
        items = list(fm.items())[-2500:]
        fm = dict(items)

    s["fwd_map"] = fm
    save_state(s)

def lookup_forward(replied_message_id: int) -> Optional[int]:
    s = load_state()
    fm = s.get("fwd_map", {})
    v = fm.get(str(replied_message_id))
    return int(v) if v else None


# =========================
# COMMANDS
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Bot is online.\n\n"
        "群内话题绑定：/bind <US|UK|DE|FR|IT|ES|CA|JP>\n"
        "查看映射：/map\n\n"
        "管理员回复：\n"
        "  /reply <user_id> <text>\n"
        "  /r <text>  （回复最近一个私聊用户）\n\n"
        "也支持：直接 Reply 那条“转发自用户”的消息 -> 输入文字发送（已修复）。\n"
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
# ADMIN: Reply 转发消息 -> 自动回用户（稳定版）
# =========================
def extract_uid_from_replied_message(replied_msg) -> Optional[int]:
    # 1) 优先：查“转发消息ID -> 原用户ID”的映射（最稳）
    uid = lookup_forward(replied_msg.message_id)
    if uid:
        return uid

    # 2) 兜底：解析 forward_origin / forward_from（可能失败）
    try:
        origin = getattr(replied_msg, "forward_origin", None)
        sender_user = getattr(origin, "sender_user", None) if origin else None
        if sender_user and getattr(sender_user, "id", None):
            return int(sender_user.id)
    except Exception:
        pass

    try:
        fwd = getattr(replied_msg, "forward_from", None)
        if fwd and getattr(fwd, "id", None):
            return int(fwd.id)
    except Exception:
        pass

    # 3) 兜底：从卡片里解析 UserID
    try:
        body = (replied_msg.text or "") + "\n" + (replied_msg.caption or "")
        m = re.search(r"UserID:\s*(\d+)", body)
        if m:
            return int(m.group(1))
    except Exception:
        pass

    return None

async def admin_reply_by_replying(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat:
        return
    if update.effective_chat.type != ChatType.PRIVATE:
        return

    uid = update.effective_user.id if update.effective_user else 0
    if not is_admin(uid):
        return

    # 管理员发普通文字但没有 reply，就不处理（避免误触）
    text = (update.message.text or "").strip()
    if not text or text.startswith("/"):
        return
    if not update.message.reply_to_message:
        return

    replied = update.message.reply_to_message
    to_user = extract_uid_from_replied_message(replied)
    if not to_user:
        await update.message.reply_text("我没能识别原用户ID。请用 /reply <user_id> <text> 或 /r <text>。")
        return

    ok = await safe_send(context.bot, to_user, text)
    await update.message.reply_text("已发送。" if ok else "发送失败：对方可能未 /start 机器人或已屏蔽机器人。")


# =========================
# USER DM -> forward to admin
# =========================
async def forward_private_to_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat:
        return
    if update.effective_chat.type != ChatType.PRIVATE:
        return

    user = update.effective_user
    uid = user.id if user else 0

    # 管理员自己发给机器人，不当成用户消息
    if is_admin(uid):
        return

    if ADMIN_ID <= 0:
        # 没设置管理员：仍然回复
        await update.message.reply_text(AUTO_REPLY_TEXT)
        return

    uname = f"@{user.username}" if user and user.username else "-"
    name = (user.full_name if user else "-").strip()

    # 1) 先把用户消息（无论什么类型）转发给管理员
    fwd_msg = None
    try:
        fwd_msg = await update.message.forward(chat_id=ADMIN_ID)
    except Exception:
        try:
            # 兜底
            fwd_msg = await context.bot.forward_message(
                chat_id=ADMIN_ID,
                from_chat_id=update.effective_chat.id,
                message_id=update.message.message_id,
            )
        except Exception:
            fwd_msg = None

    if fwd_msg:
        remember_forward(fwd_msg.message_id, uid)

    # 2) 发卡片（告诉管理员怎么回）
    msg_type = "text"
    if update.message.photo:
        msg_type = "photo"
    elif update.message.sticker:
        msg_type = "sticker"
    elif update.message.voice:
        msg_type = "voice"
    elif update.message.video:
        msg_type = "video"
    elif update.message.document:
        msg_type = "document"
    elif update.message.animation:
        msg_type = "animation"
    elif update.message.audio:
        msg_type = "audio"
    elif update.message.video_note:
        msg_type = "video_note"
    elif update.message.contact:
        msg_type = "contact"
    elif update.message.location:
        msg_type = "location"

    card = (
        "New DM\n"
        f"Type: {msg_type}\n"
        f"Name: {name}\n"
        f"Username: {uname}\n"
        f"UserID: {uid}\n\n"
        f"文字回复：/reply {uid} 你的回复内容\n"
        f"快捷回复：/r 你的回复内容（回复最近用户）\n"
        f"媒体/原样回复：直接 Reply 上面那条“转发自用户”的消息（可发文字/图片/文件/语音/贴纸等）\n"
    )
    await context.bot.send_message(chat_id=ADMIN_ID, text=card)

    # 3) 更新 last_uid（只被真实用户更新）
    s = load_state()
    s["last_uid"] = uid
    save_state(s)

    # 4) 自动回复用户
    await update.message.reply_text(AUTO_REPLY_TEXT)



# =========================
# WEBHOOK (Render)
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

    tg_app.add_handler(CommandHandler("start", start))
    tg_app.add_handler(CommandHandler("bind", bind))
    tg_app.add_handler(CommandHandler("map", show_map))
    tg_app.add_handler(CommandHandler("reply", reply_cmd))
    tg_app.add_handler(CommandHandler("r", reply_last_cmd))

    # 先处理管理员 reply 自动回复
    tg_app.add_handler(MessageHandler(~filters.COMMAND, admin_reply_by_replying), group=0)
    tg_app.add_handler(MessageHandler(~filters.COMMAND, forward_private_to_admin), group=1)


    if PUBLIC_URL:
        asyncio.run(run_webhook_server(tg_app))
    else:
        tg_app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()

