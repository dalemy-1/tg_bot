import os
import json
import re
import time
import asyncio
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

# ======================
# ENV
# ======================
TOKEN = (os.getenv("TG_BOT_TOKEN") or "").strip()
ADMIN_ID = int(os.getenv("TG_ADMIN_ID", "0") or "0")
AUTO_REPLY_TEXT = (os.getenv("AUTO_REPLY_TEXT") or "Hello, thank you for contacting us.\nPlease contact the administrator.: @Dalemy").strip()

# Render
PORT = int(os.getenv("PORT", "10000"))
PUBLIC_URL = (os.getenv("RENDER_EXTERNAL_URL") or os.getenv("PUBLIC_URL") or "").strip().rstrip("/")
WEBHOOK_SECRET = (os.getenv("WEBHOOK_SECRET") or "").strip()

HEALTH_PATH = "/healthz"

BASE_DIR = Path(__file__).resolve().parent
THREAD_MAP_FILE = BASE_DIR / "thread_map.json"   # 群话题绑定用（可选）
STATE_FILE = BASE_DIR / "admin_state.json"       # 记录 last_uid、msg->uid 映射
DM_LOG_FILE = BASE_DIR / "dm_log.jsonl"          # 简单日志

VALID_MARKETS = {"US", "UK", "DE", "FR", "IT", "ES", "CA", "JP"}

# 控制 msg->uid 映射最大条数，避免无限增长
MAX_MSG_MAP = 5000


# ======================
# Helpers
# ======================
def load_json_file(path: Path, default: Any) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return default


def save_json_file(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def load_thread_map() -> dict:
    return load_json_file(THREAD_MAP_FILE, {})


def save_thread_map(m: dict) -> None:
    save_json_file(THREAD_MAP_FILE, m)


def load_state() -> dict:
    # {"last_uid": 0, "msg2uid": {"12345": 99887766}}
    return load_json_file(STATE_FILE, {"last_uid": 0, "msg2uid": {}})


def save_state(s: dict) -> None:
    save_json_file(STATE_FILE, s)


def is_admin(uid: int) -> bool:
    return ADMIN_ID > 0 and uid == ADMIN_ID


def log_dm(event: Dict[str, Any]) -> None:
    """写入 JSONL 日志（Render 默认磁盘可能是临时的，但用于排查足够）"""
    try:
        event["ts"] = int(time.time())
        DM_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with DM_LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")
    except Exception:
        pass


def remember_msg_to_uid(message_id: int, uid: int) -> None:
    s = load_state()
    msg2uid = s.get("msg2uid", {})
    msg2uid[str(message_id)] = int(uid)

    # prune
    if len(msg2uid) > MAX_MSG_MAP:
        # 删除最早的一部分（按 key 的插入顺序无法保证，这里简单截断）
        keys = list(msg2uid.keys())
        for k in keys[: len(keys) - MAX_MSG_MAP]:
            msg2uid.pop(k, None)

    s["msg2uid"] = msg2uid
    save_state(s)


def set_last_uid(uid: int) -> None:
    s = load_state()
    s["last_uid"] = int(uid)
    save_state(s)


def get_last_uid() -> int:
    s = load_state()
    return int(s.get("last_uid") or 0)


def message_type_name(msg) -> str:
    if msg.photo:
        return "photo"
    if msg.sticker:
        return "sticker"
    if msg.voice:
        return "voice"
    if msg.video:
        return "video"
    if msg.document:
        return "document"
    if msg.animation:
        return "animation"
    if msg.audio:
        return "audio"
    if msg.video_note:
        return "video_note"
    if msg.contact:
        return "contact"
    if msg.location:
        return "location"
    if msg.poll:
        return "poll"
    if msg.text:
        return "text"
    return "unknown"


def extract_uid_from_text(text: str) -> Optional[int]:
    if not text:
        return None
    m = re.search(r"UserID\s*:\s*(\d+)", text)
    if m:
        return int(m.group(1))
    return None


def extract_uid_from_replied_message(replied_msg) -> Optional[int]:
    if not replied_msg:
        return None

    # 1) 优先从 state 的 msg2uid 找
    s = load_state()
    msg2uid = s.get("msg2uid", {})
    hit = msg2uid.get(str(replied_msg.message_id))
    if hit:
        return int(hit)

    # 2) 再从 replied 的 text / caption 里解析 UserID:
    uid = extract_uid_from_text(replied_msg.text or "")
    if uid:
        return uid
    uid = extract_uid_from_text(replied_msg.caption or "")
    if uid:
        return uid

    return None


async def safe_send_text(bot, chat_id: int, text: str) -> bool:
    try:
        await bot.send_message(chat_id=chat_id, text=text, disable_web_page_preview=True)
        return True
    except Exception:
        return False


# ======================
# Commands
# ======================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Bot is online.\n\n"
        "管理员回复：\n"
        "1) /reply <user_id> <text>\n"
        "2) /r <text>（回复最近用户）\n"
        "3) 直接 Reply ‘转发自用户’那条消息（可发文字/图片/文件/语音/贴纸等）\n\n"
        "（可选）群话题绑定：/bind <US|UK|DE|FR|IT|ES|CA|JP>\n"
        "查看绑定：/map\n"
    )


async def bind(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

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

    m = load_thread_map()
    m.setdefault(str(chat_id), {})
    m[str(chat_id)][market] = int(thread_id)
    save_thread_map(m)

    await update.message.reply_text(f"已绑定：{market} -> thread_id={thread_id}\nchat_id={chat_id}")


async def show_map(update: Update, context: ContextTypes.DEFAULT_TYPE):
    m = load_thread_map()
    text = json.dumps(m, ensure_ascii=False, indent=2)
    await update.message.reply_text(text[:3500])


async def reply_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ /reply <uid> <text> """
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
    text = " ".join(context.args[1:]).strip()
    ok = await safe_send_text(context.bot, to_user, text)
    await update.message.reply_text("已发送。" if ok else "发送失败：对方可能未 /start 或已屏蔽机器人。")


async def r_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ /r <text>  -> 回复最近一个用户 """
    if not update.message:
        return

    uid = update.effective_user.id if update.effective_user else 0
    if not is_admin(uid):
        await update.message.reply_text("无权限。")
        return

    if len(context.args) < 1:
        await update.message.reply_text("用法：/r <text>（回复最近用户）")
        return

    to_user = get_last_uid()
    if to_user <= 0:
        await update.message.reply_text("暂无最近用户。请先收到一条用户私聊消息。")
        return

    text = " ".join(context.args).strip()
    ok = await safe_send_text(context.bot, to_user, text)
    await update.message.reply_text("已发送。" if ok else "发送失败：对方可能未 /start 或已屏蔽机器人。")


async def reply_media_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /reply_media <uid>
    用法：请“回复某条消息(媒体/文字)”然后发送 /reply_media <uid>
    机器人会把你回复的那条消息复制给用户
    """
    if not update.message:
        return

    uid = update.effective_user.id if update.effective_user else 0
    if not is_admin(uid):
        await update.message.reply_text("无权限。")
        return

    if len(context.args) < 1:
        await update.message.reply_text("用法：回复一条消息后，发送 /reply_media <user_id>")
        return

    to_user = int(context.args[0])

    if not update.message.reply_to_message:
        await update.message.reply_text("请先回复（Reply）一条消息（可以是图片/文件/语音/贴纸/文字），再发送 /reply_media <user_id>。")
        return

    # 把“你正在回复的那条消息”复制给用户
    src = update.message.reply_to_message
    try:
        await context.bot.copy_message(
            chat_id=to_user,
            from_chat_id=src.chat_id,
            message_id=src.message_id,
        )
        await update.message.reply_text("已发送。")
    except Exception:
        await update.message.reply_text("发送失败：对方可能未 /start 或已屏蔽机器人。")


async def rm_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ /rm  -> 把你回复的那条消息复制给最近用户 """
    if not update.message:
        return

    uid = update.effective_user.id if update.effective_user else 0
    if not is_admin(uid):
        await update.message.reply_text("无权限。")
        return

    to_user = get_last_uid()
    if to_user <= 0:
        await update.message.reply_text("暂无最近用户。请先收到一条用户私聊消息。")
        return

    if not update.message.reply_to_message:
        await update.message.reply_text("请先回复（Reply）一条消息（可以是图片/文件/语音/贴纸/文字），再发送 /rm。")
        return

    src = update.message.reply_to_message
    try:
        await context.bot.copy_message(
            chat_id=to_user,
            from_chat_id=src.chat_id,
            message_id=src.message_id,
        )
        await update.message.reply_text("已发送。")
    except Exception:
        await update.message.reply_text("发送失败：对方可能未 /start 或已屏蔽机器人。")


# ======================
# Core: Forward user -> admin (ALL media supported)
# ======================
async def forward_private_to_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    用户私聊机器人：转发给管理员
    - 先发一条“转发自用户”信息卡片（包含 userId + 用法）
    - 再 copy 用户原消息（媒体/文字都支持）
    - 记录 msg->uid 映射，方便管理员 Reply 直接回
    """
    if not update.message or not update.effective_chat:
        return

    # 只处理私聊用户
    if update.effective_chat.type != ChatType.PRIVATE:
        return

    user = update.effective_user
    uid = user.id if user else 0

    # 管理员自己的消息不当作用户消息
    if is_admin(uid):
        return

    if ADMIN_ID <= 0:
        # 没设置管理员：仍然自动回复
        await update.message.reply_text(AUTO_REPLY_TEXT)
        return

    uname = f"@{user.username}" if user and user.username else "-"
    name = (user.full_name if user else "-").strip()
    mtype = message_type_name(update.message)

    # 1) 先发卡片（你想要的“转发自用户格式”）
    card = (
        f"转发自用户 {name}\n"
        f"Username: {uname}\n"
        f"UserID: {uid}\n"
        f"Type: {mtype}\n\n"
        f"文字回复：/reply {uid} 你的回复内容\n"
        f"快捷回复：/r 你的回复内容（回复最近一个用户）\n"
        f"媒体/直接回复：Reply 下面这条消息（可发文字/图片/文件/语音/贴纸等）\n"
        f"命令发媒体：Reply 一条消息后 /reply_media {uid} 或 /rm\n"
    )
    card_msg = await context.bot.send_message(chat_id=ADMIN_ID, text=card)
    remember_msg_to_uid(card_msg.message_id, uid)

    # 2) 再把用户原消息复制给管理员（不会显示 Telegram 的“Forwarded from”，而是你自己的“转发自用户”卡片）
    copied = None
    try:
        copied = await context.bot.copy_message(
            chat_id=ADMIN_ID,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id,
        )
    except Exception:
        copied = None

    if copied:
        remember_msg_to_uid(copied.message_id, uid)

    # 3) 记录 last_uid
    set_last_uid(uid)

    # 4) 日志
    log_dm({
        "event": "dm_in",
        "uid": uid,
        "name": name,
        "username": uname,
        "type": mtype,
        "text": (update.message.text or update.message.caption or "")[:500],
    })

    # 5) 自动回复用户
    await update.message.reply_text(AUTO_REPLY_TEXT)


# ======================
# Core: Admin reply by "Reply"
# ======================
async def admin_reply_by_replying(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    管理员在私聊里“回复(Reply)”那条转发消息：
    - 如果管理员回复的是文字：发文字给用户
    - 如果管理员回复的是媒体：copy 这条媒体给用户
    """
    if not update.message or not update.effective_chat:
        return

    if update.effective_chat.type != ChatType.PRIVATE:
        return

    uid = update.effective_user.id if update.effective_user else 0
    if not is_admin(uid):
        return

    # 必须是 Reply
    if not update.message.reply_to_message:
        return

    to_user = extract_uid_from_replied_message(update.message.reply_to_message)
    if not to_user:
        # 这里不强制报错，避免管理员随便 Reply 时被刷屏
        return

    # 管理员发的是纯文字（且不是命令）
    if update.message.text and not update.message.text.startswith("/"):
        ok = await safe_send_text(context.bot, to_user, update.message.text)
        await update.message.reply_text("已发送。" if ok else "发送失败：对方可能未 /start 或已屏蔽机器人。")
        return

    # 管理员发的是媒体/其他：复制整条消息给用户（含 caption）
    try:
        await context.bot.copy_message(
            chat_id=to_user,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id,
        )
        await update.message.reply_text("已发送。")
    except Exception:
        await update.message.reply_text("发送失败：对方可能未 /start 或已屏蔽机器人。")


# ======================
# Webhook server (Render)
# ======================
async def run_webhook_server(tg_app: Application):
    if not PUBLIC_URL:
        raise RuntimeError("Missing PUBLIC_URL (or RENDER_EXTERNAL_URL). Please set PUBLIC_URL in Render env.")
    if not WEBHOOK_SECRET:
        raise RuntimeError("Missing WEBHOOK_SECRET. Please set WEBHOOK_SECRET in Render env (random string).")

    webhook_path = f"/{WEBHOOK_SECRET}"
    webhook_url = f"{PUBLIC_URL}{webhook_path}"

    await tg_app.initialize()
    await tg_app.start()

    # 设置 webhook
    await tg_app.bot.set_webhook(url=webhook_url, drop_pending_updates=True)

    aio = web.Application()

    async def root(_request):
        return web.Response(text="ok")

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

    aio.router.add_get("/", root)
    aio.router.add_get(HEALTH_PATH, health)
    aio.router.add_post(webhook_path, handle_update)

    runner = web.AppRunner(aio)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=PORT)
    await site.start()

    print(f"[ok] webhook set: {webhook_url}")
    print(f"[ok] listening on 0.0.0.0:{PORT} health: {HEALTH_PATH}")

    await asyncio.Event().wait()


def main():
    if not TOKEN:
        raise SystemExit("Missing TG_BOT_TOKEN")
    if ADMIN_ID <= 0:
        print("[warn] TG_ADMIN_ID not set. Bot will only auto-reply.")

    tg_app = Application.builder().token(TOKEN).build()

    # Commands
    tg_app.add_handler(CommandHandler("start", start))
    tg_app.add_handler(CommandHandler("bind", bind))
    tg_app.add_handler(CommandHandler("map", show_map))

    tg_app.add_handler(CommandHandler("reply", reply_cmd))
    tg_app.add_handler(CommandHandler("r", r_cmd))
    tg_app.add_handler(CommandHandler("reply_media", reply_media_cmd))
    tg_app.add_handler(CommandHandler("rm", rm_cmd))

    # 先处理管理员 Reply（优先级高）
    tg_app.add_handler(MessageHandler(~filters.COMMAND, admin_reply_by_replying), group=0)

    # 再处理用户消息转发（接收全部非命令消息类型：文字、照片、贴纸、语音、视频、文件等）
    tg_app.add_handler(MessageHandler(~filters.COMMAND, forward_private_to_admin), group=1)

    # Render 用 webhook；本地没 PUBLIC_URL 可跑 polling
    if PUBLIC_URL and WEBHOOK_SECRET:
        asyncio.run(run_webhook_server(tg_app))
    else:
        tg_app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
