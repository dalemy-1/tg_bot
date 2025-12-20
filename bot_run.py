import os
import re
import json
import time
import base64
import hashlib
import struct
import asyncio
from pathlib import Path
from typing import Any, Dict, Optional, List, Tuple
from xml.etree import ElementTree as ET

from aiohttp import web, ClientSession, ClientTimeout

import langid
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ChatType, ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

from Crypto.Cipher import AES

# ================== ENV ==================
TOKEN = (os.getenv("TG_BOT_TOKEN") or "").strip()
ADMIN_ID = int(os.getenv("TG_ADMIN_ID", "0") or "0")

PUBLIC_URL = (os.getenv("RENDER_EXTERNAL_URL") or os.getenv("PUBLIC_URL") or "").strip().rstrip("/")
WEBHOOK_SECRET = (os.getenv("WEBHOOK_SECRET") or "").strip()
PORT = int(os.getenv("PORT", "10000"))
HEALTH_PATH = "/healthz"

ADMIN_USERNAME = (os.getenv("ADMIN_USERNAME") or "Adalemy").strip().lstrip("@")

AUTO_REPLY_TEXT = (os.getenv("AUTO_REPLY_TEXT") or "你好，已收到你的消息，我们会尽快回复。").strip()
AUTO_REPLY_COOLDOWN_SEC = int(os.getenv("AUTO_REPLY_COOLDOWN_SEC", "86400"))  # 24h 默认

TRANSLATE_ENABLED = (os.getenv("TRANSLATE_ENABLED") or "1").strip() == "1"

# 免费翻译后端：可选 LibreTranslate + 兜底 MyMemory
LIBRETRANSLATE_URL = (os.getenv("LIBRETRANSLATE_URL") or "").strip().rstrip("/")
LIBRETRANSLATE_API_KEY = (os.getenv("LIBRETRANSLATE_API_KEY") or "").strip()
MYMEMORY_EMAIL = (os.getenv("MYMEMORY_EMAIL") or "").strip()

# ========= WECOM ENV =========
WECOM_CB_TOKEN = (os.getenv("WECOM_CB_TOKEN") or "").strip()
WECOM_CB_AESKEY = (os.getenv("WECOM_CB_AESKEY") or "").strip()
WECOM_CORP_ID = (os.getenv("WECOM_CORP_ID") or "").strip()

WECOM_AGENT_ID = int(os.getenv("WECOM_AGENT_ID", "0") or "0")
WECOM_APP_SECRET = (os.getenv("WECOM_APP_SECRET") or "").strip()

print("[boot] TG_BOT_TOKEN prefix:", (TOKEN or "")[:10], "len:", len(TOKEN or ""), "tail:", (TOKEN or "")[-4:])
print("[boot] PUBLIC_URL:", PUBLIC_URL[:80])
print("[boot] ADMIN_ID:", ADMIN_ID)
print("[boot] WECOM_AGENT_ID:", WECOM_AGENT_ID)

BASE_DIR = Path(__file__).resolve().parent
STATE_FILE = BASE_DIR / "support_state.json"

MAX_MSG_INDEX = 8000
MAX_RECENT = 50  # 最近对话列表长度

STATUS_OPTIONS = ["已下单", "退货退款", "已返款", "黑名单"]
DEFAULT_STATUS = "用户来信"

# ================== STATE ==================
def _now_ts() -> int:
    return int(time.time())


def load_state() -> Dict[str, Any]:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass

    return {
        "ticket_seq": 0,
        "tickets": {},           # user_id(str) -> {ticket_id, created_at, header_msg_id}
        "msg_index": {},         # admin_message_id(str) -> user_id(int)

        "wecom_index": {},       # admin_message_id(str) -> wecom_userid(str)

        "active_user": 0,        # 当前 TG 会话用户（方案3核心）
        "active_wecom": "",      # 当前 WeCom 会话（可选）
        "recent_users": [],      # [{uid:int, last_seen:int}] 最近对话

        "last_user": 0,
        "last_auto_reply": {},   # user_id(str)->ts
        "user_meta": {},         # user_id(str)->{name, username, language_code, first_seen, last_seen, msg_count, last_detected_lang}
        "user_status": {},       # user_id(str)->status
    }


def save_state(state: Dict[str, Any]) -> None:
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def is_admin(update: Update) -> bool:
    return bool(update.effective_user and update.effective_user.id == ADMIN_ID and ADMIN_ID > 0)


def _safe(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def fmt_time(ts: int) -> str:
    if not ts:
        return "-"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))


def remember_msg_index(state: Dict[str, Any], admin_message_id: int, user_id: int) -> None:
    mi = state.setdefault("msg_index", {})
    mi[str(admin_message_id)] = int(user_id)

    if len(mi) > MAX_MSG_INDEX:
        keys = list(mi.keys())
        for k in keys[: len(keys) - MAX_MSG_INDEX]:
            mi.pop(k, None)


def remember_wecom_index(state: Dict[str, Any], admin_message_id: int, wecom_userid: str) -> None:
    m = state.setdefault("wecom_index", {})
    m[str(admin_message_id)] = wecom_userid
    if len(m) > MAX_MSG_INDEX:
        keys = list(m.keys())
        for k in keys[: len(keys) - MAX_MSG_INDEX]:
            m.pop(k, None)


def bump_recent_user(state: Dict[str, Any], uid: int) -> None:
    rec: List[Dict[str, Any]] = state.setdefault("recent_users", [])
    now = _now_ts()
    # remove old
    rec = [x for x in rec if int(x.get("uid", 0) or 0) != uid]
    rec.insert(0, {"uid": uid, "last_seen": now})
    rec = rec[:MAX_RECENT]
    state["recent_users"] = rec


# ================== HTTP / Translation ==================
_http: Optional[ClientSession] = None


async def _session() -> ClientSession:
    global _http
    if _http is None or _http.closed:
        _http = ClientSession(timeout=ClientTimeout(total=12))
    return _http


def _norm_lang(code: str) -> str:
    c = (code or "").strip().replace("_", "-")
    low = c.lower()
    if not low:
        return "auto"
    if low.startswith("zh"):
        return "zh-CN"
    if low.startswith("ja"):
        return "ja"
    if low.startswith("en"):
        return "en"
    if low.startswith("fr"):
        return "fr"
    if low.startswith("de"):
        return "de"
    if low.startswith("es"):
        return "es"
    if low.startswith("it"):
        return "it"
    if low.startswith("pt"):
        return "pt"
    if low.startswith("ru"):
        return "ru"
    return low[:2]


def _is_chinese(text: str) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", text or ""))


def detect_lang(text: str) -> str:
    t = (text or "").strip()
    if not t:
        return "auto"
    if _is_chinese(t):
        return "zh-CN"
    try:
        code, _score = langid.classify(t)
        return _norm_lang(code)
    except Exception:
        return "auto"


async def _translate_libre(text: str, src: str, tgt: str) -> Optional[str]:
    if not LIBRETRANSLATE_URL:
        return None
    url = f"{LIBRETRANSLATE_URL}/translate"
    payload = {"q": text, "source": src, "target": tgt, "format": "text"}
    if LIBRETRANSLATE_API_KEY:
        payload["api_key"] = LIBRETRANSLATE_API_KEY
    try:
        s = await _session()
        async with s.post(url, json=payload) as resp:
            data = await resp.json(content_type=None)
        tr = (data or {}).get("translatedText")
        if tr and tr.strip():
            return tr.strip()
        return None
    except Exception:
        return None


async def _translate_mymemory(text: str, src: str, tgt: str) -> Optional[str]:
    url = "https://api.mymemory.translated.net/get"
    params = {"q": text, "langpair": f"{src}|{tgt}"}
    if MYMEMORY_EMAIL:
        params["de"] = MYMEMORY_EMAIL
    try:
        s = await _session()
        async with s.get(url, params=params) as resp:
            data = await resp.json(content_type=None)
        tr = (((data or {}).get("responseData") or {}).get("translatedText") or "").strip()
        return tr or None
    except Exception:
        return None


async def translate(text: str, src: str, tgt: str) -> Optional[str]:
    if not TRANSLATE_ENABLED:
        return None
    q = (text or "").strip()
    if not q:
        return None

    src = _norm_lang(src)
    tgt = _norm_lang(tgt)

    if src == "auto":
        src = detect_lang(q)
        if src == "auto":
            src = "en" if tgt == "zh-CN" else "zh-CN"

    if src == tgt:
        return q

    tr = await _translate_libre(q, src, tgt)
    if tr:
        return tr
    tr = await _translate_mymemory(q, src, tgt)
    if tr:
        return tr
    return None


# ================== UI ==================
def contact_admin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("联系管理员", url=f"https://t.me/{ADMIN_USERNAME}")]])


def admin_panel_keyboard(state: Dict[str, Any]) -> InlineKeyboardMarkup:
    active_uid = int(state.get("active_user", 0) or 0)
    active_wecom = (state.get("active_wecom") or "").strip()

    label = "当前会话：未选择"
    if active_uid:
        meta = (state.get("user_meta") or {}).get(str(active_uid), {})
        name = meta.get("name") or str(active_uid)
        label = f"当前会话：TG {name}"
    elif active_wecom:
        label = f"当前会话：WeCom {active_wecom}"

    return InlineKeyboardMarkup([
        [InlineKeyboardButton("收件箱", callback_data="panel|inbox|0"),
         InlineKeyboardButton("最近对话", callback_data="panel|recent|0")],
        [InlineKeyboardButton("清空当前会话", callback_data="panel|clear|0")],
        [InlineKeyboardButton(label, callback_data="panel|noop|0")],
    ])


def status_keyboard(uid: int) -> InlineKeyboardMarkup:
    row1 = [
        InlineKeyboardButton("已下单", callback_data=f"status|{uid}|已下单"),
        InlineKeyboardButton("退货退款", callback_data=f"status|{uid}|退货退款"),
    ]
    row2 = [
        InlineKeyboardButton("已返款", callback_data=f"status|{uid}|已返款"),
        InlineKeyboardButton("黑名单", callback_data=f"status|{uid}|黑名单"),
    ]
    row3 = [
        InlineKeyboardButton("清空状态", callback_data=f"clear|{uid}|-"),
        InlineKeyboardButton("Profile", callback_data=f"profile|{uid}|-"),
    ]
    row4 = [
        InlineKeyboardButton("设为当前会话", callback_data=f"set|tg|{uid}"),
        InlineKeyboardButton("打开用户", url=f"tg://user?id={uid}"),
    ]
    return InlineKeyboardMarkup([row1, row2, row3, row4])


def wecom_message_keyboard(wecom_userid: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("设为当前会话(WeCom)", callback_data=f"set|wecom|{wecom_userid}")],
    ])


def render_header(state: Dict[str, Any], uid: int) -> str:
    uid_key = str(uid)
    t = (state.get("tickets") or {}).get(uid_key, {})
    meta = (state.get("user_meta") or {}).get(uid_key, {})
    status = (state.get("user_status") or {}).get(uid_key, DEFAULT_STATUS)

    ticket_id = t.get("ticket_id", "-")
    name = meta.get("name", "Unknown")
    username = meta.get("username")
    user_link = f"tg://user?id={uid}"

    first_seen = int(meta.get("first_seen", 0) or 0)
    last_seen = int(meta.get("last_seen", 0) or 0)
    msg_count = int(meta.get("msg_count", 0) or 0)
    last_lang = _norm_lang(meta.get("last_detected_lang", "auto"))

    active_uid = int(state.get("active_user", 0) or 0)
    active_flag = "✅ 当前会话" if active_uid == uid else ""

    lines = [
        f"🧾 <b>Ticket #{ticket_id}</b>   <b>Status:</b> <code>{_safe(status)}</code>   {active_flag}",
        f"<b>Name:</b> {_safe(name)}",
    ]
    if username:
        lines.append(f"<b>Username:</b> @{_safe(username)}")
    lines += [
        f"<b>UserID:</b> <code>{uid}</code>   <b>Open:</b> <a href=\"{user_link}\">Click</a>",
        f"<b>Last lang:</b> <code>{_safe(last_lang)}</code>",
        f"<b>First seen:</b> <code>{fmt_time(first_seen)}</code>",
        f"<b>Last seen:</b> <code>{fmt_time(last_seen)}</code>   <b>Msg count:</b> <code>{msg_count}</code>",
        "",
        "<b>推荐：</b>Reply（回复）任意“转发自用户”的消息即可回给对方。",
        "<b>方案3：</b>点 <b>设为当前会话</b> 后，你可以不 Reply 直接发送（文字/图片/文件）给该用户。",
    ]
    return "\n".join(lines)


async def ensure_ticket(state: Dict[str, Any], context: ContextTypes.DEFAULT_TYPE, uid: int) -> Dict[str, Any]:
    tickets = state.setdefault("tickets", {})
    uid_key = str(uid)
    t = tickets.get(uid_key)

    if t and t.get("header_msg_id"):
        return t

    state["ticket_seq"] = int(state.get("ticket_seq", 0)) + 1
    ticket_id = state["ticket_seq"]

    state.setdefault("user_status", {}).setdefault(uid_key, DEFAULT_STATUS)

    msg = await context.bot.send_message(
        chat_id=ADMIN_ID,
        text=render_header(state, uid),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
        reply_markup=status_keyboard(uid),
    )

    tickets[uid_key] = {
        "ticket_id": ticket_id,
        "created_at": _now_ts(),
        "header_msg_id": msg.message_id,
    }
    return tickets[uid_key]


async def refresh_header(state: Dict[str, Any], context: ContextTypes.DEFAULT_TYPE, uid: int) -> None:
    t = (state.get("tickets") or {}).get(str(uid))
    if not t or not t.get("header_msg_id"):
        return
    try:
        await context.bot.edit_message_text(
            chat_id=ADMIN_ID,
            message_id=int(t["header_msg_id"]),
            text=render_header(state, uid),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=status_keyboard(uid),
        )
    except Exception:
        pass


# ================== ADMIN panel list ==================
def _build_user_list_keyboard(state: Dict[str, Any], uids: List[int], page: int, page_size: int, mode: str) -> InlineKeyboardMarkup:
    start = page * page_size
    items = uids[start:start + page_size]

    rows = []
    for uid in items:
        meta = (state.get("user_meta") or {}).get(str(uid), {})
        name = meta.get("name") or str(uid)
        rows.append([InlineKeyboardButton(f"{name} ({uid})", callback_data=f"set|tg|{uid}")])

    nav = []
    if start > 0:
        nav.append(InlineKeyboardButton("⬅️ 上一页", callback_data=f"panel|{mode}|{page-1}"))
    if start + page_size < len(uids):
        nav.append(InlineKeyboardButton("下一页 ➡️", callback_data=f"panel|{mode}|{page+1}"))
    if nav:
        rows.append(nav)

    rows.append([InlineKeyboardButton("返回面板", callback_data="panel|home|0")])
    return InlineKeyboardMarkup(rows)


def _collect_inbox_uids(state: Dict[str, Any]) -> List[int]:
    # 这里简单按 recent_users 作为“收件箱”。你也可以扩展为：按 ticket_seq/未处理状态等排序
    rec = state.get("recent_users") or []
    uids = [int(x.get("uid", 0) or 0) for x in rec if int(x.get("uid", 0) or 0) > 0]
    return uids


# ================== COMMANDS ==================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    st = load_state()

    if is_admin(update):
        await update.message.reply_text(
            "管理员面板：\n"
            "1) 用户消息会转发到这里；你可以 Reply 回答。\n"
            "2) 如果你想“不 Reply 也能发”，请点击某个用户的【设为当前会话】。\n"
            "3) 也可以点下面【收件箱/最近对话】快速切换会话。\n",
            reply_markup=admin_panel_keyboard(st),
        )
    else:
        await update.message.reply_text(
            "你好，欢迎联系。\n请直接发送你的消息（文字/图片/文件等）。我们收到后会尽快回复。\n",
            reply_markup=contact_admin_keyboard()
        )


# ================== CALLBACKS ==================
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.callback_query:
        return
    q = update.callback_query
    await q.answer()

    if not is_admin(update):
        return

    st = load_state()
    data = q.data or ""
    parts = data.split("|")

    # ---- status/profile (existing) ----
    if parts[0] in ("status", "clear", "profile"):
        action = parts[0]
        if len(parts) < 2:
            return
        uid = int(parts[1])

        if action == "status" and len(parts) >= 3:
            status = parts[2]
            if status in STATUS_OPTIONS:
                st.setdefault("user_status", {})[str(uid)] = status
                save_state(st)
                await refresh_header(st, context, uid)
            return

        if action == "clear":
            st.setdefault("user_status", {})[str(uid)] = DEFAULT_STATUS
            save_state(st)
            await refresh_header(st, context, uid)
            return

        if action == "profile":
            try:
                await context.bot.send_message(
                    chat_id=ADMIN_ID,
                    text=render_header(st, uid),
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                    reply_markup=status_keyboard(uid),
                )
            except Exception:
                pass
            return

    # ---- set active session ----
    if parts[0] == "set":
        if len(parts) < 3:
            return
        kind = parts[1]
        target = parts[2]

        if kind == "tg":
            uid = int(target)
            st["active_user"] = uid
            st["active_wecom"] = ""
            save_state(st)
            # refresh header (show ✅)
            await refresh_header(st, context, uid)
            # update panel message if exists
            try:
                await q.message.edit_reply_markup(reply_markup=admin_panel_keyboard(st))
            except Exception:
                pass
            return

        if kind == "wecom":
            st["active_wecom"] = target
            st["active_user"] = 0
            save_state(st)
            try:
                await q.message.edit_reply_markup(reply_markup=admin_panel_keyboard(st))
            except Exception:
                pass
            return

    # ---- admin panel navigation ----
    if parts[0] == "panel":
        mode = parts[1] if len(parts) >= 2 else "home"
        page = int(parts[2]) if len(parts) >= 3 else 0

        if mode == "home":
            try:
                await q.message.edit_reply_markup(reply_markup=admin_panel_keyboard(st))
            except Exception:
                pass
            return

        if mode == "clear":
            st["active_user"] = 0
            st["active_wecom"] = ""
            save_state(st)
            try:
                await q.message.edit_reply_markup(reply_markup=admin_panel_keyboard(st))
            except Exception:
                pass
            return

        if mode in ("inbox", "recent"):
            uids = _collect_inbox_uids(st)
            if not uids:
                try:
                    await q.message.reply_text("暂无最近对话。需要先让用户联系机器人一次。", reply_markup=admin_panel_keyboard(st))
                except Exception:
                    pass
                return

            kb = _build_user_list_keyboard(st, uids, page=page, page_size=10, mode=mode)
            try:
                await q.message.edit_reply_markup(reply_markup=kb)
            except Exception:
                # 如果原消息无法编辑（例如太旧），就发新消息
                await q.message.reply_text("选择一个用户设为当前会话：", reply_markup=kb)
            return

        # noop
        return


# ================== USER -> ADMIN (forward + translate to zh) ==================
async def handle_user_private(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat:
        return
    if update.effective_chat.type != ChatType.PRIVATE:
        return
    if is_admin(update):
        return

    user = update.effective_user
    uid = int(getattr(user, "id", 0) or 0)
    if uid <= 0:
        return

    st = load_state()

    # meta
    meta = st.setdefault("user_meta", {}).setdefault(str(uid), {})
    meta.setdefault("first_seen", _now_ts())
    meta["last_seen"] = _now_ts()
    meta["msg_count"] = int(meta.get("msg_count", 0) or 0) + 1
    meta["name"] = (getattr(user, "full_name", "") or "Unknown").strip()
    meta["username"] = getattr(user, "username", None)
    meta["language_code"] = getattr(user, "language_code", "")

    bump_recent_user(st, uid)

    # ticket/header
    t = await ensure_ticket(st, context, uid)
    st.setdefault("user_status", {}).setdefault(str(uid), DEFAULT_STATUS)

    forwarded_id = None
    try:
        fwd = await context.bot.forward_message(
            chat_id=ADMIN_ID,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id,
        )
        forwarded_id = fwd.message_id
        remember_msg_index(st, fwd.message_id, uid)
    except Exception:
        copied = await context.bot.copy_message(
            chat_id=ADMIN_ID,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id,
        )
        forwarded_id = copied.message_id
        remember_msg_index(st, copied.message_id, uid)

    # 也把 header 记入 index（防止管理员误 Reply header）
    if t.get("header_msg_id"):
        remember_msg_index(st, int(t["header_msg_id"]), uid)

    # 检测语言
    txt = (update.message.text or update.message.caption or "").strip()
    if txt:
        src = detect_lang(txt)
        meta["last_detected_lang"] = src

        # 非中文 -> 翻译成中文贴在转发下面
        if TRANSLATE_ENABLED and _norm_lang(src) != "zh-CN" and forwarded_id:
            zh = await translate(txt, src, "zh-CN")
            if zh and zh.strip() and zh.strip() != txt.strip():
                try:
                    await context.bot.send_message(
                        chat_id=ADMIN_ID,
                        text=f"中文翻译（{_safe(src)} → zh-CN）：\n{_safe(zh)}",
                        reply_to_message_id=forwarded_id,
                    )
                except Exception:
                    pass

    # 自动回复（24小时一次）
    last_ts = int((st.get("last_auto_reply") or {}).get(str(uid), 0) or 0)
    now_ts = _now_ts()
    if now_ts - last_ts >= AUTO_REPLY_COOLDOWN_SEC:
        try:
            await update.message.reply_text(AUTO_REPLY_TEXT, reply_markup=contact_admin_keyboard())
        except Exception:
            pass
        st.setdefault("last_auto_reply", {})[str(uid)] = now_ts

    st["last_user"] = uid
    save_state(st)

    await refresh_header(st, context, uid)


# ================== ADMIN -> USER (Reply or active_user) ==================
async def handle_admin_private(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat:
        return
    if update.effective_chat.type != ChatType.PRIVATE:
        return
    if not is_admin(update):
        return

    st = load_state()
    to_user = 0
    to_wecom = ""

    # A) 如果 Reply：优先按 Reply 的目标路由
    if update.message.reply_to_message:
        rid = str(update.message.reply_to_message.message_id)

        # 1) Reply 企业微信消息 => 回发企业微信（当前仅文字）
        to_wecom = (st.get("wecom_index") or {}).get(rid, "")
        if to_wecom:
            admin_text = (update.message.text or "").strip()
            if not admin_text:
                await update.message.reply_text("当前仅支持文字回复到企业微信。")
                return
            try:
                await wecom_send_text(to_wecom, admin_text)
                st["active_wecom"] = to_wecom
                st["active_user"] = 0
                save_state(st)
                await update.message.reply_text("已回发到企业微信。")
            except Exception as e:
                await update.message.reply_text(f"回发企业微信失败：{e}")
            return

        # 2) Reply TG 用户转发消息 => 找到 TG 用户
        if rid in (st.get("msg_index") or {}):
            to_user = int(st["msg_index"][rid])

        if not to_user:
            await update.message.reply_text("没识别到用户ID：请 Reply 用户的“转发自用户”消息，或先点【设为当前会话】。")
            return

        # 方案3：Reply 过谁，就把谁设为 active_user
        st["active_user"] = to_user
        st["active_wecom"] = ""
        st["last_user"] = to_user
        save_state(st)

    # B) 不 Reply：发给 active_user（或兜底 last_user）
    else:
        to_user = int(st.get("active_user", 0) or 0)
        if to_user <= 0:
            to_user = int(st.get("last_user", 0) or 0)

        if to_user <= 0:
            await update.message.reply_text("当前没有可发送的目标用户：请先让用户联系机器人一次，或先点【设为当前会话】。")
            return

    # C) 发给 TG 用户（翻译 + 媒体 copy）
    try:
        user_meta = (st.get("user_meta") or {}).get(str(to_user), {})
        user_lang = _norm_lang(user_meta.get("last_detected_lang", "en"))
        if user_lang == "auto":
            user_lang = "en"

        admin_text = (update.message.text or "").strip()
        admin_caption = (update.message.caption or "").strip()

        if admin_text:
            send_text = admin_text
            if TRANSLATE_ENABLED and _is_chinese(admin_text) and user_lang != "zh-CN":
                tr = await translate(admin_text, "zh-CN", user_lang)
                if tr and tr.strip():
                    send_text = tr.strip()
            await context.bot.send_message(chat_id=to_user, text=send_text)
        else:
            await context.bot.copy_message(
                chat_id=to_user,
                from_chat_id=update.effective_chat.id,
                message_id=update.message.message_id,
            )
            if admin_caption and TRANSLATE_ENABLED and _is_chinese(admin_caption) and user_lang != "zh-CN":
                tr = await translate(admin_caption, "zh-CN", user_lang)
                if tr and tr.strip():
                    await context.bot.send_message(chat_id=to_user, text=tr.strip())

        st["active_user"] = to_user
        st["active_wecom"] = ""
        st["last_user"] = to_user
        save_state(st)

        await update.message.reply_text("已发送。")

    except Exception as e:
        await update.message.reply_text(f"发送失败：{e}")


# ================== WECOM: decrypt + send ==================
def _sha1_signature(token: str, timestamp: str, nonce: str, encrypt: str) -> str:
    arr = [token, timestamp, nonce, encrypt]
    arr.sort()
    return hashlib.sha1("".join(arr).encode("utf-8")).hexdigest()


def _pkcs7_unpad(data: bytes) -> bytes:
    pad = data[-1]
    if pad < 1 or pad > 32:
        raise ValueError("bad padding")
    return data[:-pad]


def _aes_key_bytes(aes_key_43: str) -> bytes:
    return base64.b64decode(aes_key_43 + "=")


def _wecom_decrypt(encrypt_b64: str) -> str:
    if not WECOM_CB_AESKEY:
        raise RuntimeError("missing WECOM_CB_AESKEY")
    key = _aes_key_bytes(WECOM_CB_AESKEY)  # 32 bytes
    cipher = AES.new(key, AES.MODE_CBC, iv=key[:16])
    plain = cipher.decrypt(base64.b64decode(encrypt_b64))
    plain = _pkcs7_unpad(plain)

    msg_len = struct.unpack("!I", plain[16:20])[0]
    msg = plain[20:20 + msg_len]
    corp = plain[20 + msg_len:].decode("utf-8")
    if WECOM_CORP_ID and corp != WECOM_CORP_ID:
        raise ValueError(f"corp_id mismatch: {corp}")
    return msg.decode("utf-8")


async def wecom_callback_get(request: web.Request):
    qs = request.query
    msg_signature = qs.get("msg_signature", "")
    timestamp = qs.get("timestamp", "")
    nonce = qs.get("nonce", "")
    echostr = qs.get("echostr", "")

    if not (msg_signature and timestamp and nonce and echostr):
        return web.Response(status=400, text="bad query")

    sig = _sha1_signature(WECOM_CB_TOKEN, timestamp, nonce, echostr)
    if sig != msg_signature:
        print("[wecom][GET] bad signature")
        return web.Response(status=403, text="bad signature")

    try:
        plain = _wecom_decrypt(echostr)
        return web.Response(text=plain)
    except Exception as e:
        print("[wecom][GET] decrypt failed:", repr(e))
        return web.Response(status=403, text="verify failed")


def wecom_callback_post_factory(tg_app: Application):
    async def wecom_callback_post(request: web.Request):
        try:
            body = await request.text()
        except Exception:
            return web.Response(status=400, text="bad body")

        resp = web.Response(text="success")

        async def _process():
            try:
                qs = request.query
                msg_signature = qs.get("msg_signature", "")
                timestamp = qs.get("timestamp", "")
                nonce = qs.get("nonce", "")

                if not (msg_signature and timestamp and nonce):
                    print("[wecom][POST] missing query params")
                    return

                root = ET.fromstring(body)
                encrypt = root.findtext("Encrypt", default="")
                if not encrypt:
                    print("[wecom][POST] missing Encrypt")
                    return

                sig = _sha1_signature(WECOM_CB_TOKEN, timestamp, nonce, encrypt)
                if sig != msg_signature:
                    print("[wecom][POST] bad signature")
                    return

                plain_xml = _wecom_decrypt(encrypt)
                px = ET.fromstring(plain_xml)

                msg_type = px.findtext("MsgType", default="")
                from_user = px.findtext("FromUserName", default="")
                content = px.findtext("Content", default="")

                if msg_type == "text" and from_user and content:
                    st = load_state()
                    msg = await tg_app.bot.send_message(
                        chat_id=ADMIN_ID,
                        text=f"[WeCom] {from_user}:\n{content}",
                        reply_markup=wecom_message_keyboard(from_user),
                    )
                    remember_wecom_index(st, msg.message_id, from_user)
                    save_state(st)

            except Exception as e:
                print("[wecom][POST] process error:", repr(e))

        asyncio.create_task(_process())
        return resp

    return wecom_callback_post


_wecom_token_cache = {"token": "", "exp": 0}


async def wecom_get_access_token() -> str:
    now = int(time.time())
    if _wecom_token_cache["token"] and now < _wecom_token_cache["exp"] - 60:
        return _wecom_token_cache["token"]

    if not (WECOM_CORP_ID and WECOM_APP_SECRET):
        raise RuntimeError("Missing WECOM_CORP_ID/WECOM_APP_SECRET")

    url = "https://qyapi.weixin.qq.com/cgi-bin/gettoken"
    params = {"corpid": WECOM_CORP_ID, "corpsecret": WECOM_APP_SECRET}

    s = await _session()
    async with s.get(url, params=params) as resp:
        data = await resp.json(content_type=None)

    if int(data.get("errcode", -1)) != 0:
        raise RuntimeError(f"wecom gettoken failed: {data}")

    token = data["access_token"]
    expires_in = int(data.get("expires_in", 7200))
    _wecom_token_cache["token"] = token
    _wecom_token_cache["exp"] = now + expires_in
    return token


async def wecom_send_text(touser: str, content: str) -> None:
    if WECOM_AGENT_ID <= 0:
        raise RuntimeError("Missing WECOM_AGENT_ID")

    token = await wecom_get_access_token()
    url = f"https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={token}"
    payload = {
        "touser": touser,
        "msgtype": "text",
        "agentid": WECOM_AGENT_ID,
        "text": {"content": content},
        "safe": 0,
    }

    s = await _session()
    async with s.post(url, json=payload) as resp:
        data = await resp.json(content_type=None)

    if int(data.get("errcode", -1)) != 0:
        raise RuntimeError(f"wecom send failed: {data}")


# ================== WEBHOOK SERVER ==================
async def run_webhook_server(tg_app: Application):
    if not PUBLIC_URL:
        raise RuntimeError("Missing PUBLIC_URL (or RENDER_EXTERNAL_URL).")
    if not WEBHOOK_SECRET:
        raise RuntimeError("Missing WEBHOOK_SECRET.")
    if ADMIN_ID <= 0:
        raise RuntimeError("Missing TG_ADMIN_ID.")

    webhook_path = f"/{WEBHOOK_SECRET}"
    webhook_url = f"{PUBLIC_URL}{webhook_path}"

    await tg_app.initialize()
    await tg_app.start()
    await tg_app.bot.set_webhook(url=webhook_url, drop_pending_updates=True)

    aio = web.Application()

    async def health(_request):
        return web.Response(text="ok")

    async def handle_update(request: web.Request):
        try:
            data = await request.json()
        except Exception:
            return web.Response(status=400, text="bad json")

        resp = web.Response(text="ok")

        async def _process():
            try:
                upd = Update.de_json(data, tg_app.bot)
                await tg_app.process_update(upd)
            except Exception as e:
                print("process_update error:", repr(e))

        asyncio.create_task(_process())
        return resp

    aio.router.add_get(HEALTH_PATH, health)
    aio.router.add_post(webhook_path, handle_update)

    # WeCom routes
    aio.router.add_get("/wecom/callback", wecom_callback_get)
    aio.router.add_post("/wecom/callback", wecom_callback_post_factory(tg_app))

    runner = web.AppRunner(aio)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=PORT)
    await site.start()

    print(f"[ok] telegram webhook: {webhook_url}")
    print(f"[ok] listening 0.0.0.0:{PORT}, health: {HEALTH_PATH}")

    await asyncio.Event().wait()


def main():
    if not TOKEN:
        raise SystemExit("Missing TG_BOT_TOKEN")
    if ADMIN_ID <= 0:
        raise SystemExit("Missing TG_ADMIN_ID")

    tg_app = Application.builder().token(TOKEN).build()

    tg_app.add_handler(CommandHandler("start", cmd_start))
    tg_app.add_handler(CallbackQueryHandler(on_callback))

    tg_app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.User(user_id=ADMIN_ID), handle_user_private))
    tg_app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.User(user_id=ADMIN_ID), handle_admin_private))

    if PUBLIC_URL:
        asyncio.run(run_webhook_server(tg_app))
    else:
        tg_app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
