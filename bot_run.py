import os
import re
import json
import time
import base64
import hashlib
import struct
import asyncio
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Union
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

# PyCryptodome
from Crypto.Cipher import AES


# ================== ENV ==================
# Telegram
TOKEN = (os.getenv("TG_BOT_TOKEN") or "").strip()
ADMIN_ID = int(os.getenv("TG_ADMIN_ID", "0") or "0")
ADMIN_USERNAME = (os.getenv("ADMIN_USERNAME") or "Adalemy").strip().lstrip("@")

PUBLIC_URL = (os.getenv("RENDER_EXTERNAL_URL") or os.getenv("PUBLIC_URL") or "").strip().rstrip("/")
WEBHOOK_SECRET = (os.getenv("WEBHOOK_SECRET") or "").strip()
PORT = int(os.getenv("PORT", "10000"))
HEALTH_PATH = "/healthz"

AUTO_REPLY_TEXT = (os.getenv("AUTO_REPLY_TEXT") or "你好，已收到你的消息，我们会尽快回复。").strip()
AUTO_REPLY_COOLDOWN_SEC = int(os.getenv("AUTO_REPLY_COOLDOWN_SEC", "86400"))  # 24h

TRANSLATE_ENABLED = (os.getenv("TRANSLATE_ENABLED") or "1").strip() == "1"
ADMIN_LANG = "zh-CN"  # 管理员侧统一中文

# Translate backends (optional)
LIBRETRANSLATE_URL = (os.getenv("LIBRETRANSLATE_URL") or "").strip().rstrip("/")
LIBRETRANSLATE_API_KEY = (os.getenv("LIBRETRANSLATE_API_KEY") or "").strip()
MYMEMORY_EMAIL = (os.getenv("MYMEMORY_EMAIL") or "").strip()

# WeCom (internal members)
WECOM_CORP_ID = (os.getenv("WECOM_CORP_ID") or "").strip()                # 企业ID：wwxxxx
WECOM_AGENT_ID = int(os.getenv("WECOM_AGENT_ID", "0") or "0")            # 应用 AgentId：1000002
WECOM_AGENT_SECRET = (os.getenv("WECOM_AGENT_SECRET") or "").strip()     # 应用 Secret

# WeCom callback verify (Token / EncodingAESKey)
WECOM_CB_TOKEN = (os.getenv("WECOM_CB_TOKEN") or "").strip()
WECOM_CB_AESKEY = (os.getenv("WECOM_CB_AESKEY") or "").strip()           # 43位 EncodingAESKey

print("[boot] TG_BOT_TOKEN prefix:", (TOKEN or "")[:10], "len:", len(TOKEN or ""), "tail:", (TOKEN or "")[-4:])
print("[boot] RENDER_EXTERNAL_URL:", (os.getenv("RENDER_EXTERNAL_URL") or "")[:80])
print("[boot] PUBLIC_URL:", (os.getenv("PUBLIC_URL") or "")[:80])
print("[boot] WECOM_CORP_ID:", (WECOM_CORP_ID or "")[:8], "...")
print("[boot] WECOM_AGENT_ID:", WECOM_AGENT_ID)


# ================== FILE STATE ==================
BASE_DIR = Path(__file__).resolve().parent
STATE_FILE = BASE_DIR / "support_state.json"
MAX_MSG_INDEX = 8000

STATUS_OPTIONS = ["已下单", "退货退款", "已返款", "黑名单"]
DEFAULT_STATUS = "用户来信"


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
        "tickets": {},            # tg user_id(str) -> {ticket_id, created_at, header_msg_id}
        "msg_index": {},          # admin_message_id(str) -> route (int tg_uid OR "wecom:<userid>" OR "tg:<uid>")
        "last_user": 0,

        "last_auto_reply": {},    # tg user_id(str) -> ts
        "user_meta": {},          # tg user_id(str) -> meta
        "user_status": {},        # tg user_id(str) -> status

        "wecom_meta": {},         # wecom_userid(str) -> meta {first_seen,last_seen,msg_count,last_detected_lang}
    }


def save_state(state: Dict[str, Any]) -> None:
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def is_admin(update: Update) -> bool:
    return bool(update.effective_user and update.effective_user.id == ADMIN_ID and ADMIN_ID > 0)


def remember_route_index(state: Dict[str, Any], admin_message_id: int, route: Union[int, str]) -> None:
    """
    route:
      - int: Telegram user id (backward compatible)
      - "tg:<uid>": Telegram user
      - "wecom:<userid>": WeCom internal member userid
    """
    mi = state.setdefault("msg_index", {})
    mi[str(admin_message_id)] = route

    if len(mi) > MAX_MSG_INDEX:
        keys = list(mi.keys())
        for k in keys[: len(keys) - MAX_MSG_INDEX]:
            mi.pop(k, None)


def resolve_route(state: Dict[str, Any], reply_to_admin_message_id: int) -> Tuple[str, Optional[Union[int, str]]]:
    v = (state.get("msg_index") or {}).get(str(reply_to_admin_message_id))
    if v is None:
        return ("", None)
    if isinstance(v, int):
        return ("tg", int(v))
    if isinstance(v, str):
        if v.startswith("wecom:"):
            return ("wecom", v.split(":", 1)[1])
        if v.startswith("tg:"):
            try:
                return ("tg", int(v.split(":", 1)[1]))
            except Exception:
                return ("", None)
    return ("", None)


def fmt_time(ts: int) -> str:
    if not ts:
        return "-"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))


def _safe(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# ================== TRANSLATION ==================
_http: Optional[ClientSession] = None


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


async def _session() -> ClientSession:
    global _http
    if _http is None or _http.closed:
        _http = ClientSession(timeout=ClientTimeout(total=12))
    return _http


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
        if tr:
            return tr
        return None
    except Exception:
        return None


async def translate(text: str, src: str, tgt: str) -> Optional[str]:
    """失败返回 None；严格互译：中文<->其它语言"""
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


# ================== TG UI ==================
def contact_admin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("联系管理员", url=f"https://t.me/{ADMIN_USERNAME}")]
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
    return InlineKeyboardMarkup([row1, row2, row3])


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

    lines = [
        f"🧾 <b>Ticket #{ticket_id}</b>   <b>Status:</b> <code>{_safe(status)}</code>",
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
        "<b>推荐：</b>在管理员私聊里 <b>Reply（回复）</b>下面那条“转发自用户”的消息，即可回复对方（支持文字/图片/文件/贴纸/语音等）。",
        f"<b>备用：</b>直接在此用户聊天窗口私聊对方：<a href=\"{user_link}\">打开对话</a>",
    ]
    return "\n".join(lines)


async def ensure_ticket(state: Dict[str, Any], context: ContextTypes.DEFAULT_TYPE, uid: int) -> Dict[str, Any]:
    tickets = state.setdefault("tickets", {})
    uid_key = str(uid)
    t = tickets.get(uid_key)

    need_new = True
    if t and t.get("header_msg_id"):
        need_new = False

    if need_new:
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


# ================== TG COMMANDS ==================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if is_admin(update):
        await update.message.reply_text(
            "机器人已上线。\n\n"
            "管理员用法：\n"
            "1) 用户给机器人发消息 -> 你会收到“转发自用户”的消息。\n"
            "2) 你只需要 Reply 那条“转发自用户”的消息（可发文字/图片/文件等），机器人会转发给用户。\n"
            "3) 支持严格互译：用户非中文 -> 自动翻译成中文发给你；你发中文 -> 自动翻译成用户语言发给用户。\n"
            "4) 企业微信内部成员消息也会进来：Reply “WeCom 来信”那条即可回企业微信（当前只支持文本）。\n"
        )
    else:
        await update.message.reply_text(
            "你好，欢迎联系。\n"
            "请直接发送你的消息（文字/图片/文件等）。我们收到后会尽快回复。\n",
            reply_markup=contact_admin_keyboard()
        )


# ================== TG CALLBACKS ==================
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.callback_query:
        return
    q = update.callback_query
    await q.answer()

    if not is_admin(update):
        return

    data = q.data or ""
    parts = data.split("|")
    if len(parts) < 2:
        return

    action = parts[0]
    uid = int(parts[1])
    st = load_state()

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


# ================== TG USER -> ADMIN ==================
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

    meta = st.setdefault("user_meta", {}).setdefault(str(uid), {})
    meta.setdefault("first_seen", _now_ts())
    meta["last_seen"] = _now_ts()
    meta["msg_count"] = int(meta.get("msg_count", 0) or 0) + 1
    meta["name"] = (getattr(user, "full_name", "") or "Unknown").strip()
    meta["username"] = getattr(user, "username", None)
    meta["language_code"] = getattr(user, "language_code", "")

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
        remember_route_index(st, fwd.message_id, uid)  # int => tg
    except Exception:
        copied = await context.bot.copy_message(
            chat_id=ADMIN_ID,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id,
        )
        forwarded_id = copied.message_id
        remember_route_index(st, copied.message_id, uid)

    if t.get("header_msg_id"):
        remember_route_index(st, int(t["header_msg_id"]), uid)

    txt = (update.message.text or update.message.caption or "").strip()
    if txt:
        src = detect_lang(txt)
        meta["last_detected_lang"] = src

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


# ================== WeCom Crypto (callback verify/decrypt) ==================
def _sha1_signature(token: str, timestamp: str, nonce: str, encrypt_or_echo: str) -> str:
    arr = [token, timestamp, nonce, encrypt_or_echo]
    arr.sort()
    s = "".join(arr).encode("utf-8")
    return hashlib.sha1(s).hexdigest()


def _pkcs7_unpad(data: bytes) -> bytes:
    pad = data[-1]
    if pad < 1 or pad > 32:
        raise ValueError("bad padding")
    return data[:-pad]


def _aes_key_bytes(aes_key_43: str) -> bytes:
    # 43位 EncodingAESKey -> base64解码后32字节
    return base64.b64decode(aes_key_43 + "=")


def _aes_decrypt(ciphertext_b64: str, aeskey_43: str) -> bytes:
    key = _aes_key_bytes(aeskey_43)
    cipher = AES.new(key, AES.MODE_CBC, iv=key[:16])
    plain = cipher.decrypt(base64.b64decode(ciphertext_b64))
    plain = _pkcs7_unpad(plain)
    return plain


def _decode_wecom_plain(plain: bytes, corp_id: str) -> str:
    # 格式：16字节随机串 + 4字节网络序长度 + msg + corpid
    msg_len = struct.unpack("!I", plain[16:20])[0]
    msg = plain[20:20 + msg_len]
    corp = plain[20 + msg_len:].decode("utf-8")
    if corp != corp_id:
        raise ValueError("corp_id mismatch")
    return msg.decode("utf-8")


def _xml_get_text(xml_str: str, tag: str) -> str:
    try:
        root = ET.fromstring(xml_str)
        el = root.find(tag)
        return (el.text or "").strip() if el is not None else ""
    except Exception:
        return ""


# ================== WeCom API (send message) ==================
_wecom_access_token: Optional[str] = None
_wecom_token_expire_at: int = 0


async def wecom_get_access_token() -> str:
    """
    企业微信 access_token（应用）
    https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid=ID&corpsecret=SECRET
    """
    global _wecom_access_token, _wecom_token_expire_at

    now = int(time.time())
    if _wecom_access_token and now < (_wecom_token_expire_at - 60):
        return _wecom_access_token

    if not (WECOM_CORP_ID and WECOM_AGENT_SECRET):
        raise RuntimeError("Missing WECOM_CORP_ID or WECOM_AGENT_SECRET")

    url = "https://qyapi.weixin.qq.com/cgi-bin/gettoken"
    params = {"corpid": WECOM_CORP_ID, "corpsecret": WECOM_AGENT_SECRET}

    s = await _session()
    async with s.get(url, params=params) as resp:
        data = await resp.json(content_type=None)

    if not isinstance(data, dict) or data.get("errcode", 0) != 0:
        raise RuntimeError(f"wecom gettoken failed: {data}")

    _wecom_access_token = data.get("access_token")
    expires_in = int(data.get("expires_in", 7200) or 7200)
    _wecom_token_expire_at = now + expires_in
    return _wecom_access_token


async def wecom_send_text(to_userid: str, text: str) -> None:
    """
    发送文本给内部成员
    https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token=ACCESS_TOKEN
    """
    if not (WECOM_CORP_ID and WECOM_AGENT_ID and WECOM_AGENT_SECRET):
        raise RuntimeError("Missing WECOM_CORP_ID / WECOM_AGENT_ID / WECOM_AGENT_SECRET")

    token = await wecom_get_access_token()
    url = "https://qyapi.weixin.qq.com/cgi-bin/message/send"
    params = {"access_token": token}

    payload = {
        "touser": to_userid,
        "msgtype": "text",
        "agentid": WECOM_AGENT_ID,
        "text": {"content": text},
        "safe": 0,
    }

    s = await _session()
    async with s.post(url, params=params, json=payload) as resp:
        data = await resp.json(content_type=None)

    if not isinstance(data, dict) or data.get("errcode", 0) != 0:
        raise RuntimeError(f"wecom send failed: {data}")


# ================== WeCom -> TG (process decrypted message) ==================
async def process_wecom_plain_xml(tg_app: Application, plain_xml: str) -> None:
    """
    将企业微信消息转发给 TG 管理员，并记录映射：
    admin_message_id -> wecom:<userid>
    """
    from_user = _xml_get_text(plain_xml, "FromUserName")   # 内部成员 userid
    msg_type = _xml_get_text(plain_xml, "MsgType")
    content = _xml_get_text(plain_xml, "Content")

    if not from_user:
        return

    st = load_state()

    wm = st.setdefault("wecom_meta", {}).setdefault(from_user, {})
    wm.setdefault("first_seen", _now_ts())
    wm["last_seen"] = _now_ts()
    wm["msg_count"] = int(wm.get("msg_count", 0) or 0) + 1

    # 只对文本做语言检测/翻译
    src_lang = "auto"
    if msg_type == "text" and content:
        src_lang = detect_lang(content)
        wm["last_detected_lang"] = src_lang

    # 发给 TG 管理员
    # 注意：让管理员 Reply 这条即可回企业微信
    title = f"📥 <b>WeCom 来信</b>\n<b>UserID:</b> <code>{_safe(from_user)}</code>\n<b>MsgType:</b> <code>{_safe(msg_type)}</code>"
    body = ""
    if msg_type == "text":
        body = f"\n\n<b>内容：</b>\n{_safe(content)}"
    else:
        body = f"\n\n<b>内容：</b>\n（暂不支持该类型：{_safe(msg_type)}，请让对方发送文本）"

    msg = await tg_app.bot.send_message(
        chat_id=ADMIN_ID,
        text=title + body,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )

    remember_route_index(st, msg.message_id, f"wecom:{from_user}")

    # 如需翻译：非中文 -> 翻译成中文，回复在管理员消息下
    if TRANSLATE_ENABLED and msg_type == "text" and content:
        if _norm_lang(src_lang) != "zh-CN":
            zh = await translate(content, src_lang, "zh-CN")
            if zh and zh.strip() and zh.strip() != content.strip():
                try:
                    await tg_app.bot.send_message(
                        chat_id=ADMIN_ID,
                        text=f"中文翻译（{_safe(src_lang)} → zh-CN）：\n{_safe(zh)}",
                        reply_to_message_id=msg.message_id,
                    )
                except Exception:
                    pass

    save_state(st)


# ================== ADMIN Reply -> (TG user OR WeCom internal member) ==================
async def handle_admin_private(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat:
        return
    if update.effective_chat.type != ChatType.PRIVATE:
        return
    if not is_admin(update):
        return
    if not update.message.reply_to_message:
        return

    st = load_state()
    reply_mid = int(update.message.reply_to_message.message_id)
    route_type, route_id = resolve_route(st, reply_mid)

    if route_type == "":
        try:
            await update.message.reply_text("没识别到目标：请 Reply 用户的“转发自用户”消息，或 Reply ‘WeCom 来信’那条消息。")
        except Exception:
            pass
        return

    admin_text = (update.message.text or "").strip()
    admin_caption = (update.message.caption or "").strip()

    # ====== Route: WeCom ======
    if route_type == "wecom":
        wecom_userid = str(route_id or "").strip()
        if not wecom_userid:
            return

        # 企业微信通道：先只支持文本（最稳）
        if not admin_text:
            try:
                await update.message.reply_text("企业微信通道当前仅支持文本回复。请直接发送文本并 Reply ‘WeCom 来信’。")
            except Exception:
                pass
            return

        # 目标语言：按 wecom_meta 里的 last_detected_lang（严格互译）
        wm = (st.get("wecom_meta") or {}).get(wecom_userid, {})
        user_lang = _norm_lang(wm.get("last_detected_lang", "zh-CN"))
        if user_lang == "auto":
            user_lang = "zh-CN"

        send_text = admin_text
        if TRANSLATE_ENABLED and _is_chinese(admin_text) and user_lang != "zh-CN":
            tr = await translate(admin_text, "zh-CN", user_lang)
            if tr and tr.strip():
                send_text = tr.strip()

        try:
            await wecom_send_text(wecom_userid, send_text)
            await update.message.reply_text("已发送到企业微信。")
        except Exception as e:
            try:
                await update.message.reply_text(f"发送到企业微信失败：{e}")
            except Exception:
                pass
        return

    # ====== Route: Telegram user ======
    if route_type == "tg":
        to_user = int(route_id or 0)
        if to_user <= 0:
            return

        user_meta = (st.get("user_meta") or {}).get(str(to_user), {})
        user_lang = _norm_lang(user_meta.get("last_detected_lang", "en"))
        if user_lang == "auto":
            user_lang = "en"

        try:
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

            st["last_user"] = to_user
            save_state(st)
            try:
                await update.message.reply_text("已发送。")
            except Exception:
                pass

        except Exception as e:
            try:
                await update.message.reply_text(f"发送失败：{e}")
            except Exception:
                pass
        return


# ================== TG: non-admin private handler ==================
# (kept as-is from your flow)
async def handle_user_private_guard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await handle_user_private(update, context)


# ================== WeCom Callback Handlers ==================
async def wecom_callback_get(request: web.Request):
    # 企业微信“保存”时 GET 校验
    if not (WECOM_CB_TOKEN and WECOM_CB_AESKEY and WECOM_CORP_ID):
        return web.Response(status=500, text="missing wecom env")

    qs = request.query
    msg_signature = qs.get("msg_signature", "")
    timestamp = qs.get("timestamp", "")
    nonce = qs.get("nonce", "")
    echostr = qs.get("echostr", "")

    if not (msg_signature and timestamp and nonce and echostr):
        return web.Response(status=400, text="bad query")

    try:
        sig = _sha1_signature(WECOM_CB_TOKEN, timestamp, nonce, echostr)
        if sig != msg_signature:
            return web.Response(status=403, text="bad signature")

        plain = _aes_decrypt(echostr, WECOM_CB_AESKEY)
        out = _decode_wecom_plain(plain, WECOM_CORP_ID)
        return web.Response(text=out)
    except Exception as e:
        print("wecom verify failed:", repr(e))
        return web.Response(status=403, text="verify failed")


async def wecom_callback_post(request: web.Request):
    """
    企业微信推送消息 POST（加密）
    - 验签：sha1(token,timestamp,nonce,Encrypt)
    - 解密：Decrypt Encrypt 得到明文 xml
    - 立即返回 success（避免重试）
    - 异步转发到 TG 管理员
    """
    if not (WECOM_CB_TOKEN and WECOM_CB_AESKEY and WECOM_CORP_ID):
        return web.Response(status=500, text="missing wecom env")

    qs = request.query
    msg_signature = qs.get("msg_signature", "")
    timestamp = qs.get("timestamp", "")
    nonce = qs.get("nonce", "")

    body = await request.read()
    if not body:
        return web.Response(status=400, text="empty body")

    try:
        root = ET.fromstring(body.decode("utf-8"))
        encrypt = (root.findtext("Encrypt") or "").strip()
    except Exception:
        return web.Response(status=400, text="bad xml")

    if not (msg_signature and timestamp and nonce and encrypt):
        return web.Response(status=400, text="bad query")

    # 先快速验签
    try:
        sig = _sha1_signature(WECOM_CB_TOKEN, timestamp, nonce, encrypt)
        if sig != msg_signature:
            return web.Response(status=403, text="bad signature")
    except Exception:
        return web.Response(status=403, text="bad signature")

    # 立刻响应，异步处理
    resp = web.Response(text="success")

    async def _process():
        try:
            plain_bytes = _aes_decrypt(encrypt, WECOM_CB_AESKEY)
            plain_xml = _decode_wecom_plain(plain_bytes, WECOM_CORP_ID)

            tg_app: Application = request.app["tg_app"]
            await process_wecom_plain_xml(tg_app, plain_xml)
        except Exception as e:
            print("wecom post process error:", repr(e))

    asyncio.create_task(_process())
    return resp


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
    aio["tg_app"] = tg_app

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
                update = Update.de_json(data, tg_app.bot)
                await tg_app.process_update(update)
            except Exception as e:
                print("process_update error:", repr(e))

        asyncio.create_task(_process())
        return resp

    aio.router.add_get(HEALTH_PATH, health)
    aio.router.add_post(webhook_path, handle_update)

    # WeCom callback
    aio.router.add_get("/wecom/callback", wecom_callback_get)
    aio.router.add_post("/wecom/callback", wecom_callback_post)

    runner = web.AppRunner(aio)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=PORT)
    await site.start()

    print(f"[ok] tg webhook set: {webhook_url}")
    print(f"[ok] listening on 0.0.0.0:{PORT}, health: {HEALTH_PATH}")
    print("[ok] wecom callback: /wecom/callback")

    await asyncio.Event().wait()


def main():
    if not TOKEN:
        raise SystemExit("Missing TG_BOT_TOKEN")
    if ADMIN_ID <= 0:
        raise SystemExit("Missing TG_ADMIN_ID")

    tg_app = Application.builder().token(TOKEN).build()

    tg_app.add_handler(CommandHandler("start", cmd_start))
    tg_app.add_handler(CallbackQueryHandler(on_callback))

    # Telegram private handlers
    tg_app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.User(user_id=ADMIN_ID), handle_user_private_guard))
    tg_app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.User(user_id=ADMIN_ID), handle_admin_private))

    if PUBLIC_URL:
        asyncio.run(run_webhook_server(tg_app))
    else:
        tg_app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
