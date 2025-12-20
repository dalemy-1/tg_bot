import asyncio
import os
import re
import json
import time
import base64
import hashlib
import struct
import asyncio
from pathlib import Path
from typing import Any, Dict, Optional
from typing import Any, Dict, Optional, Tuple, Union
from xml.etree import ElementTree as ET

from aiohttp import web, ClientSession, ClientTimeout

@@ -21,43 +24,57 @@
filters,
)

# PyCryptodome
from Crypto.Cipher import AES


# ================== ENV ==================
# Telegram
TOKEN = (os.getenv("TG_BOT_TOKEN") or "").strip()
ADMIN_ID = int(os.getenv("TG_ADMIN_ID", "0") or "0")
print("[boot] TG_BOT_TOKEN prefix:", (TOKEN or "")[:10], "len:", len(TOKEN or ""), "tail:", (TOKEN or "")[-4:])
print("[boot] RENDER_EXTERNAL_URL:", (os.getenv("RENDER_EXTERNAL_URL") or "")[:80])
print("[boot] PUBLIC_URL:", (os.getenv("PUBLIC_URL") or "")[:80])


ADMIN_USERNAME = (os.getenv("ADMIN_USERNAME") or "Adalemy").strip().lstrip("@")

PUBLIC_URL = (os.getenv("RENDER_EXTERNAL_URL") or os.getenv("PUBLIC_URL") or "").strip().rstrip("/")
WEBHOOK_SECRET = (os.getenv("WEBHOOK_SECRET") or "").strip()
PORT = int(os.getenv("PORT", "10000"))
HEALTH_PATH = "/healthz"

ADMIN_USERNAME = (os.getenv("ADMIN_USERNAME") or "Adalemy").strip().lstrip("@")

AUTO_REPLY_TEXT = (os.getenv("AUTO_REPLY_TEXT") or "你好，已收到你的消息，我们会尽快回复。").strip()
AUTO_REPLY_COOLDOWN_SEC = int(os.getenv("AUTO_REPLY_COOLDOWN_SEC", "86400"))  # 24h 默认
AUTO_REPLY_COOLDOWN_SEC = int(os.getenv("AUTO_REPLY_COOLDOWN_SEC", "86400"))  # 24h

TRANSLATE_ENABLED = (os.getenv("TRANSLATE_ENABLED") or "1").strip() == "1"
ADMIN_LANG = "zh-CN"  # 管理员侧统一中文

# 免费翻译后端：可选 LibreTranslate + 兜底 MyMemory
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


# ================== STATE ==================
def _now_ts() -> int:
return int(time.time())

@@ -70,12 +87,15 @@ def load_state() -> Dict[str, Any]:
pass
return {
"ticket_seq": 0,
        "tickets": {},          # user_id(str) -> {ticket_id, status, created_at, header_msg_id}
        "msg_index": {},        # admin_message_id(str) -> user_id(int)
        "tickets": {},            # tg user_id(str) -> {ticket_id, created_at, header_msg_id}
        "msg_index": {},          # admin_message_id(str) -> route (int tg_uid OR "wecom:<userid>" OR "tg:<uid>")
"last_user": 0,
        "last_auto_reply": {},  # user_id(str) -> ts
        "user_meta": {},        # user_id(str) -> {name, username, language_code, first_seen, last_seen, msg_count, last_detected_lang}
        "user_status": {},      # user_id(str) -> status

        "last_auto_reply": {},    # tg user_id(str) -> ts
        "user_meta": {},          # tg user_id(str) -> meta
        "user_status": {},        # tg user_id(str) -> status

        "wecom_meta": {},         # wecom_userid(str) -> meta {first_seen,last_seen,msg_count,last_detected_lang}
}


@@ -87,16 +107,39 @@ def is_admin(update: Update) -> bool:
return bool(update.effective_user and update.effective_user.id == ADMIN_ID and ADMIN_ID > 0)


def remember_msg_index(state: Dict[str, Any], admin_message_id: int, user_id: int) -> None:
def remember_route_index(state: Dict[str, Any], admin_message_id: int, route: Union[int, str]) -> None:
    """
    route:
      - int: Telegram user id (backward compatible)
      - "tg:<uid>": Telegram user
      - "wecom:<userid>": WeCom internal member userid
    """
mi = state.setdefault("msg_index", {})
    mi[str(admin_message_id)] = int(user_id)
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
@@ -107,7 +150,7 @@ def _safe(s: str) -> str:
return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# ================== TRANSLATION (strict mutual) ==================
# ================== TRANSLATION ==================
_http: Optional[ClientSession] = None


@@ -211,7 +254,6 @@ async def translate(text: str, src: str, tgt: str) -> Optional[str]:
if src == "auto":
src = detect_lang(q)
if src == "auto":
            # 兜底：如果目标是中文，就当英文；否则当中文
src = "en" if tgt == "zh-CN" else "zh-CN"

if src == tgt:
@@ -228,15 +270,14 @@ async def translate(text: str, src: str, tgt: str) -> Optional[str]:
return None


# ================== UI ==================
# ================== TG UI ==================
def contact_admin_keyboard() -> InlineKeyboardMarkup:
return InlineKeyboardMarkup([
[InlineKeyboardButton("联系管理员", url=f"https://t.me/{ADMIN_USERNAME}")]
])


def status_keyboard(uid: int) -> InlineKeyboardMarkup:
    # 只保留：已下单 / 退货退款 / 已返款 / 黑名单 / 清空状态 + Profile(可选保留)
row1 = [
InlineKeyboardButton("已下单", callback_data=f"status|{uid}|已下单"),
InlineKeyboardButton("退货退款", callback_data=f"status|{uid}|退货退款"),
@@ -298,8 +339,6 @@ async def ensure_ticket(state: Dict[str, Any], context: ContextTypes.DEFAULT_TYP
if need_new:
state["ticket_seq"] = int(state.get("ticket_seq", 0)) + 1
ticket_id = state["ticket_seq"]

        # 初始化状态
state.setdefault("user_status", {}).setdefault(uid_key, DEFAULT_STATUS)

msg = await context.bot.send_message(
@@ -336,7 +375,7 @@ async def refresh_header(state: Dict[str, Any], context: ContextTypes.DEFAULT_TY
pass


# ================== COMMANDS (keep minimal) ==================
# ================== TG COMMANDS ==================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
if is_admin(update):
await update.message.reply_text(
@@ -345,6 +384,7 @@ async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
"1) 用户给机器人发消息 -> 你会收到“转发自用户”的消息。\n"
"2) 你只需要 Reply 那条“转发自用户”的消息（可发文字/图片/文件等），机器人会转发给用户。\n"
"3) 支持严格互译：用户非中文 -> 自动翻译成中文发给你；你发中文 -> 自动翻译成用户语言发给用户。\n"
            "4) 企业微信内部成员消息也会进来：Reply “WeCom 来信”那条即可回企业微信（当前只支持文本）。\n"
)
else:
await update.message.reply_text(
@@ -354,7 +394,7 @@ async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
)


# ================== CALLBACKS (admin buttons) ==================
# ================== TG CALLBACKS ==================
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
if not update.callback_query:
return
@@ -388,7 +428,6 @@ async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
return

if action == "profile":
        # 只复发一份 header 作为 profile（无需命令）
try:
await context.bot.send_message(
chat_id=ADMIN_ID,
@@ -402,7 +441,7 @@ async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
return


# ================== USER -> ADMIN (forward + translate to zh) ==================
# ================== TG USER -> ADMIN ==================
async def handle_user_private(update: Update, context: ContextTypes.DEFAULT_TYPE):
if not update.message or not update.effective_chat:
return
@@ -418,7 +457,6 @@ async def handle_user_private(update: Update, context: ContextTypes.DEFAULT_TYPE

st = load_state()

    # meta
meta = st.setdefault("user_meta", {}).setdefault(str(uid), {})
meta.setdefault("first_seen", _now_ts())
meta["last_seen"] = _now_ts()
@@ -427,13 +465,9 @@ async def handle_user_private(update: Update, context: ContextTypes.DEFAULT_TYPE
meta["username"] = getattr(user, "username", None)
meta["language_code"] = getattr(user, "language_code", "")

    # ticket/header
t = await ensure_ticket(st, context, uid)

    # 如果状态为空/清空后，保持“用户来信”
st.setdefault("user_status", {}).setdefault(str(uid), DEFAULT_STATUS)

    # 转发给管理员（保留“转发自用户”）
forwarded_id = None
try:
fwd = await context.bot.forward_message(
@@ -442,27 +476,24 @@ async def handle_user_private(update: Update, context: ContextTypes.DEFAULT_TYPE
message_id=update.message.message_id,
)
forwarded_id = fwd.message_id
        remember_msg_index(st, fwd.message_id, uid)
        remember_route_index(st, fwd.message_id, uid)  # int => tg
except Exception:
copied = await context.bot.copy_message(
chat_id=ADMIN_ID,
from_chat_id=update.effective_chat.id,
message_id=update.message.message_id,
)
forwarded_id = copied.message_id
        remember_msg_index(st, copied.message_id, uid)
        remember_route_index(st, copied.message_id, uid)

    # 也把 header 记入 index（防止管理员误 Reply header）
if t.get("header_msg_id"):
        remember_msg_index(st, int(t["header_msg_id"]), uid)
        remember_route_index(st, int(t["header_msg_id"]), uid)

    # 检测语言（以用户消息为准）
txt = (update.message.text or update.message.caption or "").strip()
if txt:
src = detect_lang(txt)
meta["last_detected_lang"] = src

        # 严格：非中文 -> 翻译成中文发给管理员（贴在转发下面）
if TRANSLATE_ENABLED and _norm_lang(src) != "zh-CN" and forwarded_id:
zh = await translate(txt, src, "zh-CN")
if zh and zh.strip() and zh.strip() != txt.strip():
@@ -475,7 +506,6 @@ async def handle_user_private(update: Update, context: ContextTypes.DEFAULT_TYPE
except Exception:
pass

    # 自动回复（24小时一次）
last_ts = int((st.get("last_auto_reply") or {}).get(str(uid), 0) or 0)
now_ts = _now_ts()
if now_ts - last_ts >= AUTO_REPLY_COOLDOWN_SEC:
@@ -490,127 +520,292 @@ async def handle_user_private(update: Update, context: ContextTypes.DEFAULT_TYPE
await refresh_header(st, context, uid)


# ================== ADMIN Reply -> USER (support media + zh->user lang) ==================
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

    # 必须 Reply 才转发（避免误发）
if not update.message.reply_to_message:
return

st = load_state()
    reply_mid = int(update.message.reply_to_message.message_id)
    route_type, route_id = resolve_route(st, reply_mid)

    rid = str(update.message.reply_to_message.message_id)
    to_user = None
    if rid in (st.get("msg_index") or {}):
        to_user = int(st["msg_index"][rid])

    if not to_user:
        # 没识别到就不发
    if route_type == "":
try:
            await update.message.reply_text("没识别到用户ID：请 Reply 用户的“转发自用户”消息。")
            await update.message.reply_text("没识别到目标：请 Reply 用户的“转发自用户”消息，或 Reply ‘WeCom 来信’那条消息。")
except Exception:
pass
return

    # 目标语言：以用户最后检测语言为准（严格互译的关键）
    user_meta = (st.get("user_meta") or {}).get(str(to_user), {})
    user_lang = _norm_lang(user_meta.get("last_detected_lang", "en"))
    if user_lang == "auto":
        user_lang = "en"

    # 1) 先把管理员消息 copy 给用户（保证媒体可达）
    # 2) 如需翻译，则额外再发一条“翻译后的文本”（文本消息则直接发翻译）
    try:
        admin_text = (update.message.text or "").strip()
        admin_caption = (update.message.caption or "").strip()

        # 文本消息：直接发翻译后的文本
        if admin_text:
            send_text = admin_text
            if TRANSLATE_ENABLED and _is_chinese(admin_text) and user_lang != "zh-CN":
                tr = await translate(admin_text, "zh-CN", user_lang)
                if tr and tr.strip():
                    send_text = tr.strip()
            await context.bot.send_message(chat_id=to_user, text=send_text)

        else:
            # 媒体/文件/贴纸等：先 copy 原消息
            await context.bot.copy_message(
                chat_id=to_user,
                from_chat_id=update.effective_chat.id,
                message_id=update.message.message_id,
            )
    admin_text = (update.message.text or "").strip()
    admin_caption = (update.message.caption or "").strip()

            # 如果有 caption 且为中文，则再补发翻译文本
            if admin_caption and TRANSLATE_ENABLED and _is_chinese(admin_caption) and user_lang != "zh-CN":
                tr = await translate(admin_caption, "zh-CN", user_lang)
                if tr and tr.strip():
                    await context.bot.send_message(chat_id=to_user, text=tr.strip())
    # ====== Route: WeCom ======
    if route_type == "wecom":
        wecom_userid = str(route_id or "").strip()
        if not wecom_userid:
            return

        st["last_user"] = to_user
        save_state(st)
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
            await update.message.reply_text("已发送。")
        except Exception:
            pass
            await wecom_send_text(wecom_userid, send_text)
            await update.message.reply_text("已发送到企业微信。")
        except Exception as e:
            try:
                await update.message.reply_text(f"发送到企业微信失败：{e}")
            except Exception:
                pass
        return

    except Exception as e:
        try:
            await update.message.reply_text(f"发送失败：{e}")
        except Exception:
            pass
            
    # ====== Route: Telegram user ======
    if route_type == "tg":
        to_user = int(route_id or 0)
        if to_user <= 0:
            return

import base64
import hashlib
import struct
from Crypto.Cipher import AES
from xml.etree import ElementTree as ET
        user_meta = (st.get("user_meta") or {}).get(str(to_user), {})
        user_lang = _norm_lang(user_meta.get("last_detected_lang", "en"))
        if user_lang == "auto":
            user_lang = "en"

WECOM_CB_TOKEN = (os.getenv("WECOM_CB_TOKEN") or "").strip()
WECOM_CB_AESKEY = (os.getenv("WECOM_CB_AESKEY") or "").strip()
WECOM_CORP_ID = (os.getenv("WECOM_CORP_ID") or "").strip()
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

def _sha1_signature(token: str, timestamp: str, nonce: str, echostr: str) -> str:
    arr = [token, timestamp, nonce, echostr]
    arr.sort()
    s = "".join(arr).encode("utf-8")
    return hashlib.sha1(s).hexdigest()
        except Exception as e:
            try:
                await update.message.reply_text(f"发送失败：{e}")
            except Exception:
                pass
        return

def _pkcs7_unpad(data: bytes) -> bytes:
    pad = data[-1]
    if pad < 1 or pad > 32:
        raise ValueError("bad padding")
    return data[:-pad]

def _aes_key_bytes(aes_key_43: str) -> bytes:
    # 43位 EncodingAESKey -> base64解码后32字节
    return base64.b64decode(aes_key_43 + "=")
# ================== TG: non-admin private handler ==================
# (kept as-is from your flow)
async def handle_user_private_guard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await handle_user_private(update, context)

def _decrypt_echostr(echostr_b64: str) -> str:
    key = _aes_key_bytes(WECOM_CB_AESKEY)
    cipher = AES.new(key, AES.MODE_CBC, iv=key[:16])
    plain = cipher.decrypt(base64.b64decode(echostr_b64))
    plain = _pkcs7_unpad(plain)

    # 格式：16字节随机串 + 4字节网络序长度 + msg + corpid
    msg_len = struct.unpack("!I", plain[16:20])[0]
    msg = plain[20:20 + msg_len]
    corp = plain[20 + msg_len:].decode("utf-8")

    if corp != WECOM_CORP_ID:
        raise ValueError("corp_id mismatch")
    return msg.decode("utf-8")

# ================== WeCom Callback Handlers ==================
async def wecom_callback_get(request: web.Request):
    # 企业微信保存时会GET校验
    # 企业微信“保存”时 GET 校验
if not (WECOM_CB_TOKEN and WECOM_CB_AESKEY and WECOM_CORP_ID):
return web.Response(status=500, text="missing wecom env")

@@ -628,23 +823,69 @@ async def wecom_callback_get(request: web.Request):
if sig != msg_signature:
return web.Response(status=403, text="bad signature")

        plain = _decrypt_echostr(echostr)
        # 必须返回解密后的明文
        return web.Response(text=plain)
        plain = _aes_decrypt(echostr, WECOM_CB_AESKEY)
        out = _decode_wecom_plain(plain, WECOM_CORP_ID)
        return web.Response(text=out)
except Exception as e:
print("wecom verify failed:", repr(e))
return web.Response(status=403, text="verify failed")


async def wecom_callback_post(request: web.Request):
"""
    先占位：确保企业微信POST回调不会502/超时。
    真正做“双向：企业微信回复->TG”时，我们再在这里解析xml并转发。
    企业微信推送消息 POST（加密）
    - 验签：sha1(token,timestamp,nonce,Encrypt)
    - 解密：Decrypt Encrypt 得到明文 xml
    - 立即返回 success（避免重试）
    - 异步转发到 TG 管理员
   """
    return web.Response(text="success")
    if not (WECOM_CB_TOKEN and WECOM_CB_AESKEY and WECOM_CORP_ID):
        return web.Response(status=500, text="missing wecom env")

# ================== WEBHOOK SERVER ==================
import asyncio  # 建议放文件顶部（如果你顶部已经 import 过，就不要重复）
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
@@ -656,26 +897,23 @@ async def run_webhook_server(tg_app: Application):
webhook_path = f"/{WEBHOOK_SECRET}"
webhook_url = f"{PUBLIC_URL}{webhook_path}"

    # 先启动 PTB
await tg_app.initialize()
await tg_app.start()

    # 再设置 webhook
await tg_app.bot.set_webhook(url=webhook_url, drop_pending_updates=True)

aio = web.Application()
    aio["tg_app"] = tg_app

async def health(_request):
return web.Response(text="ok")

async def handle_update(request: web.Request):
        # 只做最轻量的事情：读 json + 立刻回 ok
try:
data = await request.json()
except Exception:
return web.Response(status=400, text="bad json")

        resp = web.Response(text="ok")  # 立刻响应 Telegram，避免 Read timeout expired
        resp = web.Response(text="ok")

async def _process():
try:
@@ -687,28 +925,25 @@ async def _process():
asyncio.create_task(_process())
return resp

    # 路由注册必须在这里（不能缩进到 handle_update 里）
aio.router.add_get(HEALTH_PATH, health)
aio.router.add_post(webhook_path, handle_update)
    

    # WeCom callback
aio.router.add_get("/wecom/callback", wecom_callback_get)
aio.router.add_post("/wecom/callback", wecom_callback_post)


runner = web.AppRunner(aio)
await runner.setup()
site = web.TCPSite(runner, host="0.0.0.0", port=PORT)
await site.start()

    print(f"[ok] webhook set: {webhook_url}")
    print(f"[ok] tg webhook set: {webhook_url}")
print(f"[ok] listening on 0.0.0.0:{PORT}, health: {HEALTH_PATH}")
    print("[ok] wecom callback: /wecom/callback")

    # 常驻不退出
await asyncio.Event().wait()




def main():
if not TOKEN:
raise SystemExit("Missing TG_BOT_TOKEN")
@@ -717,14 +952,11 @@ def main():

tg_app = Application.builder().token(TOKEN).build()

    # Minimal command
tg_app.add_handler(CommandHandler("start", cmd_start))

    # Buttons (admin)
tg_app.add_handler(CallbackQueryHandler(on_callback))

    # Private handlers
    tg_app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.User(user_id=ADMIN_ID), handle_user_private))
    # Telegram private handlers
    tg_app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.User(user_id=ADMIN_ID), handle_user_private_guard))
tg_app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.User(user_id=ADMIN_ID), handle_admin_private))

if PUBLIC_URL:
@@ -735,11 +967,3 @@ def main():

if __name__ == "__main__":
main()







