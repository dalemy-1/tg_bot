import asyncio
import base64
import hashlib
import json
import os
import struct
import time
from typing import Any, Dict, Optional
from xml.etree import ElementTree as ET

from aiohttp import web, ClientSession, ClientTimeout
from Crypto.Cipher import AES

from telegram import Update
from telegram.constants import ChatType
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# ================== ENV ==================
TG_TOKEN = (os.getenv("TG_BOT_TOKEN") or "").strip()
TG_ADMIN_ID = int(os.getenv("TG_ADMIN_ID", "0") or "0")

PUBLIC_URL = (os.getenv("RENDER_EXTERNAL_URL") or os.getenv("PUBLIC_URL") or "").strip().rstrip("/")
WEBHOOK_SECRET = (os.getenv("WEBHOOK_SECRET") or "").strip()
PORT = int(os.getenv("PORT", "10000"))

# WeCom
WECOM_CORP_ID = (os.getenv("WECOM_CORP_ID") or "").strip()
WECOM_AGENT_ID = (os.getenv("WECOM_AGENT_ID") or "").strip()
WECOM_APP_SECRET = (os.getenv("WECOM_APP_SECRET") or "").strip()

WECOM_CB_TOKEN = (os.getenv("WECOM_CB_TOKEN") or "").strip()
WECOM_CB_AESKEY = (os.getenv("WECOM_CB_AESKEY") or "").strip()

STATE_FILE = "bridge_state.json"

print("[boot] PUBLIC_URL:", PUBLIC_URL)
print("[boot] TG_ADMIN_ID:", TG_ADMIN_ID)
print("[boot] WECOM_CORP_ID prefix:", (WECOM_CORP_ID or "")[:6], "AGENT:", WECOM_AGENT_ID)

# ================== STATE ==================
def load_state() -> Dict[str, Any]:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"wecom_index": {}}  # tg_message_id(str) -> wecom_userid(str)

def save_state(st: Dict[str, Any]) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False, indent=2)

def is_admin(update: Update) -> bool:
    return bool(update.effective_user and update.effective_user.id == TG_ADMIN_ID and TG_ADMIN_ID > 0)

# ================== HTTP ==================
_http: Optional[ClientSession] = None

async def session() -> ClientSession:
    global _http
    if _http is None or _http.closed:
        _http = ClientSession(timeout=ClientTimeout(total=12))
    return _http

# ================== WeCom Crypto ==================
def _sha1_signature(token: str, timestamp: str, nonce: str, encrypt: str) -> str:
    arr = [token, timestamp, nonce, encrypt]
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

def wecom_decrypt(encrypt_b64: str) -> str:
    """
    解密企业微信回调的 <Encrypt>xxx</Encrypt> 内容，返回明文 XML
    格式：16随机 + 4字节len + xml + corpId
    """
    key = _aes_key_bytes(WECOM_CB_AESKEY)
    cipher = AES.new(key, AES.MODE_CBC, iv=key[:16])
    plain = cipher.decrypt(base64.b64decode(encrypt_b64))
    plain = _pkcs7_unpad(plain)

    msg_len = struct.unpack("!I", plain[16:20])[0]
    xml = plain[20:20 + msg_len]
    corp = plain[20 + msg_len:].decode("utf-8")
    if corp != WECOM_CORP_ID:
        raise ValueError("corp_id mismatch")
    return xml.decode("utf-8")

def xml_text(root: ET.Element, tag: str) -> str:
    el = root.find(tag)
    return (el.text or "").strip() if el is not None else ""

# ================== WeCom API (send message) ==================
_wecom_token_cache: Dict[str, Any] = {"token": "", "exp": 0}

async def wecom_get_access_token() -> str:
    now = int(time.time())
    if _wecom_token_cache["token"] and now < int(_wecom_token_cache["exp"]):
        return _wecom_token_cache["token"]

    if not (WECOM_CORP_ID and WECOM_APP_SECRET):
        raise RuntimeError("Missing WECOM_CORP_ID / WECOM_APP_SECRET")

    url = "https://qyapi.weixin.qq.com/cgi-bin/gettoken"
    params = {"corpid": WECOM_CORP_ID, "corpsecret": WECOM_APP_SECRET}
    s = await session()
    async with s.get(url, params=params) as resp:
        data = await resp.json(content_type=None)

    if int(data.get("errcode", -1)) != 0:
        raise RuntimeError(f"wecom gettoken failed: {data}")

    token = data.get("access_token", "")
    expires_in = int(data.get("expires_in", 7200))
    _wecom_token_cache["token"] = token
    _wecom_token_cache["exp"] = now + max(60, expires_in - 120)
    return token

async def wecom_send_text(touser: str, text: str) -> None:
    token = await wecom_get_access_token()
    url = f"https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={token}"
    payload = {
        "touser": touser,
        "msgtype": "text",
        "agentid": int(WECOM_AGENT_ID),
        "text": {"content": text},
        "safe": 0,
    }
    s = await session()
    async with s.post(url, json=payload) as resp:
        data = await resp.json(content_type=None)
    if int(data.get("errcode", -1)) != 0:
        raise RuntimeError(f"wecom send failed: {data}")

# ================== Telegram handlers ==================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    await update.message.reply_text(
        "WeCom <-> TG bridge 已启动。\n\n"
        "用法：\n"
        "1) 企业微信里给应用发消息\n"
        "2) 你会在 TG 收到 [WECOM] 消息\n"
        "3) 你 Reply 那条 [WECOM] 消息，即可回发到企业微信\n"
    )

async def handle_admin_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or update.effective_chat.type != ChatType.PRIVATE:
        return
    if not is_admin(update):
        return
    if not update.message.reply_to_message:
        return

    st = load_state()
    rid = str(update.message.reply_to_message.message_id)
    touser = (st.get("wecom_index") or {}).get(rid)
    if not touser:
        return

    text = (update.message.text or "").strip()
    if not text:
        await update.message.reply_text("目前仅支持文本回发到企业微信。")
        return

    try:
        await wecom_send_text(touser, text)
        await update.message.reply_text("已回发企业微信。")
    except Exception as e:
        await update.message.reply_text(f"回发失败：{repr(e)}")

# ================== WeCom callback ==================
async def wecom_callback_get(request: web.Request):
    # 保存配置时 GET 验证
    if not (WECOM_CB_TOKEN and WECOM_CB_AESKEY and WECOM_CORP_ID):
        return web.Response(status=500, text="missing wecom env")

    qs = request.query
    msg_signature = qs.get("msg_signature", "")
    timestamp = qs.get("timestamp", "")
    nonce = qs.get("nonce", "")
    echostr = qs.get("echostr", "")

    if not (msg_signature and timestamp and nonce and echostr):
        return web.Response(status=400, text="bad query")

    # 注意：GET 验证的签名参数是 echostr
    sig = _sha1_signature(WECOM_CB_TOKEN, timestamp, nonce, echostr)
    if sig != msg_signature:
        return web.Response(status=403, text="bad signature")

    # 解密 echostr
    try:
        key = _aes_key_bytes(WECOM_CB_AESKEY)
        cipher = AES.new(key, AES.MODE_CBC, iv=key[:16])
        plain = cipher.decrypt(base64.b64decode(echostr))
        plain = _pkcs7_unpad(plain)
        msg_len = struct.unpack("!I", plain[16:20])[0]
        msg = plain[20:20 + msg_len]
        corp = plain[20 + msg_len:].decode("utf-8")
        if corp != WECOM_CORP_ID:
            return web.Response(status=403, text="corp mismatch")
        return web.Response(text=msg.decode("utf-8"))
    except Exception as e:
        print("wecom verify failed:", repr(e))
        return web.Response(status=403, text="verify failed")

async def wecom_callback_post(request: web.Request):
    """
    接收企业微信推送：
    1) 校验 msg_signature（签名参数是 Encrypt 字段内容）
    2) 解密 Encrypt，得到明文 XML
    3) 把消息转发到 TG 管理员，并记录 message_id -> touser
    """
    try:
        body = await request.text()
        root = ET.fromstring(body)
        encrypt = xml_text(root, "Encrypt")
        if not encrypt:
            return web.Response(text="success")

        qs = request.query
        msg_signature = qs.get("msg_signature", "")
        timestamp = qs.get("timestamp", "")
        nonce = qs.get("nonce", "")

        sig = _sha1_signature(WECOM_CB_TOKEN, timestamp, nonce, encrypt)
        if sig != msg_signature:
            return web.Response(status=403, text="bad signature")

        plain_xml = wecom_decrypt(encrypt)
        msg_root = ET.fromstring(plain_xml)

        from_user = xml_text(msg_root, "FromUserName")
        msg_type = xml_text(msg_root, "MsgType")

        if msg_type == "text":
            content = xml_text(msg_root, "Content")
        else:
            content = f"[暂不支持的消息类型：{msg_type}]"

        # 转发到 TG 管理员
        tg_app: Application = request.app["tg_app"]
        text = f"[WECOM]\nFrom: {from_user}\nType: {msg_type}\n\n{content}"

        msg = await tg_app.bot.send_message(chat_id=TG_ADMIN_ID, text=text)

        st = load_state()
        st.setdefault("wecom_index", {})[str(msg.message_id)] = from_user
        save_state(st)

        return web.Response(text="success")
    except Exception as e:
        print("wecom_callback_post error:", repr(e))
        # 企业微信要求及时响应；这里仍返回 success，避免它反复重试把你打爆
        return web.Response(text="success")

# ================== Web server (TG webhook + WeCom callback) ==================
async def run_server(tg_app: Application):
    if not PUBLIC_URL or not WEBHOOK_SECRET:
        raise RuntimeError("Missing PUBLIC_URL / WEBHOOK_SECRET")
    if TG_ADMIN_ID <= 0:
        raise RuntimeError("Missing TG_ADMIN_ID")
    if not TG_TOKEN:
        raise RuntimeError("Missing TG_BOT_TOKEN")

    webhook_path = f"/{WEBHOOK_SECRET}"
    webhook_url = f"{PUBLIC_URL}{webhook_path}"

    await tg_app.initialize()
    await tg_app.start()
    await tg_app.bot.set_webhook(url=webhook_url, drop_pending_updates=True)

    aio = web.Application()
    aio["tg_app"] = tg_app

    async def tg_update(request: web.Request):
        try:
            data = await request.json()
        except Exception:
            return web.Response(status=400, text="bad json")

        # 立刻返回，后台异步处理
        resp = web.Response(text="ok")

        async def _process():
            try:
                upd = Update.de_json(data, tg_app.bot)
                await tg_app.process_update(upd)
            except Exception as e:
                print("tg process_update error:", repr(e))

        asyncio.create_task(_process())
        return resp

    aio.router.add_post(webhook_path, tg_update)
    aio.router.add_get("/wecom/callback", wecom_callback_get)
    aio.router.add_post("/wecom/callback", wecom_callback_post)

    runner = web.AppRunner(aio)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=PORT)
    await site.start()

    print("[ok] TG webhook:", webhook_url)
    print("[ok] WeCom callback: /wecom/callback")
    await asyncio.Event().wait()

def main():
    tg_app = Application.builder().token(TG_TOKEN).build()
    tg_app.add_handler(CommandHandler("start", cmd_start))
    tg_app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.User(user_id=TG_ADMIN_ID), handle_admin_reply))
    asyncio.run(run_server(tg_app))

if __name__ == "__main__":
    main()
