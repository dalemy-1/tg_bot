# sync_products.py  (FINAL)
import os
import io
import csv
import json
import time
import hashlib
from pathlib import Path

import requests

SYNC_PRODUCTS_VERSION = "2025-12-14-01"

TG_TOKEN = (os.getenv("TG_BOT_TOKEN") or "").strip()
BASE_DIR = Path(__file__).resolve().parent

MAP_FILE = BASE_DIR / "thread_map.json"
STATE_FILE = BASE_DIR / "posted_state.json"

VALID_MARKETS = {"US", "UK", "DE", "FR", "IT", "ES", "CA", "JP"}

# 编辑失败是否兜底“删旧重发”（默认关闭，避免刷屏）
EDIT_FALLBACK_REPOST = False

# 文本最大长度（caption/text）
CAPTION_MAX = 900

# 节流：每条消息后睡眠（秒），降低 429 概率
SEND_DELAY_SEC = float(os.getenv("TG_SEND_DELAY_SEC", "1.2"))

FLAG = {
    "US": "🇺🇸", "UK": "🇬🇧", "DE": "🇩🇪", "FR": "🇫🇷",
    "IT": "🇮🇹", "ES": "🇪🇸", "CA": "🇨🇦", "JP": "🇯🇵",
}

COUNTRY_CN = {
    "US": "美国", "UK": "英国", "DE": "德国", "FR": "法国",
    "IT": "意大利", "ES": "西班牙", "CA": "加拿大", "JP": "日本",
}


def load_json(p: Path, default):
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    return default


def save_json(p: Path, obj):
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _decode_bytes(b: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "gb18030"):
        try:
            return b.decode(enc)
        except UnicodeDecodeError:
            continue
    return b.decode("utf-8", errors="replace")


def tg_api(method: str, payload: dict, max_retry: int = 6):
    """
    Telegram API wrapper with auto retry on 429.
    """
    if not TG_TOKEN:
        raise RuntimeError("Missing TG_BOT_TOKEN")

    url = f"https://api.telegram.org/bot{TG_TOKEN}/{method}"

    for attempt in range(max_retry):
        r = requests.post(url, json=payload, timeout=30)
        try:
            data = r.json()
        except Exception:
            # 非 JSON
            raise RuntimeError(f"{method} HTTP {r.status_code}: {r.text}")

        if data.get("ok"):
            return data["result"]

        # 处理限流 429
        err_code = data.get("error_code")
        if err_code == 429:
            retry_after = 5
            params = data.get("parameters") or {}
            if isinstance(params, dict) and params.get("retry_after"):
                retry_after = int(params["retry_after"])
            # 多等 1 秒更稳
            wait_s = retry_after + 1
            print(f"[warn] 429 Too Many Requests, wait {wait_s}s then retry... ({attempt+1}/{max_retry})")
            time.sleep(wait_s)
            continue

        # 其他错误：直接抛出
        raise RuntimeError(f"{method} failed: {data}")

    raise RuntimeError(f"{method} failed after retries (429).")


def load_products():
    """
    From GOOGLE_SHEET_CSV_URL (preferred) or local products.csv (fallback).
    Required headers:
      market, asin, title, keyword, store, remark, link, image_url, status
    """
    def _norm_status(s: str) -> str:
        s = (s or "").strip().lower()
        if s in ("removed", "inactive", "down", "off", "0", "false", "停售", "下架"):
            return "removed"
        return "active"

    def _norm_market(s: str) -> str:
        return (s or "").strip().upper()

    def _clean(s: str) -> str:
        return (s or "").strip()

    def _normalize_row(row: dict) -> dict:
        market = _norm_market(row.get("market") or row.get("Market"))
        asin = _clean(row.get("asin") or row.get("ASIN"))
        title = _clean(row.get("title") or row.get("Title"))
        keyword = _clean(row.get("keyword") or row.get("Keyword"))
        store = _clean(row.get("store") or row.get("Store"))
        remark = _clean(row.get("remark") or row.get("Remark"))
        link = _clean(row.get("link") or row.get("Link") or row.get("url") or row.get("URL"))
        image_url = _clean(row.get("image_url") or row.get("image") or row.get("Image") or row.get("img"))
        status = _norm_status(row.get("status") or row.get("Status"))
        return {
            "market": market,
            "asin": asin,
            "title": title,
            "keyword": keyword,
            "store": store,
            "remark": remark,
            "link": link,
            "image_url": image_url,
            "status": status,
        }

    sheet_url = (os.getenv("GOOGLE_SHEET_CSV_URL") or "").strip()
    rows = []

    if sheet_url:
        r = requests.get(sheet_url, timeout=30)
        r.raise_for_status()
        text = _decode_bytes(r.content)
        reader = csv.DictReader(io.StringIO(text))
        print(f"[debug] csv fieldnames: {reader.fieldnames}")
        for row in reader:
            if not row:
                continue
            rows.append(_normalize_row(row))
        print(f"[ok] loaded from Google Sheets: {len(rows)} rows")
    else:
        csv_path = BASE_DIR / "products.csv"
        if not csv_path.exists():
            raise FileNotFoundError(f"products.csv not found: {csv_path}")
        raw = csv_path.read_bytes()
        text = _decode_bytes(raw)
        reader = csv.DictReader(io.StringIO(text))
        print(f"[debug] csv fieldnames: {reader.fieldnames}")
        for row in reader:
            if not row:
                continue
            rows.append(_normalize_row(row))
        print(f"[ok] loaded from local csv: {len(rows)} rows ({csv_path})")

    # 必须有 market + asin 才能定位 state key
    filtered = [p for p in rows if p["market"] and p["asin"]]
    dropped = len(rows) - len(filtered)
    if dropped:
        print(f"[warn] dropped {dropped} rows missing market/asin")

    return filtered


def build_caption(p: dict) -> str:
    market = (p.get("market") or "").upper().strip()
    country = COUNTRY_CN.get(market, market)
    flag = FLAG.get(market, "")

    title = (p.get("title") or "").strip()
    keyword = (p.get("keyword") or "").strip()
    store = (p.get("store") or "").strip()
    remark = (p.get("remark") or "").strip()
    link = (p.get("link") or "").strip()

    lines = []
    # 国家 + 旗帜 + 标题
    lines.append(f"{country} {flag}{title}".strip())

    if keyword:
        lines.append(f"Keyword: {keyword}")
    if store:
        lines.append(f"Store: {store}")
    if remark:
        lines.append(f"Remark: {remark}")

    lines.append(f"link: {link}")

    cap = "\n".join(lines)
    return cap[:CAPTION_MAX]


def send_new(chat_id: int, thread_id: int, p: dict) -> dict:
    caption = build_caption(p)
    img = (p.get("image_url") or "").strip()

    if img:
        try:
            res = tg_api("sendPhoto", {
                "chat_id": chat_id,
                "message_thread_id": thread_id,
                "photo": img,
                "caption": caption,
            })
            time.sleep(SEND_DELAY_SEC)
            return {"message_id": res["message_id"], "kind": "photo", "image_url": img}
        except Exception as e:
            print("sendPhoto failed (fallback to text):", p.get("market"), p.get("asin"), str(e))

    res = tg_api("sendMessage", {
        "chat_id": chat_id,
        "message_thread_id": thread_id,
        "text": caption,
        "disable_web_page_preview": True,
    })
    time.sleep(SEND_DELAY_SEC)
    return {"message_id": res["message_id"], "kind": "text", "image_url": ""}


def edit_existing(chat_id: int, message_id: int, prev: dict, p: dict) -> dict:
    caption = build_caption(p)

    prev_kind = (prev.get("kind") or "text").strip()
    prev_img = (prev.get("image_url") or "").strip()
    new_img = (p.get("image_url") or "").strip()

    if prev_kind == "photo":
        if not new_img:
            raise RuntimeError("TYPE_CHANGE_PHOTO_TO_TEXT")

        if new_img == prev_img:
            tg_api("editMessageCaption", {
                "chat_id": chat_id,
                "message_id": message_id,
                "caption": caption,
            })
            time.sleep(SEND_DELAY_SEC)
            return {"kind": "photo", "image_url": new_img}

        tg_api("editMessageMedia", {
            "chat_id": chat_id,
            "message_id": message_id,
            "media": {"type": "photo", "media": new_img, "caption": caption}
        })
        time.sleep(SEND_DELAY_SEC)
        return {"kind": "photo", "image_url": new_img}

    if prev_kind == "text":
        if new_img:
            raise RuntimeError("TYPE_CHANGE_TEXT_TO_PHOTO")

        tg_api("editMessageText", {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": caption,
            "disable_web_page_preview": True,
        })
        time.sleep(SEND_DELAY_SEC)
        return {"kind": "text", "image_url": ""}

    raise RuntimeError("UNKNOWN_KIND")


def pick_chat_id(thread_map_all: dict) -> str:
    env_chat = (os.getenv("TG_CHAT_ID") or "").strip()
    if env_chat:
        if str(env_chat) in thread_map_all:
            return str(env_chat)
        raise RuntimeError(f"TG_CHAT_ID={env_chat} not found in thread_map.json keys={list(thread_map_all.keys())}")

    keys = list(thread_map_all.keys())
    if len(keys) == 1:
        return keys[0]

    raise RuntimeError(
        "Multiple chat_id found in thread_map.json. "
        "Set env TG_CHAT_ID to choose one. "
        f"Available: {keys}"
    )


def main():
    print("SYNC_PRODUCTS_VERSION =", SYNC_PRODUCTS_VERSION)

    if not TG_TOKEN:
        raise SystemExit("Missing TG_BOT_TOKEN env var.")
    if not MAP_FILE.exists():
        raise SystemExit("Missing thread_map.json（请先在群里各话题 /bind 生成映射）")

    thread_map_all = load_json(MAP_FILE, {})
    chat_id_str = pick_chat_id(thread_map_all)
    chat_id = int(chat_id_str)
    thread_map = thread_map_all.get(chat_id_str, {})

    state = load_json(STATE_FILE, {})
    products = load_products()

    for p in products:
        if not p["asin"] or not p.get("link"):
            continue

        key = f"{p['market']}:{p['asin']}"
        content_hash = sha1(f"{p.get('title','')}|{p.get('keyword','')}|{p.get('store','')}|{p.get('remark','')}|{p.get('link','')}|{p.get('image_url','')}|{p.get('status','')}")
        prev = state.get(key)

        # 下架：删除原消息（必须 bot 有删除权限 & state 里要有 message_id）
        if p["status"] == "removed":
            if prev and prev.get("message_id"):
                try:
                    tg_api("deleteMessage", {"chat_id": chat_id, "message_id": int(prev["message_id"])})
                    print("deleted:", key, "msg", prev["message_id"])
                except Exception as e:
                    print("delete failed:", key, str(e))

            state[key] = {
                **(prev or {}),
                "status": "removed",
                "message_id": None,   # 确保再次上架一定重发
                "kind": None,
                "image_url": "",
                "hash": content_hash,
                "ts": int(time.time())
            }
            continue

        # removed -> active：重新发送
        if prev and prev.get("status") == "removed":
            thread_id = thread_map.get(p["market"])
            if not thread_id:
                print("no thread bound for:", p["market"])
                continue

            info = send_new(chat_id, int(thread_id), p)
            state[key] = {
                "message_id": info["message_id"],
                "hash": content_hash,
                "status": "active",
                "kind": info["kind"],
                "image_url": info["image_url"],
                "ts": int(time.time())
            }
            print("reposted(after relist):", key, "msg", info["message_id"])
            continue

        # active 且无变化：跳过
        if prev and prev.get("status") == "active" and prev.get("hash") == content_hash and prev.get("message_id"):
            continue

        thread_id = thread_map.get(p["market"])
        if not thread_id:
            print("no thread bound for:", p["market"])
            continue
        thread_id = int(thread_id)

        # 首次：发新
        if not prev or not prev.get("message_id"):
            info = send_new(chat_id, thread_id, p)
            state[key] = {
                "message_id": info["message_id"],
                "hash": content_hash,
                "status": "active",
                "kind": info["kind"],
                "image_url": info["image_url"],
                "ts": int(time.time())
            }
            print("posted:", key, "msg", info["message_id"])
            continue

        # 变化：编辑原消息（不重发）
        msg_id = int(prev["message_id"])
        try:
            new_meta = edit_existing(chat_id, msg_id, prev, p)
            state[key] = {
                **prev,
                "hash": content_hash,
                "status": "active",
                "kind": new_meta["kind"],
                "image_url": new_meta["image_url"],
                "ts": int(time.time())
            }
            print("edited:", key, "msg", msg_id)
        except Exception as e:
            print("edit failed:", key, str(e))
            if EDIT_FALLBACK_REPOST:
                try:
                    tg_api("deleteMessage", {"chat_id": chat_id, "message_id": msg_id})
                except Exception:
                    pass
                info = send_new(chat_id, thread_id, p)
                state[key] = {
                    "message_id": info["message_id"],
                    "hash": content_hash,
                    "status": "active",
                    "kind": info["kind"],
                    "image_url": info["image_url"],
                    "ts": int(time.time())
                }
                print("reposted(fallback):", key, "msg", info["message_id"])

    save_json(STATE_FILE, state)
    print("done. state saved ->", STATE_FILE)


if __name__ == "__main__":
    main()
