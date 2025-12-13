print("SYNC_PRODUCTS VERSION = 2025-12-14-01")
# sync_products.py
# 功能：
# 1) 从 GOOGLE_SHEET_CSV_URL 读取产品（公开CSV链接），否则读取本地 products.csv
# 2) 按 thread_map.json 将不同 market 发到不同话题（message_thread_id）
# 3) active：首次发送；内容变更则 edit（不重发）
# 4) removed：删除原消息，并清空 message_id；再次 active 会重发
# 5) 表格里整行被删除：也会删除历史消息（missing_in_sheet -> delete）
#
# 环境变量（GitHub Actions secrets）：
# - TG_BOT_TOKEN：机器人 token
# - GOOGLE_SHEET_CSV_URL：Google Sheet 导出CSV链接（推荐）
# - TG_CHAT_ID：可选（当 thread_map.json 里有多个群chat_id时必须指定）
#
# 文件：
# - thread_map.json：通过 /bind 生成
# - posted_state.json：脚本自动维护（用于记录每个商品发到哪条消息）
# - products.csv：本地回退（可选）

import os
import io
import csv
import json
import time
import hashlib
from pathlib import Path

import requests


# ========== 基础配置 ==========
TG_TOKEN = (os.getenv("TG_BOT_TOKEN") or "").strip()
SHEET_CSV_URL = (os.getenv("GOOGLE_SHEET_CSV_URL") or "").strip()
ENV_CHAT_ID = (os.getenv("TG_CHAT_ID") or "").strip()

BASE_DIR = Path(__file__).resolve().parent
MAP_FILE = BASE_DIR / "thread_map.json"
STATE_FILE = BASE_DIR / "posted_state.json"
PRODUCTS_FILE = BASE_DIR / "products.csv"

VALID_MARKETS = {"US", "UK", "DE", "FR", "IT", "ES", "CA", "JP"}

# 编辑失败是否兜底“删旧重发”（默认关闭，避免刷屏）
EDIT_FALLBACK_REPOST = False

CAPTION_MAX = 900

FLAG = {
    "US": "🇺🇸",
    "UK": "🇬🇧",
    "DE": "🇩🇪",
    "FR": "🇫🇷",
    "IT": "🇮🇹",
    "ES": "🇪🇸",
    "CA": "🇨🇦",
    "JP": "🇯🇵",
}

COUNTRY_CN = {
    "US": "美国",
    "UK": "英国",
    "DE": "德国",
    "FR": "法国",
    "IT": "意大利",
    "ES": "西班牙",
    "CA": "加拿大",
    "JP": "日本",
}


# ========== 工具函数 ==========
def load_json(p: Path, default):
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    return default


def save_json(p: Path, obj):
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def tg_api(method: str, payload: dict):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/{method}"
    r = requests.post(url, json=payload, timeout=30)

    try:
        data = r.json()
    except Exception:
        raise RuntimeError(f"{method} HTTP {r.status_code}: {r.text}")

    if not data.get("ok"):
        # 直接抛出 Telegram 的真实原因
        raise RuntimeError(f"{method} failed: {data}")

    return data["result"]


def delete_message(chat_id: int, message_id: int) -> None:
    tg_api("deleteMessage", {"chat_id": chat_id, "message_id": int(message_id)})


def pick_chat_id(thread_map_all: dict) -> str:
    """
    Choose which chat_id to use from thread_map.json.

    Priority:
    1) env TG_CHAT_ID if set
    2) if only one chat_id exists in thread_map_all, use it
    3) otherwise raise with readable message
    """
    if ENV_CHAT_ID:
        if ENV_CHAT_ID in thread_map_all:
            return ENV_CHAT_ID
        if str(ENV_CHAT_ID) in thread_map_all:
            return str(ENV_CHAT_ID)
        raise RuntimeError(
            f"TG_CHAT_ID={ENV_CHAT_ID} not found in thread_map.json. "
            f"Available keys={list(thread_map_all.keys())}"
        )

    keys = list(thread_map_all.keys())
    if len(keys) == 1:
        return keys[0]

    raise RuntimeError(
        "Multiple chat_id found in thread_map.json. "
        "Please set env TG_CHAT_ID to choose one. "
        f"Available: {keys}"
    )


# ========== 读取产品（Google Sheet CSV or 本地CSV） ==========
def _decode_bytes(b: bytes) -> str:
    # 优先 utf-8-sig（兼容 Excel 导出的 BOM），再 utf-8，最后 gb18030
    for enc in ("utf-8-sig", "utf-8", "gb18030"):
        try:
            return b.decode(enc)
        except UnicodeDecodeError:
            continue
    return b.decode("utf-8", errors="replace")


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
    # 兼容不同列名
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


def load_products():
    """
    Load products from Google Sheets CSV (if GOOGLE_SHEET_CSV_URL is set) or local products.csv.
    Returns: list[dict] with normalized keys:
      market, asin, title, keyword, store, remark, link, image_url, status
    """
    import os, io, csv
    import requests
    from pathlib import Path

    def _decode_bytes(b: bytes) -> str:
        for enc in ("utf-8-sig", "utf-8", "gb18030"):
            try:
                return b.decode(enc)
            except UnicodeDecodeError:
                continue
        return b.decode("utf-8", errors="replace")

    def _norm_status(s: str) -> str:
        s = (s or "").strip().lower()
        if s in ("removed", "inactive", "down", "off", "0", "false", "停售", "下架"):
            return "removed"
        return "active"

    def _clean(s: str) -> str:
        return (s or "").strip()

    def _norm_market(s: str) -> str:
        return _clean(s).upper()

    def _normalize_row(row: dict) -> dict:
        # 1) 统一 key：去 BOM、去空格、转小写
        norm = {}
        for k, v in (row or {}).items():
            kk = (k or "")
            kk = kk.lstrip("\ufeff").strip().lower()
            norm[kk] = v

        market = _norm_market(norm.get("market"))
        asin = _clean(norm.get("asin"))
        title = _clean(norm.get("title"))
        keyword = _clean(norm.get("keyword"))
        store = _clean(norm.get("store"))
        remark = _clean(norm.get("remark"))
        link = _clean(norm.get("link") or norm.get("url"))
        image_url = _clean(norm.get("image_url") or norm.get("image") or norm.get("img"))
        status = _norm_status(norm.get("status"))

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

    # 1) 优先：Google Sheets CSV
    sheet_url = (os.getenv("GOOGLE_SHEET_CSV_URL") or "").strip()
    rows = []

    if sheet_url:
        r = requests.get(sheet_url, timeout=30)
        r.raise_for_status()
        text = _decode_bytes(r.content)

        # 关键：彻底清 BOM（有些情况下 BOM 会残留在首列表头）
        text = text.replace("\ufeff", "")

        reader = csv.DictReader(io.StringIO(text))
        # Debug：把 fieldnames 打印出来，便于你确认表头是否正确
        print("[debug] csv fieldnames:", reader.fieldnames)

        for row in reader:
            if not row:
                continue
            rows.append(_normalize_row(row))
        print(f"[ok] loaded from Google Sheets: {len(rows)} rows")
    else:
        # 2) 回退：本地 products.csv
        base_dir = Path(__file__).resolve().parent
        csv_path = base_dir / "products.csv"
        if not csv_path.exists():
            raise FileNotFoundError(f"products.csv not found: {csv_path}")

        raw = csv_path.read_bytes()
        text = _decode_bytes(raw).replace("\ufeff", "")
        reader = csv.DictReader(io.StringIO(text))
        print("[debug] csv fieldnames:", reader.fieldnames)

        for row in reader:
            if not row:
                continue
            rows.append(_normalize_row(row))
        print(f"[ok] loaded from local csv: {len(rows)} rows ({csv_path})")

    # 必须有 market + asin
    filtered = [p for p in rows if p.get("market") and p.get("asin")]
    dropped = len(rows) - len(filtered)
    if dropped:
        print(f"[warn] dropped {dropped} rows missing market/asin")

    return filtered



# ========== 文案/发送/编辑 ==========
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
    # 你想要：英国 🇬🇧计步器（中间不强制空格）
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

    # 优先发图
    if img:
        try:
            res = tg_api(
                "sendPhoto",
                {
                    "chat_id": chat_id,
                    "message_thread_id": thread_id,
                    "photo": img,
                    "caption": caption,
                },
            )
            return {"message_id": res["message_id"], "kind": "photo", "image_url": img}
        except Exception as e:
            print("sendPhoto failed (fallback to text):", p.get("market"), p.get("asin"), str(e))

    # 降级：发文本
    res = tg_api(
        "sendMessage",
        {
            "chat_id": chat_id,
            "message_thread_id": thread_id,
            "text": caption,
            "disable_web_page_preview": True,
        },
    )
    return {"message_id": res["message_id"], "kind": "text", "image_url": ""}


def edit_existing(chat_id: int, message_id: int, prev: dict, p: dict) -> dict:
    caption = build_caption(p)

    prev_kind = (prev.get("kind") or "text").strip()
    prev_img = (prev.get("image_url") or "").strip()
    new_img = (p.get("image_url") or "").strip()

    # 之前是 photo
    if prev_kind == "photo":
        if not new_img:
            raise RuntimeError("TYPE_CHANGE_PHOTO_TO_TEXT")

        # 同图：改 caption
        if new_img == prev_img:
            tg_api(
                "editMessageCaption",
                {
                    "chat_id": chat_id,
                    "message_id": message_id,
                    "caption": caption,
                },
            )
            return {"kind": "photo", "image_url": new_img}

        # 换图：改 media
        tg_api(
            "editMessageMedia",
            {
                "chat_id": chat_id,
                "message_id": message_id,
                "media": {"type": "photo", "media": new_img, "caption": caption},
            },
        )
        return {"kind": "photo", "image_url": new_img}

    # 之前是 text
    if prev_kind == "text":
        if new_img:
            raise RuntimeError("TYPE_CHANGE_TEXT_TO_PHOTO")

        tg_api(
            "editMessageText",
            {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": caption,
                "disable_web_page_preview": True,
            },
        )
        return {"kind": "text", "image_url": ""}

    raise RuntimeError("UNKNOWN_KIND")


# ========== 主流程 ==========
def main():
    if not TG_TOKEN:
        raise SystemExit("Missing TG_BOT_TOKEN env var.")
    if not MAP_FILE.exists():
        raise SystemExit("Missing thread_map.json（请先 /bind 生成映射）")

    thread_map_all = load_json(MAP_FILE, {})
    chat_id_str = pick_chat_id(thread_map_all)
    chat_id = int(chat_id_str)
    thread_map = thread_map_all.get(chat_id_str, {})  # {"US": thread_id, ...}

    # state 使用：key = "US:ASIN"
    state: dict = load_json(STATE_FILE, {})

    products = load_products()

    # 生成本次表格里出现过的 key（用于“整行删除”的清理）
    current_keys = set()

    for p in products:
        market = p.get("market", "").strip().upper()
        asin = p.get("asin", "").strip()
        link = (p.get("link") or "").strip()
        status = (p.get("status") or "active").strip().lower()

        if not market or not asin:
            continue

        key = f"{market}:{asin}"
        current_keys.add(key)

        # 内容 hash（你想变更就 edit：标题/关键词/店铺/备注/链接/图片/状态任何改变都会触发）
        content_hash = sha1(
            f"{p.get('title','')}|{p.get('keyword','')}|{p.get('store','')}|{p.get('remark','')}|"
            f"{link}|{p.get('image_url','')}|{status}"
        )
        prev = state.get(key) or {}

        # 话题绑定检查
        thread_id = thread_map.get(market)
        if status == "active" and not thread_id:
            print("no thread bound for:", market, "skip", key)
            continue
        if thread_id:
            thread_id = int(thread_id)

        # ========== removed：删除并清空 message_id ==========
        if status == "removed":
            if prev.get("message_id"):
                try:
                    delete_message(chat_id, int(prev["message_id"]))
                    print("deleted:", key, "msg", prev["message_id"])
                except Exception as e:
                    print("delete failed:", key, "msg", prev.get("message_id"), str(e))

            # 清空 message_id，保证再次 active 会重发
            state[key] = {
                **prev,
                "status": "removed",
                "message_id": None,
                "kind": None,
                "image_url": "",
                "hash": content_hash,
                "ts": int(time.time()),
            }
            continue

        # ========== removed -> active：必须重发 ==========
        if prev.get("status") == "removed":
            info = send_new(chat_id, thread_id, p)
            state[key] = {
                "message_id": info["message_id"],
                "hash": content_hash,
                "status": "active",
                "kind": info["kind"],
                "image_url": info["image_url"],
                "ts": int(time.time()),
            }
            print("reposted(after relist):", key, "msg", info["message_id"])
            continue

        # ========== active 且无变化：跳过 ==========
        if prev.get("status") == "active" and prev.get("hash") == content_hash and prev.get("message_id"):
            continue

        # ========== 首次：发新 ==========
        if not prev.get("message_id"):
            info = send_new(chat_id, thread_id, p)
            state[key] = {
                "message_id": info["message_id"],
                "hash": content_hash,
                "status": "active",
                "kind": info["kind"],
                "image_url": info["image_url"],
                "ts": int(time.time()),
            }
            print("posted:", key, "msg", info["message_id"])
            continue

        # ========== active 但内容变化：编辑 ==========
        msg_id = int(prev["message_id"])
        try:
            new_meta = edit_existing(chat_id, msg_id, prev, p)
            state[key] = {
                **prev,
                "hash": content_hash,
                "status": "active",
                "kind": new_meta["kind"],
                "image_url": new_meta["image_url"],
                "ts": int(time.time()),
            }
            print("edited:", key, "msg", msg_id)
        except Exception as e:
            print("edit failed:", key, "msg", msg_id, str(e))

            # 可选兜底：删旧重发（默认关闭）
            if EDIT_FALLBACK_REPOST:
                try:
                    delete_message(chat_id, msg_id)
                except Exception:
                    pass

                info = send_new(chat_id, thread_id, p)
                state[key] = {
                    "message_id": info["message_id"],
                    "hash": content_hash,
                    "status": "active",
                    "kind": info["kind"],
                    "image_url": info["image_url"],
                    "ts": int(time.time()),
                }
                print("reposted(fallback):", key, "msg", info["message_id"])

    # ========== 表格里整行被删：删除旧消息 ==========
    # 如果 state 里存在，但本次 products 已不存在，说明“整行删除”或筛掉了
    for key in list(state.keys()):
        if key not in current_keys:
            prev = state.get(key) or {}
            if prev.get("message_id"):
                try:
                    delete_message(chat_id, int(prev["message_id"]))
                    print("deleted(missing_in_sheet):", key, "msg", prev["message_id"])
                except Exception as e:
                    print("delete missing failed:", key, "msg", prev.get("message_id"), str(e))

            # 标记 removed 并清空 message_id
            state[key] = {
                **prev,
                "status": "removed",
                "message_id": None,
                "kind": None,
                "image_url": "",
                "ts": int(time.time()),
            }

    save_json(STATE_FILE, state)
    print("done. state saved ->", STATE_FILE)


if __name__ == "__main__":
    main()


