# sync_products.py  (FINAL - SAFE + MONEY + NO DUPLICATE)
import os
import io
import csv
import json
import time
import hashlib
from pathlib import Path
from typing import Dict, Any, List, Optional

import requests

SYNC_PRODUCTS_VERSION = "2025-12-14-04"

TG_TOKEN = (os.getenv("TG_BOT_TOKEN") or "").strip()
BASE_DIR = Path(__file__).resolve().parent

MAP_FILE = BASE_DIR / "thread_map.json"
STATE_FILE = BASE_DIR / "posted_state.json"

VALID_MARKETS = {"US", "UK", "DE", "FR", "IT", "ES", "CA", "JP"}

CAPTION_MAX = 900

# 节流：每条消息后睡眠（秒），降低 429 概率
SEND_DELAY_SEC = float(os.getenv("TG_SEND_DELAY_SEC", "1.2"))

# Google Sheet 拉取失败，是否回退本地 products.csv
FALLBACK_TO_LOCAL_CSV = (os.getenv("FALLBACK_TO_LOCAL_CSV", "1").strip() != "0")

# 图片链接错误时：是否直接跳过该产品（1=跳过；0=降级发文本）
SKIP_ON_BAD_IMAGE = (os.getenv("TG_SKIP_ON_BAD_IMAGE", "1").strip() != "0")

FLAG = {
    "US": "🇺🇸", "UK": "🇬🇧", "DE": "🇩🇪", "FR": "🇫🇷",
    "IT": "🇮🇹", "ES": "🇪🇸", "CA": "🇨🇦", "JP": "🇯🇵",
}

# 市场默认货币符号（按你要求：尾随符号，如 10$）
CURRENCY_SYMBOL = {
    "US": "$",
    "UK": "£",
    "DE": "€",
    "FR": "€",
    "IT": "€",
    "ES": "€",
    "CA": "$",
    "JP": "¥",
}


# -----------------------
# Helpers
# -----------------------
def safe_str(x) -> str:
    # 关键修复：0 不能被 (x or "") 吃掉
    return ("" if x is None else str(x)).strip()


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


class SkipProduct(Exception):
    pass


def _to_number(s) -> Optional[float]:
    if s is None:
        return None
    s = safe_str(s)
    if not s:
        return None
    cleaned = (
        s.replace(",", "")
         .replace("$", "")
         .replace("£", "")
         .replace("€", "")
         .replace("¥", "")
         .replace("￥", "")
         .strip()
    )
    try:
        return float(cleaned)
    except Exception:
        return None


def format_money(value, market: str) -> Optional[str]:
    """
    规则：
    - 空/解析为0/文本等于0 -> None（不输出）
    - 已包含货币符号 -> 原样输出（strip 后）
    - 纯数字 -> 按市场货币符号拼成 '10$' 这种尾随格式
    """
    if value is None:
        return None
    s = safe_str(value)
    if not s:
        return None

    n = _to_number(s)
    if n is not None and abs(n) < 1e-12:
        return None
    if s in ("0", "0.0", "0.00"):
        return None

    if any(sym in s for sym in ("$", "£", "€", "¥", "￥")):
        return s

    sym = CURRENCY_SYMBOL.get((market or "").upper(), "")
    if not sym:
        return s
    return f"{s}{sym}"


def is_bad_image_error(err: Exception) -> bool:
    s = str(err).lower()
    keywords = [
        "wrong file identifier",
        "wrong type of the web page content",
        "webpage_media_empty",
        "failed to get http url content",
        "http url specified",
        "can't parse",
        "bad request",
    ]
    return any(k in s for k in keywords)


# -----------------------
# Telegram API
# -----------------------
def tg_api(method: str, payload: dict, max_retry: int = 6):
    """
    Telegram API wrapper:
    - 自动处理 429（按 retry_after 等待后重试）
    - 其他错误抛出（由外层单条产品 try/except 吃掉继续）
    """
    if not TG_TOKEN:
        raise RuntimeError("Missing TG_BOT_TOKEN")

    url = f"https://api.telegram.org/bot{TG_TOKEN}/{method}"

    for attempt in range(max_retry):
        r = requests.post(url, json=payload, timeout=30)
        try:
            data = r.json()
        except Exception:
            raise RuntimeError(f"{method} HTTP {r.status_code}: {r.text}")

        if data.get("ok"):
            return data["result"]

        err_code = data.get("error_code")

        if err_code == 429:
            retry_after = 5
            params = data.get("parameters") or {}
            if isinstance(params, dict) and params.get("retry_after"):
                retry_after = int(params["retry_after"])
            wait_s = retry_after + 1
            print(f"[warn] 429 Too Many Requests, wait {wait_s}s then retry... ({attempt+1}/{max_retry})")
            time.sleep(wait_s)
            continue

        raise RuntimeError(f"{method} failed: {data}")

    raise RuntimeError(f"{method} failed after retries (429).")


# -----------------------
# Load products
# -----------------------
def load_products() -> List[Dict[str, str]]:
    """
    支持字段：
      market, asin, title, keyword, store, remark, link, image_url, status,
      Discount Price, Commission
    """
    def _norm_status(s: str) -> str:
        s = safe_str(s).lower()
        if s in ("removed", "inactive", "down", "off", "0", "false", "停售", "下架"):
            return "removed"
        return "active"

    def _norm_market(s: str) -> str:
        return safe_str(s).upper()

    def _normalize_row(row: dict) -> dict:
        # 统一 header（去空格），避免 'Discount Price ' 这类问题
        row = {safe_str(k): ("" if v is None else v) for k, v in row.items()}

        market = _norm_market(row.get("market") or row.get("Market"))
        asin = safe_str(row.get("asin") or row.get("ASIN"))
        title = safe_str(row.get("title") or row.get("Title"))
        keyword = safe_str(row.get("keyword") or row.get("Keyword"))
        store = safe_str(row.get("store") or row.get("Store"))
        remark = safe_str(row.get("remark") or row.get("Remark"))
        link = safe_str(row.get("link") or row.get("Link") or row.get("url") or row.get("URL"))
        image_url = safe_str(row.get("image_url") or row.get("image") or row.get("Image") or row.get("img") or row.get("image_url "))
        status = _norm_status(row.get("status") or row.get("Status"))

        discount_price = safe_str(
            row.get("discount_price")
            or row.get("Discount Price")
            or row.get("DiscountPrice")
            or row.get("discount")
        )
        commission = safe_str(
            row.get("commission")
            or row.get("Commission")
            or row.get("comm")
        )

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
            "discount_price": discount_price,
            "commission": commission,
        }

    sheet_url = safe_str(os.getenv("GOOGLE_SHEET_CSV_URL"))
    rows: List[Dict[str, str]] = []

    def _load_from_local():
        csv_path = BASE_DIR / "products.csv"
        if not csv_path.exists():
            raise FileNotFoundError(f"products.csv not found: {csv_path}")
        raw = csv_path.read_bytes()
        text = _decode_bytes(raw)
        reader = csv.DictReader(io.StringIO(text))
        print(f"[debug] local csv fieldnames: {reader.fieldnames}")
        for row in reader:
            if row:
                rows.append(_normalize_row(row))
        print(f"[ok] loaded from local csv: {len(rows)} rows ({csv_path})")

    if sheet_url:
        try:
            r = requests.get(sheet_url, timeout=30)
            r.raise_for_status()
            text = _decode_bytes(r.content)
            reader = csv.DictReader(io.StringIO(text))
            print(f"[debug] sheet csv fieldnames: {reader.fieldnames}")
            for row in reader:
                if row:
                    rows.append(_normalize_row(row))
            print(f"[ok] loaded from Google Sheets: {len(rows)} rows")
        except Exception as e:
            print(f"[warn] failed to load Google Sheets CSV, err={e}")
            if FALLBACK_TO_LOCAL_CSV:
                print("[warn] fallback to local products.csv ...")
                _load_from_local()
            else:
                raise
    else:
        _load_from_local()

    return rows


# -----------------------
# Caption builder
# -----------------------
def build_caption(p: dict) -> str:
    market = safe_str(p.get("market")).upper()
    flag = FLAG.get(market, "")

    title = safe_str(p.get("title"))
    keyword = safe_str(p.get("keyword"))
    store = safe_str(p.get("store"))
    remark = safe_str(p.get("remark"))

    discount_price = format_money(p.get("discount_price"), market)
    commission = format_money(p.get("commission"), market)

    link = safe_str(p.get("link"))

    lines = []

    # 只显示国旗，不显示中文国家名
    if not title:
        head = f"{flag}(无标题)".strip() if flag else "(无标题)"
    else:
        head = f"{flag}{title}".strip() if flag else title
    lines.append(head)

    if keyword:
        lines.append(f"Keyword: {keyword}")
    if store:
        lines.append(f"Store: {store}")
    if remark:
        lines.append(f"Remark: {remark}")

    # 为 0 / 空 就不显示
    if discount_price:
        lines.append(f"Discount Price: {discount_price}")
    if commission:
        lines.append(f"Commission: {commission}")

    if link:
        lines.append(f"link:{link}")

    cap = "\n".join(lines)
    return cap[:CAPTION_MAX]


# -----------------------
# Send / Edit
# -----------------------
def send_new(chat_id: int, thread_id: int, p: dict) -> dict:
    caption = build_caption(p)
    img = safe_str(p.get("image_url"))

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
            if is_bad_image_error(e) and SKIP_ON_BAD_IMAGE:
                # 按你的要求：图片链接错误就跳过该产品，不中止脚本
                raise SkipProduct(f"bad image url -> skip. img={img} err={e}")
            print(f"[warn] sendPhoto failed -> fallback to text. market={p.get('market')} asin={p.get('asin')} img={img} err={e}")

    # 文本兜底
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

    prev_kind = safe_str(prev.get("kind") or "text")
    prev_img = safe_str(prev.get("image_url"))
    new_img = safe_str(p.get("image_url"))

    if prev_kind == "photo":
        # 尝试换图（新图坏则回退只改 caption）
        if new_img and new_img != prev_img:
            try:
                tg_api("editMessageMedia", {
                    "chat_id": chat_id,
                    "message_id": message_id,
                    "media": {"type": "photo", "media": new_img, "caption": caption}
                })
                time.sleep(SEND_DELAY_SEC)
                return {"kind": "photo", "image_url": new_img}
            except Exception as e:
                print(f"[warn] editMessageMedia failed -> fallback to edit caption only. msg={message_id} err={e}")

        tg_api("editMessageCaption", {
            "chat_id": chat_id,
            "message_id": message_id,
            "caption": caption,
        })
        time.sleep(SEND_DELAY_SEC)
        return {"kind": "photo", "image_url": prev_img}

    # text：只改文本（忽略 new_img，避免 text->photo 的类型问题）
    tg_api("editMessageText", {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": caption,
        "disable_web_page_preview": True,
    })
    time.sleep(SEND_DELAY_SEC)
    return {"kind": "text", "image_url": ""}


def pick_chat_id(thread_map_all: dict) -> str:
    env_chat = safe_str(os.getenv("TG_CHAT_ID"))
    if env_chat:
        if env_chat in thread_map_all:
            return env_chat
        raise RuntimeError(f"TG_CHAT_ID={env_chat} not found in thread_map.json keys={list(thread_map_all.keys())}")

    keys = list(thread_map_all.keys())
    if len(keys) == 1:
        return keys[0]

    raise RuntimeError(
        "Multiple chat_id found in thread_map.json. "
        "Set env TG_CHAT_ID to choose one. "
        f"Available: {keys}"
    )


# -----------------------
# Main
# -----------------------
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

    state: Dict[str, Any] = load_json(STATE_FILE, {})
    products = load_products()

    ok_count = 0
    skip_count = 0
    err_count = 0

    for p in products:
        try:
            market = safe_str(p.get("market")).upper()
            asin = safe_str(p.get("asin"))

            if not asin:
                skip_count += 1
                print("[skip] missing asin:", p)
                continue

            if market not in VALID_MARKETS:
                skip_count += 1
                print("[skip] invalid market:", market, "asin:", asin)
                continue

            thread_id = thread_map.get(market)
            if not thread_id:
                skip_count += 1
                print("[skip] no thread bound for:", market, "asin:", asin)
                continue
            thread_id = int(thread_id)

            key = f"{market}:{asin}"
            status = safe_str(p.get("status") or "active").lower()
            if status not in ("active", "removed"):
                status = "active"

            # hash：把金额字段也纳入，确保变化触发编辑
            content_hash = sha1(
                f"{safe_str(p.get('title'))}|{safe_str(p.get('keyword'))}|{safe_str(p.get('store'))}|"
                f"{safe_str(p.get('remark'))}|{safe_str(p.get('link'))}|{safe_str(p.get('image_url'))}|"
                f"{safe_str(p.get('discount_price'))}|{safe_str(p.get('commission'))}|{status}"
            )

            prev = state.get(key)

            # -------------------
            # removed：删除（但删除失败绝不清空 message_id，防止重复发）
            # 同时：避免每次 run 都重复尝试 delete（用 delete_attempted 记一次）
            # -------------------
            if status == "removed":
                already_attempted = bool(prev and prev.get("delete_attempted"))
                deleted_ok = False

                if prev and prev.get("message_id") and not already_attempted:
                    try:
                        tg_api("deleteMessage", {"chat_id": chat_id, "message_id": int(prev["message_id"])})
                        print("deleted:", key, "msg", prev["message_id"])
                        deleted_ok = True
                    except Exception as e:
                        print("[warn] delete failed; keep message_id to prevent duplicates:", key, str(e))

                state[key] = {
                    **(prev or {}),
                    "status": "removed",
                    # 只有删成功才清空；删失败保留 message_id / kind / image_url
                    "message_id": None if deleted_ok else (prev.get("message_id") if prev else None),
                    "kind": None if deleted_ok else (prev.get("kind") if prev else None),
                    "image_url": "" if deleted_ok else (prev.get("image_url") if prev else ""),
                    "hash": content_hash,
                    "ts": int(time.time()),
                    "delete_attempted": True,     # 标记已尝试过
                    "delete_ok": deleted_ok,
                }
                ok_count += 1
                continue

            # -------------------
            # active：无变化跳过
            # -------------------
            if prev and prev.get("status") == "active" and prev.get("hash") == content_hash and prev.get("message_id"):
                skip_count += 1
                continue

            # -------------------
            # removed -> active：
            # 如果之前删失败仍保留 message_id，就优先编辑旧消息避免重发
            # -------------------
            if prev and prev.get("status") == "removed" and prev.get("message_id"):
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
                        "delete_attempted": False,
                        "delete_ok": False,
                    }
                    print("edited(after relist):", key, "msg", msg_id)
                    ok_count += 1
                    continue
                except Exception as e:
                    # 编辑失败（例如 message not found）再走重发
                    print("[warn] relist edit failed -> will repost:", key, str(e))

            # -------------------
            # 首次发布（或之前删成功 message_id 已为空）
            # -------------------
            if not prev or not prev.get("message_id"):
                info = send_new(chat_id, thread_id, p)
                state[key] = {
                    "message_id": info["message_id"],
                    "hash": content_hash,
                    "status": "active",
                    "kind": info["kind"],
                    "image_url": info["image_url"],
                    "ts": int(time.time()),
                    "delete_attempted": False,
                    "delete_ok": False,
                }
                print("posted:", key, "msg", info["message_id"])
                ok_count += 1
                continue

            # -------------------
            # 编辑已有消息
            # -------------------
            msg_id = int(prev["message_id"])
            new_meta = edit_existing(chat_id, msg_id, prev, p)
            state[key] = {
                **prev,
                "hash": content_hash,
                "status": "active",
                "kind": new_meta["kind"],
                "image_url": new_meta["image_url"],
                "ts": int(time.time()),
                "delete_attempted": False,
                "delete_ok": False,
            }
            print("edited:", key, "msg", msg_id)
            ok_count += 1

        except SkipProduct as e:
            skip_count += 1
            print("[skip]", str(e))
            continue
        except Exception as e:
            err_count += 1
            print(f"[error] product failed but continue. market={p.get('market')} asin={p.get('asin')} err={e}")
            continue

    save_json(STATE_FILE, state)
    print(f"done. ok={ok_count} skip={skip_count} err={err_count}. state saved -> {STATE_FILE}")


if __name__ == "__main__":
    main()
