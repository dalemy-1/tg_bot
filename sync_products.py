import os
import io
import csv
import json
import time
import hashlib
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, Any, List, Optional

import requests

SYNC_PRODUCTS_VERSION = "2025-12-14-05"

TG_TOKEN = (os.getenv("TG_BOT_TOKEN") or "").strip()
BASE_DIR = Path(__file__).resolve().parent

MAP_FILE = BASE_DIR / "thread_map.json"
STATE_FILE = BASE_DIR / "posted_state.json"

VALID_MARKETS = {"US", "UK", "DE", "FR", "IT", "ES", "CA", "JP"}

CAPTION_MAX = 900
SEND_DELAY_SEC = float(os.getenv("TG_SEND_DELAY_SEC", "1.2"))
FALLBACK_TO_LOCAL_CSV = (os.getenv("FALLBACK_TO_LOCAL_CSV", "1").strip() != "0")

FLAG = {
    "US": "🇺🇸", "UK": "🇬🇧", "DE": "🇩🇪", "FR": "🇫🇷",
    "IT": "🇮🇹", "ES": "🇪🇸", "CA": "🇨🇦", "JP": "🇯🇵",
}

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


def safe_str(x) -> str:
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


def norm_text(s) -> str:
    s = safe_str(s)
    if not s:
        return ""
    return " ".join(s.split())


def norm_status(s) -> str:
    s = safe_str(s).lower()
    if s in ("removed", "inactive", "down", "off", "0", "false", "停售", "下架"):
        return "removed"
    return "active"


def parse_decimal_maybe(v) -> Optional[Decimal]:
    s = safe_str(v)
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
    if not cleaned:
        return None
    try:
        return Decimal(cleaned)
    except (InvalidOperation, ValueError):
        return None


def canonical_money_for_hash(v) -> str:
    """
    用于 hash：10 / 10.0 / 10.00 -> "10"
    为空或 0 -> ""
    无法解析 -> 归一化原文本
    """
    s = safe_str(v)
    if not s:
        return ""

    d = parse_decimal_maybe(s)
    if d is None:
        return norm_text(s)

    if d == 0:
        return ""

    normalized = d.normalize()
    as_str = format(normalized, "f")
    if "." in as_str:
        as_str = as_str.rstrip("0").rstrip(".")
    return as_str


def format_money_for_caption(v, market: str) -> Optional[str]:
    """
    文案显示：
    - 空/0 不显示
    - 已带符号原样
    - 纯数字：尾随符号 10$
    """
    s = safe_str(v)
    if not s:
        return None

    d = parse_decimal_maybe(s)
    if d is not None and d == 0:
        return None
    if s in ("0", "0.0", "0.00"):
        return None

    if any(sym in s for sym in ("$", "£", "€", "¥", "￥")):
        return s

    sym = CURRENCY_SYMBOL.get((market or "").upper(), "")
    if not sym:
        return s
    return f"{s}{sym}"


def tg_api(method: str, payload: dict, max_retry: int = 6):
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


def load_products() -> List[Dict[str, str]]:
    def _norm_market(s: str) -> str:
        return safe_str(s).upper()

    def _get(row: dict, *keys: str) -> str:
        for k in keys:
            if k in row and row.get(k) is not None:
                return safe_str(row.get(k))
        return ""

    def _normalize_row(row: dict) -> Dict[str, str]:
        market = _norm_market(_get(row, "market", "Market"))
        asin = _get(row, "asin", "ASIN")
        title = _get(row, "title", "Title")
        keyword = _get(row, "keyword", "Keyword")
        store = _get(row, "store", "Store")
        remark = _get(row, "remark", "Remark")
        link = _get(row, "link", "Link", "url", "URL")
        image_url = _get(row, "image_url", "image", "Image", "img")
        status = norm_status(_get(row, "status", "Status"))

        discount_price = _get(row, "discount_price", "Discount Price", "DiscountPrice", "discount")
        commission = _get(row, "commission", "Commission", "comm")

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


def build_caption(p: dict) -> str:
    market = safe_str(p.get("market")).upper()
    flag = FLAG.get(market, "")

    title = safe_str(p.get("title"))
    keyword = safe_str(p.get("keyword"))
    store = safe_str(p.get("store"))
    remark = safe_str(p.get("remark"))
    link = safe_str(p.get("link"))

    discount_price = format_money_for_caption(p.get("discount_price"), market)
    commission = format_money_for_caption(p.get("commission"), market)

    lines: List[str] = []

    # 只显示国旗，不显示中文国家名
    if title:
        head = f"{flag}{title}".strip() if flag else title
    else:
        head = f"{flag}(无标题)".strip() if flag else "(无标题)"
    lines.append(head)

    if keyword:
        lines.append(f"Keyword: {keyword}")
    if store:
        lines.append(f"Store: {store}")
    if remark:
        lines.append(f"Remark: {remark}")

    if discount_price:
        lines.append(f"Discount Price: {discount_price}")
    if commission:
        lines.append(f"Commission: {commission}")

    if link:
        lines.append(f"link:{link}")

    cap = "\n".join(lines)
    return cap[:CAPTION_MAX]


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
            print(f"[warn] sendPhoto failed -> fallback to text. market={p.get('market')} asin={p.get('asin')} img={img} err={e}")

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
            status = norm_status(p.get("status"))

            # 稳定 hash：文本归一化 + 价格规范化，避免重复发送
            content_hash = sha1(
                "|".join([
                    norm_text(p.get("title")),
                    norm_text(p.get("keyword")),
                    norm_text(p.get("store")),
                    norm_text(p.get("remark")),
                    norm_text(p.get("link")),
                    norm_text(p.get("image_url")),
                    canonical_money_for_hash(p.get("discount_price")),
                    canonical_money_for_hash(p.get("commission")),
                    status,
                ])
            )

            prev = state.get(key)

            # -------- removed：删除（关键修复：不清空 message_id）--------
            if status == "removed":
                delete_ok = bool(prev.get("delete_ok")) if isinstance(prev, dict) else False

                # 如果之前已删除成功且 hash 没变，就不再重复 delete
                if prev and prev.get("status") == "removed" and prev.get("hash") == content_hash and delete_ok:
                    skip_count += 1
                    continue

                attempted = False
                if prev and prev.get("message_id") and not delete_ok:
                    attempted = True
                    try:
                        tg_api("deleteMessage", {"chat_id": chat_id, "message_id": int(prev["message_id"])})
                        delete_ok = True
                        print("deleted:", key, "msg", prev["message_id"])
                    except Exception as e:
                        delete_ok = False
                        print("[warn] delete failed but continue:", key, str(e))

                state[key] = {
                    **(prev or {}),
                    "status": "removed",
                    "hash": content_hash,
                    "ts": int(time.time()),
                    "delete_attempted": attempted or bool((prev or {}).get("delete_attempted")),
                    "delete_ok": delete_ok,
                    # 注意：不清空 message_id，方便之后还能重试删除
                }
                ok_count += 1
                continue

            # -------- active --------
            # active 且无变化：跳过
            if prev and prev.get("status") == "active" and prev.get("hash") == content_hash and prev.get("message_id"):
                skip_count += 1
                continue

            # relist：removed -> active，强制重发（无论是否保留 message_id）
            if prev and prev.get("status") == "removed":
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
                print("reposted(after relist):", key, "msg", info["message_id"])
                ok_count += 1
                continue

            # 首次发布
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

            # 编辑已有消息
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
                ok_count += 1
            except Exception as e:
                err_count += 1
                print("[error] edit failed but continue:", key, str(e))
                continue

        except Exception as e:
            err_count += 1
            print(f"[error] product failed but continue. market={p.get('market')} asin={p.get('asin')} err={e}")
            continue

    save_json(STATE_FILE, state)
    print(f"done. ok={ok_count} skip={skip_count} err={err_count}. state saved -> {STATE_FILE}")


if __name__ == "__main__":
    main()
