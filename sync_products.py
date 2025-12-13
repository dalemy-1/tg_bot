# sync_products.py  (FINAL - SAFE + MONEY)
import os
import io
import csv
import json
import time
import hashlib
from pathlib import Path
from typing import Dict, Any, List

import requests

SYNC_PRODUCTS_VERSION = "2025-12-14-03"

TG_TOKEN = (os.getenv("TG_BOT_TOKEN") or "").strip()
BASE_DIR = Path(__file__).resolve().parent

MAP_FILE = BASE_DIR / "thread_map.json"
STATE_FILE = BASE_DIR / "posted_state.json"

VALID_MARKETS = {"US", "UK", "DE", "FR", "IT", "ES", "CA", "JP"}

# 文本最大长度（caption/text）
CAPTION_MAX = 900

# 节流：每条消息后睡眠（秒），降低 429 概率
SEND_DELAY_SEC = float(os.getenv("TG_SEND_DELAY_SEC", "1.2"))

# 如果 Google Sheet 拉取失败，是否自动回退本地 products.csv（建议开启）
FALLBACK_TO_LOCAL_CSV = (os.getenv("FALLBACK_TO_LOCAL_CSV", "1").strip() != "0")

FLAG = {
    "US": "🇺🇸", "UK": "🇬🇧", "DE": "🇩🇪", "FR": "🇫🇷",
    "IT": "🇮🇹", "ES": "🇪🇸", "CA": "🇨🇦", "JP": "🇯🇵",
}

COUNTRY_CN = {
    "US": "美国", "UK": "英国", "DE": "德国", "FR": "法国",
    "IT": "意大利", "ES": "西班牙", "CA": "加拿大", "JP": "日本",
}

# 市场默认货币符号
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
    return (x or "").strip()


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


def _to_number(s: str):
    """
    把 '10', '10.00', ' 10$ ' -> 10.0；解析失败返回 None
    """
    if s is None:
        return None
    s = safe_str(str(s))
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


def format_money(value, market: str):
    """
    规则：
    - 空/解析为0/文本等于0 -> 返回 None（不输出）
    - 若 value 已包含货币符号（$ £ € ¥ ￥）-> 原样返回（只 strip）
    - 若是纯数字 -> 按市场货币符号拼成 '10$' 这种尾随格式
    """
    if value is None:
        return None
    s = safe_str(str(value))
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

    # 输出：10$ 这种尾随符号格式（按你的示例）
    return f"{s}{sym}"


def is_bad_image_error(err: Exception) -> bool:
    """识别 Telegram 对图片 URL 的常见报错，遇到就降级发文本，不要中止。"""
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


def tg_api(method: str, payload: dict, max_retry: int = 6):
    """
    Telegram API wrapper:
    - 自动处理 429 限流（按 retry_after 等待后重试）
    - 其他错误直接抛出（由外层单条 try/except 吃掉，继续下一个产品）
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

        # 429 限流：等待后重试
        if err_code == 429:
            retry_after = 5
            params = data.get("parameters") or {}
            if isinstance(params, dict) and params.get("retry_after"):
                retry_after = int(params["retry_after"])
            wait_s = retry_after + 1
            print(f"[warn] 429 Too Many Requests, wait {wait_s}s then retry... ({attempt+1}/{max_retry})")
            time.sleep(wait_s)
            continue

        # 其他错误：抛出
        raise RuntimeError(f"{method} failed: {data}")

    raise RuntimeError(f"{method} failed after retries (429).")


def load_products() -> List[Dict[str, str]]:
    """
    From GOOGLE_SHEET_CSV_URL (preferred) or local products.csv (fallback).
    支持字段：
      market, asin, title, keyword, store, remark, link, image_url, status,
      discount_price, commission
    """
    def _norm_status(s: str) -> str:
        s = safe_str(s).lower()
        if s in ("removed", "inactive", "down", "off", "0", "false", "停售", "下架"):
            return "removed"
        return "active"

    def _norm_market(s: str) -> str:
        return safe_str(s).upper()

    def _normalize_row(row: dict) -> dict:
        market = _norm_market(row.get("market") or row.get("Market"))
        asin = safe_str(row.get("asin") or row.get("ASIN"))
        title = safe_str(row.get("title") or row.get("Title"))
        keyword = safe_str(row.get("keyword") or row.get("Keyword"))
        store = safe_str(row.get("store") or row.get("Store"))
        remark = safe_str(row.get("remark") or row.get("Remark"))
        link = safe_str(row.get("link") or row.get("Link") or row.get("url") or row.get("URL"))
        image_url = safe_str(row.get("image_url") or row.get("image") or row.get("Image") or row.get("img"))
        status = _norm_status(row.get("status") or row.get("Status"))

        # 新增：Discount Price / Commission（兼容多种列名）
        discount_price = safe_str(
            row.get("discount_price") or row.get("Discount Price") or row.get("DiscountPrice") or row.get("discount")
        )
        commission = safe_str(
            row.get("commission") or row.get("Commission") or row.get("comm")
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



def send_new(chat_id: int, thread_id: int, p: dict) -> dict:
    """
    发新消息：
    - 有图先发图
    - 图片失败：自动降级发文本（不会中止）
    """
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
    """
    编辑已有消息（不让“类型变化”导致终止）：
    - 之前是 photo：无论新数据有没有 image_url，都只编辑 caption；
      如果新 image_url 与旧不同，尝试 editMessageMedia；失败则退回只改 caption。
    - 之前是 text：只编辑 text；就算新数据有 image_url，也忽略图片（避免无法 edit text->photo）。
    """
    caption = build_caption(p)

    prev_kind = safe_str(prev.get("kind") or "text")
    prev_img = safe_str(prev.get("image_url"))
    new_img = safe_str(p.get("image_url"))

    if prev_kind == "photo":
        # 1) 优先：如果新图与旧图不同，尝试改 media
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

        # 2) 只改 caption（不管 new_img 是否为空）
        tg_api("editMessageCaption", {
            "chat_id": chat_id,
            "message_id": message_id,
            "caption": caption,
        })
        time.sleep(SEND_DELAY_SEC)
        return {"kind": "photo", "image_url": prev_img}

    # text：只改文本（忽略 new_img，避免无法编辑为图片）
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

    # 这些是“必须正确”的，否则没法工作；这里仍然要中止
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

            # 把新字段也纳入 hash，确保价格/佣金变化会触发编辑
            content_hash = sha1(
                f"{safe_str(p.get('title'))}|{safe_str(p.get('keyword'))}|{safe_str(p.get('store'))}|"
                f"{safe_str(p.get('remark'))}|{safe_str(p.get('link'))}|{safe_str(p.get('image_url'))}|"
                f"{safe_str(p.get('discount_price'))}|{safe_str(p.get('commission'))}|{status}"
            )

            prev = state.get(key)

            if status == "removed":
                if prev and prev.get("message_id"):
                    try:
                        tg_api("deleteMessage", {"chat_id": chat_id, "message_id": int(prev["message_id"])})
                        print("deleted:", key, "msg", prev["message_id"])
                    except Exception as e:
                        print("[warn] delete failed but continue:", key, str(e))

                state[key] = {
                    **(prev or {}),
                    "status": "removed",
                    "message_id": None,
                    "kind": None,
                    "image_url": "",
                    "hash": content_hash,
                    "ts": int(time.time()),
                }
                ok_count += 1
                continue

            if prev and prev.get("status") == "active" and prev.get("hash") == content_hash and prev.get("message_id"):
                skip_count += 1
                continue

            if not prev or not prev.get("message_id"):
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
                ok_count += 1
                continue

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


