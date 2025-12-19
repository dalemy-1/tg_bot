import os
import io
import csv
import json
import time
import hashlib
import signal
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

import requests

SYNC_PRODUCTS_VERSION = "2025-12-19-channels-final-slot-v1"

TG_TOKEN = (os.getenv("TG_BOT_TOKEN") or "").strip()
BASE_DIR = Path(__file__).resolve().parent

# 旧的“话题映射”（论坛群组用），仍保留兼容
MAP_FILE = BASE_DIR / "thread_map.json"

# 新的“频道映射”（方案B：每国一个频道）
CHANNEL_MAP_FILE = BASE_DIR / "channel_map.json"

STATE_FILE = BASE_DIR / "posted_state.json"

VALID_MARKETS = {"US", "UK", "DE", "FR", "IT", "ES", "CA", "JP"}

CAPTION_MAX = 900
SEND_DELAY_SEC = float(os.getenv("TG_SEND_DELAY_SEC", "2.0"))

FALLBACK_TO_LOCAL_CSV = (os.getenv("FALLBACK_TO_LOCAL_CSV", "1").strip() != "0")
BAD_IMAGE_POLICY = (os.getenv("BAD_IMAGE_POLICY") or "fallback_text").strip().lower()

# 仅用于“列表缺失则删”的安全阈值（可不启用）
PURGE_MISSING = (os.getenv("PURGE_MISSING", "1").strip() != "0")  # ✅ 默认启用：以列表为准
PURGE_MIN_ROWS = int(os.getenv("PURGE_MIN_ROWS", "50"))
PURGE_MIN_ACTIVE_RATIO = float(os.getenv("PURGE_MIN_ACTIVE_RATIO", "0.5"))

FETCH_RETRY = int(os.getenv("FETCH_RETRY", "2"))
FETCH_TIMEOUT = int(os.getenv("FETCH_TIMEOUT", "30"))

MAX_ACTIONS_PER_RUN = int(os.getenv("MAX_ACTIONS_PER_RUN", "250"))

# RESET_STATE=1 会把旧 state 备份并清空，从零开始发
RESET_STATE = (os.getenv("RESET_STATE", "0").strip() == "1")

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


# -------------------- utils --------------------

def safe_str(x) -> str:
    return ("" if x is None else str(x)).strip()


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _decode_bytes(b: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "gb18030"):
        try:
            return b.decode(enc)
        except UnicodeDecodeError:
            continue
    return b.decode("utf-8", errors="replace")


def norm_text(v) -> str:
    s = safe_str(v)
    if not s:
        return ""
    return " ".join(s.split())


def normalize_url(u: str) -> str:
    """
    防止 sheet 里出现 "https://a https://b" 或 "https://xx\n" 之类脏值导致 hash 抖动。
    规则：提取第一个 http(s):// 开头的 token。
    """
    s = safe_str(u)
    if not s:
        return ""
    s = " ".join(s.replace("\n", " ").replace("\r", " ").split())
    parts = s.split()
    for p in parts:
        if p.startswith("http://") or p.startswith("https://"):
            return p
    # 没有明显 URL，则原样压缩空白返回
    return s


def norm_status(v) -> str:
    s = safe_str(v).lower()
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


def load_json_safe(p: Path, default):
    if not p.exists():
        return default
    raw = p.read_text(encoding="utf-8", errors="replace").strip()
    if not raw:
        backup = p.with_suffix(".empty.bak")
        p.rename(backup)
        print(f"[warn] {p.name} was empty, backed up to {backup.name}, start fresh.")
        return default
    try:
        return json.loads(raw)
    except Exception as e:
        backup = p.with_suffix(f".bad_{int(time.time())}.bak")
        p.rename(backup)
        print(f"[warn] {p.name} JSON invalid, backed up to {backup.name}, start fresh. err={e}")
        return default


def save_json_atomic(p: Path, obj):
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, p)


def _looks_like_html(text: str) -> bool:
    head = (text or "").lstrip().lower()[:500]
    return head.startswith("<!doctype html") or head.startswith("<html") or "<body" in head[:250]


def _validate_header(fieldnames: Optional[List[str]], source: str):
    if not fieldnames:
        raise ValueError(f"{source}: CSV/TSV header missing/empty fieldnames.")
    cols_lower = {safe_str(c).lower() for c in fieldnames if safe_str(c)}
    if "market" not in cols_lower or "asin" not in cols_lower:
        raise ValueError(f"{source}: header invalid. Need columns market & asin. Got={fieldnames}")


def _build_reader(text: str) -> csv.DictReader:
    if not text.strip():
        raise ValueError("empty content")
    if _looks_like_html(text):
        raise ValueError("content looks like HTML, not CSV/TSV")

    sample = text[:4096]

    if "\t" in sample:
        reader = csv.DictReader(io.StringIO(text), delimiter="\t")
        if reader.fieldnames and len(reader.fieldnames) >= 2:
            return reader

    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", "\t", ";", "|"])
        reader = csv.DictReader(io.StringIO(text), delimiter=dialect.delimiter)
        if reader.fieldnames and len(reader.fieldnames) >= 2:
            return reader
    except Exception:
        pass

    lines = [ln for ln in text.splitlines() if ln.strip()]
    if not lines:
        raise ValueError("no non-empty lines")

    header_tokens = lines[0].split()
    if len(header_tokens) >= 2:
        rebuilt = "\t".join(header_tokens) + "\n"
        for ln in lines[1:]:
            rebuilt += "\t".join(ln.split()) + "\n"
        return csv.DictReader(io.StringIO(rebuilt), delimiter="\t")

    raise ValueError(f"cannot detect delimiter; header={lines[0][:200]!r}")


def _fetch_text_with_retry(url: str) -> str:
    last_err = None
    for attempt in range(FETCH_RETRY + 1):
        try:
            r = requests.get(url, timeout=FETCH_TIMEOUT)
            r.raise_for_status()
            text = _decode_bytes(r.content)

            ctype = (r.headers.get("content-type") or "").lower()
            if "text/html" in ctype or _looks_like_html(text):
                raise ValueError("URL returned HTML (likely login/error page).")

            return text
        except Exception as e:
            last_err = e
            wait = 1 + attempt * 2
            print(f"[warn] fetch failed ({attempt+1}/{FETCH_RETRY+1}): {e}. wait {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"fetch failed after retries: {last_err}")


# -------------------- Telegram --------------------

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


def is_not_modified_error(err: Exception) -> bool:
    s = str(err).lower()
    return ("message is not modified" in s) or ("specified new message content" in s)


def is_message_not_found(err: Exception) -> bool:
    s = str(err).lower()
    return ("message to edit not found" in s) or ("message to delete not found" in s)


def is_bad_image_error(err: Exception) -> bool:
    s = str(err).lower()
    keys = [
        "wrong type of the web page content",
        "failed to get http url content",
        "webpage_media_empty",
        "wrong file identifier",
        "can't parse",
        "bad request",
    ]
    return any(k in s for k in keys)


# -------------------- products load --------------------

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
        image_url = _get(row, "image_url", "image", "Image", "img", "image_urlDiscount", "image_urldiscount")

        # 你的表头可能是 Discount / Discount Price / DiscountPrice；Commission 同理
        discount_price = _get(row, "discount_price", "Discount Price", "DiscountPrice", "discount", "Discount", "Discoun", "DiscountP", "DiscountPrice ")
        commission = _get(row, "commission", "Commission", "comm", "Commissic", "Commissio", "Commiss", "Commision")

        status = norm_status(_get(row, "status", "Status", "removed"))

        return {
            "market": market,
            "asin": asin,
            "title": title,
            "keyword": keyword,
            "store": store,
            "remark": remark,
            "link": normalize_url(link),
            "image_url": normalize_url(image_url),
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
        text = _decode_bytes(csv_path.read_bytes())
        reader = _build_reader(text)
        print(f"[debug] local fieldnames: {reader.fieldnames}")
        _validate_header(reader.fieldnames, "local products.csv")
        for row in reader:
            if row:
                rows.append(_normalize_row(row))
        print(f"[ok] loaded from local: {len(rows)} rows ({csv_path})")

    if sheet_url:
        try:
            text = _fetch_text_with_retry(sheet_url)
            reader = _build_reader(text)
            print(f"[debug] remote fieldnames: {reader.fieldnames}")
            _validate_header(reader.fieldnames, "CSV/TSV URL")
            for row in reader:
                if row:
                    rows.append(_normalize_row(row))
            print(f"[ok] loaded from URL: {len(rows)} rows")
        except Exception as e:
            print(f"[warn] failed to load URL, err={e}")
            if FALLBACK_TO_LOCAL_CSV:
                print("[warn] fallback to local products.csv ...")
                _load_from_local()
            else:
                raise
    else:
        _load_from_local()

    if not rows:
        raise ValueError("No rows loaded from source (empty).")

    return rows


def assign_slots(products: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    ✅ 关键：支持重复 asin
    同一 market+asin 出现 N 次 -> 生成 asin, asin_1, asin_2...
    slot key 用 market:slot_asin 存到 state 里，确保每个重复项独立跟踪 message_id / hash。
    """
    counter: Dict[Tuple[str, str], int] = {}
    out: List[Dict[str, str]] = []
    for p in products:
        market = safe_str(p.get("market")).upper()
        asin = safe_str(p.get("asin"))
        if not asin or market not in VALID_MARKETS:
            continue
        k = (market, asin)
        idx = counter.get(k, 0)
        counter[k] = idx + 1

        slot_asin = asin if idx == 0 else f"{asin}_{idx}"
        p2 = dict(p)
        p2["_slot_asin"] = slot_asin
        p2["_slot_key"] = f"{market}:{slot_asin}"
        p2["_base_asin"] = asin
        p2["_dup_index"] = idx
        out.append(p2)
    return out


# -------------------- caption/hash --------------------

def build_caption(p: dict) -> str:
    market = safe_str(p.get("market")).upper()
    flag = FLAG.get(market, "")

    # 展示时仍使用“原始 asin”可选（你也可以不显示 asin）
    title = safe_str(p.get("title"))
    keyword = safe_str(p.get("keyword"))
    store = safe_str(p.get("store"))
    remark = safe_str(p.get("remark"))
    link = safe_str(p.get("link"))

    discount_price = format_money_for_caption(p.get("discount_price"), market)
    commission = format_money_for_caption(p.get("commission"), market)

    lines: List[str] = []

    head = f"{flag}{title}".strip() if title else f"{flag}(无标题)".strip()
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

    # 固定联系邮箱
    lines.append("Contact Email: info@omino.top")

    cap = "\n".join(lines)
    return cap[:CAPTION_MAX]


def compute_content_hash(p: dict, status: str) -> str:
    # hash 只看内容字段，不看 slot_key，确保“同内容”不乱 edit
    return sha1(
        "|".join([
            norm_text(p.get("title")),
            norm_text(p.get("keyword")),
            norm_text(p.get("store")),
            norm_text(p.get("remark")),
            norm_text(normalize_url(p.get("link"))),
            norm_text(normalize_url(p.get("image_url"))),
            canonical_money_for_hash(p.get("discount_price")),
            canonical_money_for_hash(p.get("commission")),
            status,
        ])
    )


# -------------------- send/edit/delete --------------------

def send_new(target_chat_id, thread_id: Optional[int], p: dict) -> Tuple[Optional[dict], Optional[str]]:
    caption = build_caption(p)
    img = safe_str(p.get("image_url"))

    def _payload_base():
        payload = {"chat_id": target_chat_id}
        if thread_id is not None:
            payload["message_thread_id"] = int(thread_id)
        return payload

    if img:
        try:
            payload = _payload_base()
            payload.update({"photo": img, "caption": caption})
            res = tg_api("sendPhoto", payload)
            time.sleep(SEND_DELAY_SEC)
            return {"message_id": res["message_id"], "kind": "photo", "image_url": img}, None
        except Exception as e:
            if BAD_IMAGE_POLICY == "skip" and is_bad_image_error(e):
                print(f"[skip] bad image -> skip product. market={p.get('market')} asin={p.get('asin')} err={e}")
                return None, "BAD_IMAGE_SKIP"
            print(f"[warn] sendPhoto failed -> fallback to text. market={p.get('market')} asin={p.get('asin')} img={img} err={e}")

    payload = _payload_base()
    payload.update({"text": caption, "disable_web_page_preview": True})
    res = tg_api("sendMessage", payload)
    time.sleep(SEND_DELAY_SEC)
    return {"message_id": res["message_id"], "kind": "text", "image_url": ""}, None


def edit_existing(target_chat_id, message_id: int, prev: dict, p: dict) -> Tuple[dict, bool, bool]:
    """
    返回 (new_meta, did_action, message_missing)
    - did_action=False: Telegram 提示 not modified（不计动作）
    - message_missing=True: 旧消息不存在/不在该 chat（上层应清掉 message_id 触发重发）
    """
    caption = build_caption(p)

    prev_kind = safe_str(prev.get("kind") or "text")
    prev_img = safe_str(prev.get("image_url"))
    new_img = safe_str(p.get("image_url"))

    try:
        if prev_kind == "photo":
            if new_img and new_img != prev_img:
                try:
                    tg_api("editMessageMedia", {
                        "chat_id": target_chat_id,
                        "message_id": int(message_id),
                        "media": {"type": "photo", "media": new_img, "caption": caption}
                    })
                    time.sleep(SEND_DELAY_SEC)
                    return {"kind": "photo", "image_url": new_img}, True, False
                except Exception as e:
                    if is_message_not_found(e):
                        return {"kind": "photo", "image_url": prev_img}, False, True
                    if is_not_modified_error(e):
                        return {"kind": "photo", "image_url": prev_img}, False, False
                    print(f"[warn] editMessageMedia failed -> fallback to edit caption only. msg={message_id} err={e}")

            try:
                tg_api("editMessageCaption", {
                    "chat_id": target_chat_id,
                    "message_id": int(message_id),
                    "caption": caption,
                })
                time.sleep(SEND_DELAY_SEC)
                return {"kind": "photo", "image_url": prev_img}, True, False
            except Exception as e:
                if is_message_not_found(e):
                    return {"kind": "photo", "image_url": prev_img}, False, True
                if is_not_modified_error(e):
                    return {"kind": "photo", "image_url": prev_img}, False, False
                raise

        # text
        try:
            tg_api("editMessageText", {
                "chat_id": target_chat_id,
                "message_id": int(message_id),
                "text": caption,
                "disable_web_page_preview": True,
            })
            time.sleep(SEND_DELAY_SEC)
            return {"kind": "text", "image_url": ""}, True, False
        except Exception as e:
            if is_message_not_found(e):
                return {"kind": "text", "image_url": ""}, False, True
            if is_not_modified_error(e):
                return {"kind": "text", "image_url": ""}, False, False
            raise
    except Exception as e:
        if is_message_not_found(e):
            return {"kind": prev_kind, "image_url": prev_img}, False, True
        raise


def delete_message(target_chat_id, message_id: int) -> bool:
    try:
        tg_api("deleteMessage", {"chat_id": target_chat_id, "message_id": int(message_id)})
        return True
    except Exception as e:
        if is_message_not_found(e):
            return True
        print(f"[warn] delete failed but continue: chat={target_chat_id} msg={message_id} err={e}")
        return False


# -------------------- targets (channels or forum topics) --------------------

def load_channel_map() -> Dict[str, str]:
    if not CHANNEL_MAP_FILE.exists():
        return {}
    m = load_json_safe(CHANNEL_MAP_FILE, {})
    out = {}
    if isinstance(m, dict):
        for k, v in m.items():
            mk = safe_str(k).upper()
            if mk in VALID_MARKETS and safe_str(v):
                out[mk] = safe_str(v)
    return out


def pick_forum_chat_id(thread_map_all: dict) -> str:
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


# -------------------- main --------------------

_should_exit = False

def _handle_signal(signum, frame):
    global _should_exit
    _should_exit = True
    print(f"[warn] received signal={signum}, will exit after saving state...")


def main():
    global _should_exit
    print("SYNC_PRODUCTS_VERSION =", SYNC_PRODUCTS_VERSION)
    print(f"[debug] BAD_IMAGE_POLICY={BAD_IMAGE_POLICY} PURGE_MISSING={PURGE_MISSING} TG_SEND_DELAY_SEC={SEND_DELAY_SEC}")
    print(f"[debug] PURGE_MIN_ROWS={PURGE_MIN_ROWS} PURGE_MIN_ACTIVE_RATIO={PURGE_MIN_ACTIVE_RATIO} FETCH_RETRY={FETCH_RETRY} FETCH_TIMEOUT={FETCH_TIMEOUT}")
    print(f"[debug] MAX_ACTIONS_PER_RUN={MAX_ACTIONS_PER_RUN} RESET_STATE={RESET_STATE}")

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    if not TG_TOKEN:
        raise SystemExit("Missing TG_BOT_TOKEN env var.")

    # 优先使用 channel_map.json（方案B：每国一个频道）
    channel_map = load_channel_map()
    use_channels = bool(channel_map)

    # 论坛话题模式（兼容旧方案）
    forum_chat_id = None
    thread_map = None
    if not use_channels:
        if not MAP_FILE.exists():
            raise SystemExit("Missing channel_map.json AND missing thread_map.json. Provide one of them.")
        thread_map_all = load_json_safe(MAP_FILE, {})
        forum_chat_id = pick_forum_chat_id(thread_map_all)
        thread_map = thread_map_all.get(forum_chat_id, {})
        print(f"[ok] mode=forum_topics chat_id={forum_chat_id}")
    else:
        print(f"[ok] mode=channels markets={sorted(channel_map.keys())}")

    # state reset
    if RESET_STATE and STATE_FILE.exists():
        bak = STATE_FILE.with_suffix(f".reset_{int(time.time())}.bak")
        STATE_FILE.replace(bak)
        print(f"[warn] RESET_STATE=1 -> backed up old state to {bak.name}")

    state: Dict[str, Any] = load_json_safe(STATE_FILE, {})

    raw_products = load_products()
    products = assign_slots(raw_products)  # ✅ slot 化，支持重复 asin

    ok_count = 0
    err_count = 0
    actions_done = 0
    stopped_due_to_limit = False

    def at_limit() -> bool:
        return actions_done >= MAX_ACTIONS_PER_RUN

    def target_for_market(market: str) -> Tuple[str, Optional[int]]:
        """
        返回 (chat_id, thread_id)
        - channels: (channel chat, None)
        - forum topics: (forum chat, thread_id)
        """
        if use_channels:
            cid = channel_map.get(market)
            if not cid:
                raise RuntimeError(f"channel_map missing market={market}")
            return cid, None

        if forum_chat_id is None or thread_map is None:
            raise RuntimeError("forum mode not initialized")
        tid = thread_map.get(market)
        if not tid:
            raise RuntimeError(f"thread_map missing market={market}")
        return int(forum_chat_id), int(tid)

    # 本次列表应该存在的 key（只按 active）
    seen_keys = set()
    for p in products:
        if norm_status(p.get("status")) != "active":
            continue
        seen_keys.add(p["_slot_key"])

    # -------------------- Stage 0: safety for purge --------------------
    # 目的：避免 URL 临时拉空/行数异常导致大规模删消息
    purge_allowed = True
    if PURGE_MISSING:
        curr_seen = len(seen_keys)
        prev_active = sum(1 for v in state.values() if isinstance(v, dict) and v.get("status") == "active")

        if curr_seen < PURGE_MIN_ROWS:
            purge_allowed = False
            print(f"[warn] PURGE blocked: seen_keys too small ({curr_seen} < {PURGE_MIN_ROWS})")
        elif prev_active > 0 and curr_seen < int(prev_active * PURGE_MIN_ACTIVE_RATIO):
            purge_allowed = False
            print(f"[warn] PURGE blocked: seen_keys too small vs prev_active ({curr_seen} < {int(prev_active * PURGE_MIN_ACTIVE_RATIO)})")

    # -------------------- Stage 1: delete missing slots (list is source of truth) --------------------
    if PURGE_MISSING and purge_allowed:
        to_delete = []
        for k, v in state.items():
            if not isinstance(v, dict):
                continue
            if v.get("status") != "active":
                continue
            if k not in seen_keys:
                to_delete.append(k)

        if to_delete:
            print(f"[warn] list missing -> will delete extra slots: {len(to_delete)}")

        for key in to_delete:
            if _should_exit or stopped_due_to_limit:
                break
            if at_limit():
                stopped_due_to_limit = True
                print(f"[warn] action limit reached, stop before delete-missing.")
                break
            prev = state.get(key) if isinstance(state.get(key), dict) else {}
            msg_id = prev.get("message_id")
            msg_chat = prev.get("chat_id") or (forum_chat_id if not use_channels else None)

            delete_ok = True
            if msg_chat and msg_id:
                delete_ok = delete_message(msg_chat, msg_id)
                actions_done += 1

            # 删除后直接从 state 移除，避免下次还认为存在
            if delete_ok:
                state.pop(key, None)
                ok_count += 1
                print("deleted(missing):", key, "msg", msg_id)
            else:
                # 删除失败：保留记录但标 removed，避免狂删重试
                state[key] = {**prev, "status": "removed", "ts": int(time.time())}
                print("delete_failed(mark_removed):", key, "msg", msg_id)

    elif PURGE_MISSING and not purge_allowed:
        print("[warn] PURGE_MISSING enabled but blocked by safety thresholds; skip delete-missing this run.")

    # -------------------- Stage 2: edit existing when content changed --------------------
    if not stopped_due_to_limit:
        for p in products:
            if _should_exit or stopped_due_to_limit:
                break
            try:
                if norm_status(p.get("status")) != "active":
                    continue

                key = p["_slot_key"]
                prev = state.get(key) if isinstance(state.get(key), dict) else None
                if not prev or not prev.get("message_id") or prev.get("status") != "active":
                    continue

                content_hash = compute_content_hash(p, "active")
                if prev.get("hash") == content_hash:
                    continue

                if at_limit():
                    stopped_due_to_limit = True
                    print(f"[warn] action limit reached, stop before edit: {key}")
                    break

                market = safe_str(p.get("market")).upper()
                msg_chat = prev.get("chat_id")
                if not msg_chat:
                    msg_chat, _ = target_for_market(market)

                msg_id = int(prev["message_id"])
                new_meta, did_action, missing = edit_existing(msg_chat, msg_id, prev, p)

                if missing:
                    # 旧消息不在了 -> 清掉 message_id，后续发布阶段会重发
                    state[key] = {**prev, "hash": content_hash, "ts": int(time.time()), "message_id": None}
                    print("missing(edit)->will repost:", key, "old_msg", msg_id)
                    continue

                if did_action:
                    actions_done += 1
                    print("edited:", key, "msg", msg_id)
                else:
                    print("nochange(edit):", key, "msg", msg_id)

                state[key] = {
                    **prev,
                    "hash": content_hash,
                    "status": "active",
                    "kind": new_meta["kind"],
                    "image_url": new_meta["image_url"],
                    "ts": int(time.time()),
                    "chat_id": msg_chat,
                }
                ok_count += 1

            except Exception as e:
                err_count += 1
                print(f"[error] edit failed but continue. key={p.get('_slot_key')} err={e}")

    # -------------------- Stage 3: post new slots (and repost if message_id cleared) --------------------
    if not stopped_due_to_limit:
        for p in products:
            if _should_exit or stopped_due_to_limit:
                break
            try:
                if norm_status(p.get("status")) != "active":
                    continue

                key = p["_slot_key"]
                prev = state.get(key) if isinstance(state.get(key), dict) else None

                market = safe_str(p.get("market")).upper()
                target_chat, thread_id = target_for_market(market)
                content_hash = compute_content_hash(p, "active")

                if prev and prev.get("status") == "active" and prev.get("hash") == content_hash and prev.get("message_id"):
                    continue

                if (not prev) or (not prev.get("message_id")) or (prev.get("status") != "active"):
                    if at_limit():
                        stopped_due_to_limit = True
                        print(f"[warn] action limit reached, stop before post: {key}")
                        break

                    info, err_code = send_new(target_chat, thread_id, p)
                    if err_code == "BAD_IMAGE_SKIP":
                        continue

                    actions_done += 1
                    state[key] = {
                        "chat_id": target_chat,
                        "message_id": info["message_id"],
                        "hash": content_hash,
                        "status": "active",
                        "kind": info["kind"],
                        "image_url": info["image_url"],
                        "ts": int(time.time()),
                    }
                    print("posted:", key, "msg", info["message_id"])
                    ok_count += 1

            except Exception as e:
                err_count += 1
                print(f"[error] post failed but continue. key={p.get('_slot_key')} err={e}")

    save_json_atomic(STATE_FILE, state)
    print(f"done. ok={ok_count} err={err_count} actions={actions_done}/{MAX_ACTIONS_PER_RUN}. state saved -> {STATE_FILE}")


if __name__ == "__main__":
    main()
