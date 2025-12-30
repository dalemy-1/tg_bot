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

# ==================== version ====================
SYNC_PRODUCTS_VERSION = "2025-12-19-dup-asin-final"

TG_TOKEN = (os.getenv("TG_BOT_TOKEN") or "").strip()
BASE_DIR = Path(__file__).resolve().parent

CHANNEL_MAP_FILE = BASE_DIR / "channel_map.json"
STATE_FILE = BASE_DIR / "posted_state.json"

VALID_MARKETS = {"US", "UK", "DE", "FR", "IT", "ES", "CA", "JP"}

CAPTION_MAX = 900
SEND_DELAY_SEC = float(os.getenv("TG_SEND_DELAY_SEC", "2.0"))

FALLBACK_TO_LOCAL_CSV = (os.getenv("FALLBACK_TO_LOCAL_CSV", "1").strip() != "0")
BAD_IMAGE_POLICY = (os.getenv("BAD_IMAGE_POLICY") or "fallback_text").strip().lower()

# ✅ 以“列表”为准：如果某个 market:asin 组完全从表里消失，会删除该组所有消息（受安全阈值保护）
PURGE_MISSING = (os.getenv("PURGE_MISSING", "1").strip() == "1")
PURGE_MIN_ROWS = int(os.getenv("PURGE_MIN_ROWS", "50"))
PURGE_MIN_ACTIVE_RATIO = float(os.getenv("PURGE_MIN_ACTIVE_RATIO", "0.5"))

FETCH_RETRY = int(os.getenv("FETCH_RETRY", "2"))
FETCH_TIMEOUT = int(os.getenv("FETCH_TIMEOUT", "30"))

MAX_ACTIONS_PER_RUN = int(os.getenv("MAX_ACTIONS_PER_RUN", "250"))

# RESET_STATE=1 会把旧 state 备份并清空，从零开始发（会导致全部重发）
RESET_STATE = (os.getenv("RESET_STATE", "0").strip() == "1")

# ✅ 只迁移 state，不做 Telegram 动作（用于把旧 flat posted_state.json 升级成 groups）
MIGRATE_ONLY = (os.getenv("MIGRATE_ONLY", "0").strip() == "1")

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

# ==================== utils ====================

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

def norm_status(v) -> str:
    s = safe_str(v).lower()
    if s in ("removed", "inactive", "down", "off", "0", "false", "停售", "下架"):
        return "removed"
    return "active"

def norm_asin(v) -> str:
    # ✅ 关键：ASIN 统一大写、去空格，避免 key 对不上导致“重头发”
    s = safe_str(v).upper().replace(" ", "")
    return s

def normalize_group_key(k: str) -> Optional[str]:
    if not isinstance(k, str) or ":" not in k:
        return None
    mk, asin = k.split(":", 1)
    mk = safe_str(mk).upper()
    asin = norm_asin(asin)
    if mk not in VALID_MARKETS or not asin:
        return None
    return f"{mk}:{asin}"

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

# ==================== Telegram ====================

# ✅ 仅新增：区分“网络问题”和“API 业务错误”，方便主流程优雅降级
class TelegramNetworkError(RuntimeError):
    pass

class TelegramApiError(RuntimeError):
    pass

def tg_api(method: str, payload: dict, max_retry: int = 6):
    if not TG_TOKEN:
        raise TelegramApiError("Missing TG_BOT_TOKEN")

    url = f"https://api.telegram.org/bot{TG_TOKEN}/{method}"

    for attempt in range(max_retry):
        try:
            r = requests.post(url, json=payload, timeout=30)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            # ✅ 网络不可达/超时：重试（这是 Actions “Network is unreachable” 的根因）
            wait_s = 2 + attempt * 2
            print(f"[warn] tg_api network error {type(e).__name__}: {e}. wait {wait_s}s ({attempt+1}/{max_retry})")
            time.sleep(wait_s)
            if attempt == max_retry - 1:
                raise TelegramNetworkError(f"{method} network failed after retries: {e}")
            continue

        # ✅ Telegram 偶尔返回 5xx：也重试
        if r.status_code in (500, 502, 503, 504):
            wait_s = 2 + attempt * 2
            print(f"[warn] tg_api server {r.status_code}, retry in {wait_s}s ({attempt+1}/{max_retry})")
            time.sleep(wait_s)
            if attempt == max_retry - 1:
                raise TelegramNetworkError(f"{method} server error {r.status_code} after retries: {r.text[:200]}")
            continue

        try:
            data = r.json()
        except Exception:
            raise TelegramApiError(f"{method} HTTP {r.status_code}: {r.text}")

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

        raise TelegramApiError(f"{method} failed: {data}")

    raise TelegramApiError(f"{method} failed after retries.")

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

# ==================== products load ====================

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
        asin = norm_asin(_get(row, "asin", "ASIN"))

        title = _get(row, "title", "Title")
        keyword = _get(row, "keyword", "Keyword")
        store = _get(row, "store", "Store")
        remark = _get(row, "remark", "Remark")
        link = _get(row, "link", "Link", "url", "URL")

        image_url = _get(row, "image_url", "image", "Image", "img", "imageUrl", "imageURL")

        status = norm_status(_get(row, "status", "Status"))

        discount_price = _get(row, "discount_price", "Discount Price", "DiscountPrice", "discount", "Discount", "Discoun")
        commission = _get(row, "commission", "Commission", "comm", "Commissic", "Commissio", "Commiss", "Commision")

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

# ==================== caption/hash ====================

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

    lines.append("Product list: ama.omino.top")

    cap = "\n".join(lines)
    return cap[:CAPTION_MAX]

def compute_content_hash(p: dict, status: str) -> str:
    footer = "Product list: ama.omino.top"  # ✅ 固定尾巴纳入 hash
    return sha1(
        "|".join([
            norm_text(p.get("market")),
            norm_text(p.get("asin")),
            norm_text(p.get("title")),
            norm_text(p.get("keyword")),
            norm_text(p.get("store")),
            norm_text(p.get("remark")),
            norm_text(p.get("image_url")),
            canonical_money_for_hash(p.get("discount_price")),
            canonical_money_for_hash(p.get("commission")),
            status,
            footer,  # ✅ 新增
        ])
    )

# ==================== send/edit/delete ====================

def send_new(target_chat_id, p: dict) -> Tuple[Optional[dict], Optional[str]]:
    caption = build_caption(p)
    img = safe_str(p.get("image_url"))

    if img:
        # ✅ 仅增强：图片发送遇到“网络异常”时，多重试几次，降低退化成纯文本的概率
        photo_attempts = 3
        for i in range(photo_attempts):
            try:
                res = tg_api("sendPhoto", {"chat_id": target_chat_id, "photo": img, "caption": caption})
                time.sleep(SEND_DELAY_SEC)
                return {"message_id": res["message_id"], "kind": "photo", "image_url": img}, None
            except TelegramNetworkError as e:
                wait_s = 2 + i * 2
                print(f"[warn] sendPhoto network error, retry in {wait_s}s ({i+1}/{photo_attempts}): {e}")
                time.sleep(wait_s)
                continue
            except Exception as e:
                if BAD_IMAGE_POLICY == "skip" and is_bad_image_error(e):
                    print(f"[skip] bad image -> skip product. market={p.get('market')} asin={p.get('asin')} err={e}")
                    return None, "BAD_IMAGE_SKIP"
                print(f"[warn] sendPhoto failed -> fallback to text. market={p.get('market')} asin={p.get('asin')} img={img} err={e}")
                break

    res = tg_api("sendMessage", {"chat_id": target_chat_id, "text": caption, "disable_web_page_preview": True})
    time.sleep(SEND_DELAY_SEC)
    return {"message_id": res["message_id"], "kind": "text", "image_url": ""}, None

def edit_existing(target_chat_id, message_id: int, prev: dict, p: dict) -> Tuple[dict, bool, bool]:
    """
    返回 (new_meta, did_action, message_missing)
    规则：
    - 不做“text->photo”类型转换（避免 delete+repost），保持原 kind 稳定
    - photo 消息优先 editMedia(仅当有新图且不同)，否则 editCaption
    - text 消息 editText
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

# ==================== channel map ====================

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

# ==================== state format (groups) ====================

def migrate_state_to_groups(raw_state: Any) -> Dict[str, Any]:
    """
    新格式：
    {
      "_meta": {...},
      "groups": {
         "US:B0XXX": [ meta, meta, ... ],
         ...
      }
    }

    兼容旧格式（flat key: "US:B0XXX" -> dict）
    """
    if isinstance(raw_state, dict) and "groups" in raw_state and isinstance(raw_state.get("groups"), dict):
        if "_meta" not in raw_state or not isinstance(raw_state.get("_meta"), dict):
            raw_state["_meta"] = {}
        # ✅ 顺手把 groups key 规范化一次
        fixed: Dict[str, List[dict]] = {}
        for k, lst in raw_state["groups"].items():
            nk = normalize_group_key(k)
            if not nk:
                continue
            if isinstance(lst, list):
                fixed[nk] = [m for m in lst if isinstance(m, dict)]
            elif isinstance(lst, dict):
                fixed[nk] = [lst]
        raw_state["groups"] = fixed
        return raw_state

    groups: Dict[str, List[dict]] = {}

    if isinstance(raw_state, dict):
        for k, v in raw_state.items():
            nk = normalize_group_key(k)
            if not nk:
                continue
            if isinstance(v, dict):
                groups.setdefault(nk, []).append(v)
            elif isinstance(v, list):
                groups.setdefault(nk, []).extend([m for m in v if isinstance(m, dict)])

    return {
        "_meta": {"migrated_at": int(time.time()), "from": "flat"},
        "groups": groups
    }

# ==================== main ====================

_should_exit = False

def _handle_signal(signum, frame):
    global _should_exit
    _should_exit = True
    print(f"[warn] received signal={signum}, will exit after saving state...")

def main():
    global _should_exit

    print("SYNC_PRODUCTS_VERSION =", SYNC_PRODUCTS_VERSION)
    print(f"[debug] MIGRATE_ONLY={MIGRATE_ONLY} RESET_STATE={RESET_STATE}")
    print(f"[debug] BAD_IMAGE_POLICY={BAD_IMAGE_POLICY} PURGE_MISSING={PURGE_MISSING} TG_SEND_DELAY_SEC={SEND_DELAY_SEC}")
    print(f"[debug] PURGE_MIN_ROWS={PURGE_MIN_ROWS} PURGE_MIN_ACTIVE_RATIO={PURGE_MIN_ACTIVE_RATIO} FETCH_RETRY={FETCH_RETRY} FETCH_TIMEOUT={FETCH_TIMEOUT}")
    print(f"[debug] MAX_ACTIONS_PER_RUN={MAX_ACTIONS_PER_RUN}")

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    # MIGRATE_ONLY 时不要求 TG_TOKEN（只迁移文件）
    if not MIGRATE_ONLY and not TG_TOKEN:
        raise SystemExit("Missing TG_BOT_TOKEN env var.")

    channel_map = load_channel_map()
    if not channel_map and not MIGRATE_ONLY:
        raise SystemExit("Missing channel_map.json. (channels mode required)")

    if channel_map:
        print(f"[ok] mode=channels markets={sorted(channel_map.keys())}")

    # reset
    if RESET_STATE and STATE_FILE.exists():
        bak = STATE_FILE.with_suffix(f".reset_{int(time.time())}.bak")
        STATE_FILE.replace(bak)
        print(f"[warn] RESET_STATE=1 -> backed up old state to {bak.name}")

    raw_state = load_json_safe(STATE_FILE, {})
    state = migrate_state_to_groups(raw_state)
    state["_meta"]["version"] = SYNC_PRODUCTS_VERSION
    state["_meta"]["ts"] = int(time.time())

    groups: Dict[str, List[dict]] = state.get("groups", {})
    if not isinstance(groups, dict):
        groups = {}
        state["groups"] = groups

    # ✅ 只迁移不发消息：用于把旧 posted_state.json 升级成 groups，避免“重头发”
    if MIGRATE_ONLY:
        save_json_atomic(STATE_FILE, state)
        print(f"[ok] MIGRATE_ONLY=1 -> migrated posted_state.json only, skip Telegram actions. saved -> {STATE_FILE}")
        return

    products = load_products()

    actions_done = 0
    stopped_due_to_limit = False

    # ✅ 仅新增：当 Telegram 网络不可用时，优雅停止本次 run（保存 state，避免 Actions 红叉）
    telegram_down = False

    def at_limit() -> bool:
        return actions_done >= MAX_ACTIONS_PER_RUN

    def target_chat_for_market(market: str) -> str:
        cid = channel_map.get(market)
        if not cid:
            raise RuntimeError(f"channel_map missing market={market}")
        return cid

    # ---------- build desired by group ----------
    desired: Dict[str, Dict[str, Any]] = {}
    seen_group_keys = set()

    for p in products:
        market = safe_str(p.get("market")).upper()
        asin = norm_asin(p.get("asin"))
        if not asin or market not in VALID_MARKETS:
            continue
        p["market"] = market
        p["asin"] = asin

        gk = f"{market}:{asin}"
        seen_group_keys.add(gk)

        if gk not in desired:
            desired[gk] = {"market": market, "asin": asin, "active": [], "removed_count": 0}

        if norm_status(p.get("status")) == "active":
            desired[gk]["active"].append(p)
        else:
            desired[gk]["removed_count"] += 1

    # ---------- PURGE missing groups ----------
    if PURGE_MISSING and (not _should_exit) and (not stopped_due_to_limit):
        prev_active_msgs = 0
        for gk, lst in groups.items():
            if not isinstance(lst, list):
                continue
            for m in lst:
                if isinstance(m, dict) and m.get("status") == "active" and m.get("message_id"):
                    prev_active_msgs += 1

        curr_seen = len(seen_group_keys)

        purge_allowed = True
        if curr_seen < PURGE_MIN_ROWS:
            purge_allowed = False
            print(f"[warn] PURGE blocked: seen groups too small ({curr_seen} < {PURGE_MIN_ROWS})")
        elif prev_active_msgs > 0 and curr_seen < int(prev_active_msgs * PURGE_MIN_ACTIVE_RATIO):
            purge_allowed = False
            print(f"[warn] PURGE blocked: seen groups too small vs prev_active_msgs ({curr_seen} < {int(prev_active_msgs * PURGE_MIN_ACTIVE_RATIO)})")

        if purge_allowed:
            missing_groups = [gk for gk in list(groups.keys()) if gk not in seen_group_keys]
            if missing_groups:
                print(f"[warn] PURGE_MISSING enabled, will purge groups missing in list: {len(missing_groups)}")

            for gk in missing_groups:
                if _should_exit or stopped_due_to_limit:
                    break
                if at_limit():
                    stopped_due_to_limit = True
                    print("[warn] action limit reached, stop during purge.")
                    break

                lst = groups.get(gk) if isinstance(groups.get(gk), list) else []
                new_lst: List[dict] = []
                for meta in lst:
                    if not isinstance(meta, dict):
                        continue
                    msg_id = meta.get("message_id")
                    chat_id = meta.get("chat_id")
                    if msg_id and chat_id:
                        if at_limit():
                            stopped_due_to_limit = True
                            print("[warn] action limit reached, stop during purge deletions.")
                            break
                        ok = delete_message(chat_id, msg_id)
                        actions_done += 1
                        meta["delete_ok"] = bool(ok)
                    meta["status"] = "removed"
                    meta["ts"] = int(time.time())
                groups[gk] = new_lst
        else:
            print("[warn] PURGE_MISSING enabled but blocked by safety thresholds; skip purge this run.")

    # ---------- per group: match/edit/post/delete ----------
    for gk, info in desired.items():
        if _should_exit or stopped_due_to_limit or telegram_down:
            break

        try:
            market = info["market"]
            target_chat = target_chat_for_market(market)

            desired_active: List[dict] = info["active"]
            desired_hashes = [compute_content_hash(p, "active") for p in desired_active]

            existing_list = groups.get(gk)
            if not isinstance(existing_list, list):
                existing_list = []
            existing_list = [m for m in existing_list if isinstance(m, dict)]

            existing_candidates: List[dict] = []
            for m in existing_list:
                if m.get("status") == "active" and m.get("message_id") and m.get("chat_id"):
                    existing_candidates.append(m)

            # 1) exact hash match -> keep
            used_existing_ids = set()
            matched_pairs: List[Tuple[dict, dict, str]] = []

            by_hash: Dict[str, List[dict]] = {}
            for m in existing_candidates:
                h = safe_str(m.get("hash"))
                by_hash.setdefault(h, []).append(m)

            unmatched_products: List[Tuple[dict, str]] = []
            for p, h in zip(desired_active, desired_hashes):
                if h in by_hash and by_hash[h]:
                    m = by_hash[h].pop()
                    used_existing_ids.add(id(m))
                    matched_pairs.append((m, p, h))
                else:
                    unmatched_products.append((p, h))

            remaining_existing = [m for m in existing_candidates if id(m) not in used_existing_ids]

            # 2) reuse remaining existing for unmatched products -> edit
            edit_pairs: List[Tuple[dict, dict, str]] = []
            while unmatched_products and remaining_existing:
                p, h = unmatched_products.pop(0)
                m = remaining_existing.pop(0)
                edit_pairs.append((m, p, h))

            # 3) extra existing -> delete (list减少/删除)
            extra_existing = remaining_existing[:]

            # 4) extra products -> post
            new_posts = unmatched_products[:]

            now_ts = int(time.time())
            for m, p, h in matched_pairs:
                m["hash"] = h
                m["ts"] = now_ts
                m["status"] = "active"
                m["chat_id"] = m.get("chat_id") or target_chat

            for m, p, h in edit_pairs:
                if _should_exit or stopped_due_to_limit or telegram_down:
                    break
                if at_limit():
                    stopped_due_to_limit = True
                    print(f"[warn] action limit reached, stop before edit: {gk}")
                    break

                msg_chat = m.get("chat_id") or target_chat
                msg_id = int(m["message_id"])

                new_meta, did_action, missing = edit_existing(msg_chat, msg_id, m, p)
                if missing:
                    if at_limit():
                        stopped_due_to_limit = True
                        print(f"[warn] action limit reached, stop before repost(missing): {gk}")
                        break
                    info2, err_code = send_new(target_chat, p)
                    if err_code == "BAD_IMAGE_SKIP":
                        m["status"] = "removed"
                        m["ts"] = int(time.time())
                        print(f"[skip] repost missing but bad image skip: {gk}")
                        continue
                    actions_done += 1
                    m.update({
                        "chat_id": target_chat,
                        "message_id": info2["message_id"],
                        "kind": info2["kind"],
                        "image_url": info2["image_url"],
                        "hash": h,
                        "status": "active",
                        "ts": int(time.time()),
                        "delete_attempted": False,
                        "delete_ok": False,
                    })
                    print(f"reposted(missing->send): {gk} msg {info2['message_id']}")
                    continue

                if did_action:
                    actions_done += 1
                    print(f"edited: {gk} msg {msg_id}")
                else:
                    print(f"nochange(edit): {gk} msg {msg_id}")

                m.update({
                    "chat_id": msg_chat,
                    "kind": new_meta["kind"],
                    "image_url": new_meta["image_url"],
                    "hash": h,
                    "status": "active",
                    "ts": int(time.time()),
                })

            for m in extra_existing:
                if _should_exit or stopped_due_to_limit or telegram_down:
                    break
                if at_limit():
                    stopped_due_to_limit = True
                    print(f"[warn] action limit reached, stop before delete: {gk}")
                    break
                msg_id = m.get("message_id")
                chat_id = m.get("chat_id") or target_chat
                if msg_id and chat_id:
                    ok = delete_message(chat_id, msg_id)
                    actions_done += 1
                    m["delete_ok"] = bool(ok)
                m["status"] = "removed"
                m["ts"] = int(time.time())
                m["message_id"] = None  # ✅ 防止以后又被当作可 edit 的候选
                print(f"deleted(extra): {gk}")

            for p, h in new_posts:
                if _should_exit or stopped_due_to_limit or telegram_down:
                    break
                if at_limit():
                    stopped_due_to_limit = True
                    print(f"[warn] action limit reached, stop before post: {gk}")
                    break

                info2, err_code = send_new(target_chat, p)
                if err_code == "BAD_IMAGE_SKIP":
                    continue
                actions_done += 1

                existing_list.append({
                    "chat_id": target_chat,
                    "message_id": info2["message_id"],
                    "hash": h,
                    "status": "active",
                    "kind": info2["kind"],
                    "image_url": info2["image_url"],
                    "ts": int(time.time()),
                    "delete_attempted": False,
                    "delete_ok": False
                })
                print(f"posted: {gk} msg {info2['message_id']}")

            groups[gk] = existing_list

        except TelegramNetworkError as e:
            telegram_down = True
            print(f"[warn] Telegram network unavailable, stop this run gracefully: {e}")
            break

    save_json_atomic(STATE_FILE, state)
    print(f"done. actions={actions_done}/{MAX_ACTIONS_PER_RUN}. state saved -> {STATE_FILE}")

if __name__ == "__main__":
    main()
