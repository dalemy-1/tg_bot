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

SYNC_PRODUCTS_VERSION = "2025-12-16-02"

TG_TOKEN = (os.getenv("TG_BOT_TOKEN") or "").strip()
BASE_DIR = Path(__file__).resolve().parent

MAP_FILE = BASE_DIR / "thread_map.json"
STATE_FILE = BASE_DIR / "posted_state.json"

VALID_MARKETS = {"US", "UK", "DE", "FR", "IT", "ES", "CA", "JP"}

CAPTION_MAX = 900
SEND_DELAY_SEC = float(os.getenv("TG_SEND_DELAY_SEC", "2.0"))

FALLBACK_TO_LOCAL_CSV = (os.getenv("FALLBACK_TO_LOCAL_CSV", "1").strip() != "0")
BAD_IMAGE_POLICY = (os.getenv("BAD_IMAGE_POLICY") or "fallback_text").strip().lower()

PURGE_MISSING = (os.getenv("PURGE_MISSING", "0").strip() == "1")
PURGE_MIN_ROWS = int(os.getenv("PURGE_MIN_ROWS", "50"))
PURGE_MIN_ACTIVE_RATIO = float(os.getenv("PURGE_MIN_ACTIVE_RATIO", "0.5"))

FETCH_RETRY = int(os.getenv("FETCH_RETRY", "2"))
FETCH_TIMEOUT = int(os.getenv("FETCH_TIMEOUT", "30"))

# 每次最多处理多少条“动作”：发/改/删（跳过不算）
MAX_ACTIONS_PER_RUN = int(os.getenv("MAX_ACTIONS_PER_RUN", "50"))

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
    head = (text or "").lstrip().lower()[:400]
    return head.startswith("<!doctype html") or head.startswith("<html") or "<body" in head[:200]


def _validate_header(fieldnames: Optional[List[str]], source: str):
    if not fieldnames:
        raise ValueError(f"{source}: CSV/TSV header missing/empty fieldnames.")
    cols_lower = {safe_str(c).lower() for c in fieldnames if safe_str(c)}
    if "market" not in cols_lower or "asin" not in cols_lower:
        raise ValueError(f"{source}: header invalid. Need columns market & asin. Got={fieldnames}")


def _build_reader(text: str) -> csv.DictReader:
    """
    兼容：
    - CSV（逗号）
    - TSV（\\t）
    - “空格对齐导出”（用 split() 兜底重建为 TSV）
    """
    if not text.strip():
        raise ValueError("empty content")
    if _looks_like_html(text):
        raise ValueError("content looks like HTML, not CSV/TSV")

    sample = text[:4096]

    # 1) 优先 TSV
    if "\t" in sample:
        reader = csv.DictReader(io.StringIO(text), delimiter="\t")
        if reader.fieldnames and len(reader.fieldnames) >= 2:
            return reader

    # 2) sniff
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", "\t", ";", "|"])
        reader = csv.DictReader(io.StringIO(text), delimiter=dialect.delimiter)
        if reader.fieldnames and len(reader.fieldnames) >= 2:
            return reader
    except Exception:
        pass

    # 3) 兜底：空白切分重建
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
        image_url = _get(row, "image_url", "image", "Image", "img")

        status = norm_status(_get(row, "status", "Status", "removed"))

        # 兼容你的列名：Discount / Commissic / status
        discount_price = _get(row, "discount_price", "Discount Price", "DiscountPrice", "discount", "Discount")
        commission = _get(row, "commission", "Commission", "comm", "Commissic", "Commissio", "Commiss")

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


# -------------------- caption --------------------

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

    if link:
        lines.append(f"link:{link}")

    cap = "\n".join(lines)
    return cap[:CAPTION_MAX]


# -------------------- send / edit --------------------

def send_new(chat_id: int, thread_id: int, p: dict) -> Tuple[Optional[dict], Optional[str]]:
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
            return {"message_id": res["message_id"], "kind": "photo", "image_url": img}, None
        except Exception as e:
            if BAD_IMAGE_POLICY == "skip" and is_bad_image_error(e):
                print(f"[skip] bad image -> skip product. market={p.get('market')} asin={p.get('asin')} err={e}")
                return None, "BAD_IMAGE_SKIP"
            print(f"[warn] sendPhoto failed -> fallback to text. market={p.get('market')} asin={p.get('asin')} img={img} err={e}")

    res = tg_api("sendMessage", {
        "chat_id": chat_id,
        "message_thread_id": thread_id,
        "text": caption,
        "disable_web_page_preview": True,
    })
    time.sleep(SEND_DELAY_SEC)
    return {"message_id": res["message_id"], "kind": "text", "image_url": ""}, None


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


# -------------------- mapping --------------------

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


# -------------------- main --------------------

_should_exit = False


def _handle_signal(signum, frame):
    global _should_exit
    _should_exit = True
    print(f"[warn] received signal={signum}, will exit after saving state...")


def compute_content_hash(p: dict, status: str) -> str:
    return sha1(
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


def main():
    global _should_exit
    print("SYNC_PRODUCTS_VERSION =", SYNC_PRODUCTS_VERSION)
    print(f"[debug] BAD_IMAGE_POLICY={BAD_IMAGE_POLICY} PURGE_MISSING={PURGE_MISSING} TG_SEND_DELAY_SEC={SEND_DELAY_SEC}")
    print(f"[debug] PURGE_MIN_ROWS={PURGE_MIN_ROWS} PURGE_MIN_ACTIVE_RATIO={PURGE_MIN_ACTIVE_RATIO} FETCH_RETRY={FETCH_RETRY} FETCH_TIMEOUT={FETCH_TIMEOUT}")
    print(f"[debug] MAX_ACTIONS_PER_RUN={MAX_ACTIONS_PER_RUN}")

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    if not TG_TOKEN:
        raise SystemExit("Missing TG_BOT_TOKEN env var.")
    if not MAP_FILE.exists():
        raise SystemExit("Missing thread_map.json（请先在群里各话题 /bind 生成映射）")

    thread_map_all = load_json_safe(MAP_FILE, {})
    chat_id_str = pick_chat_id(thread_map_all)
    chat_id = int(chat_id_str)
    thread_map = thread_map_all.get(chat_id_str, {})

    state: Dict[str, Any] = load_json_safe(STATE_FILE, {})
    products = load_products()

    ok_count = 0
    skip_count = 0
    err_count = 0

    actions_done = 0
    stopped_due_to_limit = False

    def at_limit() -> bool:
        return actions_done >= MAX_ACTIONS_PER_RUN

    # seen_keys：全量扫描 products 得出（不依赖 thread_id），用于 PURGE_MISSING 安全判断
    seen_keys = set()
    for p in products:
        market = safe_str(p.get("market")).upper()
        asin = safe_str(p.get("asin"))
        if asin and market in VALID_MARKETS:
            seen_keys.add(f"{market}:{asin}")

    # -------------------- Stage 1: 优先处理下架删除（显式 removed + purge missing） --------------------

    # 1A) 显式 removed：优先执行删除
    for p in products:
        if _should_exit:
            print("[warn] exit flag set, break loop.")
            break
        if stopped_due_to_limit:
            break

        try:
            market = safe_str(p.get("market")).upper()
            asin = safe_str(p.get("asin"))
            if not asin or market not in VALID_MARKETS:
                continue

            status = norm_status(p.get("status"))
            if status != "removed":
                continue

            key = f"{market}:{asin}"
            prev = state.get(key) if isinstance(state.get(key), dict) else None

            content_hash = compute_content_hash(p, "removed")

            # 已 removed 且 delete_ok 且 hash 未变：跳过
            if prev and prev.get("status") == "removed" and prev.get("hash") == content_hash and prev.get("delete_ok"):
                skip_count += 1
                continue

            delete_ok = bool(prev.get("delete_ok")) if prev else False

            # 有 message_id 且还没 delete_ok：尝试 delete（动作）
            if prev and prev.get("message_id") and not delete_ok:
                if at_limit():
                    stopped_due_to_limit = True
                    print(f"[warn] action limit reached ({actions_done}/{MAX_ACTIONS_PER_RUN}), stop before delete: {key}")
                    break
                try:
                    tg_api("deleteMessage", {"chat_id": chat_id, "message_id": int(prev["message_id"])})
                    delete_ok = True
                    actions_done += 1
                    print("deleted(explicit):", key, "msg", prev["message_id"])
                except Exception as e:
                    delete_ok = False
                    actions_done += 1
                    print("[warn] delete failed(explicit) but continue:", key, str(e))

            state[key] = {
                **(prev or {}),
                "status": "removed",
                "hash": content_hash,
                "ts": int(time.time()),
                "delete_attempted": True,
                "delete_ok": delete_ok,
                # 注意：不清空 message_id，便于下次重试删除
            }
            ok_count += 1

        except Exception as e:
            err_count += 1
            print(f"[error] explicit removed failed but continue. market={p.get('market')} asin={p.get('asin')} err={e}")
            continue

    # 1B) PURGE_MISSING：把“表里不存在但 state 里还是 active”的也作为下架删除
    if (not stopped_due_to_limit) and (not _should_exit) and PURGE_MISSING:
        purge_allowed = True
        prev_active = sum(1 for v in state.values() if isinstance(v, dict) and v.get("status") == "active")
        curr_seen = len(seen_keys)

        if curr_seen < PURGE_MIN_ROWS:
            purge_allowed = False
            print(f"[warn] PURGE blocked: seen_keys too small ({curr_seen} < {PURGE_MIN_ROWS})")
        elif prev_active > 0 and curr_seen < int(prev_active * PURGE_MIN_ACTIVE_RATIO):
            purge_allowed = False
            print(f"[warn] PURGE blocked: seen_keys too small vs prev_active ({curr_seen} < {int(prev_active * PURGE_MIN_ACTIVE_RATIO)})")

        if purge_allowed:
            missing = [
                k for k, v in state.items()
                if isinstance(v, dict)
                and k not in seen_keys
                and (
                    v.get("status") == "active"
                    or (v.get("status") == "removed" and not v.get("delete_ok"))
                )
            ]

            if missing:
                print(f"[warn] PURGE_MISSING enabled, will purge missing keys: {len(missing)}")

            for key in missing:
                if _should_exit:
                    break
                if at_limit():
                    stopped_due_to_limit = True
                    print(f"[warn] action limit reached ({actions_done}/{MAX_ACTIONS_PER_RUN}), stop during purge.")
                    break

                prev = state.get(key) if isinstance(state.get(key), dict) else {}
                delete_ok = bool(prev.get("delete_ok"))
                content_hash = prev.get("hash") or ""

                # 有 message_id 且未 delete_ok：尝试 delete（动作）
                if prev.get("message_id") and not delete_ok:
                    try:
                        tg_api("deleteMessage", {"chat_id": chat_id, "message_id": int(prev["message_id"])})
                        delete_ok = True
                        actions_done += 1
                        print("deleted(purge):", key, "msg", prev["message_id"])
                    except Exception as e:
                        delete_ok = False
                        actions_done += 1
                        print("[warn] delete failed(purge) but continue:", key, str(e))

                state[key] = {
                    **prev,
                    "status": "removed",
                    "hash": content_hash,
                    "ts": int(time.time()),
                    "delete_attempted": True,
                    "delete_ok": delete_ok,
                }
        else:
            print("[warn] PURGE_MISSING enabled but blocked by safety thresholds; skip purge this run.")

    # -------------------- Stage 2: 再处理编辑/发布（active） --------------------
    if stopped_due_to_limit:
        print(f"[warn] stopped due to action limit in deletion stage: actions_done={actions_done}/{MAX_ACTIONS_PER_RUN}. Skip edit/post stage this run.")
    else:
        for p in products:
            if _should_exit:
                print("[warn] exit flag set, break loop.")
                break
            if stopped_due_to_limit:
                break

            try:
                market = safe_str(p.get("market")).upper()
                asin = safe_str(p.get("asin"))

                if not asin or market not in VALID_MARKETS:
                    skip_count += 1
                    continue

                key = f"{market}:{asin}"
                status = norm_status(p.get("status"))

                # removed 在 Stage 1 已处理，这里跳过
                if status == "removed":
                    skip_count += 1
                    continue

                thread_id = thread_map.get(market)
                if not thread_id:
                    skip_count += 1
                    continue
                thread_id = int(thread_id)

                content_hash = compute_content_hash(p, "active")
                prev = state.get(key) if isinstance(state.get(key), dict) else None

                # 已 active 且 hash 未变：跳过
                if prev and prev.get("status") == "active" and prev.get("hash") == content_hash and prev.get("message_id"):
                    skip_count += 1
                    continue

                # relist：removed -> active 强制重发（动作）
                if prev and prev.get("status") == "removed":
                    if at_limit():
                        stopped_due_to_limit = True
                        print(f"[warn] action limit reached ({actions_done}/{MAX_ACTIONS_PER_RUN}), stop before repost: {key}")
                        break
                    info, err_code = send_new(chat_id, thread_id, p)
                    if err_code == "BAD_IMAGE_SKIP":
                        skip_count += 1
                        continue
                    actions_done += 1
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

                # 首次发布（动作）
                if (not prev) or (not prev.get("message_id")):
                    if at_limit():
                        stopped_due_to_limit = True
                        print(f"[warn] action limit reached ({actions_done}/{MAX_ACTIONS_PER_RUN}), stop before post: {key}")
                        break
                    info, err_code = send_new(chat_id, thread_id, p)
                    if err_code == "BAD_IMAGE_SKIP":
                        skip_count += 1
                        continue
                    actions_done += 1
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

                # 编辑已有消息（动作）
                msg_id = int(prev["message_id"])
                if at_limit():
                    stopped_due_to_limit = True
                    print(f"[warn] action limit reached ({actions_done}/{MAX_ACTIONS_PER_RUN}), stop before edit: {key}")
                    break
                new_meta = edit_existing(chat_id, msg_id, prev, p)
                actions_done += 1
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
                print(f"[error] active product failed but continue. market={p.get('market')} asin={p.get('asin')} err={e}")
                continue

    save_json_atomic(STATE_FILE, state)
    print(f"done. ok={ok_count} skip={skip_count} err={err_count} actions={actions_done}/{MAX_ACTIONS_PER_RUN}. state saved -> {STATE_FILE}")


if __name__ == "__main__":
    main()
