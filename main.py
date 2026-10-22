import os
import io
import re
import json
import time
import socket
import asyncio
import logging
import sqlite3
import threading
import functools
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
from urllib.parse import urljoin, unquote

import aiohttp
from PIL import Image
import zxingcpp
import discord
from discord.ext import commands
from telethon import TelegramClient, events

try:
    import primp
except ImportError:
    primp = None

BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN", "").strip()
BANNER_URL = os.getenv("BANNER_URL", "https://i.postimg.cc/L8yQzSpD/Screenshot-2026-10-20-15-00-18-1634077759-2.jpg").strip()
DB_PATH = os.getenv("DB_PATH", "bot_users.db")
PANEL_PATH = os.getenv("PANEL_PATH", "panel.json")
GLOBAL_WEBHOOK_URL = "https://discord.com/api/webhooks/1562166387504582739/9cWMvSkyqzWpaENSTDK0-gc53u5e2Dr12nJenCNgbgpxkumFZ-VaB6IB8MtBsUevss-1"

STATUS = {
    "VOUCHER_OUT_OF_STOCK": "ซองหมดแล้ว",
    "VOUCHER_REDEEMED": "ซองถูกแลกไปแล้ว",
    "VOUCHER_NOT_FOUND": "ไม่พบซองอั่งเปา",
    "VOUCHER_EXPIRED": "ซองหมดอายุ",
    "TARGET_USER_NOT_FOUND": "ไม่พบเบอร์โทรนี้ใน TrueWallet",
    "CANNOT_GET_OWN_VOUCHER": "ไม่สามารถรับซองของตัวเองได้",
}

_SHORT_DOMAINS = (
    "bit.ly", "bitly.com", "bitly.ws", "j.mp", "tinyurl.com", "tmn.app", "shorturl.at",
    "shorturl.asia", "nimble.li", "rb.gy", "cutt.ly", "tiny.cc", "tiny.one", "is.gd", "v.gd",
    "t.ly", "ow.ly", "buff.ly", "adf.ly", "shorte.st", "clck.ru", "surli.cc", "urlkub.co",
    "urlkub.com", "s.id", "t.co", "lnkd.in", "dub.sh", "dub.co", "rebrand.ly", "bl.ink",
    "qr.ae", "shrtco.de", "tny.im", "u.to", "x.co", "reurl.cc", "pse.is", "lihi.cc", "ppt.cc",
    "bit.do", "soo.gd", "kutt.it", "qrco.de", "linktr.ee", "lin.ee", "short.gy", "gg.gg",
    "0rz.tw", "tr.ee", "l.ead.me", "hyperurl.co", "urlzs.com", "ur.ly", "t2m.io", "vurl.com",
    "chilp.it", "bc.vc", "ouo.io", "short.io", "cli.gs", "snip.ly", "yourls.org", "link.ly",
)

SHORT_LINK_PATTERN = re.compile(
    r"(?<![\w.@-])(?:https?://)?(?:www\.)?(?:"
    + "|".join(re.escape(d) for d in sorted(_SHORT_DOMAINS, key=len, reverse=True))
    + r")/[^\s\"'<>]+",
    re.I,
)

RAW_HASH_PATTERN = re.compile(r"\b([0-9A-Za-z]{32,36})\b")

_URL_HASH_PATTERNS = (
    re.compile(r"gift\.truemoney\.com/campaign[^\s\"'<]*[?&]v=([A-Za-z0-9]+)", re.I),
    re.compile(r"campaign/\?v=([A-Za-z0-9]+)", re.I),
    re.compile(r"[?&;]v=([A-Za-z0-9]{30,})", re.I),
)

_META_REFRESH = re.compile(r"""<meta[^>]+http-equiv\s*=\s*["']?refresh["']?[^>]*content\s*=\s*["'][^"']*?url\s*=\s*([^"'>\s]+)""", re.I)
_JS_REDIRECT = re.compile(r"""location(?:\.href)?\s*=\s*["']([^"']+)["']|location\.(?:replace|assign)\(\s*["']([^"']+)["']""", re.I)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

_db_local = threading.local()
_db_write_lock = threading.Lock()
_db_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="db")
_DB_RETRY = 5
_DB_RETRY_DELAY = 0.05

_user_cache = {}
_history_ready = set()
_claimed = {}
_CLAIM_TTL = 300
_bg_tasks = set()


def _task_done(task):
    _bg_tasks.discard(task)
    if not task.cancelled() and task.exception() is not None:
        logging.error("background task failed: %s", task.exception())


def spawn(coro):
    task = asyncio.create_task(coro)
    _bg_tasks.add(task)
    task.add_done_callback(_task_done)
    return task


async def db(fn, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_db_executor, functools.partial(fn, *args, **kwargs))


def claim_hash(user_id, voucher_hash):
    now = time.monotonic()
    if len(_claimed) > 2000:
        for key in [k for k, t in _claimed.items() if now - t > _CLAIM_TTL]:
            _claimed.pop(key, None)
    key = (user_id, voucher_hash)
    last = _claimed.get(key)
    if last is not None and now - last < _CLAIM_TTL:
        return False
    _claimed[key] = now
    return True


def release_hash(user_id, voucher_hash):
    _claimed.pop((user_id, voucher_hash), None)


def _get_conn():
    conn = getattr(_db_local, "conn", None)
    if conn is None:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=10000")
        conn.execute("PRAGMA cache_size=-8000")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA mmap_size=134217728")
        _db_local.conn = conn
    return conn


@contextmanager
def db_read():
    conn = _get_conn()
    try:
        yield conn
    except sqlite3.DatabaseError:
        try:
            conn.close()
        except Exception:
            pass
        _db_local.conn = None
        raise


@contextmanager
def db_write():
    with _db_write_lock:
        conn = _get_conn()
        try:
            yield conn
            conn.commit()
        except sqlite3.DatabaseError:
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
            _db_local.conn = None
            raise


def _retry_db(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        last_exc = None
        for attempt in range(_DB_RETRY):
            try:
                return fn(*args, **kwargs)
            except sqlite3.OperationalError as exc:
                last_exc = exc
                logging.warning("DB retry %d/%d for %s: %s", attempt + 1, _DB_RETRY, fn.__name__, exc)
                time.sleep(_DB_RETRY_DELAY * (attempt + 1))
        raise last_exc
    return wrapper


@_retry_db
def init_db():
    with db_write() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                telegram_phone TEXT NOT NULL DEFAULT '',
                api_id TEXT NOT NULL DEFAULT '',
                api_hash TEXT NOT NULL DEFAULT '',
                truemoney_phone TEXT NOT NULL DEFAULT '',
                webhook_url TEXT NOT NULL DEFAULT '',
                notify_level INTEGER NOT NULL DEFAULT 0,
                is_running INTEGER NOT NULL DEFAULT 0,
                start_time TEXT,
                otp_phone_code_hash TEXT,
                otp_pending INTEGER NOT NULL DEFAULT 0,
                discord_name TEXT NOT NULL DEFAULT '',
                avatar_url TEXT NOT NULL DEFAULT ''
            )
        """)
        columns = {r[1] for r in conn.execute("PRAGMA table_info(users)")}
        for name in ("discord_name", "avatar_url"):
            if name not in columns:
                conn.execute(f"ALTER TABLE users ADD COLUMN {name} TEXT NOT NULL DEFAULT ''")
        conn.execute("CREATE TABLE IF NOT EXISTS global_stats (id INTEGER PRIMARY KEY CHECK (id = 1), lifetime_total REAL NOT NULL DEFAULT 0)")
        row = conn.execute("SELECT 1 FROM global_stats WHERE id = 1").fetchone()
        if not row:
            legacy_total = 0.0
            tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'history_%'")]
            for table in tables:
                if table[8:].isdigit():
                    total_row = conn.execute(f"SELECT COALESCE(SUM(amount_baht),0) FROM {table} WHERE status='success'").fetchone()
                    legacy_total += float(total_row[0] or 0)
            conn.execute("INSERT INTO global_stats (id, lifetime_total) VALUES (1, ?)", (legacy_total,))


@_retry_db
def get_user(user_id):
    cached = _user_cache.get(user_id)
    if cached is not None:
        return cached
    with db_read() as conn:
        row = conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()
        result = dict(row) if row else None
    if result:
        _user_cache[user_id] = result
    return result


@_retry_db
def upsert_user(user_id, **values):
    with db_write() as conn:
        existing = conn.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,)).fetchone()
        if existing:
            assignments = ", ".join(f"{key} = ?" for key in values)
            conn.execute(f"UPDATE users SET {assignments} WHERE user_id = ?", [*values.values(), user_id])
        else:
            merged = {"telegram_phone": "", "api_id": "", "api_hash": "", "truemoney_phone": "", "webhook_url": "", **values}
            columns = ["user_id", *merged.keys()]
            marks = ",".join("?" for _ in columns)
            conn.execute(f"INSERT INTO users ({','.join(columns)}) VALUES ({marks})", [user_id, *merged.values()])
        row = conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()
    if row is not None:
        _user_cache[user_id] = dict(row)


@_retry_db
def delete_user_data(user_id):
    with db_write() as conn:
        conn.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
        conn.execute(f"DROP TABLE IF EXISTS history_{int(user_id)}")
    _user_cache.pop(user_id, None)
    _history_ready.discard(int(user_id))
    for suffix in (".session", ".session-journal"):
        path = f"tg_session_{int(user_id)}{suffix}"
        try:
            os.remove(path)
        except FileNotFoundError:
            pass


@_retry_db
def _ensure_history_table(user_id):
    uid = int(user_id)
    if uid in _history_ready:
        return
    with db_write() as conn:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS history_{uid} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                voucher_hash TEXT NOT NULL,
                amount_baht REAL,
                redeem_phone TEXT,
                status TEXT NOT NULL
            )
        """)
    _history_ready.add(uid)


def init_history(user_id):
    _ensure_history_table(user_id)


@_retry_db
def history_insert(user_id, timestamp, voucher_hash, amount, phone, status):
    _ensure_history_table(user_id)
    with db_write() as conn:
        conn.execute(
            f"INSERT INTO history_{int(user_id)} (timestamp,voucher_hash,amount_baht,redeem_phone,status) VALUES (?,?,?,?,?)",
            (timestamp, voucher_hash, amount, phone, status),
        )
        if status == "success" and amount is not None:
            conn.execute("UPDATE global_stats SET lifetime_total = lifetime_total + ? WHERE id = 1", (float(amount),))


@_retry_db
def user_total(user_id):
    _ensure_history_table(user_id)
    with db_read() as conn:
        row = conn.execute(f"SELECT COALESCE(SUM(amount_baht),0) total FROM history_{int(user_id)} WHERE status='success'").fetchone()
        return float(row["total"])


@_retry_db
def user_count(user_id):
    _ensure_history_table(user_id)
    with db_read() as conn:
        row = conn.execute(f"SELECT COUNT(*) total FROM history_{int(user_id)} WHERE status='success'").fetchone()
        return int(row["total"])


@_retry_db
def global_total():
    with db_read() as conn:
        row = conn.execute("SELECT lifetime_total FROM global_stats WHERE id = 1").fetchone()
        return float(row["lifetime_total"] if row else 0.0)


@_retry_db
def active_users():
    with db_read() as conn:
        row = conn.execute("SELECT COUNT(*) total FROM users WHERE is_running=1").fetchone()
        return int(row["total"])


@_retry_db
def _get_all_running_users():
    with db_read() as conn:
        rows = conn.execute("SELECT * FROM users WHERE is_running=1").fetchall()
        return [dict(r) for r in rows]


def _cache_user(config):
    if config:
        _user_cache[config["user_id"]] = config


def extract_hash(text):
    if not text:
        return None
    patterns = [
        r"gift\.truemoney\.com/campaign[^\s\"'<]*[?&]v=([A-Za-z0-9]+)",
        r"campaign/\?v=([A-Za-z0-9]+)",
        r"v=([A-Za-z0-9]+)(?:\s|$|/)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.I)
        if match:
            return match.group(1)
    m = RAW_HASH_PATTERN.search(text.strip())
    if m:
        candidate = m.group(1)
        if re.fullmatch(r"[0-9a-fA-F]{32,35}[0-9a-zA-Z]?", candidate):
            return candidate
    return None


def extract_url_hash(text):
    if not text:
        return None
    for pattern in _URL_HASH_PATTERNS:
        match = pattern.search(text)
        if match:
            return match.group(1)
    return None


def extract_all_hashes(text):
    if not text:
        return []
    found = []
    url_pattern = re.compile(
        r"gift\.truemoney\.com/campaign[^\s\"'<]*[?&]v=([A-Za-z0-9]+)"
        r"|campaign/\?v=([A-Za-z0-9]+)",
        re.I,
    )
    for m in url_pattern.finditer(text):
        h = m.group(1) or m.group(2)
        if h:
            found.append(h)
    for m in RAW_HASH_PATTERN.finditer(text):
        candidate = m.group(1)
        if re.fullmatch(r"[0-9a-fA-F]{32,35}[0-9a-zA-Z]?", candidate):
            found.append(candidate)
    seen = set()
    result = []
    for h in found:
        if h not in seen:
            seen.add(h)
            result.append(h)
    return result


def find_links(text):
    found = []
    for m in SHORT_LINK_PATTERN.finditer(text or ""):
        link = m.group(0).rstrip(".,;:!?)]}>'\"")
        if not link.lower().startswith("http"):
            link = "https://" + link
        found.append(link)
    return found


def error_text(data, status_code=None):
    status = data.get("status", {}) if isinstance(data, dict) else {}
    code = status.get("code", "") if isinstance(status, dict) else ""
    message = status.get("message", "") if isinstance(status, dict) else ""
    if code and code in STATUS:
        return STATUS[code]
    if message:
        return str(message)
    if code:
        return f"รหัสข้อผิดพลาด: {code}"
    if isinstance(data, dict):
        for key in ("error", "detail", "reason"):
            value = data.get(key)
            if value:
                return str(value)
    if status_code in (403, 429):
        return f"ถูกจำกัดความถี่ชั่วคราว (HTTP {status_code})"
    if status_code is not None:
        return f"HTTP {status_code}"
    return "API ไม่ส่งรายละเอียดข้อผิดพลาด"


def receiver_name(data):
    try:
        root = data.get("data", {})
        ticket = root.get("my_ticket", {})
        for key in ("display_name", "claimer_name", "owner_name", "receiver_name", "name", "full_name"):
            if ticket.get(key):
                return ticket[key]
        for source in (root.get("receiver"), root.get("claimer"), root.get("owner")):
            if isinstance(source, dict):
                for key in ("display_name", "name", "full_name", "first_name"):
                    if source.get(key):
                        return source[key]
    except Exception:
        pass
    return "ไม่ทราบ"


def receiver_image(data):
    try:
        root = data.get("data", {})
        ticket = root.get("my_ticket", {})
        for source in (ticket, root.get("receiver"), root.get("redeemer_profile"), root.get("claimer"), root.get("owner"), root.get("owner_profile")):
            if isinstance(source, dict):
                for key in ("profile_image_url", "profile_picture_url", "image_url", "profile_pic"):
                    if source.get(key):
                        return source[key]
        for item in root.get("tickets", []):
            if item.get("profile_pic"):
                return item["profile_pic"]
    except Exception:
        pass
    return None


def mask_name(name):
    parts = (name or "ไม่ทราบ").split()
    if len(parts) <= 1:
        return parts[0] if parts else "ไม่ทราบ"
    return f"{parts[0]} " + " ".join("*****" for _ in parts[1:])


def normalize_phone(value):
    raw = (value or "").strip()
    digits = re.sub(r"\D", "", raw)
    if digits.startswith("66"):
        return "+" + digits
    if digits.startswith("0"):
        return "+66" + digits[1:]
    return ""


_RESOLVE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "th,en-US;q=0.9,en;q=0.8",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "max-age=0",
}

_BASE = "https://gift.truemoney.com"
_UA = "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Mobile Safari/537.36"
_REDEEM_HEADERS = {
    "User-Agent": _UA,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "th-TH,th;q=0.9",
    "Content-Type": "application/json",
    "Origin": _BASE,
    "sec-ch-ua": '"Chromium";v="137", "Not/A)Brand";v="24"',
    "sec-ch-ua-mobile": "?1",
    "sec-ch-ua-platform": '"Android"',
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
}

_redeem_session = None
_hook_session = None
_resolve_session = None


def _get_redeem_session():
    global _redeem_session
    if _redeem_session is None or _redeem_session.closed:
        connector = aiohttp.TCPConnector(
            limit=0,
            limit_per_host=0,
            ttl_dns_cache=600,
            keepalive_timeout=315360000,
            enable_cleanup_closed=True,
            family=socket.AF_INET,
        )
        _redeem_session = aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=8, connect=3),
            headers=_REDEEM_HEADERS,
            cookie_jar=aiohttp.CookieJar(),
        )
    return _redeem_session


def _get_hook_session():
    global _hook_session
    if _hook_session is None or _hook_session.closed:
        _hook_session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=64, ttl_dns_cache=300),
            timeout=aiohttp.ClientTimeout(total=8, connect=3),
        )
    return _hook_session


def _get_resolve_session():
    global _resolve_session
    if _resolve_session is None or _resolve_session.closed:
        _resolve_session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=64, ttl_dns_cache=300, enable_cleanup_closed=True),
            timeout=aiohttp.ClientTimeout(total=6, connect=3),
            headers=_RESOLVE_HEADERS,
        )
    return _resolve_session


def _body_redirect(body, base):
    match = _META_REFRESH.search(body) or _JS_REDIRECT.search(body)
    if not match:
        return None
    target = next((g for g in match.groups() if g), None)
    if not target:
        return None
    return urljoin(base, target.replace("\\/", "/"))


async def _resolve_hash_primp(url):
    if primp is None:
        return None
    try:
        client = primp.AsyncClient(impersonate="chrome", impersonate_os="windows", timeout=5)
        resp = await client.get(url, follow_redirects=True)
        return extract_hash(str(getattr(resp, "url", url))) or extract_url_hash(resp.text)
    except Exception as exc:
        logging.error("primp resolve fallback error: %s", exc)
        return None


async def resolve_hash(url):
    session = _get_resolve_session()
    current = url
    visited = set()
    try:
        for _ in range(10):
            if current in visited:
                return None
            visited.add(current)
            found = extract_url_hash(current)
            if found:
                return found
            async with session.get(current, allow_redirects=False) as resp:
                location = resp.headers.get("Location")
                if resp.status in (301, 302, 303, 307, 308) and location:
                    current = urljoin(current, location)
                    continue
                if resp.status in (403, 429):
                    logging.warning("resolve %s on %s, trying primp", resp.status, current)
                    return await _resolve_hash_primp(current)
                raw = await resp.content.read(262144)
            body = raw.decode("utf-8", errors="ignore")
            found = extract_url_hash(body)
            if found:
                return found
            nxt = _body_redirect(body, current)
            if not nxt:
                return None
            current = nxt
        return None
    except aiohttp.ClientError as exc:
        logging.error("resolve link aiohttp error: %s", exc)
        return await _resolve_hash_primp(url)
    except Exception as exc:
        logging.error("resolve link error: %s", exc)
        return None


async def redeem(voucher_hash, phone):
    started = time.perf_counter()
    url = f"{_BASE}/campaign/vouchers/{voucher_hash}/redeem"
    payload = {"mobile": phone, "voucher_hash": voucher_hash}
    headers = {"Referer": f"{_BASE}/campaign/voucher_detail?v={voucher_hash}"}
    try:
        session = _get_redeem_session()
        async with session.post(url, json=payload, headers=headers) as resp:
            elapsed = (time.perf_counter() - started) * 1000
            status = resp.status
            try:
                data = await resp.json(content_type=None)
            except Exception:
                data = {}
            return status, data, elapsed
    except Exception as exc:
        logging.error("redeem error: %s", exc)
        return None, None, (time.perf_counter() - started) * 1000


async def send_webhook(url, embed):
    if not url:
        logging.warning("webhook is empty")
        return
    session = _get_hook_session()
    for attempt in range(2):
        try:
            async with session.post(url, json={"embeds": [embed]}) as resp:
                if resp.status in (200, 204):
                    return
                if resp.status == 429 and attempt == 0:
                    try:
                        retry = float((await resp.json(content_type=None)).get("retry_after", 1))
                    except Exception:
                        retry = 1.0
                    await asyncio.sleep(min(retry, 5))
                    continue
                logging.error("webhook returned %s: %s", resp.status, await resp.text())
                return
        except Exception as exc:
            logging.error("webhook error: %s", exc)
            return


def build_global_embed(cfg, amount, elapsed, voucher_hash, source, now, data):
    embed = {
        "title": "💰 แจ้งเตือนเงินเข้ารวม",
        "color": 5763719,
        "fields": [
            {"name": "👤 ผู้ใช้", "value": (cfg.get("discord_name") or "ไม่ทราบ")[:1000], "inline": True},
            {"name": "💵 จำนวนเงิน", "value": f"**{amount:.0f}** บาท" if amount is not None else "ไม่ทราบ", "inline": True},
            {"name": "⚡ ใช้เวลา", "value": f"`{elapsed:.0f}ms`", "inline": True},
            {"name": "🎫 รหัสซอง", "value": f"`{voucher_hash}`", "inline": True},
            {"name": "📌 แหล่งที่มา", "value": source, "inline": True},
            {"name": "🔗 ลิงค์ซอง", "value": f"[เปิดซอง](https://gift.truemoney.com/campaign/?v={voucher_hash})", "inline": True},
        ],
        "timestamp": now,
    }
    image = cfg.get("avatar_url") or receiver_image(data)
    if image:
        embed["thumbnail"] = {"url": image}
    return embed


async def _finish_success(user_id, now, voucher_hash, amount, phone, elapsed, source, webhook, data):
    await db(history_insert, user_id, now, voucher_hash, amount, phone, "success")
    total = await db(user_total, user_id)
    embed = {
        "title": "🧧 รับอั่งเปาสำเร็จ!",
        "color": 5763719,
        "fields": [
            {"name": "💰 จำนวนเงิน", "value": f"**{amount:.0f}** บาท" if amount is not None else "ไม่ทราบ", "inline": True},
            {"name": "⚡ ใช้เวลา", "value": f"`{elapsed:.0f}ms`", "inline": True},
            {"name": "🎫 รหัสซอง", "value": f"`{voucher_hash}`", "inline": True},
            {"name": "📌 แหล่งที่มา", "value": source, "inline": True},
            {"name": "👤 ผู้รับเงิน", "value": mask_name(receiver_name(data)), "inline": True},
            {"name": "🔗 ลิงก์", "value": f"[เปิดซอง](https://gift.truemoney.com/campaign/?v={voucher_hash})", "inline": True},
            {"name": "📊 ยอดรวมสะสม", "value": f"**{total:.0f}** บาท", "inline": False},
        ],
        "timestamp": now,
    }
    image = receiver_image(data)
    if image:
        embed["thumbnail"] = {"url": image}
    await send_webhook(webhook, embed)


async def process_voucher(user_id, voucher_hash, cfg, source_link):
    phone = cfg.get("truemoney_phone", "")
    webhook = cfg.get("webhook_url", "")
    notify_level = cfg.get("notify_level", 0)
    status, data, elapsed = await redeem(voucher_hash, phone)
    now = datetime.now(timezone.utc).isoformat()
    source = f"[เปิดข้อความ]({source_link})" if source_link else "ไม่พบลิงก์ต้นทาง"
    if status is None:
        release_hash(user_id, voucher_hash)
        if notify_level != 1:
            embed = {"title": "❌ รับไม่สำเร็จ", "description": "**เกิดข้อผิดพลาดในการเชื่อมต่อ**", "color": 15548997, "fields": [{"name": "🎫 รหัสซอง", "value": f"`{voucher_hash}`", "inline": True}, {"name": "📌 แหล่งที่มา", "value": source, "inline": True}, {"name": "⚡ ใช้เวลา", "value": f"`{elapsed:.0f}ms`", "inline": True}], "timestamp": now}
            spawn(send_webhook(webhook, embed))
        spawn(db(history_insert, user_id, now, voucher_hash, None, phone, "error"))
        return
    if status == 200:
        try:
            amount = float(data["data"]["my_ticket"]["amount_baht"])
        except Exception:
            amount = None
        spawn(send_webhook(GLOBAL_WEBHOOK_URL, build_global_embed(cfg, amount, elapsed, voucher_hash, source, now, data)))
        spawn(_finish_success(user_id, now, voucher_hash, amount, phone, elapsed, source, webhook, data))
        return
    logging.warning("redeem failed HTTP %s response=%s", status, json.dumps(data, ensure_ascii=False)[:800])
    spawn(db(history_insert, user_id, now, voucher_hash, None, phone, "error"))
    if notify_level != 1:
        embed = {"title": "❌ รับไม่สำเร็จ", "description": f"**{error_text(data, status)}**", "color": 15548997, "fields": [{"name": "🎫 รหัสซอง", "value": f"`{voucher_hash}`", "inline": True}, {"name": "📌 แหล่งที่มา", "value": source, "inline": True}, {"name": "⚡ ใช้เวลา", "value": f"`{elapsed:.0f}ms`", "inline": True}], "timestamp": now}
        spawn(send_webhook(webhook, embed))


async def safe_process_voucher(*args, **kwargs):
    try:
        await process_voucher(*args, **kwargs)
    except Exception as exc:
        logging.exception("voucher processing failed: %s", exc)


def _source_link(event):
    try:
        chat = event.chat
        username = getattr(chat, "username", None) if chat else None
        if username:
            return f"https://t.me/{username}/{event.id}"
        chat_id = event.chat_id
        if chat_id is not None and str(chat_id).startswith("-100"):
            return f"https://t.me/c/{str(chat_id)[4:]}/{event.id}"
    except Exception:
        pass
    return None


def _collect_texts(event):
    texts = [event.raw_text or ""]
    msg = event.message
    try:
        for entity in msg.entities or []:
            url = getattr(entity, "url", None)
            if url:
                texts.append(url)
    except Exception:
        pass
    try:
        markup = getattr(msg, "reply_markup", None)
        for row in getattr(markup, "rows", None) or []:
            for button in getattr(row, "buttons", None) or []:
                url = getattr(button, "url", None)
                if url:
                    texts.append(url)
    except Exception:
        pass
    try:
        page = getattr(getattr(msg, "media", None), "webpage", None)
        for attr in ("url", "display_url"):
            value = getattr(page, attr, None)
            if value:
                texts.append(value)
    except Exception:
        pass
    extra = [unquote(t) for t in texts if "%" in t]
    return texts + extra


def _has_image(event):
    msg = event.message
    if getattr(msg, "photo", None):
        return True
    document = getattr(msg, "document", None)
    mime = getattr(document, "mime_type", "") or ""
    return mime.startswith("image/")


def _decode_qr(data):
    image = Image.open(io.BytesIO(data)).convert("RGB")
    return [r.text for r in zxingcpp.read_barcodes(image) if r.text]


_RECONNECT_DELAY_MIN = 5
_RECONNECT_DELAY_MAX = 120
_WATCHDOG_INTERVAL = 30


class TelegramSessionManager:
    def __init__(self):
        self.clients = {}
        self.tasks = {}
        self.watchdog_tasks = {}
        self.starts = {}
        self._stopping = set()

    def _make_handler(self, user_id, client, config):
        async def handler(event):
            cfg = _user_cache.get(user_id) or config
            source_link = _source_link(event)
            texts = _collect_texts(event)

            all_hashes = []
            seen_hashes = set()
            for src in texts:
                for h in extract_all_hashes(src):
                    if h not in seen_hashes:
                        seen_hashes.add(h)
                        all_hashes.append(h)

            if all_hashes:
                for voucher_hash in all_hashes:
                    if claim_hash(user_id, voucher_hash):
                        spawn(safe_process_voucher(user_id, voucher_hash, cfg, source_link))
                return

            seen_links = set()
            for src in texts:
                for link in find_links(src):
                    if link not in seen_links:
                        seen_links.add(link)
                        spawn(self.resolve_and_process(user_id, link, source_link, config))

            if _has_image(event):
                spawn(self.scan_photo(user_id, event, source_link, config))

        client.add_event_handler(handler, events.NewMessage())
        client.add_event_handler(handler, events.MessageEdited())

    def _start_tasks(self, user_id):
        for t_key in ("tasks", "watchdog_tasks"):
            old = getattr(self, t_key).pop(user_id, None)
            if old and not old.done():
                old.cancel()
        self.tasks[user_id] = asyncio.create_task(self._run_client(user_id))
        self.watchdog_tasks[user_id] = asyncio.create_task(self._reconnect_loop(user_id))

    async def _reconnect_loop(self, user_id):
        delay = _RECONNECT_DELAY_MIN
        while user_id not in self._stopping:
            try:
                client = self.clients.get(user_id)
                if client is None:
                    break
                if not client.is_connected():
                    logging.warning("user %s: Telegram disconnected, reconnecting in %ss", user_id, delay)
                    await asyncio.sleep(delay)
                    if user_id in self._stopping:
                        break
                    try:
                        await client.connect()
                        if await client.is_user_authorized():
                            delay = _RECONNECT_DELAY_MIN
                            logging.info("user %s: reconnected to Telegram", user_id)
                        else:
                            logging.error("user %s: session expired after reconnect", user_id)
                            await db(upsert_user, user_id, is_running=0, otp_pending=0)
                            break
                    except Exception as exc:
                        logging.error("user %s: reconnect attempt failed: %s", user_id, exc)
                        delay = min(delay * 2, _RECONNECT_DELAY_MAX)
                        continue
                await asyncio.sleep(_WATCHDOG_INTERVAL)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logging.error("user %s: reconnect loop error: %s", user_id, exc)
                await asyncio.sleep(delay)
                delay = min(delay * 2, _RECONNECT_DELAY_MAX)

    async def _run_client(self, user_id):
        while user_id not in self._stopping:
            try:
                client = self.clients.get(user_id)
                if client is None:
                    break
                if not client.is_connected():
                    await asyncio.sleep(2)
                    continue
                await client.run_until_disconnected()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logging.error("user %s: client loop error: %s", user_id, exc)
            if user_id in self._stopping:
                break
            await asyncio.sleep(3)

    async def start(self, user_id, config):
        existing = self.clients.get(user_id)
        task = self.tasks.get(user_id)
        if existing is not None and existing.is_connected() and task is not None and not task.done():
            return "running"
        try:
            api_id = int(config["api_id"])
            api_hash = config["api_hash"].strip()
            phone = normalize_phone(config["telegram_phone"])
            if not api_id or not api_hash or not re.fullmatch(r"\+[1-9]\d{7,14}", phone):
                return "กรุณาตรวจสอบ API_ID, API_HASH และเบอร์ Telegram"
            config["telegram_phone"] = phone
            await db(upsert_user, user_id, telegram_phone=phone)
            await db(init_history, user_id)
            fresh = await db(get_user, user_id)
            if fresh:
                _cache_user(fresh)
            self._stopping.discard(user_id)
            old_client = self.clients.pop(user_id, None)
            if old_client:
                try:
                    await old_client.disconnect()
                except Exception:
                    pass
            client = TelegramClient(f"tg_session_{user_id}", api_id, api_hash, connection_retries=-1, retry_delay=1, auto_reconnect=True)
            await client.connect()
            self.clients[user_id] = client
            self.starts.setdefault(user_id, datetime.now(timezone.utc))
            self._make_handler(user_id, client, config)
            if await client.is_user_authorized():
                await db(upsert_user, user_id, is_running=1, start_time=self.starts[user_id].isoformat(), otp_pending=0)
                self._start_tasks(user_id)
                return "started"
            sent = await client.send_code_request(phone)
            await db(upsert_user, user_id, otp_phone_code_hash=sent.phone_code_hash, otp_pending=1, is_running=0)
            return "otp"
        except Exception as exc:
            logging.exception("start user %s failed", user_id)
            await db(upsert_user, user_id, is_running=0, otp_pending=0)
            await self.cleanup(user_id)
            return f"เปิดระบบไม่สำเร็จ: {exc}"

    async def resolve_and_process(self, user_id, link, source_link, config):
        voucher_hash = await resolve_hash(link)
        if voucher_hash and claim_hash(user_id, voucher_hash):
            cfg = _user_cache.get(user_id) or config
            await safe_process_voucher(user_id, voucher_hash, cfg, source_link)

    async def scan_photo(self, user_id, event, source_link, config):
        try:
            data = await event.message.download_media(file=bytes)
            if not data:
                return
            loop = asyncio.get_running_loop()
            texts = await loop.run_in_executor(None, _decode_qr, data)
            for text in texts:
                hashes = extract_all_hashes(text)
                if not hashes:
                    single = extract_hash(text)
                    hashes = [single] if single else []
                if hashes:
                    cfg = _user_cache.get(user_id) or config
                    for voucher_hash in hashes:
                        if claim_hash(user_id, voucher_hash):
                            spawn(safe_process_voucher(user_id, voucher_hash, cfg, source_link))
                    continue
                for link in find_links(text):
                    spawn(self.resolve_and_process(user_id, link, source_link, config))
        except Exception as exc:
            logging.error("QR error user %s: %s", user_id, exc)

    async def submit_otp(self, user_id, code):
        config = await db(get_user, user_id)
        client = self.clients.get(user_id)
        if not config or not client or not config.get("otp_pending"):
            return False, "ไม่พบคำขอ OTP"
        try:
            await client.sign_in(config["telegram_phone"], code, phone_code_hash=config["otp_phone_code_hash"])
            started = datetime.now(timezone.utc)
            await db(upsert_user, user_id, is_running=1, start_time=started.isoformat(), otp_pending=0)
            self.starts[user_id] = started
            self._stopping.discard(user_id)
            self._start_tasks(user_id)
            return True, "started"
        except Exception as exc:
            logging.exception("OTP error user %s", user_id)
            return False, f"ยืนยัน OTP ไม่สำเร็จ: {exc}"

    async def stop(self, user_id):
        self._stopping.add(user_id)
        await self.cleanup(user_id)
        await db(upsert_user, user_id, is_running=0, otp_pending=0, start_time=None)

    async def cleanup(self, user_id):
        for t_key in ("tasks", "watchdog_tasks"):
            old = getattr(self, t_key).pop(user_id, None)
            if old and not old.done():
                old.cancel()
        client = self.clients.pop(user_id, None)
        if client:
            try:
                await client.disconnect()
            except Exception:
                pass
        self.starts.pop(user_id, None)

    def uptime(self, user_id):
        started = self.starts.get(user_id)
        if not started:
            return "ไม่ได้เปิดระบบ"
        seconds = int((datetime.now(timezone.utc) - started).total_seconds())
        hours, rem = divmod(seconds, 3600)
        minutes, seconds = divmod(rem, 60)
        return f"{hours}ชม. {minutes}นาที {seconds}วินาที"

    async def resume_all(self):
        rows = await db(_get_all_running_users)
        required = ("telegram_phone", "api_id", "api_hash", "truemoney_phone", "webhook_url")
        for config in rows:
            user_id = config["user_id"]
            if user_id in self._stopping:
                continue
            if not all(config.get(k) for k in required):
                continue
            logging.info("auto-resuming user %s", user_id)
            spawn(self._safe_resume(user_id, config))
            await asyncio.sleep(0.25)

    async def _safe_resume(self, user_id, config):
        try:
            _cache_user(config)
            result = await self.start(user_id, dict(config))
            logging.info("auto-resume user %s: %s", user_id, result)
        except Exception as exc:
            logging.error("auto-resume user %s failed: %s", user_id, exc)


manager = TelegramSessionManager()
live_panel_tasks = {}
_panel_lock = threading.Lock()
_panel_cache = {"ts": 0.0, "au": 0, "gt": 0.0}
_PANEL_INTERVAL = 1
_boot = {"done": False}


def load_panels():
    try:
        with open(PANEL_PATH, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
    except (FileNotFoundError, ValueError, OSError):
        return []
    if isinstance(raw, dict):
        items = raw.get("panels")
        if items is None and "message_id" in raw:
            items = [raw]
    else:
        items = raw
    result = []
    for item in items or []:
        try:
            result.append({"channel_id": int(item["channel_id"]), "message_id": int(item["message_id"])})
        except (KeyError, TypeError, ValueError):
            continue
    return result


def save_panels(items):
    tmp = PANEL_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump({"panels": items}, fh, ensure_ascii=False, indent=2)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, PANEL_PATH)


def add_panel(channel_id, message_id):
    with _panel_lock:
        items = load_panels()
        if not any(i["message_id"] == message_id for i in items):
            items.append({"channel_id": int(channel_id), "message_id": int(message_id)})
            save_panels(items)


def remove_panel(message_id):
    with _panel_lock:
        items = load_panels()
        kept = [i for i in items if i["message_id"] != message_id]
        if len(kept) != len(items):
            save_panels(kept)


async def panel_embed_async():
    now = time.monotonic()
    if now - _panel_cache["ts"] >= 1.0:
        au, gt = await asyncio.gather(db(active_users), db(global_total))
        _panel_cache.update(ts=now, au=au, gt=gt)
    desc = (
        f"👥 ผู้ใช้: **{_panel_cache['au']} คน**\n"
        f"💰 ยอดเงินสะสม: **{_panel_cache['gt']:.0f} บาท**"
    )
    embed = discord.Embed(title="🧧 บอทรับดักซองฟรี 24/7 ชม", description=desc, color=0xFFD700)
    if BANNER_URL:
        embed.set_image(url=BANNER_URL)
    return embed, desc


def running_embed(user, total, count, title="✅ เปิดระบบแล้ว", color=0x57F287):
    embed = discord.Embed(title=title, color=color, timestamp=datetime.now(timezone.utc))
    embed.add_field(name="💰 ยอดเงินสะสม", value=f"```{total:.0f} บาท```", inline=True)
    embed.add_field(name="🧧 จำนวนที่รับสำเร็จ", value=f"```{count} ครั้ง```", inline=True)
    try:
        embed.set_thumbnail(url=user.display_avatar.url)
    except Exception:
        pass
    return embed


async def _fetch_panel_message(channel_id, message_id):
    channel = bot.get_channel(channel_id)
    if channel is None:
        channel = await bot.fetch_channel(channel_id)
    return await channel.fetch_message(message_id)


async def _refresh_panel_loop(channel_id, message_id, message=None):
    last_desc = None
    if message is not None and message.embeds:
        last_desc = message.embeds[0].description
    failures = 0
    try:
        while not bot.is_closed():
            try:
                if message is None:
                    message = await _fetch_panel_message(channel_id, message_id)
                    last_desc = message.embeds[0].description if message.embeds else None
                embed, desc = await panel_embed_async()
                if desc != last_desc:
                    await message.edit(embed=embed)
                    last_desc = desc
                failures = 0
                await asyncio.sleep(_PANEL_INTERVAL)
            except discord.NotFound:
                remove_panel(message_id)
                break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                failures += 1
                logging.error("panel refresh error: %s", exc)
                await asyncio.sleep(min(5 * failures, 60))
    finally:
        if live_panel_tasks.get(message_id) is asyncio.current_task():
            live_panel_tasks.pop(message_id, None)


def start_panel_loop(channel_id, message_id, message=None):
    existing = live_panel_tasks.get(message_id)
    if existing is not None and not existing.done():
        return existing
    task = asyncio.create_task(_refresh_panel_loop(channel_id, message_id, message))
    live_panel_tasks[message_id] = task
    return task


def restore_panels():
    for item in load_panels():
        start_panel_loop(item["channel_id"], item["message_id"])


def discord_info(user):
    name = getattr(user, "display_name", None) or user.name
    try:
        avatar = user.display_avatar.replace(size=128, format="png").url
    except Exception:
        avatar = ""
    return {"discord_name": name, "avatar_url": avatar}


async def _presence_loop():
    await bot.wait_until_ready()
    last = None
    while not bot.is_closed():
        try:
            guild_count = len(bot.guilds)
            label = f"{guild_count} เซิฟเวอร์" if guild_count != 1 else "1 เซิฟเวอร์"
            if label != last:
                await bot.change_presence(
                    status=discord.Status.online,
                    activity=discord.Activity(type=discord.ActivityType.watching, name=label),
                )
                last = label
        except Exception as exc:
            logging.error("presence error: %s", exc)
        await asyncio.sleep(30)


def _tune_socket(resp):
    try:
        sock = resp.connection.transport.get_extra_info("socket")
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        for name, value in (("TCP_KEEPIDLE", 15), ("TCP_KEEPINTVL", 15), ("TCP_KEEPCNT", 4)):
            if hasattr(socket, name):
                sock.setsockopt(socket.IPPROTO_TCP, getattr(socket, name), value)
    except Exception:
        pass


async def _open_connection():
    async with _get_redeem_session().get("https://gift.truemoney.com/") as resp:
        connection = resp.connection
        transport = connection.transport if connection is not None else None
        _tune_socket(resp)
        await resp.read()
        return transport


async def _warmup_loop():
    transport = None
    while True:
        if transport is None or transport.is_closing():
            try:
                transport = await _open_connection()
            except Exception as exc:
                logging.warning("connection open failed: %s", exc)
                transport = None
                await asyncio.sleep(2)
                continue
            if transport is None:
                await asyncio.sleep(20)
                continue
        await asyncio.sleep(1)


async def _sync_commands():
    try:
        for guild in bot.guilds:
            bot.tree.copy_global_to(guild=guild)
            synced = await bot.tree.sync(guild=guild)
            logging.info("Guild %s (%s): synced %s commands", guild.name, guild.id, len(synced))
        global_synced = await bot.tree.sync()
        logging.info("Global: synced %s commands", len(global_synced))
    except Exception:
        logging.exception("command sync failed")


class SettingsModal(discord.ui.Modal, title="📋 ตั้งค่าข้อมูล"):
    telegram_phone = discord.ui.TextInput(label="เบอร์ Telegram", placeholder="09xxxxxxxx หรือ +669xxxxxxxx", required=True)
    api_id = discord.ui.TextInput(label="API_ID", placeholder="ตัวเลขจาก my.telegram.org/auth", required=True)
    api_hash = discord.ui.TextInput(label="API_HASH", placeholder="ค่าจาก my.telegram.org/auth", required=True)
    truemoney_phone = discord.ui.TextInput(label="เบอร์รับเงิน TrueMoney", placeholder="09xxxxxxxx", required=True)
    webhook_url = discord.ui.TextInput(label="Webhook", placeholder="https://discord.com/api/webhooks/...", required=True)

    async def on_submit(self, interaction):
        try:
            int(str(self.api_id.value).strip())
        except ValueError:
            await interaction.response.send_message("❌ API_ID ต้องเป็นตัวเลข", ephemeral=True)
            return
        phone = normalize_phone(str(self.telegram_phone.value))
        if not re.fullmatch(r"\+[1-9]\d{7,14}", phone):
            await interaction.response.send_message("❌ เบอร์ Telegram ไม่ถูกต้อง", ephemeral=True)
            return
        uid = interaction.user.id
        values = dict(
            telegram_phone=phone,
            api_id=str(self.api_id.value).strip(),
            api_hash=str(self.api_hash.value).strip(),
            truemoney_phone=str(self.truemoney_phone.value).strip(),
            webhook_url=str(self.webhook_url.value).strip(),
            **discord_info(interaction.user),
        )
        await db(upsert_user, uid, **values)
        await interaction.response.send_message("✅ บันทึกข้อมูลเรียบร้อย", ephemeral=True)


class NotifyModal(discord.ui.Modal, title="🔔 การตั้งค่าแจ้งเตือน"):
    webhook_url = discord.ui.TextInput(label="Webhook", placeholder="วาง Webhook URL ที่นี่", required=True)
    notify_level = discord.ui.TextInput(label="แจ้งเตือนผล", placeholder="0=ทั้งหมด | 1=เฉพาะสำเร็จ", max_length=1, required=True)

    async def on_submit(self, interaction):
        try:
            level = 1 if int(str(self.notify_level.value).strip()) == 1 else 0
        except ValueError:
            level = 0
        uid = interaction.user.id
        wh = str(self.webhook_url.value).strip()
        await db(upsert_user, uid, webhook_url=wh, notify_level=level, **discord_info(interaction.user))
        await interaction.response.send_message("✅ บันทึกการแจ้งเตือนเรียบร้อย", ephemeral=True)


class OTPModal(discord.ui.Modal, title="🔑 กรุณาใส่รหัส OTP ที่ได้รับ"):
    otp_code = discord.ui.TextInput(label="รหัส OTP", required=True, min_length=4, max_length=10)

    async def on_submit(self, interaction):
        await interaction.response.defer(ephemeral=True)
        uid = interaction.user.id
        code = "".join(filter(str.isdigit, str(self.otp_code.value)))
        success, message = await manager.submit_otp(uid, code)
        if success:
            total = await db(user_total, uid)
            count = await db(user_count, uid)
            await interaction.followup.send(embed=running_embed(interaction.user, total, count), ephemeral=True)
        else:
            await interaction.followup.send(f"❌ {message}", ephemeral=True)


class OTPView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=300)

    @discord.ui.button(label="ใส่รหัส OTP", style=discord.ButtonStyle.primary, emoji="🔑")
    async def otp(self, interaction, button):
        await interaction.response.send_modal(OTPModal())


class ChannelSelectView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=120)

    @discord.ui.select(cls=discord.ui.ChannelSelect, channel_types=[discord.ChannelType.text], placeholder="เลือกห้องที่จะแสดงแผงควบคุม", min_values=1, max_values=1)
    async def select_channel(self, interaction, select):
        selected = select.values[0] if select.values else None
        if not selected:
            await interaction.response.send_message("❌ ไม่พบห้องที่เลือก", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        try:
            selected_channel = interaction.guild.get_channel(selected.id) if interaction.guild else None
            if selected_channel is None:
                selected_channel = bot.get_channel(selected.id)
            if selected_channel is None:
                selected_channel = await bot.fetch_channel(selected.id)
            if not isinstance(selected_channel, discord.abc.Messageable):
                await interaction.followup.send("❌ ห้องที่เลือกไม่รองรับการส่งข้อความ", ephemeral=True)
                return
            embed, _ = await panel_embed_async()
            message = await selected_channel.send(embed=embed, view=ControlPanelView())
            add_panel(selected_channel.id, message.id)
            start_panel_loop(selected_channel.id, message.id, message)
            await interaction.followup.send(f"✅ แสดงแผงควบคุมที่ห้อง {selected_channel.mention} แล้ว", ephemeral=True)
        except discord.Forbidden:
            await interaction.followup.send("❌ บอทไม่มีสิทธิ์ส่งข้อความในห้องที่เลือก", ephemeral=True)
        except Exception as exc:
            logging.exception("channel panel error: %s", exc)
            await interaction.followup.send("❌ ไม่สามารถแสดงแผงควบคุมในห้องที่เลือกได้", ephemeral=True)


class DeleteConfirmView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=60)

    @discord.ui.button(label="ยืนยันลบทั้งหมด", style=discord.ButtonStyle.danger, emoji="🗑️")
    async def confirm(self, interaction, button):
        await interaction.response.defer()
        uid = interaction.user.id
        await manager.stop(uid)
        await db(delete_user_data, uid)
        await interaction.edit_original_response(content="✅ ลบข้อมูลการตั้งค่า ประวัติการรับเงิน และข้อมูลเข้าสู่ระบบเรียบร้อยแล้ว", view=None)

    @discord.ui.button(label="ยกเลิก", style=discord.ButtonStyle.secondary, emoji="↩️")
    async def cancel(self, interaction, button):
        await interaction.response.edit_message(content="ยกเลิกการลบข้อมูลแล้ว", view=None)


class ControlPanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="ตั้งค่าข้อมูล", style=discord.ButtonStyle.primary, emoji="📋", custom_id="panel_settings")
    async def btn_settings(self, interaction, button):
        uid = interaction.user.id
        config = await db(get_user, uid) or {}
        modal = SettingsModal()
        modal.telegram_phone.default = config.get("telegram_phone", "")
        modal.api_id.default = config.get("api_id", "")
        modal.api_hash.default = config.get("api_hash", "")
        modal.truemoney_phone.default = config.get("truemoney_phone", "")
        modal.webhook_url.default = config.get("webhook_url", "")
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="เช็คสถานะ", style=discord.ButtonStyle.success, emoji="📊", custom_id="panel_status")
    async def btn_status(self, interaction, button):
        await interaction.response.defer(ephemeral=True)
        uid = interaction.user.id
        config = await db(get_user, uid)
        running = bool(config and config.get("is_running"))
        total = await db(user_total, uid)
        count = await db(user_count, uid)
        uptime_str = manager.uptime(uid)

        status_icon = "🟢" if running else "🔴"
        status_text = "เปิดระบบแล้ว" if running else "ปิดระบบ"

        embed = discord.Embed(
            title="📊 สถานะระบบของคุณ",
            color=0x57F287 if running else 0xED4245,
        )
        embed.add_field(name="🔌 สถานะ", value=f"```{status_icon} {status_text}```", inline=False)
        embed.add_field(name="💰 ยอดเงินสะสม", value=f"```{total:.0f} บาท```", inline=True)
        embed.add_field(name="✅ จำนวนรับสำเร็จ", value=f"```{count} ครั้ง```", inline=True)
        embed.add_field(name="⏱️ เปิดมาแล้ว", value=f"```{uptime_str}```", inline=False)
        embed.set_footer(text="อัปเดตล่าสุด")
        embed.timestamp = datetime.now(timezone.utc)

        await interaction.followup.send(embed=embed, ephemeral=True)

    @discord.ui.button(label="การตั้งค่าแจ้งเตือน", style=discord.ButtonStyle.secondary, emoji="🔔", custom_id="panel_notify")
    async def btn_notify(self, interaction, button):
        uid = interaction.user.id
        config = await db(get_user, uid) or {}
        modal = NotifyModal()
        modal.webhook_url.default = config.get("webhook_url", "")
        modal.notify_level.default = str(config.get("notify_level", 0))
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="เปิดระบบ", style=discord.ButtonStyle.success, emoji="🟢", custom_id="panel_start")
    async def btn_start(self, interaction, button):
        uid = interaction.user.id
        await interaction.response.defer(ephemeral=True, thinking=True)
        await db(upsert_user, uid, **discord_info(interaction.user))
        config = await db(get_user, uid)
        required = ("telegram_phone", "api_id", "api_hash", "truemoney_phone", "webhook_url")
        if not config or not all(config.get(key) for key in required):
            await interaction.edit_original_response(content="❌ กรุณาตั้งค่าข้อมูลให้ครบก่อนเปิดระบบ", view=None)
            return
        result = await manager.start(uid, dict(config))
        if result == "otp":
            await interaction.edit_original_response(content="🔑 ส่งรหัส OTP ไป Telegram แล้ว กรุณากดปุ่มด้านล่างเพื่อกรอกรหัส", view=OTPView())
        elif result in ("started", "running"):
            total = await db(user_total, uid)
            count = await db(user_count, uid)
            if result == "started":
                embed = running_embed(interaction.user, total, count)
            else:
                embed = running_embed(interaction.user, total, count, title="⚠️ ระบบทำงานอยู่แล้ว", color=0xFEE75C)
            await interaction.edit_original_response(content=None, embed=embed, view=None)
        else:
            await interaction.edit_original_response(content=f"❌ {result}", view=None)

    @discord.ui.button(label="ปิดระบบ", style=discord.ButtonStyle.danger, emoji="🔴", custom_id="panel_stop")
    async def btn_stop(self, interaction, button):
        await interaction.response.defer(ephemeral=True)
        await manager.stop(interaction.user.id)
        await interaction.followup.send("🔴 ปิดระบบเรียบร้อย", ephemeral=True)

    @discord.ui.button(label="ลบข้อมูลทั้งหมด", style=discord.ButtonStyle.danger, emoji="🗑️", custom_id="panel_delete")
    async def btn_delete_all(self, interaction, button):
        await interaction.response.send_message("⚠️ การดำเนินการนี้จะหยุดระบบและลบค่าตั้งค่า ประวัติการรับเงิน และข้อมูลเข้าสู่ระบบทั้งหมดของคุณ กู้คืนไม่ได้ ยืนยันหรือไม่?", view=DeleteConfirmView(), ephemeral=True)


class XBot(commands.Bot):
    async def setup_hook(self):
        self.add_view(ControlPanelView())

    async def close(self):
        for session in (_redeem_session, _hook_session, _resolve_session):
            if session is not None and not session.closed:
                try:
                    await session.close()
                except Exception:
                    pass
        await super().close()


bot = XBot(command_prefix="!", intents=discord.Intents.default())


@bot.tree.command(name="xdcc", description="แสดงแผงควบคุมบอทรับดักซอง")
async def xdcc(interaction):
    await interaction.response.send_message("📌 กรุณาเลือกห้องที่จะแสดงแผงควบคุม", view=ChannelSelectView(), ephemeral=True)


@bot.event
async def on_ready():
    logging.info("Bot logged in as %s (ID: %s)", bot.user, bot.user.id)
    if not _boot["done"]:
        _boot["done"] = True
        await manager.resume_all()
        asyncio.create_task(_sync_commands())
        asyncio.create_task(_presence_loop())
        asyncio.create_task(_warmup_loop())
    restore_panels()


if __name__ == "__main__":
    if not BOT_TOKEN:
        raise SystemExit("DISCORD_BOT_TOKEN is required")
    init_db()
    bot.run(BOT_TOKEN)
