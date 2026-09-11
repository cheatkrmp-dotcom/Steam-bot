#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
LZT Steam Resale Bot: гость-скрейпинг → Gemini → Telegram (Railway)
---------------------------------------------------------------------
Заходит на lzt.market/steam как гость (без аккаунта Lolz и без API),
парсит свежие лоты → фильтры → Gemini (4 ключа, ротация) →
«‼️ НАЙДЕН ОКУПАЕМЫЙ STEAM АККАУНТ ‼️».

Если гостевой доступ перестанет работать (Cloudflare/закрыли) — бот
сообщит; тогда добавь переменную LZT_COOKIE (строка cookie из браузера),
код переключится сам, менять ничего не надо.

Команды бота:
  /start /help, /guide, /checklist, /boost
  /price 0 150, /profit 70, /markup 70, /level 5, /age 60
  /criteria <текст>, /poll 60, /heartbeat 3, /model <имя>
  /status, /logs, /filters, /pause, /resume, /now
"""

import asyncio
import contextlib
import html
import json
import logging
import os
import re
import sqlite3
import time
from collections import deque
from dataclasses import dataclass
from logging.handlers import RotatingFileHandler
from typing import Any, Optional

import aiohttp
from aiohttp import web as aioweb
from aiogram import Bot, Dispatcher, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject
from aiogram.types import LinkPreviewOptions, Message

from google import genai
from google.genai import types as gtypes

import datetime as _dt
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# .env — только для локального запуска (на Railway переменные с платформы)
# ---------------------------------------------------------------------------


def _load_dotenv() -> None:
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if not os.path.isfile(env_path):
        return
    with open(env_path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key, value = key.strip(), value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


_load_dotenv()

# ---------------------------------------------------------------------------
# Конфиг
# ---------------------------------------------------------------------------

TG_TOKEN = os.getenv("TG_TOKEN", "")
TG_ADMIN_ID = int(os.getenv("TG_ADMIN_ID", "0") or 0)

# Гость-режим. LZT_COOKIE — опционально (запасной вариант, если гостей закроют)
LZT_COOKIE = os.getenv("LZT_COOKIE", "")
LZT_LIST_URL = os.getenv("LZT_LIST_URL", "https://lzt.market/steam/")
LZT_PAGES = int(os.getenv("LZT_PAGES", "2"))
DETAIL_BUDGET = int(os.getenv("DETAIL_BUDGET", "15"))
LZT_USER_AGENT = os.getenv(
    "LZT_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
)

# Gemini: до 4 ключей с ротацией
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-flash-lite-latest")
QUOTA_COOLDOWN_BASE = int(os.getenv("QUOTA_COOLDOWN_BASE", "300"))
QUOTA_COOLDOWN_MAX = int(os.getenv("QUOTA_COOLDOWN_MAX", "21600"))

# --- Дефолты фильтров (схема из видео) --------------------------------------
DEFAULT_PMAX = float(os.getenv("DEFAULT_PMAX", "150"))
DEFAULT_MIN_PROFIT = float(os.getenv("DEFAULT_MIN_PROFIT", "70"))
DEFAULT_MARKUP = float(os.getenv("DEFAULT_MARKUP", "70"))
DEFAULT_MIN_LEVEL = int(os.getenv("DEFAULT_MIN_LEVEL", "5"))
DEFAULT_AGE_MIN = int(os.getenv("DEFAULT_AGE_MIN", "60"))
DEFAULT_POLL = int(os.getenv("POLL_INTERVAL", "60"))
DEFAULT_HEARTBEAT_H = float(os.getenv("HEARTBEAT_H", "3"))

DEFAULT_CRITERIA = (
    "Steam-аккаунт под перепродажу на Funpay: уровень 5+, без VAC и Game-банов, "
    "хотя бы одна известная игра (GTA 5, CS2, Dota 2...), желательно игры без лимита, "
    "почта в комплекте — приоритет. Часы > 1000 — риск возврата владельцем, минус."
)

DB_PATH = os.getenv("DB_PATH") or (
    "/data/lzt_monitor.db"
    if os.path.isdir("/data")
    else os.path.join(os.path.dirname(os.path.abspath(__file__)), "lzt_monitor.db")
)

# ---------------------------------------------------------------------------
# Логирование: stdout (Railway Logs) + файл (Volume) + буфер (/logs)
# ---------------------------------------------------------------------------

LOG_FMT = "%(asctime)s | %(levelname)-7s | %(message)s"
LOG_DATEFMT = "%H:%M:%S"

_recent_logs: deque = deque(maxlen=60)


class MemoryLogHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            _recent_logs.append(self.format(record))
        except Exception:
            pass


def _setup_logging() -> logging.Logger:
    logger = logging.getLogger("lzt-resale")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(LOG_FMT, datefmt=LOG_DATEFMT)

    stream = logging.StreamHandler()
    stream.setFormatter(formatter)
    logger.addHandler(stream)

    mem = MemoryLogHandler()
    mem.setFormatter(formatter)
    logger.addHandler(mem)

    log_dir = os.path.dirname(DB_PATH)
    if log_dir:
        with contextlib.suppress(OSError):
            os.makedirs(log_dir, exist_ok=True)
    try:
        file_h = RotatingFileHandler(
            os.path.join(log_dir or ".", "bot.log"),
            maxBytes=1_000_000, backupCount=2, encoding="utf-8",
        )
        file_h.setFormatter(formatter)
        logger.addHandler(file_h)
    except OSError:
        logger.warning("Файловый лог недоступен, пишу только в stdout")

    logger.propagate = False
    return logger


log = _setup_logging()
_started = time.time()

# ---------------------------------------------------------------------------
# SQLite
# ---------------------------------------------------------------------------

_conn: Optional[sqlite3.Connection] = None


def db() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        _conn = sqlite3.connect(DB_PATH)
        _conn.execute(
            "CREATE TABLE IF NOT EXISTS seen_items ("
            "item_id INTEGER PRIMARY KEY, seen_at REAL NOT NULL)"
        )
        _conn.execute(
            "CREATE TABLE IF NOT EXISTS settings ("
            "key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        _conn.commit()
    return _conn


def get_setting(key: str, default: str = "") -> str:
    row = db().execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    return row[0] if row else default


def set_setting(key: str, value: Any) -> None:
    db().execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
        (key, str(value)),
    )
    db().commit()


def get_stat(key: str) -> float:
    try:
        return float(get_setting(key, "0") or 0)
    except ValueError:
        return 0.0


def bump_stat(key: str, add: float = 1) -> float:
    val = get_stat(key) + add
    set_setting(key, val)
    return val


def is_seen(item_id: int) -> bool:
    return (
        db().execute("SELECT 1 FROM seen_items WHERE item_id = ?", (item_id,)).fetchone()
        is not None
    )


def mark_seen(item_id: int) -> None:
    db().execute(
        "INSERT OR IGNORE INTO seen_items (item_id, seen_at) VALUES (?, ?)",
        (item_id, time.time()),
    )
    db().commit()


_last_cleanup = 0.0


def maybe_cleanup() -> None:
    global _last_cleanup
    if time.time() - _last_cleanup < 3600:
        return
    db().execute("DELETE FROM seen_items WHERE seen_at < ?", (time.time() - 7 * 86400,))
    db().commit()
    _last_cleanup = time.time()

# ---------------------------------------------------------------------------
# Фильтры
# ---------------------------------------------------------------------------


@dataclass
class Filters:
    pmin: float = 0.0
    pmax: float = DEFAULT_PMAX
    min_profit: float = DEFAULT_MIN_PROFIT
    markup_pct: float = DEFAULT_MARKUP
    min_level: int = DEFAULT_MIN_LEVEL
    age_min: int = DEFAULT_AGE_MIN
    poll_sec: int = DEFAULT_POLL
    heartbeat_h: float = DEFAULT_HEARTBEAT_H
    criteria: str = ""
    paused: bool = False


def _to_float(s: str, default: float) -> float:
    try:
        return float(s)
    except (TypeError, ValueError):
        return default


def _to_int(s: str, default: int) -> int:
    try:
        return int(s)
    except (TypeError, ValueError):
        return default


def load_filters() -> Filters:
    return Filters(
        pmin=_to_float(get_setting("pmin", "0"), 0.0),
        pmax=_to_float(get_setting("pmax", str(DEFAULT_PMAX)), DEFAULT_PMAX),
        min_profit=_to_float(get_setting("min_profit", str(DEFAULT_MIN_PROFIT)), DEFAULT_MIN_PROFIT),
        markup_pct=_to_float(get_setting("markup_pct", str(DEFAULT_MARKUP)), DEFAULT_MARKUP),
        min_level=_to_int(get_setting("min_level", str(DEFAULT_MIN_LEVEL)), DEFAULT_MIN_LEVEL),
        age_min=_to_int(get_setting("age_min", str(DEFAULT_AGE_MIN)), DEFAULT_AGE_MIN),
        poll_sec=_to_int(get_setting("poll_sec", str(DEFAULT_POLL)), DEFAULT_POLL),
        heartbeat_h=_to_float(get_setting("heartbeat_h", str(DEFAULT_HEARTBEAT_H)), DEFAULT_HEARTBEAT_H),
        criteria=get_setting("criteria", DEFAULT_CRITERIA),
        paused=get_setting("paused", "0") == "1",
    )

# ---------------------------------------------------------------------------
# Лот
# ---------------------------------------------------------------------------


@dataclass
class Listing:
    item_id: int
    title: str
    description: str
    price: Optional[float]
    start_date: int
    link: str


_LEVEL_PATTERNS = (
    r"уровень\D{0,5}(\d{1,3})",
    r"lvl\.?\s*(\d{1,3})",
    r"level\D{0,3}(\d{1,3})",
    r"(\d{1,3})\s*lvl",
)


def extract_level(listing: Listing) -> Optional[int]:
    text = f"{listing.title} {listing.description}"
    for pat in _LEVEL_PATTERNS:
        m = re.search(pat, text, re.I)
        if m:
            try:
                return int(m.group(1))
            except ValueError:
                continue
    return None

# ---------------------------------------------------------------------------
# Lolz Market: гость-скрейпинг (без API, без cookie по умолчанию)
# ---------------------------------------------------------------------------

_http: Optional[aiohttp.ClientSession] = None


def http() -> aiohttp.ClientSession:
    if _http is None or _http.closed:
        raise RuntimeError("HTTP-сессия ещё не создана")
    return _http


_CF_MARKERS = ("just a moment", "cf-chl", "challenge-platform", "attention required")


def _cookie_dict() -> dict:
    out = {}
    for part in LZT_COOKIE.split(";"):
        if "=" in part:
            k, _, v = part.partition("=")
            if k.strip():
                out[k.strip()] = v.strip()
    return out


def _looks_like_cf(html_text: str) -> bool:
    low = html_text[:4000].lower()
    return any(m in low for m in _CF_MARKERS)


def _parse_when(node) -> Optional[int]:
    """Unix-время из <time datetime=...> или текста «5 минут назад»."""
    if node is None:
        return None
    el = node if getattr(node, "name", None) == "time" else node.find("time")
    raw = None
    if el is not None:
        raw = el.get("datetime") or el.get("title") or el.get_text(strip=True)
    else:
        txt = node.get_text(" ", strip=True) if hasattr(node, "get_text") else ""
        m = re.search(r"(\d+)\s*(секунд\w*|минут\w*|час\w*)\s*назад", txt, re.I)
        if m:
            n, unit = int(m.group(1)), m.group(2).lower()
            secs = n if "секунд" in unit else n * 60 if "минут" in unit else n * 3600
            return int(time.time() - secs)
        return None
    if not raw:
        return None
    raw = raw.strip()
    if raw.isdigit():
        ts = int(raw)
        return ts // 1000 if ts > 10**12 else ts
    with contextlib.suppress(ValueError):
        d = _dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if d.tzinfo is None:
            d = d.replace(tzinfo=_dt.timezone(_dt.timedelta(hours=3)))
        return int(d.timestamp())
    return None


def _parse_listings_html(html_text: str) -> list[Listing]:
    """Лоты из страницы списка: ссылка /steam/<id>/ + карточка с ценой рядом."""
    soup = BeautifulSoup(html_text, "lxml")
    listings: list[Listing] = []
    seen_ids: set[int] = set()

    for a in soup.select('a[href*="/steam/"]'):
        href = a.get("href", "") or ""
        m = re.search(r"/steam/(\d+)", href)
        if not m:
            continue
        item_id = int(m.group(1))
        if item_id in seen_ids:
            continue

        node, card = a, None
        for _ in range(7):
            node = node.parent if node else None
            if node is None:
                break
            txt = node.get_text(" ", strip=True)
            if node.name in ("div", "li", "article") and ("₽" in txt or "руб" in txt.lower()):
                card = node
                break

        seen_ids.add(item_id)
        title = a.get_text(strip=True)[:300]
        text = card.get_text(" ", strip=True) if card else title

        price = None
        pm = re.search(r"([\d\u00a0\s]{2,})\s*(?:₽|руб\.?)", text)
        if pm:
            with contextlib.suppress(ValueError):
                price = float(pm.group(1).replace("\u00a0", "").replace(" ", ""))

        start = _parse_when(card) or 0
        link = href if href.startswith("http") else f"https://lzt.market{href}"
        desc = (text.replace(title, " ", 1) if title and title in text else text)[:4000]
        listings.append(Listing(item_id, title, desc, price, start, link))

    return listings


async def _fetch_html_cloudscraper(url: str, params: dict) -> Optional[str]:
    def _sync() -> Optional[str]:
        try:
            import cloudscraper
            scraper = cloudscraper.create_scraper(
                browser={"browser": "chrome", "platform": "windows", "desktop": True}
            )
            for k, v in _cookie_dict().items():
                scraper.cookies.set(k, v, domain=".zelenka.guru")
            resp = scraper.get(url, params=params, timeout=30)
            return resp.text if resp.status_code == 200 else None
        except Exception as exc:
            log.error("LZT (cloudscraper): %s", exc)
            return None

    return await asyncio.to_thread(_sync)


async def _fetch_html(url: str, params: dict) -> Optional[str]:
    headers = {
        "User-Agent": LZT_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "ru,en;q=0.9",
        "Referer": "https://lzt.market/",
    }
    try:
        async with http().get(
            url, params=params, headers=headers,
            cookies=_cookie_dict() or None,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            if resp.status == 429:
                log.warning("LZT: 429 — слишком часто, пропускаю цикл")
                bump_stat("stat_lzt_errors")
                return None
            if resp.status in (401, 403):
                log.warning("LZT: %d — доступ закрыт", resp.status)
                bump_stat("stat_lzt_errors")
                return None
            html_text = await resp.text()
    except Exception as exc:
        log.error("LZT: ошибка запроса %s: %s", url, exc)
        bump_stat("stat_lzt_errors")
        return None

    if _looks_like_cf(html_text):
        log.info("LZT: Cloudflare — пробую через cloudscraper")
        return await _fetch_html_cloudscraper(url, params)
    return html_text


async def fetch_listings(flt: Filters) -> list[Listing]:
    listings: list[Listing] = []
    for page in range(1, max(1, LZT_PAGES) + 1):
        html_text = await _fetch_html(LZT_LIST_URL, {"page": str(page)})
        if not html_text:
            break
        page_items = _parse_listings_html(html_text)
        if not page_items:
            log.warning("LZT: стр. %d — лоты не распознаны (разметка или доступ)", page)
            bump_stat("stat_lzt_errors")
            break
        listings.extend(page_items)
        if page < LZT_PAGES:
            await asyncio.sleep(2.0)

    dated = [l for l in listings if l.start_date]
    if dated:
        dated.sort(key=lambda l: l.start_date, reverse=True)
        undated = [l for l in listings if not l.start_date]
        listings = dated + undated
    return listings


async def fill_item_details(item: Listing) -> Listing:
    """Открывает страницу лота: точная дата + полное описание."""
    html_text = await _fetch_html(item.link, {})
    if not html_text:
        return item
    soup = BeautifulSoup(html_text, "lxml")

    start = _parse_when(soup) or item.start_date

    desc = item.description
    node = None
    for pat in ("description", "content", "message"):
        found = soup.find(class_=re.compile(pat, re.I))
        if found and len(found.get_text(strip=True)) > 150:
            node = found
            break
    if node is None:
        blocks = [
            t for t in soup.find_all(["div", "blockquote", "article"])
            if 150 < len(t.get_text(strip=True)) < 8000
        ]
        if blocks:
            node = max(blocks, key=lambda t: len(t.get_text(strip=True)))
    if node is not None:
        desc = node.get_text(" ", strip=True)[:6000]

    if item.price is None:
        pm = re.search(r"([\d\u00a0\s]{2,})\s*(?:₽|руб\.?)", soup.get_text(" ", strip=True))
        if pm:
            with contextlib.suppress(ValueError):
                item.price = float(pm.group(1).replace("\u00a0", "").replace(" ", ""))

    return Listing(item.item_id, item.title, desc, item.price, start, item.link)

# ---------------------------------------------------------------------------
# Gemini: 4 ключа с ротацией
# ---------------------------------------------------------------------------


def _is_quota_error(exc: Exception) -> bool:
    text = f"{type(exc).__name__} {exc}".lower()
    markers = ("429", "resource_exhausted", "quota", "rate limit",
               "ratelimit", "too many requests")
    return any(s in text for s in markers)


class GeminiKeyManager:
    def __init__(self, keys: list[str]) -> None:
        self.keys = keys
        self.clients: dict[int, Any] = {}
        self.cooldown_until: dict[int, float] = {i: 0.0 for i in range(len(keys))}
        self.quota_streak: dict[int, int] = {i: 0 for i in range(len(keys))}
        self.calls: dict[int, int] = {i: 0 for i in range(len(keys))}
        self.errors: dict[int, int] = {i: 0 for i in range(len(keys))}
        self._cur = 0

    @property
    def size(self) -> int:
        return len(self.keys)

    def client(self, idx: int) -> Any:
        if idx not in self.clients:
            self.clients[idx] = genai.Client(api_key=self.keys[idx])
        return self.clients[idx]

    def pick(self) -> int:
        now = time.time()
        n = len(self.keys)
        for off in range(n):
            idx = (self._cur + off) % n
            if self.cooldown_until.get(idx, 0) <= now:
                self._cur = idx
                return idx
        idx = min(range(n), key=lambda i: self.cooldown_until.get(i, 0.0))
        self._cur = idx
        return idx

    def note_quota_error(self, idx: int) -> int:
        self.quota_streak[idx] = self.quota_streak.get(idx, 0) + 1
        secs = min(QUOTA_COOLDOWN_BASE * (2 ** (self.quota_streak[idx] - 1)),
                   QUOTA_COOLDOWN_MAX)
        self.cooldown_until[idx] = max(self.cooldown_until.get(idx, 0), time.time() + secs)
        return secs

    def note_success(self, idx: int) -> None:
        self.calls[idx] = self.calls.get(idx, 0) + 1
        self.quota_streak[idx] = 0

    def note_error(self, idx: int) -> None:
        self.errors[idx] = self.errors.get(idx, 0) + 1

    def status_lines(self) -> list[str]:
        now = time.time()
        lines = []
        for i in range(len(self.keys)):
            if self.cooldown_until.get(i, 0) > now:
                until = time.strftime("%H:%M", time.localtime(self.cooldown_until[i]))
                lines.append(f"   #{i + 1}: 🧊 лимит, освободится ~{until} "
                             f"(запросов: {self.calls.get(i, 0)})")
            else:
                lines.append(f"   #{i + 1}: ✅ ок (запросов: {self.calls.get(i, 0)}, "
                             f"ошибок: {self.errors.get(i, 0)})")
        return lines


_gemini_mgr: Optional[GeminiKeyManager] = None


def gemini_manager() -> GeminiKeyManager:
    global _gemini_mgr
    if _gemini_mgr is None:
        keys: list[str] = []
        for name in ("GEMINI_API_KEY", "GEMINI_API_KEY_1", "GEMINI_API_KEY_2",
                     "GEMINI_API_KEY_3", "GEMINI_API_KEY_4"):
            v = os.getenv(name, "").strip()
            if v and v not in keys:
                keys.append(v)
        _gemini_mgr = GeminiKeyManager(keys)
    return _gemini_mgr


def current_model() -> str:
    return get_setting("gemini_model", "") or GEMINI_MODEL


GEMINI_PROMPT = """Ты — помощник трейдера по схеме: скупка дешёвых Steam-аккаунтов на lzt.market → обработка → продажа на Funpay с наценкой ~{markup_pct}%.

КРИТЕРИИ ПОКУПАТЕЛЯ:
{criteria}

ЛОТ №{item_id}: «{title}»
Цена продавца: {price} RUB

Описание лота:
---
{description}
---

Оцени:
1. matches — стоит ли брать для перепродажи? (true/false)
   Критично: уровень {min_level}+, без VAC/Game-банов, есть хотя бы одна известная игра
   или инвентарь. Часы > 1000 или лимитные игры — риск возврата владельцем, не берём.
2. summary — 2-4 строки: что есть (игры, CS2/Prime, уровень, скины на сумму, почта, гарантии).
3. funpay_price — число, ₽: за сколько реально продать на Funpay с учётом конкуренции.
4. sell_speed — скорость продажи на Funpay: «быстро», «средне» или «долго».
5. reason — 1 предложение: почему стоит/не стоит.

Ответь СТРОГО валидным JSON без markdown:
{{"matches": true, "summary": "...", "funpay_price": 0, "sell_speed": "быстро", "reason": "..."}}"""


async def analyze_listing(item: Listing, flt: Filters) -> Optional[dict]:
    mgr = gemini_manager()
    if mgr.size == 0:
        log.error("Gemini: не задано ни одного ключа")
        return None

    price_str = f"{item.price:.0f}" if item.price is not None else "не указана"
    prompt = GEMINI_PROMPT.format(
        markup_pct=int(flt.markup_pct) if flt.markup_pct > 0 else 60,
        criteria=flt.criteria.strip() or DEFAULT_CRITERIA,
        item_id=item.item_id,
        title=item.title or "без названия",
        price=price_str,
        min_level=flt.min_level,
        description=(item.description or "(описание пустое)"),
    )

    for _ in range(max(1, mgr.size)):
        idx = mgr.pick()
        try:
            response = await mgr.client(idx).aio.models.generate_content(
                model=current_model(),
                contents=prompt,
                config=gtypes.GenerateContentConfig(
                    temperature=0.2,
                    response_mime_type="application/json",
                ),
            )
            text = (response.text or "").strip()
        except Exception as exc:
            if _is_quota_error(exc):
                secs = mgr.note_quota_error(idx)
                log.warning("Gemini: ключ #%d/%d в лимите (пауза %d мин) — переключаюсь",
                            idx + 1, mgr.size, secs // 60)
                bump_stat("stat_gemini_rotations")
                continue
            mgr.note_error(idx)
            bump_stat("stat_gemini_errors")
            log.error("Gemini: ошибка (лот %s, ключ #%d): %s", item.item_id, idx + 1, exc)
            return None
        mgr.note_success(idx)

        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            m = re.search(r"\{.*\}", text, re.S)
            if not m:
                log.error("Gemini: не JSON (лот %s): %.200s", item.item_id, text)
                return None
            try:
                data = json.loads(m.group(0))
            except json.JSONDecodeError:
                return None

        try:
            funpay_price = float(data.get("funpay_price"))
        except (TypeError, ValueError):
            funpay_price = None

        return {
            "matches": bool(data.get("matches")),
            "funpay_price": funpay_price,
            "sell_speed": str(data.get("sell_speed") or "").strip(),
            "summary": str(data.get("summary") or "").strip(),
            "reason": str(data.get("reason") or "").strip(),
        }

    log.warning("Gemini: все %d ключ(а) в лимите — лот %s отложен", mgr.size, item.item_id)
    return None

# ---------------------------------------------------------------------------
# Уведомление
# ---------------------------------------------------------------------------


def fmt_price(value) -> str:
    try:
        return f"{float(value):,.0f}".replace(",", " ") + " ₽"
    except (TypeError, ValueError):
        return "—"


async def send_notification(bot: Bot, item: Listing, analysis: dict, age_min: int) -> None:
    price = item.price or 0
    funpay = analysis.get("funpay_price") or 0
    profit = funpay - price
    roi = (profit / price * 100) if price > 0 and profit > 0 else 0

    lines = ["‼️ <b>НАЙДЕН ОКУПАЕМЫЙ STEAM АККАУНТ</b> ‼️", ""]
    lines.append(f"🏷 <b>Купить за:</b> {fmt_price(price)}")
    if funpay:
        lines.append(f"📈 <b>Продать на Funpay за:</b> ~{fmt_price(funpay)}")
    if profit:
        lines.append(f"💰 <b>Профит:</b> {fmt_price(profit)} (окупаемость ~{roi:.0f}%)")
    if analysis.get("sell_speed"):
        lines.append(f"⏱ <b>Скорость продажи (ИИ):</b> {html.escape(analysis['sell_speed'])}")
    if analysis.get("summary"):
        lines += ["", "📋 <b>Что на аккаунте:</b>", html.escape(analysis["summary"])]
    if analysis.get("reason"):
        lines += ["", "🤖 <b>ИИ-вердикт:</b>", html.escape(analysis["reason"])]
    lines += [
        "",
        "⚠️ Гарантия лота 12/24 ч — проверь акк сразу после покупки.",
        "🔧 После покупки: /checklist • Полный план: /guide",
        f'🔗 <a href="{item.link}">Открыть лот на LZT</a>',
        f"⏰ Выложен {age_min} мин. назад",
    ]
    try:
        await bot.send_message(
            TG_ADMIN_ID, "\n".join(lines),
            link_preview_options=LinkPreviewOptions(is_disabled=True),
        )
        bump_stat("stat_sent")
    except Exception as exc:
        log.error("Telegram: не смог отправить: %s", exc)
        bump_stat("stat_tg_errors")

# ---------------------------------------------------------------------------
# Ядро
# ---------------------------------------------------------------------------

_check_lock = asyncio.Lock()
_boot_done = False
_empty_streak = 0


async def check_market(bot: Bot, first_run: bool = False) -> int:
    global _boot_done, _empty_streak
    async with _check_lock:
        maybe_cleanup()
        flt = load_filters()
        set_setting("last_check_at", time.time())
        bump_stat("stat_checks")

        listings = await fetch_listings(flt)

        # детект: гость-доступ перестал работать?
        if not listings:
            _empty_streak += 1
            if _empty_streak == 3:
                set_setting("last_error", "Гость-доступ к lzt.market не отвечает (3 пустых цикла)")
                with contextlib.suppress(Exception):
                    await bot.send_message(
                        TG_ADMIN_ID,
                        "⚠️ <b>Лолз не отдаёт лоты 3 цикла подряд.</b>\n"
                        "Возможно, Cloudflare усилился или гостевой просмотр закрыли.\n"
                        "Финт: зайди на lzt.market в браузере → F12 → Network → первый запрос → "
                        "Request Headers → скопируй всю строку <code>cookie:</code> → "
                        "добавь в Railway переменную <code>LZT_COOKIE</code> (редеплой не нужен, "
                        "просто сохрани Variables и рестартни сервис).\n"
                        "/status — детали",
                    )
        else:
            _empty_streak = 0

        now = time.time()
        max_age = flt.age_min * 60

        with_date = [l for l in listings if l.start_date and not is_seen(l.item_id)]
        without_date = [l for l in listings if not l.start_date and not is_seen(l.item_id)][:DETAIL_BUDGET]

        bump_stat("stat_scanned", len(listings))
        set_setting("last_ok_at", time.time())
        log.info(
            "Проверка #%d: лотов %d (без даты — обогащу %d), режим: %s",
            int(get_stat("stat_checks")), len(listings), len(without_date),
            "гость" if not LZT_COOKIE else "с cookie",
        )

        sent = 0
        boot_marked = 0
        detail_budget = DETAIL_BUDGET

        for item in with_date + without_date:
            if is_seen(item.item_id):
                continue

            if first_run or not _boot_done:
                mark_seen(item.item_id)
                boot_marked += 1
                continue

            if not item.start_date and detail_budget > 0:
                detail_budget -= 1
                item = await fill_item_details(item)
                await asyncio.sleep(1.5)
                if not item.start_date:
                    mark_seen(item.item_id)
                    log.info("Лот %d: дату не удалось определить — пропускаю", item.item_id)
                    continue

            if not (0 <= now - item.start_date <= max_age):
                mark_seen(item.item_id)
                continue

            if flt.pmax > 0 and (item.price or 0) > flt.pmax:
                mark_seen(item.item_id)
                continue
            if flt.pmin > 0 and (item.price or 0) < flt.pmin:
                mark_seen(item.item_id)
                continue

            if flt.min_level > 0:
                lvl = extract_level(item)
                if lvl is not None and lvl < flt.min_level:
                    mark_seen(item.item_id)
                    log.info("Фильтр: лот %d — уровень %d < %d, пропуск",
                             item.item_id, lvl, flt.min_level)
                    continue

            analysis = await analyze_listing(item, flt)
            if analysis is None:
                continue
            mark_seen(item.item_id)
            bump_stat("stat_analyzed")

            if analysis["matches"]:
                price = item.price or 0
                funpay = analysis.get("funpay_price") or 0
                profit = funpay - price
                roi = (profit / price * 100) if price > 0 else 0
                ok_profit = flt.min_profit <= 0 or profit >= flt.min_profit
                ok_markup = flt.markup_pct <= 0 or roi >= flt.markup_pct
                if ok_profit and ok_markup:
                    age_min = max(0, int((now - item.start_date) // 60))
                    await send_notification(bot, item, analysis, age_min)
                    sent += 1
                    log.info("→ ОКУПАЕМЫЙ лот %d: купить %.0f₽, продать ~%.0f₽, профит %.0f₽ (%.0f%%)",
                             item.item_id, price, funpay, profit, roi)
                else:
                    log.info("Маржа мала: лот %d, профит %.0f₽ (нужно ≥%.0f), наценка %.0f%%",
                             item.item_id, profit, flt.min_profit, roi)

            await asyncio.sleep(1.0)

        if first_run or not _boot_done:
            if boot_marked:
                log.info("Старт: пометил %d текущих лотов как просмотренные", boot_marked)
            _boot_done = True

        return sent


async def poll_loop(bot: Bot) -> None:
    fail_streak = 0
    log.info("Слежу за маркетом как гость (интервал %d сек)…", DEFAULT_POLL)
    first = True
    while True:
        interval = max(60, load_filters().poll_sec)   # гость-режим: не чаще раз в минуту
        try:
            if not load_filters().paused:
                await check_market(bot, first_run=first)
                first = False
                fail_streak = 0
            else:
                log.info("На паузе — пропуск цикла")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            fail_streak += 1
            log.exception("Ошибка цикла #%d", fail_streak)
            set_setting("last_error", f"{type(exc).__name__}: {exc}")
            bump_stat("stat_errors")
            if fail_streak == 3 or fail_streak % 10 == 0:
                with contextlib.suppress(Exception):
                    await bot.send_message(
                        TG_ADMIN_ID,
                        f"⚠️ Бот: {fail_streak} ошибок подряд. Последняя:\n"
                        f"<code>{html.escape(str(exc)[:300])}</code>\n"
                        "Продолжаю попытки. /status — детали.",
                    )
        await asyncio.sleep(interval)


async def heartbeat_loop(bot: Bot) -> None:
    while True:
        hours = load_filters().heartbeat_h
        if hours <= 0:
            await asyncio.sleep(600)
            continue
        await asyncio.sleep(hours * 3600)
        if load_filters().paused:
            continue
        with contextlib.suppress(Exception):
            await bot.send_message(
                TG_ADMIN_ID,
                _status_text() + f"\n\n(авто-статус каждые {hours:g} ч, /heartbeat N — изменить)",
            )

# ---------------------------------------------------------------------------
# Статус
# ---------------------------------------------------------------------------


def _fmt_duration(seconds: float) -> str:
    seconds = int(max(0, seconds))
    d, seconds = divmod(seconds, 86400)
    h, seconds = divmod(seconds, 3600)
    m, s = divmod(seconds, 60)
    if d:
        return f"{d}д {h}ч {m}м"
    if h:
        return f"{h}ч {m}м"
    if m:
        return f"{m}м {s}с"
    return f"{s}с"


def _ago(ts: Optional[float]) -> str:
    if not ts:
        return "никогда"
    return f"{_fmt_duration(time.time() - ts)} назад"


def _status_text() -> str:
    flt = load_filters()
    mgr = gemini_manager()
    last_check = float(get_setting("last_check_at", "0") or 0)
    last_ok = float(get_setting("last_ok_at", "0") or 0)
    last_error = get_setting("last_error", "")
    status = "⏸ пауза" if flt.paused else "✅ работает"
    ok_mark = "✅" if (last_check and last_check == last_ok) else "⚠️"
    mode = "гость (без аккаунта Lolz)" if not LZT_COOKIE else "с cookie"
    lines = [
        "🩺 <b>Статус бота</b>",
        "",
        f"Состояние: {status}",
        f"🕵️ Режим маркета: {mode}",
        f"⏱ Аптайм: {_fmt_duration(time.time() - _started)}",
        f"{ok_mark} Последняя проверка маркета: {_ago(last_check or None)}",
        "",
        f"🔑 <b>Gemini:</b> {mgr.size} ключ(ей), модель <code>{html.escape(current_model())}</code>",
    ]
    lines += mgr.status_lines()
    lines += [
        f"🔁 Переключений ключей (лимиты): {int(get_stat('stat_gemini_rotations'))}",
        "",
        "📊 <b>Счётчики:</b>",
        f"🔁 Проверок: {int(get_stat('stat_checks'))}",
        f"📦 Лотов просканировано: {int(get_stat('stat_scanned'))}",
        f"🤖 Проанализировано ИИ: {int(get_stat('stat_analyzed'))}",
        f"💸 Уведомлений отправлено: {int(get_stat('stat_sent'))}",
        f"⚠️ Ошибок: LZT {int(get_stat('stat_lzt_errors'))} / "
        f"Gemini {int(get_stat('stat_gemini_errors'))} / TG {int(get_stat('stat_tg_errors'))}",
    ]
    if last_error:
        lines += ["", f"Последняя ошибка: <code>{html.escape(last_error[:200])}</code>"]
    lines += ["", "⚙️ Фильтры: /filters • Логи: /logs • Гайд: /guide"]
    return "\n".join(lines)

# ---------------------------------------------------------------------------
# HTTP healthcheck
# ---------------------------------------------------------------------------


async def run_health_server() -> None:
    port = int(os.getenv("PORT", "8080") or 8080)

    async def ok(_: aioweb.Request) -> aioweb.Response:
        return aioweb.Response(text="OK")

    app = aioweb.Application()
    app.router.add_get("/", ok)
    app.router.add_get("/health", ok)

    runner = aioweb.AppRunner(app)
    try:
        await runner.setup()
        site = aioweb.TCPSite(runner, host="0.0.0.0", port=port)
        await site.start()
        log.info("Healthcheck HTTP: 0.0.0.0:%d/health", port)
        await asyncio.Event().wait()
    except OSError as exc:
        log.warning("Healthcheck не поднялся: %s (бот продолжит)", exc)
        await runner.cleanup()
        return
    finally:
        with contextlib.suppress(Exception):
            await runner.cleanup()

# ---------------------------------------------------------------------------
# Тексты: гайд и чек-лист
# ---------------------------------------------------------------------------

GUIDE_TEXT = (
    "💼 <b>ГАЙД: ПЕРЕПРОДАЖА STEAM-АККАУНТОВ</b>\n"
    "(схема: Lolz → обработка → Funpay)\n\n"
    "📈 <b>ШАГ 1. ПОКУПКА на Lolz (lzt.market/steam)</b>\n"
    "• Цена: до 150 ₽ — дороже брать невыгодно\n"
    "• Уровень Steam: 5+ — такие разбирают быстро даже с наценкой\n"
    "• Игры: без лимита — лимитные никому не нужны\n"
    "• Без банов (VAC/Game) — забаненные стоят меньше и висят\n"
    "• Известная игра (GTA 5, CS) — плюс к продаваемости\n"
    "• Часы &gt; 1000 — не трогаем: риск возврата владельцем\n"
    "• Гарантия лота 12/24 ч — за это время можно вернуть деньги\n\n"
    "🔧 <b>ШАГ 2. ОБРАБОТКА</b> — см. /checklist\n\n"
    "💰 <b>ШАГ 3. ПРОДАЖА на Funpay</b>\n"
    "• Объявление с плюсами, скринами, смайликами — выделяемся\n"
    "• Наценка ~70%: купил за 100 ₽ → продаём за ~170 ₽\n"
    "• ⬆️ Буст объявления каждые 4 часа — сильно бустит продажи (/boost)\n\n"
    "🧮 <b>ЭКОНОМИКА</b>\n"
    "Акк за ~100 ₽ → продажа ~170 ₽ → профит ~70 ₽/акк.\n"
    "5 акков/день ≈ 350 ₽/день при стабильной ротации.\n\n"
    "⚠️ <b>Риски:</b> возврат аккаунта владельцем (держи гарантию),\n"
    "конкуренция и авто-байеры, комиссия Funpay при выводе.\n"
    "Начинай с 3–5 акков, отладь процесс, потом масштабируй."
)

CHECKLIST_TEXT = (
    "🔧 <b>ЧЕК-ЛИСТ ОБРАБОТКИ АККАУНТА</b>\n\n"
    "1. Привязать свою почту (сменить email)\n"
    "2. Сменить пароль от Steam\n"
    "3. Сменить аватарку, ник, страну\n"
    "4. В поле имени вписать логин акка — удобно для логов\n"
    "5. Удалить: друзей, комментарии, группы, обзоры\n"
    "6. Проверить акк в течение гарантии (12/24 ч)\n\n"
    "После обработки → выставляй на Funpay (плюсы, скрины, смайлики)\n"
    "и бусть каждые 4 часа (/boost)."
)

# ---------------------------------------------------------------------------
# Telegram-бот
# ---------------------------------------------------------------------------

router = Router()
_boost_task: Optional[asyncio.Task] = None


def _admin(m: Message) -> bool:
    return bool(m.from_user and TG_ADMIN_ID and m.from_user.id == TG_ADMIN_ID)


@router.message(Command("start", "help"))
async def cmd_start(m: Message) -> None:
    if not _admin(m):
        await m.answer("⛔ Бот личный, доступ только для владельца.")
        return
    flt = load_filters()
    mode = "гость (без аккаунта Lolz)" if not LZT_COOKIE else "с cookie"
    await m.answer(
        "💸 <b>Монитор для перепродажи Steam-аккаунтов</b>\n"
        f"Режим маркета: {mode}\n\n"
        f"Лоты ≤ {flt.age_min} мин, профит ≥ {flt.min_profit:.0f} ₽, "
        f"окупаемость ≥ {flt.markup_pct:.0f}%.\n\n"
        "<b>Фильтры:</b>\n"
        "/price 0 150 • /profit 70 • /markup 70 • /level 5 • /age 60\n"
        "/criteria &lt;текст&gt; • /poll 60 • /heartbeat 3 • /model &lt;имя&gt;\n\n"
        "<b>Гайд:</b> /guide • /checklist • /boost\n\n"
        "<b>Мониторинг:</b> /status • /logs • /filters • /pause • /resume • /now"
    )


@router.message(Command("guide"))
async def cmd_guide(m: Message) -> None:
    if not _admin(m):
        return
    await m.answer(GUIDE_TEXT, link_preview_options=LinkPreviewOptions(is_disabled=True))


@router.message(Command("checklist"))
async def cmd_checklist(m: Message) -> None:
    if not _admin(m):
        return
    await m.answer(CHECKLIST_TEXT)


@router.message(Command("model"))
async def cmd_model(m: Message, command: CommandObject) -> None:
    if not _admin(m):
        return
    arg = (command.args or "").strip()
    if not arg:
        await m.answer(
            f"🧠 Модель Gemini: <code>{html.escape(current_model())}</code>\n\n"
            "Дефолт <code>gemini-flash-lite-latest</code> — всегда последняя Flash-Lite.\n"
            "Сменить: <code>/model gemini-3.1-flash-lite</code>"
        )
        return
    set_setting("gemini_model", arg[:100])
    await m.answer(f"✅ Модель: <code>{html.escape(arg[:100])}</code>")


@router.message(Command("boost"))
async def cmd_boost(m: Message, command: CommandObject) -> None:
    global _boost_task
    if not _admin(m):
        return
    arg = (command.args or "").strip()
    hours = 4.0
    if arg:
        try:
            hours = float(arg)
            if not (0.5 <= hours <= 24):
                raise ValueError
        except ValueError:
            await m.answer("Пример: <code>/boost 4</code> — напомню через 4 часа (0.5–24)")
            return

    if _boost_task and not _boost_task.done():
        _boost_task.cancel()

    async def _remind() -> None:
        await asyncio.sleep(hours * 3600)
        with contextlib.suppress(Exception):
            await m.bot.send_message(
                TG_ADMIN_ID,
                "⬆️ <b>Пора бустить объявления на Funpay!</b>\n"
                "Каждые 4 часа — сильно бустит продажи.",
            )

    _boost_task = asyncio.create_task(_remind())
    await m.answer(f"✅ Напомню про буст через {hours:g} ч. Повторный /boost — сбросить.")


@router.message(Command("filters"))
async def cmd_filters(m: Message) -> None:
    if not _admin(m):
        return
    flt = load_filters()
    if flt.pmin <= 0 and flt.pmax <= 0:
        price = "любая"
    elif flt.pmax <= 0:
        price = f"от {flt.pmin:.0f} ₽"
    else:
        price = f"{flt.pmin:.0f}–{flt.pmax:.0f} ₽"
    status = "⏸ пауза" if flt.paused else "✅ активен"
    await m.answer(
        "⚙️ <b>Фильтры</b>\n\n"
        f"💰 Цена покупки: {html.escape(price)}\n"
        f"📈 Мин. профит: {flt.min_profit:.0f} ₽ {'(выкл)' if flt.min_profit <= 0 else ''}\n"
        f"📊 Мин. окупаемость: {flt.markup_pct:.0f}% {'(выкл)' if flt.markup_pct <= 0 else ''}\n"
        f"🎖 Мин. уровень Steam: {flt.min_level}\n"
        f"⏰ Возраст лота: ≤ {flt.age_min} мин\n"
        f"🔁 Опрос маркета: каждые {flt.poll_sec} сек (гость-режим: минимум 60)\n"
        f"💓 Авто-статус: каждые {flt.heartbeat_h:g} ч\n\n"
        "🤖 <b>Критерии ИИ:</b>\n"
        f"{html.escape(flt.criteria)}\n\n"
        f"Статус: {status}"
    )


@router.message(Command("price"))
async def cmd_price(m: Message, command: CommandObject) -> None:
    if not _admin(m):
        return
    args = (command.args or "").split()
    if len(args) != 2 or not all(a.isdigit() for a in args):
        flt = load_filters()
        await m.answer(
            "Пример: <code>/price 0 150</code> — мин. и макс. цена покупки в ₽ (0 = без лимита)\n"
            f"Сейчас: {flt.pmin:.0f}–{flt.pmax:.0f} ₽"
        )
        return
    pmin, pmax = int(args[0]), int(args[1])
    if pmax and pmin > pmax:
        await m.answer("Минимум больше максимума 🤔")
        return
    set_setting("pmin", pmin)
    set_setting("pmax", pmax)
    log.info("Фильтр цены: %d–%d ₽", pmin, pmax)
    await m.answer(f"✅ Цена покупки: {pmin}–{pmax} ₽" + ("" if pmax else " (без верхнего лимита)"))


@router.message(Command("profit"))
async def cmd_profit(m: Message, command: CommandObject) -> None:
    if not _admin(m):
        return
    arg = (command.args or "").strip()
    try:
        value = float(arg)
        if value < 0:
            raise ValueError
    except ValueError:
        await m.answer(
            "Пример: <code>/profit 70</code> — присылать лоты с профитом от 70 ₽ (0 = выкл)\n"
            f"Сейчас: {load_filters().min_profit:.0f} ₽"
        )
        return
    set_setting("min_profit", value)
    await m.answer(f"✅ Минимальная маржа: {value:.0f} ₽" + (" (выкл)" if value == 0 else ""))


@router.message(Command("markup"))
async def cmd_markup(m: Message, command: CommandObject) -> None:
    if not _admin(m):
        return
    arg = (command.args or "").strip()
    try:
        value = float(arg)
        if not (0 <= value <= 1000):
            raise ValueError
    except ValueError:
        await m.answer(
            "Пример: <code>/markup 70</code> — требовать окупаемость от 70% (0 = выкл)\n"
            f"Сейчас: {load_filters().markup_pct:.0f}%"
        )
        return
    set_setting("markup_pct", value)
    await m.answer(f"✅ Минимальная окупаемость: {value:.0f}%" + (" (выкл)" if value == 0 else ""))


@router.message(Command("level"))
async def cmd_level(m: Message, command: CommandObject) -> None:
    if not _admin(m):
        return
    arg = (command.args or "").strip()
    if not arg.isdigit() or not (0 <= int(arg) <= 500):
        await m.answer(
            "Пример: <code>/level 5</code> — мин. уровень Steam (0 = выкл)\n"
            f"Сейчас: {load_filters().min_level}"
        )
        return
    set_setting("min_level", int(arg))
    await m.answer(f"✅ Минимальный уровень Steam: {int(arg)}" + (" (выкл)" if int(arg) == 0 else ""))


@router.message(Command("age"))
async def cmd_age(m: Message, command: CommandObject) -> None:
    if not _admin(m):
        return
    arg = (command.args or "").strip()
    if not arg.isdigit() or not (1 <= int(arg) <= 1440):
        await m.answer(
            f"Пример: <code>/age 60</code> — учитывать лоты не старше 60 минут\n"
            f"Сейчас: {load_filters().age_min} мин"
        )
        return
    set_setting("age_min", int(arg))
    await m.answer(f"✅ Лоты не старше {int(arg)} мин")


@router.message(Command("criteria"))
async def cmd_criteria(m: Message, command: CommandObject) -> None:
    if not _admin(m):
        return
    text = (command.args or "").strip()
    if not text:
        current = load_filters().criteria or DEFAULT_CRITERIA
        await m.answer(
            "🤖 Текущие критерии ИИ:\n\n"
            f"{html.escape(current)}\n\n"
            "Изменить: <code>/criteria уровень 5+, без VAC, GTA 5 или CS2, почта в комплекте</code>"
        )
        return
    set_setting("criteria", text[:2000])
    await m.answer("✅ Критерии обновлены:\n\n" + html.escape(text[:2000]))


@router.message(Command("poll"))
async def cmd_poll(m: Message, command: CommandObject) -> None:
    if not _admin(m):
        return
    arg = (command.args or "").strip()
    if not arg.isdigit() or not (60 <= int(arg) <= 3600):
        await m.answer(
            "Пример: <code>/poll 60</code> — интервал опроса, сек (в гость-режиме минимум 60)\n"
            f"Сейчас: {load_filters().poll_sec} сек"
        )
        return
    set_setting("poll_sec", int(arg))
    await m.answer(f"✅ Опрос маркета каждые {int(arg)} сек")


@router.message(Command("heartbeat"))
async def cmd_heartbeat(m: Message, command: CommandObject) -> None:
    if not _admin(m):
        return
    arg = (command.args or "").strip()
    try:
        value = float(arg)
        if not (0 <= value <= 48):
            raise ValueError
    except ValueError:
        await m.answer(
            "Пример: <code>/heartbeat 3</code> — авто-статус каждые 3 ч (0 = выкл)\n"
            f"Сейчас: каждые {load_filters().heartbeat_h:g} ч"
        )
        return
    set_setting("heartbeat_h", value)
    await m.answer(f"✅ Авто-статус каждые {value:g} ч" if value > 0 else "✅ Авто-статус выключен")


@router.message(Command("status"))
async def cmd_status(m: Message) -> None:
    if not _admin(m):
        return
    await m.answer(_status_text())


@router.message(Command("logs"))
async def cmd_logs(m: Message) -> None:
    if not _admin(m):
        return
    if not _recent_logs:
        await m.answer("Логов пока нет 🤔")
        return
    tail = list(_recent_logs)[-20:]
    await m.answer("<code>" + html.escape("\n".join(tail)) + "</code>")


@router.message(Command("pause"))
async def cmd_pause(m: Message) -> None:
    if not _admin(m):
        return
    set_setting("paused", "1")
    log.info("Пауза")
    await m.answer("⏸ На паузе. /resume — продолжить.")


@router.message(Command("resume"))
async def cmd_resume(m: Message) -> None:
    if not _admin(m):
        return
    set_setting("paused", "0")
    log.info("Снята пауза")
    await m.answer("▶️ Продолжаю.")


@router.message(Command("now"))
async def cmd_now(m: Message) -> None:
    if not _admin(m):
        return
    await m.answer("🔎 Проверяю маркет…")
    try:
        sent = await check_market(m.bot)
    except Exception as exc:
        log.exception("Ручная проверка не удалась")
        await m.answer("❌ Ошибка: " + html.escape(str(exc)))
        return
    await m.answer("🤷 Новых окупаемых лотов нет" if not sent else f"✅ Прислано лотов: {sent}")

# ---------------------------------------------------------------------------
# Запуск
# ---------------------------------------------------------------------------


async def main() -> None:
    missing = [
        name
        for name, value in (
            ("TG_TOKEN", TG_TOKEN),
            ("TG_ADMIN_ID", str(TG_ADMIN_ID) if TG_ADMIN_ID else ""),
        )
        if not value
    ]
    mgr = gemini_manager()
    if mgr.size == 0:
        missing.append("GEMINI_API_KEY_1..4 (хотя бы один)")
    if missing:
        raise SystemExit(f"❗ Задай переменные: {', '.join(missing)}")

    mode = "гость (без аккаунта Lolz)" if not LZT_COOKIE else "с cookie"
    log.info("Старт. База: %s | Режим маркета: %s", DB_PATH, mode)
    log.info("Gemini: %d ключ(ей), модель: %s", mgr.size, current_model())
    flt = load_filters()
    log.info(
        "Фильтры: цена ≤%.0f₽, уровень %d+, профит ≥%.0f₽, окупаемость ≥%.0f%%, лоты ≤%d мин",
        flt.pmax, flt.min_level, flt.min_profit, flt.markup_pct, flt.age_min,
    )

    bot = Bot(TG_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.include_router(router)

    global _http
    async with aiohttp.ClientSession() as session:
        _http = session

        web_task: Optional[asyncio.Task] = None
        if os.getenv("PORT"):
            web_task = asyncio.create_task(run_health_server())

        with contextlib.suppress(Exception):
            await bot.delete_webhook(drop_pending_updates=True)

        with contextlib.suppress(Exception):
            await bot.send_message(
                TG_ADMIN_ID,
                "🚀 <b>Бот запущен.</b>\n\n"
                f"Режим маркета: {mode}\n"
                f"Профит ≥ {flt.min_profit:.0f} ₽, окупаемость ≥ {flt.markup_pct:.0f}%, "
                f"лоты ≤ {flt.age_min} мин.\n"
                "Гайд: /guide • Статус: /status",
            )

        poller = asyncio.create_task(poll_loop(bot))
        heart = asyncio.create_task(heartbeat_loop(bot))

        try:
            await dp.start_polling(bot)
        finally:
            for task in (t for t in (poller, heart, web_task) if t):
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            with contextlib.suppress(Exception):
                await bot.send_message(TG_ADMIN_ID, "🛑 Бот остановлен.")
            log.info("Остановлено.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("\nОстановлено.")
