import os
import re
import math
import sqlite3
import asyncio
import hmac
import hashlib
import time
import copy
import unicodedata
import difflib
import uuid
from collections import Counter, defaultdict, OrderedDict
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Dict, List, Set, Tuple

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"), override=True)

app = FastAPI(title="Topical Map Generator", version="1.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def no_cache_middleware(request: Request, call_next):
    response = await call_next(request)
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

STATIC_DIR = os.path.join(BASE_DIR, "static")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

DATAFORSEO_LOCATION_CODE = int(os.getenv("DATAFORSEO_LOCATION_CODE", "2616"))
DATAFORSEO_LANGUAGE_CODE = os.getenv("DATAFORSEO_LANGUAGE_CODE", "pl")
DATAFORSEO_MAX_KEYWORDS = int(os.getenv("DATAFORSEO_MAX_KEYWORDS", "1000"))
MAX_FREE_SEARCHES = int(os.getenv("MAX_FREE_SEARCHES", "5"))
FREE_SEARCH_WINDOW_HOURS = 24
LIMIT_BYPASS_IPS = {
    ip.strip()
    for ip in os.getenv("LIMIT_BYPASS_IPS", "").split(",")
    if ip.strip()
}
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "").strip()
LOG_DB_PATH = os.getenv("LOG_DB_PATH", os.path.join(BASE_DIR, "search_logs.db"))
COOKIE_NAME = "tm_client_id"
ADMIN_AUTH_COOKIE_NAME = "tm_admin_auth"
ADMIN_AUTH_COOKIE_MAX_AGE = int(os.getenv("ADMIN_AUTH_COOKIE_MAX_AGE", str(60 * 60 * 12)))
ADMIN_SESSION_SECRET = os.getenv("ADMIN_SESSION_SECRET", "tm-admin-session-secret-change-me")
COOKIE_SECURE = os.getenv("COOKIE_SECURE", "false").strip().lower() in {"1", "true", "yes", "on"}

DEFAULT_TARGET_KEYWORDS = int(os.getenv("DEFAULT_TARGET_KEYWORDS", "300"))
MAX_TARGET_KEYWORDS = int(os.getenv("MAX_TARGET_KEYWORDS", "500"))
MAX_CLUSTERS = int(os.getenv("MAX_CLUSTERS", "22"))
MIN_CLUSTER_KEYWORDS = int(os.getenv("MIN_CLUSTER_KEYWORDS", "3"))
MIN_CLUSTER_TOTAL_VOLUME = int(os.getenv("MIN_CLUSTER_TOTAL_VOLUME", "5000"))
MAP_CACHE_TTL_SECONDS = int(os.getenv("MAP_CACHE_TTL_SECONDS", "1200"))
MAP_CACHE_MAX_ENTRIES = int(os.getenv("MAP_CACHE_MAX_ENTRIES", "200"))

DATAFORSEO_IDEAS_ENDPOINT = "https://api.dataforseo.com/v3/dataforseo_labs/google/keyword_ideas/live"
DATAFORSEO_SUGGESTIONS_ENDPOINT = "https://api.dataforseo.com/v3/dataforseo_labs/google/keyword_suggestions/live"

STOPWORDS = {
    "a", "aby", "albo", "ale", "bo", "by", "byc", "co", "czy", "dla", "do", "i",
    "ich", "jak", "jako", "jest", "jego", "jej", "juz", "lub", "ma", "na", "nad", "nie",
    "o", "od", "oraz", "po", "pod", "przez", "sie", "sa", "te", "to", "u", "w",
    "we", "z", "za", "ze", "the", "and", "of", "for", "in", "on", "with"
}
NOISE_TOKENS = {"film", "filmweb", "obsada", "wikipedia", "synonim"}
OFF_TOPIC_NOISE_TOKENS = {
    "gta", "minecraft", "pokemon", "fallout", "filmweb", "wikipedia", "synonim",
    "obsada", "cda", "youtube", "tiktok", "instagram",
}
PERSONA_PREFIX_TOKENS = {"pan", "pani", "dr", "doktor", "prof", "blog"}
GAMBLING_BRANDS = {
    "fortuna", "sts", "superbet", "totolotek", "betclic", "forbet", "lvbet",
    "etoto", "vulkan", "noblebet", "fuksiarz", "betfan", "comeon", "vavada",
    "mostbet", "pzbuk", "ggbet", "mrbet", "ice", "vegas", "efortuna"
}
SPORTSBOOK_EXPANSION_SEEDS = [
    "bonus bukmacherski",
    "typy bukmacherskie",
    "kursy bukmacherskie",
    "promocje bukmacherskie",
    "legalny bukmacher",
    "ranking bukmacherow",
    "aplikacja bukmacherska",
    "podatek bukmacherski",
    "zaklady live",
    "zaklady na zywo",
]
CASINO_EXPANSION_SEEDS = [
    "kasyno online",
    "kasyno internetowe",
    "bonus bez depozytu kasyno",
    "automaty kasyno online",
    "sloty kasyno",
    "free spiny kasyno",
    "ruletka online kasyno",
    "blackjack online kasyno",
    "poker online kasyno",
    "legalne kasyno",
]
BETTING_SIGNAL_PREFIXES = (
    "bukmach", "kurs", "typ", "kupon", "handicap", "fortuna", "sts", "superbet",
    "betclic", "forbet", "lvbet", "totolotek", "etoto", "legaln", "podat",
    "promoc", "bonus", "live", "zwyr", "akumul", "over", "under", "gole", "gol",
    "mecz", "liga", "ranking", "operator",
)
BETTING_ANCHOR_PREFIXES = ("zaklad", "bukmach", "kupon", "kurs", "typ", "bet", "handicap")
CASINO_SIGNAL_PREFIXES = (
    "kasyn", "slot", "rulet", "blackjack", "poker", "jackpot", "depozyt",
    "spiny", "spin", "hazard", "automat", "casino",
)
SPORTSBOOK_NOISE_PREFIXES = (
    "bukmach", "kurs", "typ", "kupon", "handicap", "mecz", "liga",
    "zaklady", "zaklad", "fortuna", "sts", "superbet", "betclic",
)
SPORTSWEAR_NOISE_PREFIXES = (
    "but", "obuw", "koszulk", "skarpet", "ubran", "spodni", "kurtk", "plecak",
    "damsk", "mesk", "dziewczyn", "dziec", "szkol", "lice", "talent", "krzyzowk",
    "bluz", "czapk", "odziez", "komplet",
)
GENERIC_SEED_TOKENS = {
    "ranking", "opinie", "opinia", "cena", "ceny", "online", "darmowe",
    "najlepsze", "najlepszy", "top", "lista", "porownanie",
}
TOPICAL_MAP_CACHE: OrderedDict[str, Dict[str, object]] = OrderedDict()
TOPICAL_MAP_CACHE_LOCK = Lock()


def init_db() -> None:
    db_dir = os.path.dirname(LOG_DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    with sqlite3.connect(LOG_DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS search_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                client_id TEXT NOT NULL,
                ip TEXT NOT NULL,
                user_agent TEXT,
                input_seed TEXT NOT NULL,
                resolved_seed TEXT,
                target_keywords INTEGER,
                min_volume INTEGER,
                strict_relevance INTEGER,
                include_brands INTEGER,
                status TEXT NOT NULL,
                http_status INTEGER,
                keywords_after_filter INTEGER,
                nodes_count INTEGER,
                links_count INTEGER,
                pillars_count INTEGER,
                error_detail TEXT
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_logs_client_id ON search_logs(client_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_logs_created_at ON search_logs(created_at)")
        conn.commit()


def get_client_ip(request: Request) -> str:
    forwarded_for = request.headers.get("x-forwarded-for", "").strip()
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()

    real_ip = request.headers.get("x-real-ip", "").strip()
    if real_ip:
        return real_ip

    return request.client.host if request.client else "unknown"


def is_limit_bypass_ip(client_ip: str) -> bool:
    return bool(client_ip and client_ip in LIMIT_BYPASS_IPS)


def get_or_create_client_id(request: Request, response: Response) -> str:
    existing = (request.cookies.get(COOKIE_NAME) or "").strip()
    if existing and re.fullmatch(r"[a-f0-9-]{16,64}", existing):
        return existing

    client_id = str(uuid.uuid4())
    response.set_cookie(
        key=COOKIE_NAME,
        value=client_id,
        max_age=60 * 60 * FREE_SEARCH_WINDOW_HOURS,
        httponly=True,
        samesite="lax",
        secure=COOKIE_SECURE,
        path="/",
    )
    return client_id


def count_consumed_searches(client_id: str) -> int:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=FREE_SEARCH_WINDOW_HOURS)).isoformat()
    with sqlite3.connect(LOG_DB_PATH) as conn:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM search_logs
            WHERE client_id = ?
              AND status IN ('ok', 'empty')
              AND created_at >= ?
            """,
            (client_id, cutoff),
        ).fetchone()
    return int(row[0] if row else 0)


def log_search_event(
    *,
    client_id: str,
    ip: str,
    user_agent: str,
    input_seed: str,
    resolved_seed: str | None,
    target_keywords: int,
    min_volume: int,
    strict_relevance: bool,
    include_brands: bool,
    status: str,
    http_status: int,
    keywords_after_filter: int = 0,
    nodes_count: int = 0,
    links_count: int = 0,
    pillars_count: int = 0,
    error_detail: str = "",
) -> None:
    created_at = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(LOG_DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO search_logs (
                created_at, client_id, ip, user_agent, input_seed, resolved_seed,
                target_keywords, min_volume, strict_relevance, include_brands, status,
                http_status, keywords_after_filter, nodes_count, links_count, pillars_count,
                error_detail
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                created_at,
                client_id,
                ip,
                user_agent,
                input_seed,
                resolved_seed,
                target_keywords,
                min_volume,
                int(strict_relevance),
                int(include_brands),
                status,
                http_status,
                keywords_after_filter,
                nodes_count,
                links_count,
                pillars_count,
                error_detail[:400],
            ),
        )
        conn.commit()


def get_recent_logs(limit: int = 200) -> List[Dict[str, object]]:
    with sqlite3.connect(LOG_DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT created_at, client_id, ip, input_seed, resolved_seed, target_keywords,
                   min_volume, strict_relevance, include_brands, status, http_status,
                   keywords_after_filter, nodes_count, links_count, pillars_count, error_detail
            FROM search_logs
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def get_client_summary(limit: int = 200) -> List[Dict[str, object]]:
    with sqlite3.connect(LOG_DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT
                client_id,
                MAX(ip) AS ip,
                MIN(created_at) AS first_seen,
                MAX(created_at) AS last_seen,
                COUNT(*) AS total_events,
                SUM(CASE WHEN status = 'ok' THEN 1 ELSE 0 END) AS ok_events,
                SUM(CASE WHEN status = 'empty' THEN 1 ELSE 0 END) AS empty_events,
                SUM(CASE WHEN status = 'blocked_limit' THEN 1 ELSE 0 END) AS blocked_events,
                SUM(CASE WHEN status IN ('ok', 'empty') THEN 1 ELSE 0 END) AS consumed_checks,
                GROUP_CONCAT(DISTINCT input_seed) AS searched_seeds
            FROM search_logs
            GROUP BY client_id
            ORDER BY last_seen DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    result = [dict(row) for row in rows]
    for row in result:
        seeds = row.get("searched_seeds") or ""
        row["searched_seeds"] = [x.strip() for x in seeds.split(",") if x and x.strip()][:30]
    return result


def build_admin_auth_token() -> str:
    return hmac.new(
        ADMIN_SESSION_SECRET.encode("utf-8"),
        ADMIN_API_KEY.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def is_admin_authenticated(request: Request, provided_key: str | None = None) -> bool:
    if not ADMIN_API_KEY:
        return False

    if provided_key and hmac.compare_digest(provided_key, ADMIN_API_KEY):
        return True

    header_key = request.headers.get("x-admin-key", "").strip()
    if header_key and hmac.compare_digest(header_key, ADMIN_API_KEY):
        return True

    cookie_token = (request.cookies.get(ADMIN_AUTH_COOKIE_NAME) or "").strip()
    if not cookie_token:
        return False

    return hmac.compare_digest(cookie_token, build_admin_auth_token())


def set_admin_auth_cookie(response: Response) -> None:
    response.set_cookie(
        key=ADMIN_AUTH_COOKIE_NAME,
        value=build_admin_auth_token(),
        max_age=ADMIN_AUTH_COOKIE_MAX_AGE,
        httponly=True,
        samesite="lax",
        secure=COOKIE_SECURE,
        path="/",
    )


def clear_admin_auth_cookie(response: Response) -> None:
    response.delete_cookie(key=ADMIN_AUTH_COOKIE_NAME, path="/")


def build_map_cache_key(
    seed: str,
    target_keywords: int,
    strict_relevance: bool,
    min_volume: int,
    include_brands: bool,
) -> str:
    normalized_seed = ascii_fold(seed.lower()).strip()
    raw = f"{normalized_seed}|{target_keywords}|{int(strict_relevance)}|{min_volume}|{int(include_brands)}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def get_cached_topical_map(cache_key: str) -> Dict[str, object] | None:
    if MAP_CACHE_TTL_SECONDS <= 0 or MAP_CACHE_MAX_ENTRIES <= 0:
        return None

    now = time.time()
    with TOPICAL_MAP_CACHE_LOCK:
        item = TOPICAL_MAP_CACHE.get(cache_key)
        if not item:
            return None

        created_at = float(item.get("created_at", 0.0))
        if now - created_at > MAP_CACHE_TTL_SECONDS:
            TOPICAL_MAP_CACHE.pop(cache_key, None)
            return None

        TOPICAL_MAP_CACHE.move_to_end(cache_key)
        return copy.deepcopy(item.get("payload"))


def set_cached_topical_map(cache_key: str, payload: Dict[str, object]) -> None:
    if MAP_CACHE_TTL_SECONDS <= 0 or MAP_CACHE_MAX_ENTRIES <= 0:
        return

    with TOPICAL_MAP_CACHE_LOCK:
        TOPICAL_MAP_CACHE[cache_key] = {
            "created_at": time.time(),
            "payload": copy.deepcopy(payload),
        }
        TOPICAL_MAP_CACHE.move_to_end(cache_key)
        while len(TOPICAL_MAP_CACHE) > MAP_CACHE_MAX_ENTRIES:
            TOPICAL_MAP_CACHE.popitem(last=False)


init_db()


@app.get("/")
def serve_index() -> FileResponse:
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


@app.get("/admin")
def serve_admin(request: Request) -> FileResponse:
    if not is_admin_authenticated(request):
        return FileResponse(os.path.join(STATIC_DIR, "admin-login.html"))
    return FileResponse(os.path.join(STATIC_DIR, "admin.html"))


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/api/admin/search-logs")
def admin_search_logs(
    request: Request,
    limit: int = Query(200, ge=1, le=1000),
    client_limit: int = Query(200, ge=1, le=1000),
    admin_key: str | None = Query(None),
) -> Dict[str, object]:
    if not ADMIN_API_KEY:
        raise HTTPException(status_code=500, detail="Missing ADMIN_API_KEY in .env")

    if not is_admin_authenticated(request, admin_key):
        raise HTTPException(status_code=401, detail="Unauthorized")

    return {
        "max_free_searches": MAX_FREE_SEARCHES,
        "window_hours": FREE_SEARCH_WINDOW_HOURS,
        "admin_key_configured": True,
        "recent_events": get_recent_logs(limit),
        "clients": get_client_summary(client_limit),
    }


@app.post("/api/admin/login")
async def admin_login(request: Request, response: Response) -> Dict[str, object]:
    if not ADMIN_API_KEY:
        raise HTTPException(status_code=500, detail="Missing ADMIN_API_KEY in .env")

    body = await request.json()
    provided_password = str(body.get("password") or "").strip()
    if not provided_password or not hmac.compare_digest(provided_password, ADMIN_API_KEY):
        raise HTTPException(status_code=401, detail="Niepoprawne haslo.")

    set_admin_auth_cookie(response)
    return {"ok": True}


@app.post("/api/admin/logout")
def admin_logout(response: Response) -> Dict[str, object]:
    clear_admin_auth_cookie(response)
    return {"ok": True}


@app.get("/api/topical-map")
async def topical_map(
    request: Request,
    response: Response,
    seed: str = Query(..., min_length=2, max_length=100),
    target_keywords: int = Query(DEFAULT_TARGET_KEYWORDS, ge=20, le=MAX_TARGET_KEYWORDS),
    strict_relevance: bool = Query(True),
    min_volume: int = Query(20, ge=0, le=10000),
    include_brands: bool = Query(False),
    admin_key: str | None = Query(None),
) -> Dict[str, object]:
    client_id = get_or_create_client_id(request, response)
    client_ip = get_client_ip(request)
    user_agent = request.headers.get("user-agent", "")
    consumed_before = count_consumed_searches(client_id)
    admin_bypass = is_admin_authenticated(request, admin_key)
    ip_bypass = is_limit_bypass_ip(client_ip)
    cache_key = build_map_cache_key(seed, target_keywords, strict_relevance, min_volume, include_brands)

    if consumed_before >= MAX_FREE_SEARCHES and not admin_bypass and not ip_bypass:
        detail = (
            f"Limit darmowych generacji wykorzystany ({MAX_FREE_SEARCHES}/{MAX_FREE_SEARCHES}) "
            f"w ciagu {FREE_SEARCH_WINDOW_HOURS}h."
        )
        log_search_event(
            client_id=client_id,
            ip=client_ip,
            user_agent=user_agent,
            input_seed=seed,
            resolved_seed=None,
            target_keywords=target_keywords,
            min_volume=min_volume,
            strict_relevance=strict_relevance,
            include_brands=include_brands,
            status="blocked_limit",
            http_status=429,
            error_detail=detail,
        )
        raise HTTPException(status_code=429, detail=detail)

    cached_payload = get_cached_topical_map(cache_key)
    if cached_payload:
        cached_meta = dict(cached_payload.get("meta") or {})
        resolved_seed = cached_meta.get("resolved_seed")
        nodes = list(cached_payload.get("nodes") or [])
        links = list(cached_payload.get("links") or [])
        pillars = list(cached_payload.get("pillars") or [])

        consumed_after = consumed_before if (admin_bypass or ip_bypass) else consumed_before + 1
        remaining_attempts = max(0, MAX_FREE_SEARCHES - consumed_after)

        log_search_event(
            client_id=client_id,
            ip=client_ip,
            user_agent=user_agent,
            input_seed=seed,
            resolved_seed=resolved_seed,
            target_keywords=target_keywords,
            min_volume=min_volume,
            strict_relevance=strict_relevance,
            include_brands=include_brands,
            status="ok_admin" if admin_bypass else ("ok_ip_bypass" if ip_bypass else "ok"),
            http_status=200,
            keywords_after_filter=int(cached_meta.get("keywords_after_filter") or 0),
            nodes_count=len(nodes),
            links_count=len(links),
            pillars_count=len(pillars),
            error_detail="cache_hit",
        )

        cached_meta.update({
            "input_seed": seed,
            "strict_relevance": strict_relevance,
            "min_volume": min_volume,
            "include_brands": include_brands,
            "admin_bypass": admin_bypass,
            "ip_limit_bypass": ip_bypass,
            "remaining_attempts": remaining_attempts,
            "max_free_searches": MAX_FREE_SEARCHES,
            "used_attempts": consumed_after,
            "window_hours": FREE_SEARCH_WINDOW_HOURS,
            "from_cache": True,
        })

        return {
            "nodes": nodes,
            "links": links,
            "pillars": pillars,
            "meta": cached_meta,
        }

    login, password = get_dataforseo_credentials()
    if not login or not password:
        detail = "Missing DataForSEO credentials. Set DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD in .env"
        log_search_event(
            client_id=client_id,
            ip=client_ip,
            user_agent=user_agent,
            input_seed=seed,
            resolved_seed=None,
            target_keywords=target_keywords,
            min_volume=min_volume,
            strict_relevance=strict_relevance,
            include_brands=include_brands,
            status="config_error",
            http_status=500,
            error_detail=detail,
        )
        raise HTTPException(
            status_code=500,
            detail=detail,
        )

    auto_relaxed = False
    used_strict_relevance = strict_relevance
    used_min_volume = min_volume

    try:
        keyword_rows, resolved_seed = await fetch_keywords_from_dataforseo(
            seed, login, password, target_keywords, strict_relevance, min_volume
        )
        if not keyword_rows and (strict_relevance or min_volume > 0):
            keyword_rows, resolved_seed = await fetch_keywords_from_dataforseo(
                seed, login, password, target_keywords, False, 0
            )
            if keyword_rows:
                auto_relaxed = True
                used_strict_relevance = False
                used_min_volume = 0
    except HTTPException as exc:
        log_search_event(
            client_id=client_id,
            ip=client_ip,
            user_agent=user_agent,
            input_seed=seed,
            resolved_seed=None,
            target_keywords=target_keywords,
            min_volume=min_volume,
            strict_relevance=strict_relevance,
            include_brands=include_brands,
            status="dataforseo_error",
            http_status=exc.status_code,
            error_detail=str(exc.detail),
        )
        raise
    except Exception as exc:
        log_search_event(
            client_id=client_id,
            ip=client_ip,
            user_agent=user_agent,
            input_seed=seed,
            resolved_seed=None,
            target_keywords=target_keywords,
            min_volume=min_volume,
            strict_relevance=strict_relevance,
            include_brands=include_brands,
            status="server_error",
            http_status=500,
            error_detail=str(exc),
        )
        raise HTTPException(status_code=500, detail="Internal server error")

    if not keyword_rows:
        detail = "No related keywords found for this seed keyword."
        log_search_event(
            client_id=client_id,
            ip=client_ip,
            user_agent=user_agent,
            input_seed=seed,
            resolved_seed=resolved_seed,
            target_keywords=target_keywords,
            min_volume=min_volume,
            strict_relevance=strict_relevance,
            include_brands=include_brands,
            status="empty",
            http_status=404,
            error_detail=detail,
        )
        raise HTTPException(status_code=404, detail=detail)

    map_seed = resolved_seed or seed
    nodes, links, pillars = build_topical_map(map_seed, keyword_rows, target_keywords, include_brands)
    if not pillars and not auto_relaxed and (strict_relevance or min_volume > 0):
        fallback_rows, fallback_resolved_seed = await fetch_keywords_from_dataforseo(
            seed, login, password, target_keywords, False, 0
        )
        if fallback_rows:
            keyword_rows = fallback_rows
            resolved_seed = fallback_resolved_seed
            map_seed = resolved_seed or seed
            nodes, links, pillars = build_topical_map(map_seed, keyword_rows, target_keywords, include_brands)
            auto_relaxed = True
            used_strict_relevance = False
            used_min_volume = 0

    consumed_after = consumed_before if (admin_bypass or ip_bypass) else consumed_before + 1
    remaining_attempts = max(0, MAX_FREE_SEARCHES - consumed_after)

    log_search_event(
        client_id=client_id,
        ip=client_ip,
        user_agent=user_agent,
        input_seed=seed,
        resolved_seed=resolved_seed,
        target_keywords=target_keywords,
        min_volume=min_volume,
        strict_relevance=strict_relevance,
        include_brands=include_brands,
        status="ok_admin" if admin_bypass else ("ok_ip_bypass" if ip_bypass else "ok"),
        http_status=200,
        keywords_after_filter=len(keyword_rows),
        nodes_count=len(nodes),
        links_count=len(links),
        pillars_count=len(pillars),
    )

    meta_static = {
        "seed": map_seed,
        "resolved_seed": resolved_seed,
        "target_keywords": target_keywords,
        "strict_relevance": strict_relevance,
        "min_volume": min_volume,
        "include_brands": include_brands,
        "keywords_after_filter": len(keyword_rows),
        "auto_relaxed": auto_relaxed,
        "used_strict_relevance": used_strict_relevance,
        "used_min_volume": used_min_volume,
    }

    set_cached_topical_map(
        cache_key,
        {
            "nodes": nodes,
            "links": links,
            "pillars": pillars,
            "meta": meta_static,
        },
    )

    return {
        "nodes": nodes,
        "links": links,
        "pillars": pillars,
        "meta": {
            **meta_static,
            "input_seed": seed,
            "admin_bypass": admin_bypass,
            "ip_limit_bypass": ip_bypass,
            "remaining_attempts": remaining_attempts,
            "max_free_searches": MAX_FREE_SEARCHES,
            "used_attempts": consumed_after,
            "window_hours": FREE_SEARCH_WINDOW_HOURS,
            "from_cache": False,
        },
    }

def get_dataforseo_credentials() -> Tuple[str, str]:
    load_dotenv(os.path.join(BASE_DIR, ".env"), override=True)
    return os.getenv("DATAFORSEO_LOGIN", ""), os.getenv("DATAFORSEO_PASSWORD", "")


async def fetch_keywords_from_dataforseo(
    seed: str,
    login: str,
    password: str,
    target_keywords: int,
    strict_relevance: bool,
    min_volume: int,
) -> Tuple[List[Dict], str | None]:
    request_limit = min(DATAFORSEO_MAX_KEYWORDS, max(120, target_keywords + 120))
    active_seed = seed
    seed_tokens = set(tokenize(active_seed))
    seed_like = derive_seed_like_pattern(seed_tokens)

    # Suggestions are usually more on-topic for topical maps.
    primary = await query_keyword_suggestions(
        active_seed,
        request_limit,
        login,
        password,
        keyword_like=seed_like if strict_relevance else None,
    )

    # Autocorrect fallback for typoed seeds.
    if strict_relevance and not primary:
        broad_ideas = await query_keyword_ideas([active_seed], request_limit, login, password, keyword_like=None)
        corrected = infer_corrected_seed(active_seed, broad_ideas)
        if corrected:
            active_seed = corrected
            seed_tokens = set(tokenize(active_seed))
            seed_like = derive_seed_like_pattern(seed_tokens)
            primary = await query_keyword_suggestions(
                active_seed,
                request_limit,
                login,
                password,
                keyword_like=seed_like if strict_relevance else None,
            )

    merged = dedupe_rows(primary)

    # Use ideas as fallback/complement only when suggestions are clearly too sparse.
    ideas_threshold = min(target_keywords, 180)
    if len(merged) < ideas_threshold:
        ideas = await query_keyword_ideas(
            [active_seed],
            request_limit,
            login,
            password,
            keyword_like=seed_like if strict_relevance else None,
        )
        merged = dedupe_rows(merged + ideas)

    seed_categories = derive_seed_categories(merged, active_seed, seed_tokens)
    merged = filter_rows_by_relevance(merged, seed_tokens, seed_categories, min_volume)

    # Industry boost only when niche data is still sparse after initial filtering.
    if is_gambling_seed(seed_tokens):
        expansion_threshold = min(120, max(40, target_keywords // 3))
        if len(merged) < expansion_threshold:
            boosted = await fetch_industry_expansion_rows(
                seed_tokens,
                request_limit,
                login,
                password,
                strict_relevance,
            )
            if boosted:
                merged = dedupe_rows(merged + boosted)
                seed_categories = derive_seed_categories(merged, active_seed, seed_tokens)
                merged = filter_rows_by_relevance(merged, seed_tokens, seed_categories, min_volume)

    # Expansion pass only in broad mode; strict mode keeps precision.
    if not strict_relevance and len(merged) < ideas_threshold:
        expansion_seeds = select_expansion_seeds(primary, active_seed, seed_tokens, 16, min_volume)
        if expansion_seeds:
            secondary = await query_keyword_ideas(
                expansion_seeds,
                request_limit,
                login,
                password,
                keyword_like=seed_like if strict_relevance else None,
            )
            merged = dedupe_rows(merged + secondary)
            seed_categories = derive_seed_categories(merged, active_seed, seed_tokens)
            merged = filter_rows_by_relevance(merged, seed_tokens, seed_categories, min_volume)

    seed_lc = active_seed.lower()
    cleaned = [row for row in merged if row["keyword"].lower() != seed_lc]
    resolved_seed = active_seed if active_seed.lower() != seed.lower() else None
    return sorted(cleaned, key=lambda x: x["volume"], reverse=True), resolved_seed

async def query_keyword_ideas(
    keywords: List[str],
    limit: int,
    login: str,
    password: str,
    keyword_like: str | None = None,
) -> List[Dict]:
    payload_item = {
        "keywords": keywords,
        "location_code": DATAFORSEO_LOCATION_CODE,
        "language_code": DATAFORSEO_LANGUAGE_CODE,
        "include_seed_keyword": True,
        "limit": limit,
    }
    if keyword_like:
        payload_item["filters"] = [["keyword", "like", f"%{keyword_like}%"]]
    payload = [payload_item]

    timeout = httpx.Timeout(45.0, connect=12.0)
    try:
        async with httpx.AsyncClient(timeout=timeout, auth=(login, password)) as client:
            response = await client.post(DATAFORSEO_IDEAS_ENDPOINT, json=payload)
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"DataForSEO network error (ideas): {str(exc) or 'request failed'}")

    if response.status_code != 200:
        raise HTTPException(status_code=502, detail=f"DataForSEO request failed ({response.status_code})")

    body = response.json()
    if body.get("status_code") != 20000:
        raise HTTPException(status_code=502, detail=f"DataForSEO error: {body.get('status_message', 'Unknown error')}")

    tasks = body.get("tasks") or []
    for task in tasks:
        if task.get("status_code") != 20000:
            raise HTTPException(
                status_code=502,
                detail=f"DataForSEO task error: {task.get('status_message', 'Unknown task error')}",
            )

    items = []
    for task in tasks:
        for result in task.get("result") or []:
            items.extend(result.get("items") or [])

    parsed_items = []
    for item in items:
        keyword = sanitize_keyword_text((item.get("keyword") or "").strip())
        if not keyword:
            continue

        info = item.get("keyword_info") or {}
        volume = info.get("search_volume")
        if volume is None:
            monthly = info.get("monthly_searches") or []
            if monthly:
                volume = monthly[-1].get("search_volume")

        parsed_items.append({"keyword": keyword, "volume": int(volume or 0)})
        parsed_items[-1]["categories"] = info.get("categories") or []
        parsed_items[-1]["main_intent"] = (item.get("search_intent_info") or {}).get("main_intent") or ""

    return parsed_items


async def query_keyword_suggestions(
    keyword: str,
    limit: int,
    login: str,
    password: str,
    keyword_like: str | None = None,
) -> List[Dict]:
    payload_item = {
        "keyword": keyword,
        "location_code": DATAFORSEO_LOCATION_CODE,
        "language_code": DATAFORSEO_LANGUAGE_CODE,
        "limit": limit,
    }
    if keyword_like:
        payload_item["filters"] = [["keyword", "like", f"%{keyword_like}%"]]
    payload = [payload_item]

    timeout = httpx.Timeout(45.0, connect=12.0)
    try:
        async with httpx.AsyncClient(timeout=timeout, auth=(login, password)) as client:
            response = await client.post(DATAFORSEO_SUGGESTIONS_ENDPOINT, json=payload)
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"DataForSEO network error (suggestions): {str(exc) or 'request failed'}")

    if response.status_code != 200:
        raise HTTPException(status_code=502, detail=f"DataForSEO request failed ({response.status_code})")

    body = response.json()
    if body.get("status_code") != 20000:
        raise HTTPException(status_code=502, detail=f"DataForSEO error: {body.get('status_message', 'Unknown error')}")

    tasks = body.get("tasks") or []
    for task in tasks:
        if task.get("status_code") != 20000:
            raise HTTPException(
                status_code=502,
                detail=f"DataForSEO task error: {task.get('status_message', 'Unknown task error')}",
            )

    items = []
    for task in tasks:
        for result in task.get("result") or []:
            items.extend(result.get("items") or [])

    parsed_items = []
    for item in items:
        kw = sanitize_keyword_text((item.get("keyword") or "").strip())
        if not kw:
            continue

        info = item.get("keyword_info") or {}
        volume = info.get("search_volume")
        if volume is None:
            monthly = info.get("monthly_searches") or []
            if monthly:
                volume = monthly[-1].get("search_volume")

        parsed_items.append({
            "keyword": kw,
            "volume": int(volume or 0),
            "categories": info.get("categories") or [],
            "main_intent": (item.get("search_intent_info") or {}).get("main_intent") or "",
        })

    return parsed_items


def derive_seed_like_pattern(seed_tokens: Set[str]) -> str | None:
    if not seed_tokens:
        return None
    token = max(seed_tokens, key=len)
    if len(token) < 4:
        return None
    return token[:5]


async def fetch_industry_expansion_rows(
    seed_tokens: Set[str],
    limit: int,
    login: str,
    password: str,
    strict_relevance: bool,
) -> List[Dict]:
    rows: List[Dict] = []
    expansion_seeds = SPORTSBOOK_EXPANSION_SEEDS if is_sportsbook_intent_seed(seed_tokens) else CASINO_EXPANSION_SEEDS
    per_seed_limit = min(180, limit)
    tasks = []
    for expansion_seed in expansion_seeds:
        expansion_tokens = set(tokenize(expansion_seed))
        like = derive_seed_like_pattern(expansion_tokens if strict_relevance else set())
        tasks.append(
            query_keyword_suggestions(
                expansion_seed,
                per_seed_limit,
                login,
                password,
                keyword_like=like if strict_relevance else None,
            )
        )

    results = await asyncio.gather(*tasks, return_exceptions=True)
    for result in results:
        if isinstance(result, Exception):
            continue
        rows.extend(result)

    return rows


def infer_corrected_seed(input_seed: str, rows: List[Dict]) -> str | None:
    input_norm = ascii_fold(input_seed.lower()).strip()
    if not input_norm:
        return None

    best_kw = None
    best_score = 0.0
    for row in rows[:500]:
        kw = row.get("keyword", "")
        kw_norm = ascii_fold(kw.lower()).strip()
        if not kw_norm:
            continue
        sim = difflib.SequenceMatcher(None, input_norm, kw_norm).ratio()
        if sim < 0.68:
            continue
        volume_boost = math.log10(max(1, int(row.get("volume", 0))) + 1) / 10
        score = sim + volume_boost
        if score > best_score:
            best_score = score
            best_kw = kw

    if best_kw and ascii_fold(best_kw.lower()) != input_norm:
        return sanitize_keyword_text(best_kw)
    return None

def dedupe_rows(rows: List[Dict]) -> List[Dict]:
    unique: Dict[str, Dict] = {}
    for row in rows:
        existing = unique.get(row["keyword"])
        if existing is None or row["volume"] > existing["volume"]:
            unique[row["keyword"]] = row
    return list(unique.values())


def select_expansion_seeds(
    rows: List[Dict], seed: str, seed_tokens: Set[str], max_count: int, min_volume: int
) -> List[str]:
    picked = []
    seed_lc = seed.lower()
    for row in rows:
        kw = row["keyword"]
        if kw.lower() == seed_lc:
            continue
        if row.get("volume", 0) < min_volume:
            continue
        if row.get("main_intent") == "navigational":
            continue
        if keyword_matches_seed(kw, seed_tokens):
            picked.append(kw)
        if len(picked) >= max_count:
            break
    return picked


def filter_rows_by_relevance(
    rows: List[Dict], seed_tokens: Set[str], seed_categories: Set[int], min_volume: int
) -> List[Dict]:
    if not seed_tokens:
        return [row for row in rows if row.get("volume", 0) >= min_volume]

    sportsbook_seed = is_sportsbook_intent_seed(seed_tokens)
    casino_seed = is_casino_intent_seed(seed_tokens)
    multi_token_seed = len(seed_tokens) >= 2
    seed_specific_tokens = {token for token in seed_tokens if token not in GENERIC_SEED_TOKENS}
    if not seed_specific_tokens:
        seed_specific_tokens = set(seed_tokens)

    def passes_guardrails(row: Dict, token_set: Set[str]) -> bool:
        if row.get("volume", 0) < min_volume:
            return False
        if row.get("main_intent") == "navigational":
            return False
        if is_noisy_keyword(row["keyword"], seed_tokens):
            return False
        if sportsbook_seed and has_sportswear_noise(token_set):
            return False
        if sportsbook_seed and not has_betting_anchor(token_set) and not has_betting_signal(token_set):
            return False
        if casino_seed and has_sportsbook_noise(token_set) and not has_casino_signal(token_set):
            return False
        if multi_token_seed and not sportsbook_seed:
            has_specific_match = any(token_matches_seed(token, seed_specific_tokens) for token in token_set)
            if not has_specific_match and not (casino_seed and has_casino_signal(token_set)):
                row_categories = set(row.get("categories") or [])
                if not (seed_categories and row_categories and (row_categories & seed_categories)):
                    return False
        return True

    prepared_rows = []
    for row in rows:
        tokens = tokenize(row["keyword"])
        if not tokens:
            continue
        token_set = set(tokens)
        if not passes_guardrails(row, token_set):
            continue
        prepared_rows.append((row, tokens, token_set))

    if not prepared_rows:
        return []

    # Build context terms from keywords that already match the seed.
    seed_matched = [row for row, _tokens, _token_set in prepared_rows if keyword_matches_seed(row["keyword"], seed_tokens)]
    if not seed_matched:
        return [row for row, _tokens, _token_set in prepared_rows]

    context_counts: Counter = Counter()
    for row in seed_matched:
        for token in tokenize(row["keyword"]):
            if not token_matches_seed(token, seed_tokens):
                context_counts[token] += 1

    context_terms = {
        token
        for token, freq in context_counts.most_common(30)
        if freq >= 2
    }

    filtered = []
    for row, tokens, token_set in prepared_rows:
        if keyword_matches_seed(row["keyword"], seed_tokens):
            filtered.append(row)
            continue
        if sportsbook_seed and has_betting_signal(token_set):
            filtered.append(row)
            continue
        if casino_seed and has_casino_signal(token_set):
            filtered.append(row)
            continue

        row_categories = set(row.get("categories") or [])
        if seed_categories and row_categories and not (row_categories & seed_categories):
            continue

        context_hits = sum(1 for t in tokens if t in context_terms)
        if context_hits >= 2:
            filtered.append(row)

    return filtered


def derive_seed_categories(rows: List[Dict], seed: str, seed_tokens: Set[str]) -> Set[int]:
    seed_lc = seed.lower()
    for row in rows:
        if row["keyword"].lower() == seed_lc and row.get("categories"):
            return set(row["categories"])

    counts: Counter = Counter()
    for row in rows:
        if keyword_matches_seed(row["keyword"], seed_tokens):
            for cat in (row.get("categories") or []):
                counts[cat] += max(1, int(row.get("volume", 0)))

    return {cat for cat, _ in counts.most_common(8)}


def sanitize_keyword_text(text: str) -> str:
    if not text:
        return text
    if any(marker in text for marker in ("Ã", "Å", "Ä", "â", "�")):
        try:
            return text.encode("latin1").decode("utf-8")
        except UnicodeError:
            return text
    return text


def is_noisy_keyword(keyword: str, seed_tokens: Set[str]) -> bool:
    tokens = tokenize(keyword)
    if not tokens:
        return True

    has_seed = any(token_matches_seed(t, seed_tokens) for t in tokens)
    cleaned = re.sub(r"[^\w\s-]", " ", keyword.lower(), flags=re.UNICODE)
    raw_tokens = [t for t in re.split(r"[\s_-]+", cleaned) if t]

    if has_seed and any(t in NOISE_TOKENS for t in tokens):
        return True
    if has_seed and any(t in OFF_TOPIC_NOISE_TOKENS for t in tokens):
        return True

    if has_seed and raw_tokens:
        first = raw_tokens[0]
        if first in PERSONA_PREFIX_TOKENS:
            return True
        if len(first) <= 2 and not token_matches_seed(first, seed_tokens):
            return True

    return False


def build_topical_map(
    seed: str, keyword_rows: List[Dict], target_keywords: int, include_brands: bool
) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    seed_tokens = set(tokenize(seed))
    sportsbook_seed = is_sportsbook_intent_seed(seed_tokens)

    token_counts = Counter()
    token_volume = Counter()
    bigram_counts = Counter()
    bigram_volume = Counter()
    tokenized_rows = []
    for row in keyword_rows:
        raw_tokens = [normalize_cluster_token(t) for t in tokenize(row["keyword"])]
        tokens = [t for t in raw_tokens if t not in seed_tokens]
        if not tokens and raw_tokens:
            # Keep one token fallback so sparse keyword sets still form clusters.
            tokens = [raw_tokens[0]]
        tokenized_rows.append((row, tokens))
        for token in set(tokens):
            token_counts[token] += 1
            token_volume[token] += row["volume"]
        for bg in set((tokens[i], tokens[i + 1]) for i in range(len(tokens) - 1)):
            bigram_counts[bg] += 1
            bigram_volume[bg] += row["volume"]

    anchors = rank_cluster_anchors(
        seed_tokens,
        token_counts,
        token_volume,
        bigram_counts,
        bigram_volume,
    )

    cluster_map: Dict[str, List[Dict]] = defaultdict(list)
    for row, tokens in tokenized_rows:
        token_set = set(tokens)
        bigram_set = set((tokens[i], tokens[i + 1]) for i in range(len(tokens) - 1))

        cluster_name = None
        for bg in anchors["bigrams"]:
            if bg in bigram_set:
                cluster_name = f"{bg[0]} {bg[1]}"
                break

        if cluster_name is None:
            for token in anchors["tokens"]:
                if token in token_set:
                    cluster_name = token
                    break

        if cluster_name is None:
            if tokens:
                cluster_name = f"{tokens[0]} {tokens[1]}" if len(tokens) >= 2 else tokens[0]
            else:
                cluster_name = pick_cluster_name(tokens, token_counts, bigram_counts)

        cluster_map[cluster_name].append(row)

    # Keep only robust clusters; do not force all leftovers into one giant bucket.
    min_cluster_keywords = 2 if len(seed_tokens) > 1 else MIN_CLUSTER_KEYWORDS
    if sportsbook_seed:
        min_cluster_keywords = 1
    elif len(keyword_rows) < 120:
        min_cluster_keywords = min(min_cluster_keywords, 2)
    if len(keyword_rows) < 60:
        min_cluster_keywords = 1 if sportsbook_seed or len(seed_tokens) <= 1 else max(2, min_cluster_keywords)

    filtered_cluster_map: Dict[str, List[Dict]] = {
        cluster_name: rows
        for cluster_name, rows in cluster_map.items()
        if cluster_name != "Pozostale Tematy" and len(rows) >= min_cluster_keywords
    }

    if len(filtered_cluster_map) < 6:
        sparse_min = 1 if (sportsbook_seed or len(keyword_rows) < 120) else 2
        if len(seed_tokens) >= 2 and not sportsbook_seed:
            sparse_min = max(2, sparse_min)
        filtered_cluster_map = {
            cluster_name: rows
            for cluster_name, rows in cluster_map.items()
            if cluster_name != "Pozostale Tematy" and len(rows) >= sparse_min
        }

    cluster_map = filtered_cluster_map

    min_cluster_total = MIN_CLUSTER_TOTAL_VOLUME
    if len(seed_tokens) > 1:
        min_cluster_total = min(min_cluster_total, 600)
    if is_gambling_seed(seed_tokens):
        min_cluster_total = min(min_cluster_total, 400)
    if sportsbook_seed:
        min_cluster_total = min(min_cluster_total, 120)
    elif len(keyword_rows) < 120:
        min_cluster_total = min(min_cluster_total, 250)
    if len(keyword_rows) < 60:
        min_cluster_total = min(min_cluster_total, 80)

    all_clusters_sorted = sorted(
        (
            (name, rows)
            for name, rows in cluster_map.items()
            if sum(x["volume"] for x in rows) >= min_cluster_total
        ),
        key=lambda pair: (sum(x["volume"] for x in pair[1]), len(pair[1])),
        reverse=True,
    )
    if sportsbook_seed and len(all_clusters_sorted) < 6:
        all_clusters_sorted = sorted(
            (
                (name, rows)
                for name, rows in cluster_map.items()
                if sum(x["volume"] for x in rows) >= 40
            ),
            key=lambda pair: (sum(x["volume"] for x in pair[1]), len(pair[1])),
            reverse=True,
        )

    clusters_sorted = all_clusters_sorted
    dynamic_cluster_limit = min(MAX_CLUSTERS, max(8, min(20, len(keyword_rows) // 3)))
    if is_gambling_seed(seed_tokens):
        thematic = [c for c in clusters_sorted if not is_brand_cluster_label(c[0])]
        brand = [c for c in clusters_sorted if is_brand_cluster_label(c[0])]
        max_brand = min(5, max(2, dynamic_cluster_limit // 3)) if include_brands else 0
        picked = thematic[: max(0, dynamic_cluster_limit - max_brand)]
        picked += brand[: max_brand]
        if len(picked) < dynamic_cluster_limit:
            picked += thematic[len(picked) : dynamic_cluster_limit]
        if include_brands and len(picked) < dynamic_cluster_limit:
            picked += brand[len(brand[:max_brand]) : dynamic_cluster_limit]
        clusters_sorted = sorted(
            picked[:dynamic_cluster_limit],
            key=lambda pair: (sum(x["volume"] for x in pair[1]), len(pair[1])),
            reverse=True,
        )
    else:
        clusters_sorted = clusters_sorted[:dynamic_cluster_limit]

    clusters_sorted = dedupe_clusters_by_display(seed, clusters_sorted, dynamic_cluster_limit)

    selected_keywords: Dict[str, List[Dict]] = {
        name: sorted(rows, key=lambda x: x["volume"], reverse=True)
        for name, rows in clusters_sorted
    }
    per_cluster_idx = {name: 0 for name, _ in clusters_sorted}
    selected_total = 0

    while selected_total < target_keywords:
        picked_in_cycle = False
        for cluster_name, _rows in clusters_sorted:
            idx = per_cluster_idx[cluster_name]
            if idx < len(selected_keywords[cluster_name]):
                per_cluster_idx[cluster_name] += 1
                selected_total += 1
                picked_in_cycle = True
                if selected_total >= target_keywords:
                    break
        if not picked_in_cycle:
            break

    nodes: List[Dict] = []
    links: List[Dict] = []
    pillars: List[Dict] = []

    max_volume = max((x["volume"] for x in keyword_rows), default=1000)
    nodes.append({
        "id": "seed",
        "label": seed,
        "type": "seed",
        "volume": max(1, max_volume),
        "group": 0,
        "size": 24,
    })

    for idx, (cluster_name, _rows) in enumerate(clusters_sorted, start=1):
        display_cluster_name = format_cluster_label(seed, cluster_name)
        take_count = per_cluster_idx[cluster_name]
        rows_taken = selected_keywords[cluster_name][:take_count]
        min_rows_for_cluster = 2 if len(seed_tokens) >= 2 and not sportsbook_seed else 1
        if len(rows_taken) < min_rows_for_cluster:
            continue

        cluster_id = f"cluster::{display_cluster_name}"
        cluster_volume = sum(x["volume"] for x in rows_taken)
        cluster_size = 10 + min(10, int(math.log10(cluster_volume + 10) * 3))

        nodes.append({
            "id": cluster_id,
            "label": display_cluster_name,
            "type": "cluster",
            "volume": cluster_volume,
            "group": idx,
            "size": cluster_size,
            "show_label": True,
        })
        links.append({"source": "seed", "target": cluster_id, "strength": 1.0})

        pillars.append({
            "pillar": display_cluster_name,
            "total_volume": cluster_volume,
            "keywords_count": len(rows_taken),
            "content_pillars": build_content_pillars(seed, display_cluster_name, rows_taken),
            "sample_topics": build_article_topics(seed, display_cluster_name, rows_taken),
        })

        for kw_idx, kw in enumerate(rows_taken):
            kw_id = f"kw::{kw['keyword']}"
            kw_size = 2 + min(7, int(math.log10(kw["volume"] + 10) * 2.2))
            nodes.append({
                "id": kw_id,
                "label": kw["keyword"],
                "type": "keyword",
                "volume": kw["volume"],
                "group": idx,
                "size": kw_size,
                "show_label": kw_idx < 3,
            })
            links.append({"source": cluster_id, "target": kw_id, "strength": 0.5})

    return nodes, links, pillars


def dedupe_clusters_by_display(
    seed: str,
    candidates: List[Tuple[str, List[Dict]]],
    limit: int,
) -> List[Tuple[str, List[Dict]]]:
    unique: List[Tuple[str, List[Dict]]] = []
    seen: Set[str] = set()

    for cluster_name, rows in candidates:
        display = format_cluster_label(seed, cluster_name)
        display_tokens = tokenize(display)
        if len(display_tokens) >= 2:
            key = " ".join(sorted(display_tokens[:2]))
        else:
            key = display.lower()

        if key in seen:
            continue
        seen.add(key)
        unique.append((cluster_name, rows))
        if len(unique) >= limit:
            break

    return unique


def build_content_pillars(seed: str, cluster_name: str, rows_taken: List[Dict]) -> List[str]:
    top_kws = [x["keyword"] for x in rows_taken[:8]]
    cluster_topic = cluster_name.strip()
    year = datetime.now().year
    head_kw = top_kws[0] if top_kws else cluster_topic.lower()
    compare_tail = top_kws[1] if len(top_kws) > 1 else f"{cluster_topic.lower()} opinie"
    support_kws = [kw for kw in top_kws[1:5] if ascii_fold(kw.lower()) != ascii_fold(head_kw.lower())]
    support_line = ", ".join(support_kws) if support_kws else compare_tail
    faq_kw = next((kw for kw in top_kws if len(tokenize(kw)) >= 3), head_kw)
    betting_seed = is_betting_intent_seed(set(tokenize(seed)))

    if betting_seed:
        faq_line = (
            f"FAQ i intent: legalnosc, bonusy, limity, wyplaty i podatek dla zapytan typu '{faq_kw}'."
        )
    else:
        faq_line = (
            f"FAQ i intent: parametry wyboru, cena/jakosc, bezpieczenstwo i scenariusze uzycia dla '{faq_kw}'."
        )

    return [
        f"Strona filarowa: {head_kw} - przewodnik i ranking {year}.",
        f"Sekcje wspierajace: {support_line}.",
        f"Porownanie: {head_kw} vs {compare_tail} (warunki, koszty, plusy/minusy).",
        faq_line,
    ]


def build_article_topics(seed: str, cluster_name: str, rows_taken: List[Dict]) -> List[str]:
    top_kws = [x["keyword"] for x in rows_taken[:8]]
    seed_topic = seed.strip()
    cluster_topic = cluster_name.strip()
    year = datetime.now().year

    topics: List[str] = []
    if len(top_kws) >= 2:
        topics.append(f"{top_kws[0]} vs {top_kws[1]}: pelne porownanie krok po kroku")

    for kw in top_kws[:6]:
        topics.append(f"{kw}: ranking {year} i najwazniejsze kryteria wyboru")
        topics.append(f"{kw}: koszty, warunki i najczestsze bledy")
        topics.append(f"{kw}: FAQ - odpowiedzi na pytania przed decyzja")

    topics.append(f"{cluster_topic}: plan tresci i mapa artykulow dla frazy '{seed_topic}'")

    unique_topics: List[str] = []
    seen: Set[str] = set()
    for topic in topics:
        key = ascii_fold(topic.lower()).strip()
        if not key or key in seen:
            continue
        seen.add(key)
        unique_topics.append(topic)
        if len(unique_topics) >= 8:
            break

    return unique_topics


def tokenize(text: str) -> List[str]:
    text = ascii_fold(text.lower())
    cleaned = re.sub(r"[^\w\s-]", " ", text, flags=re.UNICODE)
    tokens = re.split(r"[\s_-]+", cleaned)
    return [t for t in tokens if t and t not in STOPWORDS and len(t) > 2 and not t.isdigit()]


def ascii_fold(text: str) -> str:
    pl_map = str.maketrans({
        "ą": "a", "ć": "c", "ę": "e", "ł": "l", "ń": "n", "ó": "o", "ś": "s", "ż": "z", "ź": "z",
        "Ą": "A", "Ć": "C", "Ę": "E", "Ł": "L", "Ń": "N", "Ó": "O", "Ś": "S", "Ż": "Z", "Ź": "Z",
    })
    text = text.translate(pl_map)
    return "".join(ch for ch in unicodedata.normalize("NFKD", text) if not unicodedata.combining(ch))


def token_matches_seed(token: str, seed_tokens: Set[str]) -> bool:
    for seed_token in seed_tokens:
        if token == seed_token:
            return True
        if token.startswith(seed_token) or seed_token.startswith(token):
            return True
        if len(token) >= 5 and len(seed_token) >= 5 and token[:5] == seed_token[:5]:
            return True
    return False


def keyword_matches_seed(keyword: str, seed_tokens: Set[str]) -> bool:
    tokens = tokenize(keyword)
    return any(token_matches_seed(token, seed_tokens) for token in tokens)


def is_sportsbook_intent_seed(seed_tokens: Set[str]) -> bool:
    if not seed_tokens:
        return False

    has_zaklad = any(token.startswith("zaklad") for token in seed_tokens)
    has_sport = any(token.startswith("sport") for token in seed_tokens)
    explicit_betting = any(
        token.startswith(prefix)
        for token in seed_tokens
        for prefix in ("bukmach", "bet", "kurs", "typ", "kupon")
    )
    return explicit_betting or (has_zaklad and has_sport)


def is_casino_intent_seed(seed_tokens: Set[str]) -> bool:
    if not seed_tokens:
        return False
    return any(any(token.startswith(prefix) for prefix in CASINO_SIGNAL_PREFIXES) for token in seed_tokens)


def is_betting_intent_seed(seed_tokens: Set[str]) -> bool:
    # Backward-compatible alias: "betting" in this app means sportsbook intent.
    return is_sportsbook_intent_seed(seed_tokens)


def has_betting_signal(tokens: Set[str]) -> bool:
    for token in tokens:
        if token in GAMBLING_BRANDS:
            return True
        if any(token.startswith(prefix) for prefix in BETTING_SIGNAL_PREFIXES):
            return True
    return False


def has_betting_anchor(tokens: Set[str]) -> bool:
    return any(any(token.startswith(prefix) for prefix in BETTING_ANCHOR_PREFIXES) for token in tokens)


def has_casino_signal(tokens: Set[str]) -> bool:
    return any(any(token.startswith(prefix) for prefix in CASINO_SIGNAL_PREFIXES) for token in tokens)


def has_sportsbook_noise(tokens: Set[str]) -> bool:
    return any(any(token.startswith(prefix) for prefix in SPORTSBOOK_NOISE_PREFIXES) for token in tokens)


def has_sportswear_noise(tokens: Set[str]) -> bool:
    return any(any(token.startswith(prefix) for prefix in SPORTSWEAR_NOISE_PREFIXES) for token in tokens)


def normalize_cluster_token(token: str) -> str:
    token = ascii_fold(token)
    replacements = {
        "polska": "polskie",
        "polsce": "polskie",
        "internetowe": "online",
        "internetowy": "online",
        "depo": "depozytu",
        "depozycie": "depozytu",
        "bonusy": "bonus",
        "weryfikacji": "weryfikacja",
        "prawdziwe": "pieniadze",
    }
    return replacements.get(token, token)


def is_gambling_seed(seed_tokens: Set[str]) -> bool:
    return is_casino_intent_seed(seed_tokens) or is_sportsbook_intent_seed(seed_tokens)


def rank_cluster_anchors(
    seed_tokens: Set[str],
    token_counts: Counter,
    token_volume: Counter,
    bigram_counts: Counter,
    bigram_volume: Counter,
) -> Dict[str, List]:
    token_candidates = [
        token
        for token, freq in token_counts.items()
        if freq >= 2 and token not in NOISE_TOKENS
    ]
    token_candidates.sort(key=lambda t: (token_counts[t], token_volume[t]), reverse=True)

    bigram_candidates = [
        bg
        for bg, freq in bigram_counts.items()
        if freq >= 2 and bg[0] != bg[1] and bg[0] not in NOISE_TOKENS and bg[1] not in NOISE_TOKENS
    ]
    bigram_candidates.sort(key=lambda bg: (bigram_counts[bg], bigram_volume[bg]), reverse=True)

    if is_gambling_seed(seed_tokens):
        thematic_tokens = [t for t in token_candidates if t not in GAMBLING_BRANDS]
        brand_tokens = [t for t in token_candidates if t in GAMBLING_BRANDS]
        thematic_bigrams = [bg for bg in bigram_candidates if bg[0] not in GAMBLING_BRANDS and bg[1] not in GAMBLING_BRANDS]
        brand_bigrams = [bg for bg in bigram_candidates if bg[0] in GAMBLING_BRANDS or bg[1] in GAMBLING_BRANDS]

        return {
            "tokens": thematic_tokens[:18] + brand_tokens[:8],
            "bigrams": thematic_bigrams[:14] + brand_bigrams[:5],
        }

    if len(seed_tokens) > 1:
        return {"tokens": token_candidates[:22], "bigrams": bigram_candidates[:16]}

    return {"tokens": token_candidates[:18], "bigrams": bigram_candidates[:12]}


def is_brand_cluster_label(cluster_name: str) -> bool:
    tokens = tokenize(cluster_name)
    return any(token in GAMBLING_BRANDS for token in tokens)


def pick_cluster_name(tokens: List[str], token_counts: Counter, bigram_counts: Counter) -> str:
    if not tokens:
        return "Pozostale Tematy"

    if len(tokens) >= 2:
        best_bigram = max(
            ((tokens[i], tokens[i + 1]) for i in range(len(tokens) - 1)),
            key=lambda bg: bigram_counts[bg],
        )
        if bigram_counts[best_bigram] >= MIN_CLUSTER_KEYWORDS:
            if best_bigram[0] == best_bigram[1]:
                return f"{best_bigram[0].title()} Tematy"
            return f"{best_bigram[0].title()} {best_bigram[1].title()}"

    best = max(tokens, key=lambda t: token_counts[t])
    if token_counts[best] < MIN_CLUSTER_KEYWORDS:
        return "Pozostale Tematy"
    return best.title()


def format_cluster_label(seed: str, cluster_name: str) -> str:
    seed_tokens = tokenize(seed)
    seed_head = seed_tokens[0].title() if seed_tokens else seed.strip().split()[0].title()

    normalized = ascii_fold(sanitize_keyword_text(cluster_name)).replace("Tematy", "").strip()
    words = [w for w in normalized.split() if w]

    # Enforce exactly two words in final pillar label.
    if len(words) >= 2:
        return f"{words[0].title()} {words[1].title()}"
    if len(words) == 1:
        word = words[0].title()
        if word == seed_head:
            return f"{seed_head} Online"
        return f"{seed_head} {word}"
    return f"{seed_head} Online"





