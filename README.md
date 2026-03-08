# Topical Map Generator (FastAPI + Force Graph)

Lekki generator interaktywnych map tematycznych SEO oparty o DataForSEO Labs API.

## 1) Uruchomienie lokalne

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Skopiuj plik srodowiskowy:

```bash
copy .env.example .env
```

Uzupelnij w `.env`:

- `DATAFORSEO_LOGIN`
- `DATAFORSEO_PASSWORD`
- `ADMIN_API_KEY` (haslo do panelu admin; ustaw swoje mocne haslo)

Opcjonalnie:

- `DATAFORSEO_LOCATION_CODE` (domyslnie `2616`)
- `DATAFORSEO_LANGUAGE_CODE` (domyslnie `pl`)
- `DATAFORSEO_MAX_KEYWORDS` (domyslnie `500`)
- `MAX_FREE_SEARCHES` (domyslnie `5`)
- `LIMIT_BYPASS_IPS` (lista IP po przecinku bez limitu, np. `31.183.210.223,1.2.3.4`)
- `LOG_DB_PATH` (domyslnie `search_logs.db`)
- `ADMIN_SESSION_SECRET` (sekret podpisu sesji panelu admin)
- `COOKIE_SECURE` (`true` na HTTPS/prod, lokalnie `false`)
- `MAP_CACHE_TTL_SECONDS` (domyslnie `1200`)
- `MAP_CACHE_MAX_ENTRIES` (domyslnie `200`)

Start API i frontendu:

```bash
uvicorn main:app --reload
```

Aplikacja bedzie dostepna pod: `http://127.0.0.1:8000`

## 1b) Deploy na Railway

1. Wrzuć projekt do GitHub (z plikami `Procfile` i `railway.json`).
2. W Railway: `New Project` -> `Deploy from GitHub repo` -> wybierz repo.
3. W `Variables` ustaw:
   - `DATAFORSEO_LOGIN`
   - `DATAFORSEO_PASSWORD`
   - `ADMIN_API_KEY`
   - `ADMIN_SESSION_SECRET`
   - `COOKIE_SECURE=true`
   - `LIMIT_BYPASS_IPS=31.183.210.223`
4. Po deployu wejdź w wygenerowaną domenę Railway i sprawdź:
   - `/health`
   - `/`
   - `/admin`

## 2) Limity i logi zapytan

- Kazdy uzytkownik dostaje cookie `tm_client_id`.
- Darmowy limit to `MAX_FREE_SEARCHES` (domyslnie 5 udanych/pustych sprawdzen) na 24h.
- Logi trafiaja do SQLite (`LOG_DB_PATH`), z polami: IP, fraza, seed po korekcie, status, timestamp i statystyki mapy.

Podglad logow (admin):

```bash
curl "http://127.0.0.1:8000/api/admin/search-logs?admin_key=TU_TWOJ_ADMIN_API_KEY"
```

Panel web admin:

- `http://127.0.0.1:8000/admin` (ekran logowania, potem dashboard po sesji cookie)

## 3) Endpoint

- `GET /api/topical-map?seed=pozycjonowanie stron`
- zwraca JSON: `nodes`, `links`, `pillars`, `meta`

## 4) Jak dziala grupowanie

- backend pobiera keyword ideas z DataForSEO
- usuwa duplikaty i bierze najwyzszy wolumen dla frazy
- buduje klastry na podstawie najczesciej wystepujacego tokenu w frazie
- tworzy strukture:
  - seed (centrum)
  - klastry (1. poziom)
  - slowa kluczowe (2. poziom)

## 5) Frontend

- Loader podczas generowania mapy.
- Eksport sekcji `Content Pillars` do pliku CSV (przycisk `Export CSV`).
