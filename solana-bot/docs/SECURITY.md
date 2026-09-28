# Raport bezpieczeństwa (PAPER build)

## Separacja uprawnień

| Zabezpieczenie | Gdzie | Test |
|---|---|---|
| Start PAPER/DEMO/SHADOW przerwany, gdy w env jest zmienna wyglądająca na sekret podpisu | `packages/config/src/env.ts` | `config.test.ts` |
| `MODE=LIVE*` i `LIVE_ENABLED≠false` odrzucone | jw. | jw. |
| Transport read-only blokuje `/execute`, `/submit`, `sendTransaction`, `sendRawTransaction`, `sendBundle`, `simulateBundle`, `simulateTransaction`, `requestAirdrop` przed I/O | `packages/providers/src/transport.ts` | `providers.contract.test.ts` |
| Brak kodu kluczy/podpisu/wysyłki w źródłach produkcyjnych | skan statyczny | `no-signer.test.ts` |
| `sessions.mode` w SQL dopuszcza tylko DEMO/PAPER/SHADOW | migracja 0001 | `db.db.test.ts` |
| Paper fill ma id `paper_…` (CHECK w SQL), nie wygląda jak podpis | migracja 0001 | `db.db.test.ts` |
| Brak `/api/sign`, `/api/withdraw` | `apps/api` | `api.db.test.ts` |

## API właściciela

* Bearer token (porównanie w stałym czasie), tylko nagłówek — brak cookies, więc brak
  ambientnego poświadczenia dla CSRF; dodatkowo POST z obcego `Origin` → 403.
* Rate limit na POST (20/min/IP), audyt każdej akcji sterującej (`audit_events` append-only).
* Webhook Helius: stały sekret w `Authorization` (porównanie w stałym czasie), trwały zapis,
  deduplikacja po `(network, signature, leg, owner)`, szybka odpowiedź; ciężka praca w workerze.

## Dane niezaufane

* Nazwy, symbole, URI, opisy tokenów są danymi: raport HTML escapuje każdą wartość, CSP
  `default-src 'none'`, CSV neutralizuje formuły (`=`, `+`, `@`). Test z `<script>`,
  `onerror`, `=HYPERLINK`, „ignore previous instructions”.
* Worker nie pobiera obrazków/URL-i z metadanych (brak powierzchni SSRF w tym buildzie).
  Jeśli panel zacznie pokazywać logo — tylko przez proxy z blokadą adresów prywatnych i
  metadata services.
* Treść metadanych nie trafia do żadnego LLM ani interpretera; strategia nie czyta pól tekstowych.

## Sekrety

* `.env.example` tylko z nazwami; `.env*` w `.gitignore`.
* Klucz Helius jest w URL — `redactUrl`/`redactSecrets` usuwają go z logów i błędów; błędy
  transportu nie zawierają URL. Token Telegram (w ścieżce URL) redagowany w błędach.
* Szablon CI (`ci/github-workflow.yml`): typecheck, testy, `pnpm audit`, gitleaks.

## Ryzyka otwarte

* Zależności npm przypięte lockfile'em, ale nie audytowane ręcznie; `pnpm audit` w CI.
* LIVE (Etap F) wymaga osobnego przeglądu: walidacja transakcji, allowlista programów,
  limity signera; przejęcie signera zagraża całemu saldu jego walleta.
