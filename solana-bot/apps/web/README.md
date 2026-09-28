# apps/web — panel (odłożony)

Zgodnie z briefem panel nie powstał przed księgą, risk engine i wykonaniem PAPER.
Wszystkie dane panelu są dostępne przez API (`/api/dashboard`, `/api/positions`,
`/api/signals`, `/api/orders/:id`, `/api/sessions/:id/report?format=html|md|csv|json`,
`/api/events` SSE). Panel ma pokazywać stale PAPER/LIVE, session id i pozostały czas,
a przed pierwszym pomiarem „brak danych” — nigdy przykładowe wyniki.
