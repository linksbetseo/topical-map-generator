const limitEl = document.getElementById("limit-input");
const loadBtnEl = document.getElementById("load-btn");
const logoutBtnEl = document.getElementById("logout-btn");
const statsEl = document.getElementById("stats");
const msgEl = document.getElementById("msg");
const eventsBodyEl = document.getElementById("events-body");
const clientsBodyEl = document.getElementById("clients-body");

loadBtnEl.addEventListener("click", () => {
  void loadDashboard();
});
logoutBtnEl.addEventListener("click", async () => {
  try {
    await fetch("/api/admin/logout", { method: "POST" });
  } finally {
    window.location.href = "/admin";
  }
});

void loadDashboard();

async function loadDashboard() {
  const limit = normalizeLimit(limitEl.value);
  limitEl.value = String(limit);
  setLoading(true);
  setMessage("Pobieram logi...", false);

  try {
    const response = await fetch(`/api/admin/search-logs?limit=${limit}&client_limit=${limit}`);
    const payload = await response.json();
    if (response.status === 401) {
      window.location.href = "/admin";
      return;
    }
    if (!response.ok) {
      throw new Error(payload.detail || "Nie udalo sie pobrac logow.");
    }

    renderStats(payload);
    renderEvents(payload.recent_events || []);
    renderClients(payload.clients || []);
    setMessage(`Odswiezono. Eventow: ${(payload.recent_events || []).length}.`, false);
  } catch (error) {
    renderStats({});
    renderEvents([]);
    renderClients([]);
    setMessage(error.message || "Blad podczas pobierania logow.", true);
  } finally {
    setLoading(false);
  }
}

function renderStats(payload) {
  const events = Array.isArray(payload.recent_events) ? payload.recent_events : [];
  const clients = Array.isArray(payload.clients) ? payload.clients : [];

  let blocked = 0;
  let ok = 0;
  for (const item of events) {
    if (item.status === "blocked_limit") blocked += 1;
    if (item.status === "ok") ok += 1;
  }

  statsEl.innerHTML = `
    <div class="stat"><b>${events.length}</b><span>Eventy</span></div>
    <div class="stat"><b>${clients.length}</b><span>Uzytkownicy</span></div>
    <div class="stat"><b>${ok}</b><span>Udane</span></div>
    <div class="stat"><b>${blocked}</b><span>Zablokowane limitem</span></div>
    <div class="stat"><b>${payload.max_free_searches || 5}</b><span>Limit / ${payload.window_hours || 24}h</span></div>
  `;
}

function renderEvents(events) {
  if (!events.length) {
    eventsBodyEl.innerHTML = `<tr><td colspan="6">Brak danych.</td></tr>`;
    return;
  }

  eventsBodyEl.innerHTML = events
    .map((item) => {
      const statusClass = item.status === "ok"
        ? "status-ok"
        : item.status === "blocked_limit"
          ? "status-blocked"
          : "status-err";

      return `
        <tr>
          <td>${escapeHtml(String(item.created_at || ""))}</td>
          <td>${escapeHtml(String(item.ip || ""))}</td>
          <td>${escapeHtml(String(item.input_seed || ""))}</td>
          <td>${escapeHtml(String(item.resolved_seed || ""))}</td>
          <td class="${statusClass}">${escapeHtml(String(item.status || ""))}</td>
          <td>${escapeHtml(String(item.http_status || ""))}</td>
        </tr>
      `;
    })
    .join("");
}

function renderClients(clients) {
  if (!clients.length) {
    clientsBodyEl.innerHTML = `<tr><td colspan="4">Brak danych.</td></tr>`;
    return;
  }

  clientsBodyEl.innerHTML = clients
    .map((item) => {
      const seeds = Array.isArray(item.searched_seeds) ? item.searched_seeds : [];
      const seedList = seeds.length
        ? `<ul class="seed-list">${seeds.slice(0, 10).map((x) => `<li>${escapeHtml(String(x))}</li>`).join("")}</ul>`
        : "<span>-</span>";

      return `
        <tr>
          <td>${escapeHtml(shortClientId(String(item.client_id || "")))}</td>
          <td>${escapeHtml(String(item.ip || ""))}</td>
          <td>${escapeHtml(String(item.consumed_checks || 0))}</td>
          <td>${seedList}</td>
        </tr>
      `;
    })
    .join("");
}

function shortClientId(value) {
  if (value.length <= 14) return value;
  return `${value.slice(0, 8)}...${value.slice(-4)}`;
}

function normalizeLimit(value) {
  const parsed = Number(value);
  if (Number.isNaN(parsed)) return 300;
  return Math.min(1000, Math.max(20, Math.round(parsed)));
}

function setMessage(text, isError) {
  msgEl.textContent = text;
  msgEl.style.color = isError ? "#ff6a95" : "#8fa3ca";
}

function setLoading(isLoading) {
  loadBtnEl.disabled = isLoading;
  limitEl.disabled = isLoading;
  loadBtnEl.textContent = isLoading ? "Ladowanie..." : "Odswiez";
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}
