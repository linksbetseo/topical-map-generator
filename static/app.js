const graphEl = document.getElementById("graph");
const formEl = document.getElementById("search-form");
const seedInputEl = document.getElementById("seed-input");
const targetInputEl = document.getElementById("target-input");
const minVolumeInputEl = document.getElementById("min-volume-input");
const strictInputEl = document.getElementById("strict-input");
const brandsInputEl = document.getElementById("brands-input");
const submitBtnEl = document.getElementById("submit-btn");
const statusEl = document.getElementById("status");
const tooltipEl = document.getElementById("tooltip");
const pillarsGridEl = document.getElementById("pillars-grid");
const exportCsvBtnEl = document.getElementById("export-csv-btn");
const graphLoaderEl = document.getElementById("graph-loader");

const colorByType = {
  seed: "#27f2ff",
  cluster: "#70ffad",
  keyword: "#8ba7ff",
};

let hoverNode = null;
let lastPayload = null;

const Graph = ForceGraph()(graphEl)
  .backgroundColor("#070b14")
  .nodeId("id")
  .nodeRelSize(5)
  .nodeVal((n) => n.size || 4)
  .nodeColor((n) => colorByType[n.type] || "#9da9ff")
  .linkColor(() => "rgba(130, 160, 255, 0.20)")
  .linkWidth((l) => (l.strength || 0.4) * 1.2)
  .linkDirectionalParticles((l) => (l.strength > 0.9 ? 2 : 0))
  .linkDirectionalParticleWidth(1.1)
  .linkDirectionalParticleColor(() => "rgba(84, 222, 255, 0.75)")
  .nodeCanvasObjectMode(() => "after")
  .nodeCanvasObject((node, ctx, globalScale) => {
    const shouldDrawLabel =
      node.type === "seed" ||
      node.type === "cluster" ||
      Boolean(node.show_label) ||
      node === hoverNode;

    if (!shouldDrawLabel) {
      return;
    }

    const label = node.type === "cluster"
      ? `${node.label} (${formatVolume(node.volume || 0)})`
      : String(node.label || "");

    const fontSize = node.type === "seed"
      ? Math.max(11, Math.min(20, 17 / Math.max(globalScale, 0.55)))
      : Math.max(8, Math.min(14, 12 / Math.max(globalScale, 0.55)));

    const x = Number(node.x || 0);
    const y = Number(node.y || 0) - (node.type === "keyword" ? 10 : 0);

    ctx.font = `${fontSize}px Sora, sans-serif`;
    ctx.textAlign = "center";
    ctx.textBaseline = "middle";

    const textWidth = ctx.measureText(label).width;
    const padX = 5;
    const padY = 3;

    ctx.fillStyle = "rgba(4, 10, 24, 0.78)";
    ctx.fillRect(
      x - textWidth / 2 - padX,
      y - fontSize / 2 - padY,
      textWidth + padX * 2,
      fontSize + padY * 2
    );

    ctx.fillStyle = node.type === "seed" ? "#d5f8ff" : "#dfe6ff";
    ctx.fillText(label, x, y);
  })
  .onNodeHover((node) => {
    hoverNode = node || null;
    graphEl.style.cursor = node ? "pointer" : "default";

    if (!node) {
      tooltipEl.classList.add("hidden");
      return;
    }

    tooltipEl.classList.remove("hidden");
    tooltipEl.textContent = `${node.label} | Volume: ${formatVolume(node.volume || 0)}`;
  })
  .onEngineStop(() => Graph.zoomToFit(900, 110));

Graph.d3Force("charge").strength((node) => (node.type === "seed" ? -420 : node.type === "cluster" ? -160 : -45));
Graph.d3Force("link").distance((link) => {
  const sourceType = typeof link.source === "object" ? link.source.type : "";
  return sourceType === "seed" ? 230 : 72;
});
Graph.d3VelocityDecay(0.34);

window.addEventListener("mousemove", (event) => {
  if (tooltipEl.classList.contains("hidden")) {
    return;
  }
  const rect = graphEl.getBoundingClientRect();
  tooltipEl.style.left = `${event.clientX - rect.left}px`;
  tooltipEl.style.top = `${event.clientY - rect.top}px`;
});

formEl.addEventListener("submit", async (event) => {
  event.preventDefault();

  const seed = seedInputEl.value.trim();
  const target = normalizeTarget(targetInputEl.value);
  const minVolume = normalizeMinVolume(minVolumeInputEl.value);
  const strict = strictInputEl.checked;
  const includeBrands = brandsInputEl.checked;

  if (seed.length < 2) {
    setStatus("Wpisz minimum 2 znaki.", true);
    return;
  }

  targetInputEl.value = String(target);
  setLoading(true);
  minVolumeInputEl.value = String(minVolume);
  setStatus(`Pobieram dane (cel: ${target} fraz, min volume: ${minVolume}, strict: ${String(strict)}, brands: ${String(includeBrands)})...`, false);

  try {
    const url = `/api/topical-map?seed=${encodeURIComponent(seed)}&target_keywords=${target}&strict_relevance=${String(strict)}&min_volume=${String(minVolume)}&include_brands=${String(includeBrands)}`;
    const response = await fetch(url);
    const payload = await response.json();

    if (!response.ok) {
      throw new Error(payload.detail || "Nie udalo sie pobrac danych.");
    }

    const graphData = layoutGraphData(payload.nodes, payload.links);
    Graph.graphData(graphData);
    renderPillars(payload.pillars || []);
    lastPayload = payload;
    exportCsvBtnEl.disabled = !Array.isArray(payload.pillars) || payload.pillars.length === 0;

    const remainingAttempts = Number(payload.meta?.remaining_attempts);
    const attemptsSuffix = Number.isFinite(remainingAttempts)
      ? ` Pozostalo prob: ${remainingAttempts}.`
      : "";
    const relaxedSuffix = payload.meta?.auto_relaxed
      ? ` Brak wynikow w trybie strict, automatycznie rozszerzylem filtr (strict: false, min volume: 0).`
      : "";

    setStatus(
      `Wygenerowano: ${payload.nodes.length.toLocaleString("pl-PL")} wezlow, ${payload.links.length.toLocaleString("pl-PL")} polaczen, ${String((payload.pillars || []).length)} filarow. Fraz po filtrze: ${String(payload.meta?.keywords_after_filter || 0)}.${attemptsSuffix}${relaxedSuffix}`,
      false
    );
  } catch (error) {
    setStatus(error.message || "Wystapil blad podczas generowania mapy.", true);
  } finally {
    setLoading(false);
  }
});

exportCsvBtnEl.addEventListener("click", () => {
  if (!hasExportablePillars(lastPayload)) {
    setStatus("Najpierw wygeneruj mape z filarami, zeby pobrac CSV.", true);
    return;
  }
  downloadPillarsCsv(lastPayload);
});

function layoutGraphData(nodes, links) {
  const clonedNodes = nodes.map((node) => ({ ...node }));
  const clonedLinks = links.map((link) => ({ ...link }));

  const nodeById = new Map(clonedNodes.map((node) => [node.id, node]));
  const seed = clonedNodes.find((node) => node.type === "seed");
  const clusters = clonedNodes
    .filter((node) => node.type === "cluster")
    .sort((a, b) => (b.volume || 0) - (a.volume || 0));

  if (seed) {
    seed.fx = 0;
    seed.fy = 0;
  }

  const keywordsByCluster = new Map();
  for (const cluster of clusters) {
    keywordsByCluster.set(cluster.id, []);
  }

  for (const link of clonedLinks) {
    const sourceId = String(link.source);
    const targetId = String(link.target);
    if (sourceId.startsWith("cluster::") && targetId.startsWith("kw::") && keywordsByCluster.has(sourceId)) {
      const node = nodeById.get(targetId);
      if (node) {
        keywordsByCluster.get(sourceId).push(node);
      }
    }
  }

  const clusterRadius = 300;
  const clusterCount = Math.max(1, clusters.length);

  clusters.forEach((cluster, idx) => {
    const angle = -Math.PI / 2 + (idx / clusterCount) * Math.PI * 2;
    const cx = Math.cos(angle) * clusterRadius;
    const cy = Math.sin(angle) * clusterRadius;

    cluster.fx = cx;
    cluster.fy = cy;

    const kwNodes = (keywordsByCluster.get(cluster.id) || [])
      .sort((a, b) => (b.volume || 0) - (a.volume || 0));

    kwNodes.forEach((kwNode, kwIdx) => {
      const lane = Math.floor(kwIdx / 14);
      const slot = kwIdx % 14;
      const localAngle = angle + (slot / 14) * Math.PI * 2;
      const ringRadius = 95 + lane * 48;

      kwNode.fx = cx + Math.cos(localAngle) * ringRadius;
      kwNode.fy = cy + Math.sin(localAngle) * ringRadius;
    });
  });

  return { nodes: clonedNodes, links: clonedLinks };
}

function renderPillars(pillars) {
  pillarsGridEl.innerHTML = "";

  if (!Array.isArray(pillars) || pillars.length === 0) {
    pillarsGridEl.innerHTML = '<div class="pillar-empty">Brak danych dla filarow.</div>';
    return;
  }

  for (const pillar of pillars) {
    const card = document.createElement("article");
    card.className = "pillar-card";

    const contentPillars = (pillar.content_pillars || [])
      .map((item) => `<li>${escapeHtml(item)}</li>`)
      .join("");

    const sampleTopics = (pillar.sample_topics || [])
      .slice(0, 6)
      .map((item) => `<li>${escapeHtml(item)}</li>`)
      .join("");

    card.innerHTML = `
      <h3>${escapeHtml(pillar.pillar || "Pillar")}</h3>
      <p class="pillar-meta">Wolumen: ${formatVolume(pillar.total_volume || 0)} | Fraz: ${String(pillar.keywords_count || 0)}</p>
      <span class="pillar-list-title">Pillars Content</span>
      <ul>${contentPillars}</ul>
      <span class="pillar-list-title">Przykladowe Tematy</span>
      <ul>${sampleTopics}</ul>
    `;

    pillarsGridEl.appendChild(card);
  }
}

function hasExportablePillars(payload) {
  return Boolean(payload && Array.isArray(payload.pillars) && payload.pillars.length > 0);
}

function downloadPillarsCsv(payload) {
  const seed = String(payload.meta?.seed || payload.meta?.input_seed || seedInputEl.value || "").trim();
  const pillars = Array.isArray(payload.pillars) ? payload.pillars : [];
  const header = [
    "seed",
    "pillar",
    "total_volume",
    "keywords_count",
    "content_pillars",
    "sample_topics",
  ];

  const rows = [header];
  for (const pillar of pillars) {
    rows.push([
      seed,
      String(pillar.pillar || ""),
      String(Number(pillar.total_volume || 0)),
      String(Number(pillar.keywords_count || 0)),
      (pillar.content_pillars || []).join(" | "),
      (pillar.sample_topics || []).join(" | "),
    ]);
  }

  const csvBody = rows
    .map((row) => row.map((cell) => csvEscape(cell)).join(","))
    .join("\r\n");

  const blob = new Blob([csvBody], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const safeSeed = seed.toLowerCase().replaceAll(/[^a-z0-9]+/g, "-").replaceAll(/^-+|-+$/g, "") || "topical-map";
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = `content-pillars-${safeSeed}.csv`;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(url);
}

function csvEscape(value) {
  const normalized = String(value ?? "");
  if (normalized.includes(",") || normalized.includes("\"") || normalized.includes("\n") || normalized.includes("\r")) {
    return `"${normalized.replaceAll("\"", "\"\"")}"`;
  }
  return normalized;
}

function formatVolume(value) {
  return Number(value || 0).toLocaleString("pl-PL");
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function normalizeTarget(value) {
  const parsed = Number(value);
  if (Number.isNaN(parsed)) {
    return 300;
  }
  return Math.min(500, Math.max(20, Math.round(parsed)));
}

function normalizeMinVolume(value) {
  const parsed = Number(value);
  if (Number.isNaN(parsed)) {
    return 100;
  }
  return Math.min(10000, Math.max(0, Math.round(parsed)));
}

function setStatus(message, isError) {
  statusEl.textContent = message;
  statusEl.classList.toggle("error", Boolean(isError));
}

function setLoading(isLoading) {
  submitBtnEl.disabled = isLoading;
  seedInputEl.disabled = isLoading;
  targetInputEl.disabled = isLoading;
  minVolumeInputEl.disabled = isLoading;
  strictInputEl.disabled = isLoading;
  brandsInputEl.disabled = isLoading;
  submitBtnEl.textContent = isLoading ? "Generowanie..." : "Generuj";
  graphLoaderEl.classList.toggle("hidden", !isLoading);
  graphEl.classList.toggle("is-loading", isLoading);
  exportCsvBtnEl.disabled = isLoading || !hasExportablePillars(lastPayload);
}
