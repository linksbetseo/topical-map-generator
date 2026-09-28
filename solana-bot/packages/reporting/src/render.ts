import type { Report } from "./report.ts";

/** HTML escaping for every value (token metadata and reasons are untrusted input). */
export function esc(v: unknown): string {
  return String(v ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

const na = (v: unknown) => (v === null || v === undefined ? "brak danych" : String(v));

export function toJson(r: Report): string {
  return JSON.stringify(r, null, 2);
}

export function toCsv(r: Report): string {
  const cell = (v: unknown) => {
    const t = v === null || v === undefined ? "" : String(v);
    // neutralise spreadsheet formula injection and quote
    const safe = /^[=+\-@\t\r]/.test(t) && !/^-?[0-9.]+$/.test(t) ? `'${t}` : t;
    return `"${safe.replace(/"/g, '""')}"`;
  };
  const head = ["position_id", "mint", "status", "entry_at", "exit_at", "exit_reason", "cost_usd", "pnl_usd"];
  const lines = [head.join(",")];
  for (const d of r.decisions) lines.push([d.positionId, d.mint, d.status, d.entryAt, d.exitAt, d.exitReason, d.costUsd, d.pnlUsd].map(cell).join(","));
  return lines.join("\n") + "\n";
}

export function toMarkdown(r: Report): string {
  const t = r.trades as Record<string, unknown>;
  const out: string[] = [];
  out.push(`# Raport sesji ${r.session.id}`);
  if (r.dataDisclaimer) out.push(`> **${r.dataDisclaimer}**`);
  out.push(`> ${r.verdicts.note}`);
  out.push("");
  out.push(`| Pole | Wartość |\n|---|---|`);
  out.push(`| Tryb / typ | ${r.session.mode} / ${r.session.kind} |`);
  out.push(`| Stan | ${r.session.state}${r.session.intervention ? " (z interwencją)" : ""} |`);
  out.push(`| T0 (UTC / Warszawa) | ${na(r.session.t0)} / ${na(r.session.t0Warsaw)} |`);
  out.push(`| T_end (UTC / Warszawa) | ${na(r.session.tEnd)} / ${na(r.session.tEndWarsaw)} |`);
  out.push(`| Strategia / profil | ${r.session.strategy} / ${r.session.executionProfile} |`);
  out.push(`| Config hash / kod | ${r.session.configHash} / ${r.codeVersion} |`);
  out.push("");
  out.push(`## Kapitał`);
  out.push(`- Start: ${r.capital.initialUsd} USD (wirtualne)`);
  out.push(`- Equity total (dolna granica): ${na(r.capital.equityTotalLowerBoundUsd)} USD; płynne: ${na(r.capital.equityLiquidLowerBoundUsd)}; świeże: ${na(r.capital.equityTotalFreshUsd)}`);
  out.push(`- Rent zablokowany: ${na(r.capital.rentLockedUsd)} USD`);
  out.push(`- Otwarte pozycje: ${r.capital.openPositions.length ? r.capital.openPositions.map((p) => `${p.mint} (${p.status}, wycena ${na(p.valuation)})`).join("; ") : "brak"}`);
  out.push(`- Stan w T_end: ${r.capital.atTEnd ? "`" + JSON.stringify(r.capital.atTEnd) + "`" : "brak danych"}`);
  out.push("");
  out.push(`## Wynik`);
  out.push(`- PnL transakcji po kosztach: ${r.pnl.realizedTradePnlUsd} USD`);
  out.push(`- Wynik rachunku (equity − 500): ${na(r.pnl.netPortfolioResultUsd)} USD`);
  out.push(`- Benchmark 480 USDC + 20 USD SOL bez handlu: ${na(r.pnl.benchmarkHoldStartAllocUsd)} USD; całość w USDC: ${na(r.pnl.benchmarkAllUsdcUsd)} USD`);
  out.push(`- Wpływ kursu alokacji startowej: ${na(r.pnl.fxEffectOfStartAllocationUsd)} USD`);
  out.push(`- Koszt infrastruktury (168 h): ${r.pnl.infrastructureCostUsd}; wynik po infrastrukturze: ${na(r.pnl.resultAfterInfrastructureUsd)}`);
  out.push(`- Opłaty: ${Object.entries(r.pnl.feesByKind).map(([k, v]) => `${k} ${v.usd} USD${v.estimate ? " (szacunek)" : ""}`).join(", ") || "brak"}`);
  out.push("");
  out.push(`## Transakcje`);
  out.push(`- Sygnały: ${r.counts.signals}; próby wejścia: ${r.counts.entryAttempts}; fille: ${r.counts.fills}; nieudane próby: ${r.counts.failedAttempts}`);
  out.push(`- Zamknięte epizody: ${na(t.closed)}; różne tokeny: ${na(t.distinctTokens)}`);
  const wr = t.winRate as { value: string | null; n: number };
  out.push(`- Win rate: ${na(wr.value)} (n=${wr.n}); profit factor: ${na((t.profitFactor as { value: unknown }).value)} [${(t.profitFactor as { status: string }).status}]`);
  out.push(`- Expectancy: ${na(t.expectancyUsd)} USD/pozycję; mediana: ${na(t.medianPnlUsd)}; bez najlepszego tokena: ${na(t.pnlWithoutBestTokenUsd)}`);
  out.push(`- Max drawdown (świeże wyceny): ${r.drawdown.freshUsd} USD; (dolna granica z lukami): ${r.drawdown.lowerBoundUsd} USD`);
  out.push(`- Odrzucenia: ${Object.entries(r.counts.rejectionsByReason).map(([k, v]) => `${k}=${v}`).join(", ") || "brak"}`);
  out.push("");
  out.push(`## Jakość danych`);
  out.push(`- Luki: ${r.dataQuality.gaps.length}; nieudane uzgodnienia: ${r.dataQuality.reconciliationFailures}`);
  out.push(`- Latencja quote p50/p95/p99: ${na(r.dataQuality.quoteLatencyMsP50)}/${na(r.dataQuality.quoteLatencyMsP95)}/${na(r.dataQuality.quoteLatencyMsP99)} ms`);
  out.push(`- Stress replay: ${r.dataQuality.stressCoverage.attempts} prób, +5 s: ${r.dataQuality.stressCoverage.withAnalytic5s}, +15 s: ${r.dataQuality.stressCoverage.withAnalytic15s}`);
  out.push("");
  out.push(`## Werdykty (nie włączają LIVE)`);
  out.push(`- Techniczny: ${r.verdicts.technical}${r.verdicts.technicalReasons.length ? ` — ${r.verdicts.technicalReasons.join("; ")}` : ""}`);
  out.push(`- Próba: ${r.verdicts.sample}`);
  out.push(`- Rekomendacja: ${r.verdicts.canaryRecommendation}${r.verdicts.canaryReasons.length ? ` — ${r.verdicts.canaryReasons.join("; ")}` : ""}`);
  out.push("");
  out.push(`## Wszystkie decyzje`);
  out.push("| Pozycja | Mint | Status | Wejście | Wyjście | Powód | Koszt | PnL |\n|---|---|---|---|---|---|---|---|");
  for (const d of r.decisions) out.push(`| ${d.positionId} | ${d.mint} | ${d.status} | ${na(d.entryAt)} | ${na(d.exitAt)} | ${na(d.exitReason)} | ${na(d.costUsd)} | ${na(d.pnlUsd)} |`);
  out.push("");
  out.push(`Status buildu: ${Object.entries(r.buildStatus).map(([k, v]) => `${k}=${v}`).join(", ")}`);
  return out.join("\n") + "\n";
}

export function toHtml(r: Report): string {
  const md = toMarkdown(r);
  // Minimal, dependency-free rendering: escaped preformatted Markdown. No scripts, no remote assets.
  return `<!doctype html><html lang="pl"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Raport ${esc(r.session.id)}</title>
<style>body{font:14px/1.5 system-ui,sans-serif;margin:16px;max-width:1100px}pre{white-space:pre-wrap;word-break:break-word}.mode{font-weight:700;padding:2px 8px;border:1px solid}</style></head>
<body><div class="mode">${esc(r.session.mode)}${r.dataDisclaimer ? " — " + esc(r.dataDisclaimer) : ""}</div><pre>${esc(md)}</pre></body></html>`;
}
