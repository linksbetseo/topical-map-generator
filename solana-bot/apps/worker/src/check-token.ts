/**
 * Read-only evaluation of v1 token filters for one mint (or the newest tokens from Jupiter "recent").
 * Sends no transactions, writes nothing.
 *   pnpm --filter @solbot/worker check-token [mint]
 */
import { D, ReasonCode, reason, systemClock } from "@solbot/domain";
import { loadRuntimeEnv, parseConfig } from "@solbot/config";
import { HeliusRpc, JupiterClient, Priority, ReadOnlyTransport, SlidingWindowLimiter, evaluateMintRisk, holderConcentration, parseMintAccount, tokenAccountsViaProgramAccounts, type TokenInfo } from "@solbot/providers";
import { evaluateTokenFilters } from "@solbot/strategy";

const env = loadRuntimeEnv();
const cfg = parseConfig();
const transport = new ReadOnlyTransport(fetch as never, systemClock, 20_000);
const jup = new JupiterClient(transport, new SlidingWindowLimiter(systemClock, env.jupiterApiKey ? 60 : 30), env.jupiterApiKey).withPriority(Priority.RECONCILE);
const rpc = new HeliusRpc(transport, new SlidingWindowLimiter(systemClock, env.rpc.kind === "helius" ? 300 : 120), env.rpc.url, env.rpc.supportsDas);

async function evaluate(t: TokenInfo): Promise<void> {
  const now = new Date();
  const acc = await rpc.getAccountInfo(t.mint, Priority.RECONCILE);
  let risk = null;
  let holders = null;
  if (acc.ok && acc.value) {
    const m = parseMintAccount(acc.value.owner, acc.value.data);
    if (m.ok) {
      const r = evaluateMintRisk(t.mint, m.mint, cfg.universe.allowed_token2022_extensions);
      risk = { passed: r.verdict !== "REJECTED", reasons: r.reasons, availableAt: acc.receivedAt };
      const list = rpc.supportsDas ? await rpc.getAllTokenAccounts(t.mint, { priority: Priority.RECONCILE }) : await tokenAccountsViaProgramAccounts(rpc, t.mint, m.mint.tokenProgram, Priority.RECONCILE);
      if (list.ok) {
        const h = holderConcentration(list.value.accounts, m.mint.supplyRaw, list.value.complete, new Set());
        holders = { ...h, availableAt: list.receivedAt };
      } else holders = { ok: false, reasons: [reason(ReasonCode.HOLDER_DATA_UNAVAILABLE, list.code)], holderCount: 0, top10Bps: null, largestBps: null, availableAt: now };
    } else risk = { passed: false, reasons: [m.reason], availableAt: acc.receivedAt };
  }
  const vol = t.stats5m && t.stats5m.buyVolumeUsd && t.stats5m.sellVolumeUsd ? t.stats5m.buyVolumeUsd.add(t.stats5m.sellVolumeUsd) : null;
  const res = evaluateTokenFilters(
    new Date(),
    { mint: t.mint, firstPoolId: t.firstPoolId, firstPoolCreatedAt: t.firstPoolCreatedAt, liquidityUsd: t.liquidityUsd, volume5mUsd: vol, sells5m: t.stats5m?.numSells ?? null, priceChange5mPct: t.stats5m?.priceChangePct ?? null, launchpad: t.launchpad, graduatedAt: t.graduatedAt, availableAt: t.receivedAt },
    risk,
    holders,
    cfg,
  );
  // symbol is untrusted display text: printed via JSON.stringify, never interpreted
  console.log(`\n${t.mint} ${JSON.stringify(t.untrusted.symbol)} -> ${res.passed ? "PASSED" : "REJECTED"} (holders by owner: ${holders?.holderCount ?? "n/a"}, provider holderCount: ${t.providerHolderCount ?? "n/a"})`);
  for (const c of res.checks) console.log(`  ${c.passed ? "ok  " : "FAIL"} ${c.filter}: ${c.observed} (wymagane ${c.required})`);
}

const mint = process.argv[2];
if (mint) {
  const r = await jup.searchTokens([mint]);
  if (!r.ok || r.tokens.length === 0) throw new Error("token not found in Jupiter search");
  await evaluate(r.tokens[0]!);
} else {
  const r = await jup.recentTokens();
  if (!r.ok) throw new Error(`${r.code}: ${r.detail}`);
  const n = Number(process.env.CHECK_TOKEN_LIMIT ?? 3);
  console.log(`recent: ${r.tokens.length} tokens (first pool time, not mint time); evaluating ${n}`);
  for (const t of r.tokens.slice(0, n)) await evaluate(t);
}
void D;
