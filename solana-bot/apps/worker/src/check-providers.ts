/**
 * Read-only connectivity & contract check against Solana mainnet providers.
 * Sends no transactions (ReadOnlyTransport). Prints per-check status; exit code 1 if any check fails.
 *
 *   JUPITER_API_KEY=... HELIUS_API_KEY=... pnpm --filter @solbot/worker check-providers
 */
import { D, SPL_TOKEN_PROGRAM, USDC_MINT, WSOL_MINT, systemClock } from "@solbot/domain";
import { loadRuntimeEnv, redactSecrets } from "@solbot/config";
import { HeliusRpc, JupiterClient, Priority, ReadOnlyTransport, SlidingWindowLimiter, parseMintAccount } from "@solbot/providers";

type Status = "VERIFIED_READ_ONLY_MAINNET" | "FAILED";
const results: Array<{ check: string; status: Status; detail: string }> = [];

const env = loadRuntimeEnv();
const secrets = [env.jupiterApiKey, env.heliusApiKey];
const transport = new ReadOnlyTransport(fetch as never, systemClock, 10_000);
const jupLimiter = new SlidingWindowLimiter(systemClock, env.jupiterApiKey ? 60 : 30); // keyless: 0.5 RPS
const jup = new JupiterClient(transport, jupLimiter, env.jupiterApiKey).withPriority(Priority.RECONCILE);

async function check(name: string, fn: () => Promise<string>): Promise<void> {
  try {
    results.push({ check: name, status: "VERIFIED_READ_ONLY_MAINNET", detail: await fn() });
  } catch (e) {
    results.push({ check: name, status: "FAILED", detail: redactSecrets(e instanceof Error ? e.message : String(e), secrets) });
  }
}

await check("jupiter.price.v3 SOL,USDC", async () => {
  const r = await jup.usdPrices([WSOL_MINT, USDC_MINT]);
  if (!r.ok) throw new Error(`${r.code}: ${r.detail}`);
  if (!r.prices.has(WSOL_MINT) || !r.prices.has(USDC_MINT)) throw new Error("price missing for SOL or USDC");
  return `SOL=${r.prices.get(WSOL_MINT)!.usdPrice} USDC=${r.prices.get(USDC_MINT)!.usdPrice}`;
});

await check("jupiter.tokens.v2.recent", async () => {
  const r = await jup.recentTokens();
  if (!r.ok) throw new Error(`${r.code}: ${r.detail}`);
  return `${r.tokens.length} tokens parsed, ${r.rejected} rejected by parser`;
});

await check("jupiter.swap.v2.order quote-only 1 USDC->SOL (manual profile)", async () => {
  const r = await jup.quote({ inputMint: USDC_MINT, outputMint: WSOL_MINT, amountRaw: 1_000_000n, slippageBps: 100 });
  if (!r.ok) throw new Error(`${r.code}: ${r.detail}`);
  const q = r.quote;
  return `router=${q.router} mode=${q.mode} out_net=${q.outAmountNetRaw} fee_semantics=${q.feeSemantics} fee_bps=${q.feeBpsTotal} impact_bps=${q.priceImpactBps} fidelity=${q.executionFidelity}`;
});

{
  const rpc = new HeliusRpc(transport, new SlidingWindowLimiter(systemClock, 60), env.rpc.url, env.rpc.supportsDas);
  await check(`rpc(${env.rpc.kind}).getAccountInfo USDC mint (canonical check)`, async () => {
    const r = await rpc.getAccountInfo(USDC_MINT, Priority.RECONCILE);
    if (!r.ok) throw new Error(`${r.code}: ${r.detail}`);
    if (!r.value) throw new Error("USDC mint account not found");
    const m = parseMintAccount(r.value.owner, r.value.data);
    if (!m.ok) throw new Error(m.reason.detail ?? m.reason.code);
    if (r.value.owner !== SPL_TOKEN_PROGRAM || m.mint.decimals !== 6) throw new Error(`unexpected owner/decimals ${r.value.owner}/${m.mint.decimals}`);
    return `owner=SPL Token, decimals=6, slot=${r.value.slot}, supply=${new D(m.mint.supplyRaw.toString()).div(1e6).toFixed(0)}`;
  });
}

for (const r of results) console.log(`${r.status.padEnd(28)} ${r.check} :: ${r.detail}`);
process.exit(results.some((r) => r.status === "FAILED") ? 1 : 0);
