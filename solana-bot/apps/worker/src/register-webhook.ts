/**
 * Creates (or updates) the Helius webhook that streams SWAP events of the session's watched wallets
 * to the deployed API. Configuration call only — no transaction is signed or sent.
 *   API_PUBLIC_URL=https://<api>.up.railway.app pnpm --filter @solbot/worker register-webhook <sessionId>
 */
import { systemClock } from "@solbot/domain";
import { loadRuntimeEnv, redactSecrets } from "@solbot/config";
import { createPool } from "@solbot/db";
import { ReadOnlyTransport } from "@solbot/providers";

const env = loadRuntimeEnv();
const sessionId = process.argv[2];
const apiUrl = process.env.API_PUBLIC_URL;
if (!env.databaseUrl || !env.heliusApiKey || !env.heliusWebhookAuth || !apiUrl || !sessionId) {
  throw new Error("need DATABASE_URL, HELIUS_API_KEY, HELIUS_WEBHOOK_AUTH, API_PUBLIC_URL and <sessionId>");
}
if (!apiUrl.startsWith("https://")) throw new Error("API_PUBLIC_URL must be https");
const pool = createPool(env.databaseUrl);
const wallets = (
  await pool.query<{ address: string }>(
    `SELECT q.address FROM wallet_qualification q JOIN wallet_clusters c ON c.session_id=q.session_id AND c.address=q.address
     WHERE q.session_id=$1 AND q.status='QUALIFIED' AND c.link_check='CHECKED' ORDER BY q.address`,
    [sessionId],
  )
).rows.map((r) => r.address);
await pool.end();
if (wallets.length === 0) throw new Error("no qualified, link-checked wallets for this session — run bootstrap-wallets first");

const t = new ReadOnlyTransport(fetch as never, systemClock, 30_000);
const base = `https://api-mainnet.helius-rpc.com/v0/webhooks?api-key=${env.heliusApiKey}`;
const body = {
  webhookURL: `${apiUrl.replace(/\/$/, "")}/api/webhooks/helius`,
  transactionTypes: ["SWAP"],
  accountAddresses: wallets,
  webhookType: "enhanced",
  authHeader: env.heliusWebhookAuth,
  txnStatus: "success",
};
const existing = await t.request("GET", base);
const list = existing.status === 200 ? (JSON.parse(existing.text) as Array<{ webhookID: string; webhookURL: string }>) : [];
const mine = list.find((w) => w.webhookURL === body.webhookURL);
if (mine) {
  console.log(`webhook for ${body.webhookURL} already exists (${mine.webhookID}); update its addresses in the Helius dashboard or delete it and rerun`);
  process.exit(0);
}
const res = await t.request("POST", base, { json: body });
console.log(res.status, redactSecrets(res.text.slice(0, 500), [env.heliusApiKey, env.heliusWebhookAuth]));
console.log(`watched wallets: ${wallets.length}`);
