import { Mode, modeMaySend } from "@solbot/domain";

/**
 * Environment validation. PAPER/DEMO/SHADOW must not even *see* a signing secret:
 * if one is present in the process environment, start-up is aborted (brief §2).
 */
const SECRET_PATTERNS: readonly RegExp[] = [
  /PRIVATE_?KEY/i,
  /SECRET_?KEY/i,
  /SEED_?PHRASE/i,
  /MNEMONIC/i,
  /KEYPAIR/i,
  /^SIGNER_/i,
  /WALLET_?SECRET/i,
];

export class ConfigurationError extends Error {
  constructor(
    public readonly code: string,
    message: string,
  ) {
    super(message);
  }
}

export function findSignerSecrets(env: Record<string, string | undefined>): string[] {
  return Object.keys(env)
    .filter((k) => env[k] !== undefined && env[k] !== "")
    .filter((k) => SECRET_PATTERNS.some((p) => p.test(k)))
    .sort();
}

export interface RuntimeEnv {
  mode: Mode;
  liveEnabled: false;
  jupiterApiKey: string | null;
  heliusApiKey: string | null;
  databaseUrl: string | null;
  heliusWebhookAuth: string | null;
  telegramBotToken: string | null;
  telegramChatId: string | null;
  /** Solana RPC: Helius when HELIUS_API_KEY is set, otherwise SOLANA_RPC_URL or the public mainnet endpoint (no key). */
  rpc: { url: string; kind: "helius" | "public"; supportsDas: boolean };
}

export const PUBLIC_SOLANA_RPC = "https://api.mainnet-beta.solana.com";

export function loadRuntimeEnv(env: Record<string, string | undefined> = process.env): RuntimeEnv {
  const modeRaw = env.MODE ?? Mode.PAPER;
  if (!Object.values(Mode).includes(modeRaw as Mode)) {
    throw new ConfigurationError("INVALID_MODE", `MODE=${modeRaw} is not a known mode`);
  }
  const mode = modeRaw as Mode;

  if (modeMaySend(mode)) {
    // Stage F is not implemented; there is no approval flow in this build.
    throw new ConfigurationError("LIVE_NOT_AVAILABLE", `MODE=${mode} is not available in this build (LIVE disabled)`);
  }
  if (env.LIVE_ENABLED !== undefined && env.LIVE_ENABLED !== "false") {
    throw new ConfigurationError("LIVE_NOT_AVAILABLE", "LIVE_ENABLED must be false in this build");
  }

  const secrets = findSignerSecrets(env);
  if (secrets.length > 0) {
    throw new ConfigurationError(
      "SIGNER_SECRET_IN_PAPER",
      `refusing to start ${mode}: signing secret-like variables present in environment: ${secrets.join(", ")}`,
    );
  }

  const opt = (k: string): string | null => {
    const v = env[k];
    return v === undefined || v === "" ? null : v;
  };

  return {
    mode,
    liveEnabled: false,
    jupiterApiKey: opt("JUPITER_API_KEY"),
    heliusApiKey: opt("HELIUS_API_KEY"),
    databaseUrl: opt("DATABASE_URL"),
    heliusWebhookAuth: opt("HELIUS_WEBHOOK_AUTH"),
    telegramBotToken: opt("TELEGRAM_BOT_TOKEN"),
    telegramChatId: opt("TELEGRAM_CHAT_ID"),
    rpc: opt("HELIUS_API_KEY")
      ? { url: `https://mainnet.helius-rpc.com/?api-key=${opt("HELIUS_API_KEY")}`, kind: "helius", supportsDas: true }
      : { url: opt("SOLANA_RPC_URL") ?? PUBLIC_SOLANA_RPC, kind: "public", supportsDas: false },
  };
}

/** Redacts API keys that providers put in URLs (e.g. Helius `?api-key=`). */
export function redactUrl(url: string): string {
  return url.replace(/([?&](?:api[-_]?key|key|token)=)[^&#]+/gi, "$1[REDACTED]");
}

export function redactSecrets(text: string, secrets: readonly (string | null)[]): string {
  let out = text;
  for (const s of secrets) {
    if (s && s.length >= 6) out = out.split(s).join("[REDACTED]");
  }
  return redactUrl(out);
}
