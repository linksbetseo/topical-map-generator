import { describe, expect, it } from "vitest";
import { ConfigurationError, configHash, effectiveJupiterRps, findSignerSecrets, jupiterBudget, loadRuntimeEnv, parseConfig, redactUrl } from "../src/index.ts";

describe("config defaults follow the brief", () => {
  const c = parseConfig();
  it("capital and sizing", () => {
    expect(c.capital.initial_total_usd).toBe("500");
    expect(c.capital.initial_sol_value_usd).toBe("20");
    expect(c.sizing.max_position_usd).toBe("25");
    expect(c.sizing.max_position_equity_bps).toBe(500);
    expect(c.sizing.max_open_positions).toBe(3); // owner decision (brief: 4)
    expect(c.sizing.max_exposure_equity_bps).toBe(2_000);
    expect(c.sizing.max_entry_attempts_per_utc_day).toBe(8);
    expect(c.sizing.max_entry_notional_per_utc_day_usd).toBe("100"); // owner decision (brief: 200)
  });
  it("risk", () => {
    expect(c.risk.entry_slippage_bps).toBe(100);
    expect(c.risk.exit_slippage_bps).toBe(150);
    expect(c.risk.emergency_exit_slippage_bps).toBe(300);
    expect(c.exits.stop_loss_bps).toBe(1_000);
    expect(c.experiment.duration_hours).toBe(168);
  });
  it("paid plans cannot be enabled through config", () => {
    expect(() => parseConfig({ budget: { paid_plan_purchase_allowed: true } })).toThrow();
    expect(() => parseConfig({ budget: { auto_upgrade: true } })).toThrow();
  });
  it("hash is stable and changes with any decision parameter", () => {
    expect(configHash(parseConfig())).toBe(configHash(parseConfig({})));
    expect(configHash(parseConfig({ exits: { stop_loss_bps: 900 } }))).not.toBe(configHash(c));
  });
  it("rejects wrong units", () => {
    expect(() => parseConfig({ exits: { stop_loss_bps: 10.5 } })).toThrow();
    expect(() => parseConfig({ capital: { initial_total_usd: "500 USD" } })).toThrow();
  });
});

describe("runtime env safety", () => {
  it("defaults to PAPER with live disabled", () => {
    const env = loadRuntimeEnv({});
    expect(env.mode).toBe("PAPER");
    expect(env.liveEnabled).toBe(false);
  });

  it("aborts PAPER start when a signer secret is present", () => {
    for (const key of ["SIGNER_PRIVATE_KEY", "BS58_PRIVATE_KEY", "SOLANA_KEYPAIR_PATH", "WALLET_SEED_PHRASE", "MNEMONIC", "SIGNER_URL"]) {
      try {
        loadRuntimeEnv({ MODE: "PAPER", [key]: "x" });
        expect.unreachable(`${key} should abort`);
      } catch (e) {
        expect(e).toBeInstanceOf(ConfigurationError);
        expect((e as ConfigurationError).code).toBe("SIGNER_SECRET_IN_PAPER");
      }
    }
  });

  it("empty secret variable is not a secret", () => {
    expect(findSignerSecrets({ SIGNER_PRIVATE_KEY: "" })).toEqual([]);
  });

  it("LIVE modes and LIVE_ENABLED are refused in this build", () => {
    expect(() => loadRuntimeEnv({ MODE: "LIVE" })).toThrow(/LIVE disabled/);
    expect(() => loadRuntimeEnv({ MODE: "LIVE_CANARY" })).toThrow();
    expect(() => loadRuntimeEnv({ LIVE_ENABLED: "true" })).toThrow();
    expect(() => loadRuntimeEnv({ MODE: "YOLO" })).toThrow();
  });

  it("redacts api keys in URLs", () => {
    expect(redactUrl("https://mainnet.helius-rpc.com/?api-key=abc123&x=1")).toBe("https://mainnet.helius-rpc.com/?api-key=[REDACTED]&x=1");
  });
});

describe("keyless operation", () => {
  it("without HELIUS_API_KEY uses the public Solana RPC (no DAS); SOLANA_RPC_URL overrides", () => {
    expect(loadRuntimeEnv({}).rpc).toEqual({ url: "https://api.mainnet-beta.solana.com", kind: "public", supportsDas: false });
    expect(loadRuntimeEnv({ SOLANA_RPC_URL: "https://rpc.example" }).rpc.url).toBe("https://rpc.example");
    expect(loadRuntimeEnv({ HELIUS_API_KEY: "abc" }).rpc).toMatchObject({ kind: "helius", supportsDas: true });
  });

  it("keyless Jupiter is capped at 0.5 RPS and the budget check reflects it", () => {
    const def = parseConfig();
    expect(effectiveJupiterRps(def, false)).toBe(0.5);
    expect(effectiveJupiterRps(def, true)).toBe(1);
    expect(jupiterBudget(def, 0.5)).toMatchObject({ requiredPerMinute: 46, availablePerMinute: 24, ok: false });
    expect(jupiterBudget(def, 1)).toMatchObject({ requiredPerMinute: 46, availablePerMinute: 48, ok: true });
    expect(jupiterBudget(parseConfig({ sizing: { max_open_positions: 4 } }), 1).ok).toBe(false);
    expect(jupiterBudget(parseConfig({ sizing: { max_open_positions: 1 }, budget: { jupiter_rps: 0.5 } }), 0.5).ok).toBe(true);
  });
});
