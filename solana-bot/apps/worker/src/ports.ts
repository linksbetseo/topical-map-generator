import type { Dec } from "@solbot/domain";
import type { FxSnapshot } from "@solbot/ledger";
import type { FlowEvent, HolderView, MintRiskView, TokenView, WalletStatus } from "@solbot/strategy";

/** Data ports of the engine. Implementations: live providers (PAPER) or fixtures (DEMO/tests). */
export interface MarketData {
  readonly source: "LIVE" | "FIXTURE";
  fx(now: Date): Promise<FxSnapshot>;
  tokenView(mint: string, now: Date): Promise<TokenView | null>;
  mintRisk(mint: string, now: Date): Promise<(MintRiskView & { decimals: number; tokenProgram: string }) | null>;
  holders(mint: string, now: Date): Promise<HolderView | null>;
  /** Rent for our token account for this mint (depends on token program / extensions); null = unknown. */
  rentLamports(mint: string): Promise<bigint | null>;
  deployerGroup(mint: string): Promise<string | null>;
}

export interface FlowStore {
  /** Confirmed flow events for a mint with block_time in [from, to] and available_at <= to. */
  events(mint: string, from: Date, to: Date): Promise<FlowEvent[]>;
  /** Wallet's quantity of a mint as known at `at` from confirmed swaps; null if unknown. */
  walletQty(wallet: string, mint: string, at: Date): Promise<bigint | null>;
}

export interface WalletBook {
  /** Frozen at T0 for the whole session (brief §6.2). */
  statuses(): ReadonlyMap<string, WalletStatus>;
  qualifiedCount(): number;
}

export type NotifyEvent =
  | { kind: "ENTRY"; mint: string; notionalUsd: Dec; fillId: string }
  | { kind: "EXIT"; mint: string; reason: string; pnlUsd: Dec; fillId: string }
  | { kind: "STATE"; from: string; to: string; reason: string | null }
  | { kind: "ALERT"; severity: "WARN" | "CRITICAL"; message: string }
  | { kind: "REPORT"; title: string; body: string };

export interface Notifier {
  notify(e: NotifyEvent): Promise<void>;
}

export const nullNotifier: Notifier = { notify: async () => undefined };
