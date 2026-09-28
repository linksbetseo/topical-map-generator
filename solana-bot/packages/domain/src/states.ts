/** Operating modes. Only LIVE_CANARY / LIVE may ever send transactions (Stage F, separate approval). */
export const Mode = {
  DEMO: "DEMO",
  PAPER: "PAPER",
  SHADOW: "SHADOW",
  LIVE_CANARY: "LIVE_CANARY",
  LIVE: "LIVE",
} as const;
export type Mode = (typeof Mode)[keyof typeof Mode];

export function modeMaySend(mode: Mode): boolean {
  return mode === Mode.LIVE_CANARY || mode === Mode.LIVE;
}

export const SessionKind = {
  CONFLUENCE: "CONFLUENCE",
  /** Infrastructure test: runs the pipeline but its report never scores confluence. */
  INFRA_TEST: "INFRA_TEST",
  DEMO: "DEMO",
} as const;
export type SessionKind = (typeof SessionKind)[keyof typeof SessionKind];

export const SessionState = {
  DRAFT: "DRAFT",
  VALIDATING: "VALIDATING",
  BOOTSTRAP: "BOOTSTRAP",
  INSUFFICIENT_DATA: "INSUFFICIENT_DATA",
  READY: "READY",
  RUNNING: "RUNNING",
  EXIT_ONLY: "EXIT_ONLY",
  PAUSED_DATA: "PAUSED_DATA",
  HALTED_RISK: "HALTED_RISK",
  SETTLING: "SETTLING",
  COMPLETED: "COMPLETED",
  INCOMPLETE: "INCOMPLETE",
} as const;
export type SessionState = (typeof SessionState)[keyof typeof SessionState];

const S = SessionState;

/** Allowed session transitions. Anything else is a bug and is rejected. */
export const SESSION_TRANSITIONS: Readonly<Record<SessionState, readonly SessionState[]>> = {
  DRAFT: [S.VALIDATING],
  VALIDATING: [S.BOOTSTRAP, S.DRAFT],
  BOOTSTRAP: [S.READY, S.INSUFFICIENT_DATA],
  INSUFFICIENT_DATA: [S.BOOTSTRAP],
  READY: [S.RUNNING, S.BOOTSTRAP],
  RUNNING: [S.EXIT_ONLY, S.PAUSED_DATA, S.HALTED_RISK, S.SETTLING],
  // EXIT_ONLY returns to RUNNING only on a new UTC day after reconciliation (enforced by controller).
  EXIT_ONLY: [S.RUNNING, S.PAUSED_DATA, S.HALTED_RISK, S.SETTLING],
  PAUSED_DATA: [S.RUNNING, S.EXIT_ONLY, S.HALTED_RISK, S.SETTLING],
  // HALTED_RISK never resumes automatically; it only settles at T_end.
  HALTED_RISK: [S.SETTLING],
  SETTLING: [S.COMPLETED, S.INCOMPLETE],
  COMPLETED: [],
  INCOMPLETE: [],
};

/** States in which a new entry may be considered at all (further gated by risk). */
export function sessionAllowsEntries(state: SessionState): boolean {
  return state === S.RUNNING;
}

/** States in which open positions are still actively managed (exits allowed). */
export function sessionManagesPositions(state: SessionState): boolean {
  return state === S.RUNNING || state === S.EXIT_ONLY || state === S.PAUSED_DATA || state === S.HALTED_RISK || state === S.SETTLING;
}

export class IllegalTransitionError extends Error {
  constructor(kind: string, from: string, to: string) {
    super(`illegal ${kind} transition ${from} -> ${to}`);
  }
}

export function assertSessionTransition(from: SessionState, to: SessionState): void {
  if (!SESSION_TRANSITIONS[from].includes(to)) throw new IllegalTransitionError("session", from, to);
}

export const OrderState = {
  INTENT: "INTENT",
  VALIDATED: "VALIDATED",
  RESERVED: "RESERVED",
  QUOTED: "QUOTED",
  BUILT: "BUILT",
  POLICY_APPROVED: "POLICY_APPROVED",
  SIGNED: "SIGNED",
  SUBMITTED: "SUBMITTED",
  CONFIRMED: "CONFIRMED",
  FINALIZED: "FINALIZED",
  RECONCILED: "RECONCILED",
  // paper terminal success: simulated fill booked, no chain involvement
  PAPER_FILLED: "PAPER_FILLED",
  // branches
  REJECTED: "REJECTED",
  EXPIRED: "EXPIRED",
  FAILED_ONCHAIN: "FAILED_ONCHAIN",
  /** Paper: attempt reached the modeled "sent" phase and failed (min_out, modeled failure). */
  FAILED_PAPER: "FAILED_PAPER",
  STATUS_UNKNOWN: "STATUS_UNKNOWN",
  CANCELLED_BEFORE_SEND: "CANCELLED_BEFORE_SEND",
} as const;
export type OrderState = (typeof OrderState)[keyof typeof OrderState];

const O = OrderState;

export const ORDER_TRANSITIONS: Readonly<Record<OrderState, readonly OrderState[]>> = {
  INTENT: [O.VALIDATED, O.REJECTED],
  VALIDATED: [O.RESERVED, O.REJECTED],
  RESERVED: [O.QUOTED, O.CANCELLED_BEFORE_SEND, O.EXPIRED],
  // Paper path: QUOTED -> PAPER_FILLED | FAILED_PAPER | CANCELLED_BEFORE_SEND
  QUOTED: [O.BUILT, O.PAPER_FILLED, O.FAILED_PAPER, O.CANCELLED_BEFORE_SEND, O.EXPIRED],
  BUILT: [O.POLICY_APPROVED, O.CANCELLED_BEFORE_SEND, O.EXPIRED],
  POLICY_APPROVED: [O.SIGNED, O.CANCELLED_BEFORE_SEND, O.EXPIRED],
  SIGNED: [O.SUBMITTED, O.STATUS_UNKNOWN, O.EXPIRED],
  SUBMITTED: [O.CONFIRMED, O.FAILED_ONCHAIN, O.STATUS_UNKNOWN, O.EXPIRED],
  CONFIRMED: [O.FINALIZED, O.STATUS_UNKNOWN],
  FINALIZED: [O.RECONCILED],
  RECONCILED: [],
  PAPER_FILLED: [],
  REJECTED: [],
  EXPIRED: [],
  FAILED_ONCHAIN: [],
  FAILED_PAPER: [],
  // Unknown status is resolved only by reading chain state, never by resending a new trade.
  STATUS_UNKNOWN: [O.CONFIRMED, O.FAILED_ONCHAIN, O.EXPIRED],
  CANCELLED_BEFORE_SEND: [],
};

export function assertOrderTransition(from: OrderState, to: OrderState): void {
  if (!ORDER_TRANSITIONS[from].includes(to)) throw new IllegalTransitionError("order", from, to);
}

export function orderIsTerminal(state: OrderState): boolean {
  return ORDER_TRANSITIONS[state].length === 0;
}

/** An unresolved order keeps capital reserved and blocks new entries. */
export function orderIsUnresolved(state: OrderState): boolean {
  return !orderIsTerminal(state);
}

export const PositionStatus = {
  OPEN: "OPEN",
  EXITING: "EXITING",
  CLOSED: "CLOSED",
} as const;
export type PositionStatus = (typeof PositionStatus)[keyof typeof PositionStatus];

/** Valuation status of an open position. */
export const ValuationStatus = {
  FRESH: "FRESH",
  STALE: "STALE",
  /** API healthy but no sell route: conservative value 0. */
  UNLIQUIDATABLE: "UNLIQUIDATABLE",
  /** Provider outage: value unknown; lower bound 0, but no loss is booked. */
  UNKNOWN_VALUATION: "UNKNOWN_VALUATION",
} as const;
export type ValuationStatus = (typeof ValuationStatus)[keyof typeof ValuationStatus];
