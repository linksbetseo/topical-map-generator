import type { Clock } from "@solbot/domain";

/**
 * Sliding-window limiter (60 s, per organisation — Jupiter limits are per org, not per key).
 * Priorities: exits and reconciliation always come first; low priorities may only use the
 * window up to `lowPriorityShare`, so a discovery burst can never starve position monitoring.
 */
export const Priority = { EXIT: 0, RECONCILE: 1, ENTRY: 2, ANALYTICS: 3, DISCOVERY: 4 } as const;
export type Priority = (typeof Priority)[keyof typeof Priority];

export class SlidingWindowLimiter {
  private readonly stamps: number[] = [];
  private blockedUntil = 0;

  constructor(
    private readonly clock: Clock,
    private readonly perMinute: number,
    private readonly lowPriorityShare = 0.6,
    private readonly windowMs = 60_000,
  ) {}

  private prune(now: number): void {
    while (this.stamps.length > 0 && this.stamps[0]! <= now - this.windowMs) this.stamps.shift();
  }

  private capacityFor(p: Priority): number {
    if (p <= Priority.ENTRY) return this.perMinute;
    return Math.max(1, Math.floor(this.perMinute * this.lowPriorityShare));
  }

  /** Non-blocking: returns false if the call would exceed the budget for this priority. */
  tryAcquire(p: Priority): boolean {
    const now = this.clock.now().getTime();
    this.prune(now);
    if (now < this.blockedUntil) return false;
    if (this.stamps.length >= this.capacityFor(p)) return false;
    this.stamps.push(now);
    return true;
  }

  /** Blocking acquire for high priorities; waits until a slot frees. */
  async acquire(p: Priority, maxWaitMs = 30_000): Promise<boolean> {
    const start = this.clock.now().getTime();
    while (!this.tryAcquire(p)) {
      const now = this.clock.now().getTime();
      if (now - start >= maxWaitMs) return false;
      const oldest = this.stamps[0] ?? now;
      const wait = Math.max(50, Math.min(oldest + this.windowMs - now, this.blockedUntil - now, 1_000));
      await this.clock.sleep(wait);
    }
    return true;
  }

  /** Feeds provider rate-limit headers back (x-ratelimit-remaining / x-ratelimit-reset, unix seconds). */
  observeHeaders(h: Record<string, string>): void {
    const remaining = h["x-ratelimit-remaining"];
    const reset = h["x-ratelimit-reset"];
    if (remaining !== undefined && Number(remaining) <= 0 && reset !== undefined && Number.isFinite(Number(reset))) {
      this.blockedUntil = Math.max(this.blockedUntil, Number(reset) * 1000);
    }
  }

  used(): number {
    this.prune(this.clock.now().getTime());
    return this.stamps.length;
  }
}
