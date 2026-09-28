export interface Clock {
  now(): Date;
  sleep(ms: number): Promise<void>;
}

export const systemClock: Clock = {
  now: () => new Date(),
  sleep: (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
};

/** Deterministic clock for tests: sleep advances time instantly. */
export class FakeClock implements Clock {
  private t: number;
  constructor(start: Date | string | number) {
    this.t = new Date(start).getTime();
  }
  now(): Date {
    return new Date(this.t);
  }
  async sleep(ms: number): Promise<void> {
    this.advance(ms);
  }
  advance(ms: number): void {
    if (ms < 0) throw new RangeError("cannot move time backwards");
    this.t += ms;
  }
  set(to: Date | string | number): void {
    const next = new Date(to).getTime();
    if (next < this.t) throw new RangeError("cannot move time backwards");
    this.t = next;
  }
}

export const HOUR_MS = 3_600_000;
export const MINUTE_MS = 60_000;
export const SECOND_MS = 1_000;

export function utcDayStart(at: Date): Date {
  return new Date(Date.UTC(at.getUTCFullYear(), at.getUTCMonth(), at.getUTCDate()));
}

export function utcDayKey(at: Date): string {
  return at.toISOString().slice(0, 10);
}

export function ageMs(from: Date, now: Date): number {
  return now.getTime() - from.getTime();
}

export function formatWarsaw(at: Date): string {
  return new Intl.DateTimeFormat("pl-PL", {
    timeZone: "Europe/Warsaw",
    dateStyle: "short",
    timeStyle: "medium",
  }).format(at);
}
