import { createHash, randomBytes } from "node:crypto";

/** Local identifiers. Paper fills use the `paper_` prefix so they never look like a chain signature. */
export type IdPrefix = "ses" | "int" | "att" | "q" | "paper" | "pos" | "ltx" | "res" | "sig" | "job" | "evt" | "rep" | "alr";

export function newId(prefix: IdPrefix): string {
  const time = Date.now().toString(36).padStart(9, "0");
  return `${prefix}_${time}${randomBytes(8).toString("hex")}`;
}

export function sha256Hex(data: string | Uint8Array): string {
  return createHash("sha256").update(data).digest("hex");
}

/** Canonical JSON: sorted keys, bigint as decimal string. Used for hashes of configs and payloads. */
export function canonicalJson(value: unknown): string {
  return JSON.stringify(sortKeys(value));
}

function sortKeys(value: unknown): unknown {
  if (typeof value === "bigint") return value.toString();
  if (Array.isArray(value)) return value.map(sortKeys);
  if (value !== null && typeof value === "object") {
    if (value instanceof Date) return value.toISOString();
    const out: Record<string, unknown> = {};
    for (const key of Object.keys(value as Record<string, unknown>).sort()) {
      out[key] = sortKeys((value as Record<string, unknown>)[key]);
    }
    return out;
  }
  return value;
}
