import { describe, expect, it } from "vitest";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";

/**
 * Static guard (brief §2, §21): no production source file of this build may import key material
 * helpers or call signing / sending APIs. The read-only transport mentions blocked method names
 * as data; that file is the single allowed exception for those strings.
 */
const ROOT = fileURLToPath(new URL("../../../", import.meta.url));

function sources(dir: string): string[] {
  const out: string[] = [];
  for (const e of readdirSync(dir)) {
    if (e === "node_modules" || e === "test" || e === "dist") continue;
    const p = join(dir, e);
    const s = statSync(p);
    if (s.isDirectory()) out.push(...sources(p));
    else if (/\.(ts|tsx|js|mjs)$/.test(e)) out.push(p);
  }
  return out;
}

const FORBIDDEN: Array<[RegExp, string]> = [
  [/createKeyPairSignerFromBytes|createKeyPairFromBytes|generateKeyPairSigner/, "key pair creation"],
  [/\bKeypair\b/, "web3.js Keypair"],
  [/partiallySignTransaction|signTransaction\s*\(|signAllTransactions|\.sign\(\s*\[/, "transaction signing"],
  [/from\s+["'](?:bs58|@solana\/web3\.js|@solbot\/signer|tweetnacl|ed25519)/, "signing-related import"],
  [/["']sendTransaction["']|["']sendRawTransaction["']|["']sendBundle["']/, "send method literal"],
  [/\/swap\/v2\/execute/, "Jupiter execute path"],
];

const ALLOWED_FILES = new Set(["packages/providers/src/transport.ts"]);

describe("no signer / no send in production sources", () => {
  const files = [...sources(join(ROOT, "packages")), ...sources(join(ROOT, "apps"))];
  it("scans a non-trivial number of files", () => {
    expect(files.length).toBeGreaterThan(15);
  });
  for (const f of files) {
    const rel = relative(ROOT, f);
    if (ALLOWED_FILES.has(rel)) continue;
    it(rel, () => {
      const text = readFileSync(f, "utf8");
      for (const [re, what] of FORBIDDEN) expect(re.test(text), `${rel}: ${what}`).toBe(false);
    });
  }
});
