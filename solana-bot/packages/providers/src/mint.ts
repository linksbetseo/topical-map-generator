import { BASE_ASSETS, ReasonCode, SPL_TOKEN_PROGRAM, TOKEN_2022_PROGRAM, reason, type Reason } from "@solbot/domain";

/**
 * Explicit mint parser (SPL Token + Token-2022 TLV). Decisions are made from on-chain bytes,
 * never from a token's name or a provider's "verified" flag.
 * Layout: COption<Pubkey> mint_authority (4+32), u64 supply, u8 decimals, bool is_initialized,
 * COption<Pubkey> freeze_authority (4+32) = 82 bytes. Token-2022: padding to 165, account type byte
 * (1 = Mint) at 165, then TLV entries (u16 type, u16 length, value).
 */

/** Token-2022 ExtensionType ids (verified against @solana-program/token-2022@0.19.0). */
export const TOKEN_2022_EXTENSION_NAMES: Readonly<Record<number, string>> = {
  0: "Uninitialized",
  1: "TransferFeeConfig",
  2: "TransferFeeAmount",
  3: "MintCloseAuthority",
  4: "ConfidentialTransferMint",
  5: "ConfidentialTransferAccount",
  6: "DefaultAccountState",
  7: "ImmutableOwner",
  8: "MemoTransfer",
  9: "NonTransferable",
  10: "InterestBearingConfig",
  11: "CpiGuard",
  12: "PermanentDelegate",
  13: "NonTransferableAccount",
  14: "TransferHook",
  15: "TransferHookAccount",
  16: "ConfidentialTransferFee",
  17: "ConfidentialTransferFeeAmount",
  18: "MetadataPointer",
  19: "TokenMetadata",
  20: "GroupPointer",
  21: "TokenGroup",
  22: "GroupMemberPointer",
  23: "TokenGroupMember",
  24: "ConfidentialMintBurn",
  25: "ScaledUiAmountConfig",
  26: "PausableConfig",
  27: "PausableAccount",
  28: "PermissionedBurn",
};

export interface ParsedMint {
  tokenProgram: string;
  decimals: number;
  supplyRaw: bigint;
  isInitialized: boolean;
  mintAuthority: string | null;
  freezeAuthority: string | null;
  extensions: number[];
  dataLength: number;
}

export type MintParseResult = { ok: true; mint: ParsedMint } | { ok: false; reason: Reason };

const B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

export function base58Encode(bytes: Uint8Array): string {
  let n = 0n;
  for (const b of bytes) n = (n << 8n) + BigInt(b);
  let out = "";
  while (n > 0n) {
    out = B58[Number(n % 58n)] + out;
    n /= 58n;
  }
  for (const b of bytes) {
    if (b !== 0) break;
    out = "1" + out;
  }
  return out;
}

const u32 = (d: Uint8Array, o: number) => new DataView(d.buffer, d.byteOffset, d.byteLength).getUint32(o, true);
const u16 = (d: Uint8Array, o: number) => new DataView(d.buffer, d.byteOffset, d.byteLength).getUint16(o, true);
const u64 = (d: Uint8Array, o: number) => new DataView(d.buffer, d.byteOffset, d.byteLength).getBigUint64(o, true);

function coptionPubkey(d: Uint8Array, o: number): { ok: true; value: string | null } | { ok: false } {
  const tag = u32(d, o);
  if (tag === 0) return { ok: true, value: null };
  if (tag === 1) return { ok: true, value: base58Encode(d.subarray(o + 4, o + 36)) };
  return { ok: false };
}

const invalid = (detail: string): MintParseResult => ({ ok: false, reason: reason(ReasonCode.MINT_DATA_INVALID, detail) });

export function parseMintAccount(ownerProgram: string, data: Uint8Array): MintParseResult {
  if (ownerProgram !== SPL_TOKEN_PROGRAM && ownerProgram !== TOKEN_2022_PROGRAM) {
    return { ok: false, reason: reason(ReasonCode.TOKEN_PROGRAM_UNSUPPORTED, ownerProgram) };
  }
  if (data.length < 82) return invalid(`length ${data.length} < 82`);
  const ma = coptionPubkey(data, 0);
  const fa = coptionPubkey(data, 46);
  if (!ma.ok || !fa.ok) return invalid("bad COption tag");
  const decimals = data[44]!;
  const isInitialized = data[45] === 1;
  if (data[45]! > 1) return invalid("bad is_initialized");
  if (!isInitialized) return invalid("mint not initialized");

  const extensions: number[] = [];
  if (ownerProgram === SPL_TOKEN_PROGRAM) {
    if (data.length !== 82) return invalid(`SPL mint length ${data.length} != 82`);
  } else if (data.length !== 82) {
    if (data.length < 166) return invalid(`Token-2022 mint with extensions too short: ${data.length}`);
    for (let i = 82; i < 165; i++) if (data[i] !== 0) return invalid("non-zero padding before account type");
    if (data[165] !== 1) return invalid(`account type ${data[165]} is not Mint`);
    let o = 166;
    while (o + 4 <= data.length) {
      const type = u16(data, o);
      const len = u16(data, o + 2);
      if (type === 0 && len === 0) break; // uninitialized tail
      if (o + 4 + len > data.length) return invalid(`TLV entry ${type} overruns account data`);
      extensions.push(type);
      o += 4 + len;
    }
  }
  return {
    ok: true,
    mint: {
      tokenProgram: ownerProgram,
      decimals,
      supplyRaw: u64(data, 36),
      isInitialized,
      mintAuthority: ma.value,
      freezeAuthority: fa.value,
      extensions,
      dataLength: data.length,
    },
  };
}

export interface MintRiskResult {
  /** Never "safe": at best, none of the listed restrictions were detected. */
  verdict: "NO_LISTED_RESTRICTIONS_DETECTED" | "REJECTED";
  reasons: Reason[];
  extensionNames: string[];
}

export function evaluateMintRisk(mintAddress: string, m: ParsedMint, allowedExtensions: readonly number[]): MintRiskResult {
  const reasons: Reason[] = [];
  if (BASE_ASSETS.has(mintAddress)) reasons.push(reason(ReasonCode.BASE_ASSET_NOT_TRADABLE));
  if (m.mintAuthority !== null) reasons.push(reason(ReasonCode.MINT_AUTHORITY_ACTIVE, m.mintAuthority));
  if (m.freezeAuthority !== null) reasons.push(reason(ReasonCode.FREEZE_AUTHORITY_ACTIVE, m.freezeAuthority));
  for (const ext of m.extensions) {
    const name = TOKEN_2022_EXTENSION_NAMES[ext];
    if (name === undefined) reasons.push(reason(ReasonCode.TOKEN_EXTENSION_UNKNOWN, `type ${ext}`));
    else if (!allowedExtensions.includes(ext)) reasons.push(reason(ReasonCode.TOKEN_EXTENSION_NOT_ALLOWED, name));
  }
  return {
    verdict: reasons.length === 0 ? "NO_LISTED_RESTRICTIONS_DETECTED" : "REJECTED",
    reasons,
    extensionNames: m.extensions.map((e) => TOKEN_2022_EXTENSION_NAMES[e] ?? `unknown(${e})`),
  };
}
