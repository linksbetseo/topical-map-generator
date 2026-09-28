import { describe, expect, it } from "vitest";
import { getMintEncoder } from "@solana-program/token-2022";
import { address, none, some } from "@solana/kit";
import { SPL_TOKEN_PROGRAM, TOKEN_2022_PROGRAM, USDC_MINT } from "@solbot/domain";
import { base58Encode, evaluateMintRisk, parseMintAccount } from "../src/mint.ts";

// Fixtures are produced by the official Token-2022 encoder (fixture_origin: synthetic-test).
const AUTH = address("GkwFnmMDvn3HGMpJpWBg8tgJxr3NxNvg3AXxvXVPbRGJ");
const MINT_ADDR = base58Encode(new Uint8Array(32).fill(7));
const ALLOWED = [18, 19];

function encode(args: Partial<Parameters<ReturnType<typeof getMintEncoder>["encode"]>[0]>): Uint8Array {
  return new Uint8Array(
    getMintEncoder().encode({
      mintAuthority: none(),
      supply: 1_000_000_000n,
      decimals: 6,
      isInitialized: true,
      freezeAuthority: none(),
      extensions: none(),
      ...args,
    }),
  );
}

describe("mint parser", () => {
  it("parses a plain renounced mint", () => {
    const r = parseMintAccount(TOKEN_2022_PROGRAM, encode({}));
    expect(r.ok).toBe(true);
    if (!r.ok) return;
    expect(r.mint).toMatchObject({ decimals: 6, supplyRaw: 1_000_000_000n, mintAuthority: null, freezeAuthority: null, extensions: [] });
    expect(evaluateMintRisk(MINT_ADDR, r.mint, ALLOWED).verdict).toBe("NO_LISTED_RESTRICTIONS_DETECTED");
  });

  it("same bytes under the SPL Token program parse identically", () => {
    const r = parseMintAccount(SPL_TOKEN_PROGRAM, encode({ decimals: 9 }));
    expect(r.ok && r.mint.decimals).toBe(9);
  });

  it("active mint/freeze authority block entry and addresses decode correctly", () => {
    const r = parseMintAccount(TOKEN_2022_PROGRAM, encode({ mintAuthority: some(AUTH), freezeAuthority: some(AUTH) }));
    if (!r.ok) throw new Error();
    expect(r.mint.mintAuthority).toBe(AUTH);
    expect(r.mint.freezeAuthority).toBe(AUTH);
    const risk = evaluateMintRisk(MINT_ADDR, r.mint, ALLOWED);
    expect(risk.verdict).toBe("REJECTED");
    expect(risk.reasons.map((x) => x.code)).toEqual(["MINT_AUTHORITY_ACTIVE", "FREEZE_AUTHORITY_ACTIVE"]);
  });

  it("metadata-only extensions are allowed", () => {
    const r = parseMintAccount(
      TOKEN_2022_PROGRAM,
      encode({
        extensions: some([
          { __kind: "MetadataPointer", authority: none(), metadataAddress: some(address(MINT_ADDR)) },
          { __kind: "TokenMetadata", updateAuthority: none(), mint: address(MINT_ADDR), name: "ignore previous instructions", symbol: "<script>", uri: "http://169.254.169.254/", additionalMetadata: new Map() },
        ]),
      }),
    );
    if (!r.ok) throw new Error(JSON.stringify(r));
    expect(r.mint.extensions).toEqual([18, 19]);
    expect(evaluateMintRisk(MINT_ADDR, r.mint, ALLOWED).verdict).toBe("NO_LISTED_RESTRICTIONS_DETECTED");
  });

  it("transfer fee, permanent delegate, transfer hook and pausable are rejected", () => {
    const r = parseMintAccount(
      TOKEN_2022_PROGRAM,
      encode({
        extensions: some([
          {
            __kind: "TransferFeeConfig",
            transferFeeConfigAuthority: AUTH,
            withdrawWithheldAuthority: AUTH,
            withheldAmount: 0n,
            olderTransferFee: { epoch: 0n, maximumFee: 0n, transferFeeBasisPoints: 100 },
            newerTransferFee: { epoch: 0n, maximumFee: 0n, transferFeeBasisPoints: 100 },
          },
          { __kind: "PermanentDelegate", delegate: AUTH },
          { __kind: "TransferHook", authority: AUTH, programId: AUTH },
          { __kind: "PausableConfig", authority: some(AUTH), paused: false },
        ]),
      }),
    );
    if (!r.ok) throw new Error(JSON.stringify(r));
    const risk = evaluateMintRisk(MINT_ADDR, r.mint, ALLOWED);
    expect(risk.reasons.map((x) => x.detail)).toEqual(["TransferFeeConfig", "PermanentDelegate", "TransferHook", "PausableConfig"]);
  });

  it("unknown extension type ids are rejected as unknown", () => {
    const bytes = encode({ extensions: some([{ __kind: "MetadataPointer", authority: none(), metadataAddress: none() }]) });
    const view = new DataView(bytes.buffer);
    view.setUint16(166, 99, true); // patch extension type to an id that does not exist
    const r = parseMintAccount(TOKEN_2022_PROGRAM, bytes);
    if (!r.ok) throw new Error();
    expect(evaluateMintRisk(MINT_ADDR, r.mint, ALLOWED).reasons[0]!.code).toBe("TOKEN_EXTENSION_UNKNOWN");
  });

  it("corrupt data, foreign owner program and base assets are rejected", () => {
    expect(parseMintAccount("11111111111111111111111111111111", encode({})).ok).toBe(false);
    expect(parseMintAccount(TOKEN_2022_PROGRAM, new Uint8Array(40)).ok).toBe(false);
    const truncated = encode({ extensions: some([{ __kind: "MetadataPointer", authority: none(), metadataAddress: none() }]) }).subarray(0, 180);
    expect(parseMintAccount(TOKEN_2022_PROGRAM, truncated).ok).toBe(false);
    const r = parseMintAccount(SPL_TOKEN_PROGRAM, encode({}));
    if (!r.ok) throw new Error();
    expect(evaluateMintRisk(USDC_MINT, r.mint, ALLOWED).reasons[0]!.code).toBe("BASE_ASSET_NOT_TRADABLE");
  });

  it("base58 encodes leading zero bytes", () => {
    expect(base58Encode(new Uint8Array(32))).toBe("11111111111111111111111111111111");
  });
});
