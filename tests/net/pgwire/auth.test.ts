//
// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
//
// Copyright (c) 2023-2026 Cognica, Inc.
//

// Unit tests for authentication methods.

import { createHash, createHmac, pbkdf2Sync } from "node:crypto";
import { describe, it, expect } from "vitest";
import {
  CleartextAuthenticator,
  MD5Authenticator,
  ScramSHA256Authenticator,
  TrustAuthenticator,
  createAuthenticator,
} from "../../../src/net/pgwire/auth.js";
import { InvalidPassword } from "../../../src/net/pgwire/errors.js";

describe("TestTrustAuth", () => {
  it("test_trust_immediate_success", () => {
    const auth = new TrustAuthenticator("alice", null);
    const [response, done] = auth.initial();
    expect(done).toBe(true);
    expect(response).toEqual(Buffer.alloc(0));
  });
});

describe("TestCleartextAuth", () => {
  it("test_cleartext_success", () => {
    const auth = new CleartextAuthenticator("alice", "secret123");
    const [response, done] = auth.initial();
    expect(done).toBe(false);
    expect(response.length).toBeGreaterThan(0); // AuthenticationCleartextPassword

    const [response2, done2] = auth.step(Buffer.from("secret123\x00"));
    expect(done2).toBe(true);
    // response2 is authOk or empty
    void response2;
  });

  it("test_cleartext_wrong_password", () => {
    const auth = new CleartextAuthenticator("alice", "secret123");
    auth.initial();

    expect(() => auth.step(Buffer.from("wrong\x00"))).toThrow(InvalidPassword);
  });

  it("test_cleartext_no_password_configured", () => {
    const auth = new CleartextAuthenticator("alice", null);
    auth.initial();

    expect(() => auth.step(Buffer.from("anything\x00"))).toThrow(InvalidPassword);
  });
});

describe("TestMD5Auth", () => {
  it("test_md5_success", () => {
    const auth = new MD5Authenticator("alice", "secret123");
    const [response, done] = auth.initial();
    expect(done).toBe(false);
    // Extract salt from the response.
    // Response format: R(1) + length(4) + type=5(4) + salt(4) = 13 bytes
    const salt = response.subarray(9, 13);

    // Compute the expected MD5 hash.
    const inner = createHash("md5").update("secret123alice").digest("hex");
    const outer =
      "md5" +
      createHash("md5")
        .update(inner + salt.toString("binary"), "binary")
        .digest("hex");

    const [, done2] = auth.step(
      Buffer.concat([Buffer.from(outer, "utf-8"), Buffer.from([0x00])]),
    );
    expect(done2).toBe(true);
  });

  it("test_md5_wrong_password", () => {
    const auth = new MD5Authenticator("alice", "secret123");
    auth.initial();

    expect(() => auth.step(Buffer.from("md5wrong\x00"))).toThrow(InvalidPassword);
  });
});

describe("TestScramSHA256Auth", () => {
  it("test_scram_full_flow", () => {
    const auth = new ScramSHA256Authenticator("alice", "secret123");

    // Step 1: Initial -- server sends AuthenticationSASL.
    const [response1, done1] = auth.initial();
    expect(done1).toBe(false);
    expect(response1.includes(Buffer.from("SCRAM-SHA-256"))).toBe(true);

    // Step 2: Client sends client-first-message.
    const clientNonce = "rOprNGfwEbeRWgbNEkqO";
    const clientFirstBare = `n=alice,r=${clientNonce}`;
    const clientFirstMsg = `n,,${clientFirstBare}`;

    const [response2, done2] = auth.step(Buffer.from(clientFirstMsg, "utf-8"));
    expect(done2).toBe(false);

    // Parse server-first-message from the SASL continue response.
    // Skip the 'R' header: R(1) + length(4) + type=11(4) = 9 bytes
    const serverFirst = response2.subarray(9).toString("utf-8");
    const serverAttrs: Record<string, string> = {};
    for (const part of serverFirst.split(",")) {
      if (part.includes("=")) {
        const key = part[0]!;
        const val = part.substring(2);
        serverAttrs[key] = val;
      }
    }

    const combinedNonce = serverAttrs["r"]!;
    const salt = Buffer.from(serverAttrs["s"]!, "base64");
    const iterations = parseInt(serverAttrs["i"]!, 10);

    expect(combinedNonce.startsWith(clientNonce)).toBe(true);
    expect(salt.length).toBeGreaterThan(0);
    expect(iterations).toBeGreaterThan(0);

    // Step 3: Client computes proof and sends client-final-message.
    const password = "secret123"; // Already NFC normalized for ASCII
    const saltedPassword = pbkdf2Sync(password, salt, iterations, 32, "sha256");
    const clientKey = createHmac("sha256", saltedPassword)
      .update("Client Key")
      .digest();
    const storedKey = createHash("sha256").update(clientKey).digest();
    const serverKey = createHmac("sha256", saltedPassword)
      .update("Server Key")
      .digest();

    const channelBinding = Buffer.from("n,,").toString("base64");
    const clientFinalWithoutProof = `c=${channelBinding},r=${combinedNonce}`;
    const authMessage = `${clientFirstBare},${serverFirst},${clientFinalWithoutProof}`;
    const clientSignature = createHmac("sha256", storedKey)
      .update(authMessage)
      .digest();
    const clientProof = Buffer.alloc(clientKey.length);
    for (let i = 0; i < clientKey.length; i++) {
      clientProof[i] = clientKey[i]! ^ clientSignature[i]!;
    }
    const proofB64 = clientProof.toString("base64");

    const clientFinal = `${clientFinalWithoutProof},p=${proofB64}`;

    const [response3, done3] = auth.step(Buffer.from(clientFinal, "utf-8"));
    expect(done3).toBe(true);

    // Verify server signature.
    const serverSigExpected = createHmac("sha256", serverKey)
      .update(authMessage)
      .digest();
    const serverFinal = response3.subarray(9).toString("utf-8");
    expect(serverFinal.startsWith("v=")).toBe(true);
    const serverSigReceived = Buffer.from(serverFinal.substring(2), "base64");
    expect(serverSigReceived).toEqual(serverSigExpected);
  });
});

describe("TestCreateAuthenticator", () => {
  it("test_create_trust", () => {
    const auth = createAuthenticator("trust", "alice", null);
    expect(auth).toBeInstanceOf(TrustAuthenticator);
  });

  it("test_create_cleartext", () => {
    const auth = createAuthenticator("password", "alice", { alice: "pw" });
    expect(auth).toBeInstanceOf(CleartextAuthenticator);
  });

  it("test_create_md5", () => {
    const auth = createAuthenticator("md5", "alice", { alice: "pw" });
    expect(auth).toBeInstanceOf(MD5Authenticator);
  });

  it("test_create_scram", () => {
    const auth = createAuthenticator("scram-sha-256", "alice", {
      alice: "pw",
    });
    expect(auth).toBeInstanceOf(ScramSHA256Authenticator);
  });

  it("test_unknown_method", () => {
    expect(() => createAuthenticator("kerberos", "alice", null)).toThrow(/[Uu]nknown/);
  });
});
