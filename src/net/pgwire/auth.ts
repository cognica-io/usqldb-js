// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
// Copyright (c) 2023-2026 Cognica, Inc.

// PostgreSQL authentication methods.
//
// Supports trust, cleartext password, MD5 password, and SCRAM-SHA-256
// authentication, matching the methods available in PostgreSQL 17.
//
// Each authenticator is a stateful, per-connection object.  The
// step() method drives the authentication handshake one message at
// a time.
//
// Usage from the connection handler:
//
//     const auth = createAuthenticator(method, username, credentials);
//     let [response, done] = auth.initial();
//     send(response);
//     while (!done) {
//         const clientData = await readAuthMessage();
//         [response, done] = auth.step(clientData);
//         send(response);
//     }
//     send(encodeAuthOk());

import * as crypto from "node:crypto";

import { MessageCodec } from "./message-codec.js";
import { InvalidPassword } from "./errors.js";

export enum AuthMethod {
  TRUST = "trust",
  CLEARTEXT = "password",
  MD5 = "md5",
  SCRAM_SHA_256 = "scram-sha-256",
}

// ======================================================================
// Authenticator base
// ======================================================================

export abstract class Authenticator {
  protected readonly _username: string;
  protected readonly _password: string | null;

  constructor(username: string, password: string | null) {
    this._username = username;
    this._password = password;
  }

  /** Produce the first server authentication message. */
  abstract initial(): [Buffer, boolean];

  /** Process a client authentication message. */
  abstract step(data: Buffer): [Buffer, boolean];
}

// ======================================================================
// Trust
// ======================================================================

export class TrustAuthenticator extends Authenticator {
  initial(): [Buffer, boolean] {
    return [Buffer.alloc(0), true];
  }

  step(_data: Buffer): [Buffer, boolean] {
    return [Buffer.alloc(0), true];
  }
}

// ======================================================================
// Cleartext password
// ======================================================================

export class CleartextAuthenticator extends Authenticator {
  initial(): [Buffer, boolean] {
    return [MessageCodec.encodeAuthCleartext(), false];
  }

  step(data: Buffer): [Buffer, boolean] {
    // data is the raw payload of the PasswordMessage ('p').
    // Extract the null-terminated password string.
    let received: string;
    if (data.length > 0 && data[data.length - 1] === 0) {
      received = data.subarray(0, data.length - 1).toString("utf-8");
    } else {
      received = data.toString("utf-8");
    }

    if (this._password === null || !timingSafeEqual(received, this._password)) {
      throw new InvalidPassword(
        `password authentication failed for user "${this._username}"`,
      );
    }
    return [Buffer.alloc(0), true];
  }
}

// ======================================================================
// MD5 password
// ======================================================================

export class MD5Authenticator extends Authenticator {
  private readonly _salt: Buffer;

  constructor(username: string, password: string | null) {
    super(username, password);
    this._salt = crypto.randomBytes(4);
  }

  initial(): [Buffer, boolean] {
    return [MessageCodec.encodeAuthMD5(this._salt), false];
  }

  step(data: Buffer): [Buffer, boolean] {
    let received: string;
    if (data.length > 0 && data[data.length - 1] === 0) {
      received = data.subarray(0, data.length - 1).toString("utf-8");
    } else {
      received = data.toString("utf-8");
    }

    if (this._password === null) {
      throw new InvalidPassword(
        `password authentication failed for user "${this._username}"`,
      );
    }

    // Compute expected hash: md5(md5(password + username) + salt)
    const inner = crypto
      .createHash("md5")
      .update(this._password + this._username, "utf-8")
      .digest("hex");
    const expected =
      "md5" +
      crypto
        .createHash("md5")
        .update(Buffer.concat([Buffer.from(inner, "utf-8"), this._salt]))
        .digest("hex");

    if (!timingSafeEqual(received, expected)) {
      throw new InvalidPassword(
        `password authentication failed for user "${this._username}"`,
      );
    }
    return [Buffer.alloc(0), true];
  }
}

// ======================================================================
// SCRAM-SHA-256
// ======================================================================

export class ScramSHA256Authenticator extends Authenticator {
  private static readonly _ITERATIONS = 4096; // PostgreSQL 17 default

  private _serverNonce = "";
  private _combinedNonce = "";
  private _salt: Buffer = Buffer.alloc(0);
  private _clientFirstBare = "";
  private _serverFirst = "";
  private _storedKey: Buffer = Buffer.alloc(0);
  private _serverKey: Buffer = Buffer.alloc(0);
  _phase = 0;

  initial(): [Buffer, boolean] {
    return [MessageCodec.encodeAuthSASL(["SCRAM-SHA-256"]), false];
  }

  step(data: Buffer): [Buffer, boolean] {
    if (this._phase === 0) {
      return this._handleClientFirst(data);
    }
    if (this._phase === 1) {
      return this._handleClientFinal(data);
    }
    throw new InvalidPassword(
      `password authentication failed for user "${this._username}"`,
    );
  }

  private _handleClientFirst(data: Buffer): [Buffer, boolean] {
    const clientFirstMsg = data.toString("utf-8");

    // Parse gs2-header and client-first-bare.
    // Expected format: "n,,n=<user>,r=<client_nonce>"
    const parts = clientFirstMsg.split(",");
    if (parts.length < 3) {
      throw new InvalidPassword(
        `password authentication failed for user "${this._username}"`,
      );
    }

    const gs2CbindFlag = parts[0]!;
    // We only support "n" (no channel binding) and "y" (client supports
    // but not using).
    if (gs2CbindFlag !== "n" && gs2CbindFlag !== "y") {
      throw new InvalidPassword(
        `password authentication failed for user "${this._username}"`,
      );
    }

    // parts[1] is the authzid (empty for PostgreSQL)
    // Everything after the gs2 header (first two comma-separated parts)
    const firstComma = clientFirstMsg.indexOf(",");
    const secondComma = clientFirstMsg.indexOf(",", firstComma + 1);
    this._clientFirstBare = clientFirstMsg.slice(secondComma + 1);

    // Extract client nonce from client-first-bare.
    const attrs: Record<string, string> = {};
    for (const attr of this._clientFirstBare.split(",")) {
      if (attr.includes("=")) {
        const key = attr[0]!;
        const val = attr.slice(2);
        attrs[key] = val;
      }
    }

    const clientNonce = attrs["r"] ?? "";
    if (!clientNonce) {
      throw new InvalidPassword(
        `password authentication failed for user "${this._username}"`,
      );
    }

    // Generate server nonce and derive keys.
    this._serverNonce = crypto.randomBytes(24).toString("base64");
    this._combinedNonce = clientNonce + this._serverNonce;

    if (this._password === null) {
      throw new InvalidPassword(
        `password authentication failed for user "${this._username}"`,
      );
    }

    // Derive SCRAM keys from password.
    this._salt = crypto.randomBytes(16);
    const saltedPassword = crypto.pbkdf2Sync(
      saslprep(this._password),
      this._salt,
      ScramSHA256Authenticator._ITERATIONS,
      32,
      "sha256",
    );
    const clientKey = hmacSHA256(saltedPassword, Buffer.from("Client Key"));
    this._storedKey = sha256(clientKey);
    this._serverKey = hmacSHA256(saltedPassword, Buffer.from("Server Key"));

    // Build server-first-message.
    const saltB64 = this._salt.toString("base64");
    this._serverFirst = `r=${this._combinedNonce},s=${saltB64},i=${ScramSHA256Authenticator._ITERATIONS}`;

    this._phase = 1;
    return [
      MessageCodec.encodeAuthSASLContinue(Buffer.from(this._serverFirst, "utf-8")),
      false,
    ];
  }

  private _handleClientFinal(data: Buffer): [Buffer, boolean] {
    const clientFinalMsg = data.toString("utf-8");

    // Parse client-final-message: c=<channel_binding>,r=<nonce>,p=<proof>
    const attrs: Record<string, string> = {};
    for (const part of clientFinalMsg.split(",")) {
      if (part.includes("=")) {
        const key = part[0]!;
        const val = part.slice(2);
        attrs[key] = val;
      }
    }

    // Verify nonce.
    if (attrs["r"] !== this._combinedNonce) {
      throw new InvalidPassword(
        `password authentication failed for user "${this._username}"`,
      );
    }

    // Verify channel binding: must be base64("n,,") = "biws"
    const cb = attrs["c"] ?? "";
    const expectedCb = Buffer.from("n,,").toString("base64");
    if (cb !== expectedCb) {
      // Also accept base64("y,,") for clients that support but
      // don't use channel binding.
      const expectedCbY = Buffer.from("y,,").toString("base64");
      if (cb !== expectedCbY) {
        throw new InvalidPassword(
          `password authentication failed for user "${this._username}"`,
        );
      }
    }

    // Extract client proof.
    const clientProofB64 = attrs["p"] ?? "";
    if (!clientProofB64) {
      throw new InvalidPassword(
        `password authentication failed for user "${this._username}"`,
      );
    }
    const clientProof = Buffer.from(clientProofB64, "base64");

    // Build client-final-without-proof (everything before ",p=...").
    const proofIdx = clientFinalMsg.lastIndexOf(",p=");
    const clientFinalWithoutProof = clientFinalMsg.slice(0, proofIdx);

    // AuthMessage = client-first-bare + "," + server-first + "," +
    //               client-final-without-proof
    const authMessage = `${this._clientFirstBare},${this._serverFirst},${clientFinalWithoutProof}`;
    const authMessageBytes = Buffer.from(authMessage, "utf-8");

    // Verify ClientProof.
    const clientSignature = hmacSHA256(this._storedKey, authMessageBytes);
    const recoveredKey = xorBytes(clientProof, clientSignature);
    if (!sha256(recoveredKey).equals(this._storedKey)) {
      throw new InvalidPassword(
        `password authentication failed for user "${this._username}"`,
      );
    }

    // Compute ServerSignature.
    const serverSignature = hmacSHA256(this._serverKey, authMessageBytes);
    const serverFinal = "v=" + serverSignature.toString("base64");

    this._phase = 2;
    return [MessageCodec.encodeAuthSASLFinal(Buffer.from(serverFinal, "utf-8")), true];
  }
}

// ======================================================================
// SCRAM helper functions
// ======================================================================

function hmacSHA256(key: Buffer, msg: Buffer): Buffer {
  return crypto.createHmac("sha256", key).update(msg).digest();
}

function sha256(data: Buffer): Buffer {
  return crypto.createHash("sha256").update(data).digest();
}

function xorBytes(a: Buffer, b: Buffer): Buffer {
  const result = Buffer.alloc(a.length);
  for (let i = 0; i < a.length; i++) {
    result[i] = (a[i]! ^ b[i]!) & 0xff;
  }
  return result;
}

function saslprep(password: string): string {
  // Minimal SASLprep normalization (NFC) for ASCII-safe passwords.
  // Full SASLprep (RFC 4013) requires a complete Unicode profile.
  // PostgreSQL clients typically send ASCII or NFC-normalized passwords.
  // This covers the common case.
  return password.normalize("NFC");
}

/** Timing-safe string comparison to prevent timing attacks. */
function timingSafeEqual(a: string, b: string): boolean {
  const bufA = Buffer.from(a, "utf-8");
  const bufB = Buffer.from(b, "utf-8");
  if (bufA.length !== bufB.length) {
    // Compare against self to maintain constant time, then return false
    crypto.timingSafeEqual(bufA, bufA);
    return false;
  }
  return crypto.timingSafeEqual(bufA, bufB);
}

// ======================================================================
// Factory
// ======================================================================

export function createAuthenticator(
  method: string,
  username: string,
  credentials: Record<string, string> | null,
): Authenticator {
  const password = credentials?.[username] ?? null;

  if (method === AuthMethod.TRUST) {
    return new TrustAuthenticator(username, password);
  }
  if (method === AuthMethod.CLEARTEXT) {
    return new CleartextAuthenticator(username, password);
  }
  if (method === AuthMethod.MD5) {
    return new MD5Authenticator(username, password);
  }
  if (method === AuthMethod.SCRAM_SHA_256) {
    return new ScramSHA256Authenticator(username, password);
  }

  throw new Error(`Unknown authentication method: '${method}'`);
}
