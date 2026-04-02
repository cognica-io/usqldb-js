// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
// Copyright (c) 2023-2026 Cognica, Inc.

// Convert between JavaScript values and PostgreSQL wire format.
//
// The TypeCodec provides static methods for encoding JavaScript
// values to PostgreSQL text or binary format, decoding wire bytes back
// to JavaScript values, and inferring PostgreSQL type OIDs from JavaScript types.
//
// Text format is the default for psql, psycopg2, JDBC.  Binary format
// is used exclusively by asyncpg and optionally by psycopg3.
//
// References:
//     https://www.postgresql.org/docs/17/protocol-overview.html#PROTOCOL-FORMAT-CODES

import { TYPE_LENGTHS, TYPE_OIDS } from "../../pg-compat/oid.js";

// PostgreSQL epoch: 2000-01-01 00:00:00 UTC
const PG_EPOCH_MS = Date.UTC(2000, 0, 1); // milliseconds since Unix epoch

// eslint-disable-next-line @typescript-eslint/no-extraneous-class
export class TypeCodec {
  // ==================================================================
  // Text format encoding (JS -> PG text bytes)
  // ==================================================================

  static encodeText(value: unknown, _typeOid: number = 0): Buffer | null {
    if (value === null || value === undefined) {
      return null;
    }

    if (typeof value === "boolean") {
      return Buffer.from(value ? "t" : "f", "ascii");
    }

    if (typeof value === "number") {
      if (Number.isNaN(value)) {
        return Buffer.from("NaN", "ascii");
      }
      if (value === Infinity) {
        return Buffer.from("Infinity", "ascii");
      }
      if (value === -Infinity) {
        return Buffer.from("-Infinity", "ascii");
      }
      if (Number.isInteger(value)) {
        return Buffer.from(String(value), "ascii");
      }
      // Floating point -- use full precision representation
      return Buffer.from(String(value), "ascii");
    }

    if (typeof value === "bigint") {
      return Buffer.from(String(value), "ascii");
    }

    if (typeof value === "string") {
      return Buffer.from(value, "utf-8");
    }

    if (Buffer.isBuffer(value)) {
      return Buffer.from("\\x" + value.toString("hex"), "ascii");
    }

    if (value instanceof Date) {
      return Buffer.from(value.toISOString(), "ascii");
    }

    if (Array.isArray(value)) {
      return Buffer.from(encodeArrayText(value), "utf-8");
    }

    // Fallback: use String()
    return Buffer.from(String(value), "utf-8");
  }

  // ==================================================================
  // Binary format encoding (JS -> PG binary bytes)
  // ==================================================================

  static encodeBinary(value: unknown, typeOid: number = 0): Buffer | null {
    if (value === null || value === undefined) {
      return null;
    }

    if (typeof value === "boolean") {
      return Buffer.from([value ? 0x01 : 0x00]);
    }

    if (typeof value === "number") {
      if (typeOid === TYPE_OIDS["smallint"]) {
        const buf = Buffer.alloc(2);
        buf.writeInt16BE(value);
        return buf;
      }
      if (typeOid === TYPE_OIDS["bigint"]) {
        const buf = Buffer.alloc(8);
        buf.writeBigInt64BE(BigInt(value));
        return buf;
      }
      if (Number.isInteger(value)) {
        // Default to int4 for integer
        if (value >= -(2 ** 31) && value <= 2 ** 31 - 1) {
          const buf = Buffer.alloc(4);
          buf.writeInt32BE(value);
          return buf;
        }
        const buf = Buffer.alloc(8);
        buf.writeBigInt64BE(BigInt(value));
        return buf;
      }
      if (typeOid === (TYPE_OIDS["real"] ?? 700)) {
        const buf = Buffer.alloc(4);
        buf.writeFloatBE(value);
        return buf;
      }
      const buf = Buffer.alloc(8);
      buf.writeDoubleBE(value);
      return buf;
    }

    if (typeof value === "string") {
      return Buffer.from(value, "utf-8");
    }

    if (Buffer.isBuffer(value)) {
      return value;
    }

    if (value instanceof Date) {
      const diffMs = value.getTime() - PG_EPOCH_MS;
      const usec = BigInt(diffMs) * BigInt(1000);
      const buf = Buffer.alloc(8);
      buf.writeBigInt64BE(usec);
      return buf;
    }

    // Fallback: use text encoding
    return TypeCodec.encodeText(value, typeOid);
  }

  // ==================================================================
  // Text format decoding (PG text bytes -> JS)
  // ==================================================================

  static decodeText(data: Buffer, typeOid: number): unknown {
    const text = data.toString("utf-8");

    if (typeOid === TYPE_OIDS["boolean"]) {
      return ["t", "true", "1", "yes", "on"].includes(text.toLowerCase());
    }

    if (
      typeOid === TYPE_OIDS["integer"] ||
      typeOid === TYPE_OIDS["smallint"] ||
      typeOid === TYPE_OIDS["bigint"]
    ) {
      return parseInt(text, 10);
    }

    if (typeOid === TYPE_OIDS["real"] || typeOid === TYPE_OIDS["double precision"]) {
      return parseFloat(text);
    }

    if (typeOid === TYPE_OIDS["numeric"]) {
      // JavaScript has no Decimal -- return as number or string
      const n = Number(text);
      if (Number.isFinite(n)) {
        return n;
      }
      return text;
    }

    if (typeOid === TYPE_OIDS["uuid"]) {
      return text;
    }

    if (typeOid === TYPE_OIDS["bytea"]) {
      if (text.startsWith("\\x")) {
        return Buffer.from(text.slice(2), "hex");
      }
      return Buffer.from(text, "utf-8");
    }

    // text, varchar, name, json, jsonb, xml, etc.
    return text;
  }

  // ==================================================================
  // Binary format decoding (PG binary bytes -> JS)
  // ==================================================================

  static decodeBinary(data: Buffer, typeOid: number): unknown {
    if (typeOid === TYPE_OIDS["boolean"]) {
      return data[0] !== 0;
    }

    if (typeOid === TYPE_OIDS["smallint"]) {
      return data.readInt16BE(0);
    }

    if (typeOid === TYPE_OIDS["integer"]) {
      return data.readInt32BE(0);
    }

    if (typeOid === TYPE_OIDS["bigint"]) {
      return Number(data.readBigInt64BE(0));
    }

    if (typeOid === TYPE_OIDS["real"]) {
      return data.readFloatBE(0);
    }

    if (typeOid === TYPE_OIDS["double precision"]) {
      return data.readDoubleBE(0);
    }

    if (typeOid === TYPE_OIDS["uuid"]) {
      // Format UUID bytes as string
      const hex = data.toString("hex");
      return (
        hex.slice(0, 8) +
        "-" +
        hex.slice(8, 12) +
        "-" +
        hex.slice(12, 16) +
        "-" +
        hex.slice(16, 20) +
        "-" +
        hex.slice(20)
      );
    }

    if (typeOid === TYPE_OIDS["bytea"]) {
      return data;
    }

    if (typeOid === TYPE_OIDS["date"]) {
      const days = data.readInt32BE(0);
      // PG epoch is 2000-01-01; add days in milliseconds
      const ms = PG_EPOCH_MS + days * 86_400_000;
      return new Date(ms);
    }

    if (typeOid === TYPE_OIDS["timestamp"] || typeOid === TYPE_OIDS["timestamptz"]) {
      const usec = Number(data.readBigInt64BE(0));
      const ms = PG_EPOCH_MS + usec / 1000;
      return new Date(ms);
    }

    if (typeOid === (TYPE_OIDS["oid"] ?? 26)) {
      return data.readUInt32BE(0);
    }

    // Default: treat as UTF-8 text
    return data.toString("utf-8");
  }

  // ==================================================================
  // Type inference
  // ==================================================================

  static inferTypeOid(value: unknown): number {
    if (value === null || value === undefined) {
      return TYPE_OIDS["text"]!;
    }
    if (typeof value === "boolean") {
      return TYPE_OIDS["boolean"]!;
    }
    if (typeof value === "number") {
      if (Number.isInteger(value)) {
        if (value >= -(2 ** 31) && value <= 2 ** 31 - 1) {
          return TYPE_OIDS["integer"]!;
        }
        return TYPE_OIDS["bigint"]!;
      }
      return TYPE_OIDS["double precision"]!;
    }
    if (typeof value === "bigint") {
      return TYPE_OIDS["bigint"]!;
    }
    if (typeof value === "string") {
      return TYPE_OIDS["text"]!;
    }
    if (Buffer.isBuffer(value)) {
      return TYPE_OIDS["bytea"]!;
    }
    if (value instanceof Date) {
      return TYPE_OIDS["timestamptz"]!;
    }
    return TYPE_OIDS["text"]!;
  }

  static typeSize(typeOid: number): number {
    return TYPE_LENGTHS[typeOid] ?? -1;
  }
}

// ======================================================================
// Internal helpers
// ======================================================================

function encodeArrayText(values: unknown[]): string {
  const elements: string[] = [];
  for (const v of values) {
    if (v === null || v === undefined) {
      elements.push("NULL");
    } else if (typeof v === "string") {
      const escaped = v.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
      elements.push(`"${escaped}"`);
    } else if (typeof v === "boolean") {
      elements.push(v ? "t" : "f");
    } else if (Array.isArray(v)) {
      elements.push(encodeArrayText(v));
    } else {
      elements.push(String(v));
    }
  }
  return "{" + elements.join(",") + "}";
}
