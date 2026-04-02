//
// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
//
// Copyright (c) 2023-2026 Cognica, Inc.
//

// Unit tests for TypeCodec.

import { describe, it, expect } from "vitest";
import { TypeCodec } from "../../../src/net/pgwire/type-codec.js";
import { TYPE_OIDS } from "../../../src/pg-compat/oid.js";

/** Helper: look up a type OID by name, asserting it exists. */
function oid(name: string): number {
  const v = TYPE_OIDS[name];
  if (v === undefined) throw new Error(`Unknown type OID: ${name}`);
  return v;
}

describe("TestEncodeText", () => {
  it("test_none", () => {
    expect(TypeCodec.encodeText(null)).toBe(null);
  });

  it("test_bool_true", () => {
    expect(TypeCodec.encodeText(true)).toEqual(Buffer.from("t"));
  });

  it("test_bool_false", () => {
    expect(TypeCodec.encodeText(false)).toEqual(Buffer.from("f"));
  });

  it("test_int", () => {
    expect(TypeCodec.encodeText(42)).toEqual(Buffer.from("42"));
    expect(TypeCodec.encodeText(-1)).toEqual(Buffer.from("-1"));
    expect(TypeCodec.encodeText(0)).toEqual(Buffer.from("0"));
  });

  it("test_float", () => {
    const result = TypeCodec.encodeText(3.14);
    expect(result!.includes(Buffer.from("3.14"))).toBe(true);
  });

  it("test_float_nan", () => {
    expect(TypeCodec.encodeText(NaN)).toEqual(Buffer.from("NaN"));
  });

  it("test_float_inf", () => {
    expect(TypeCodec.encodeText(Infinity)).toEqual(Buffer.from("Infinity"));
    expect(TypeCodec.encodeText(-Infinity)).toEqual(Buffer.from("-Infinity"));
  });

  it("test_str", () => {
    expect(TypeCodec.encodeText("hello")).toEqual(Buffer.from("hello"));
    expect(TypeCodec.encodeText("")).toEqual(Buffer.from(""));
  });

  it("test_bytes", () => {
    const result = TypeCodec.encodeText(Buffer.from([0x01, 0x02, 0x03]));
    expect(result).toEqual(Buffer.from("\\x010203"));
  });

  it("test_date", () => {
    const d = new Date(Date.UTC(2024, 0, 15));
    const result = TypeCodec.encodeText(d);
    // JS Date always includes time; check the date portion is present
    expect(result!.includes(Buffer.from("2024-01-15"))).toBe(true);
  });

  it("test_datetime", () => {
    const dt = new Date(Date.UTC(2024, 0, 15, 10, 30, 0));
    const result = TypeCodec.encodeText(dt);
    expect(result!.includes(Buffer.from("2024-01-15"))).toBe(true);
  });

  it("test_uuid", () => {
    const u = "12345678-1234-5678-1234-567812345678";
    expect(TypeCodec.encodeText(u)).toEqual(
      Buffer.from("12345678-1234-5678-1234-567812345678"),
    );
  });

  it("test_decimal", () => {
    expect(TypeCodec.encodeText("3.14")).toEqual(Buffer.from("3.14"));
  });

  it("test_list", () => {
    expect(TypeCodec.encodeText([1, 2, 3])).toEqual(Buffer.from("{1,2,3}"));
  });

  it("test_list_with_strings", () => {
    const result = TypeCodec.encodeText(["a", "b"]);
    expect(result).toEqual(Buffer.from('{"a","b"}'));
  });

  it("test_list_with_null", () => {
    const result = TypeCodec.encodeText([1, null, 3]);
    expect(result).toEqual(Buffer.from("{1,NULL,3}"));
  });
});

describe("TestEncodeBinary", () => {
  it("test_none", () => {
    expect(TypeCodec.encodeBinary(null)).toBe(null);
  });

  it("test_bool", () => {
    expect(TypeCodec.encodeBinary(true)).toEqual(Buffer.from([0x01]));
    expect(TypeCodec.encodeBinary(false)).toEqual(Buffer.from([0x00]));
  });

  it("test_int_default", () => {
    const result = TypeCodec.encodeBinary(42);
    expect(result!.readInt32BE(0)).toBe(42);
  });

  it("test_int_bigint", () => {
    const result = TypeCodec.encodeBinary(42, oid("bigint"));
    expect(result!.readBigInt64BE(0)).toBe(BigInt(42));
  });

  it("test_int_smallint", () => {
    const result = TypeCodec.encodeBinary(42, oid("smallint"));
    expect(result!.readInt16BE(0)).toBe(42);
  });

  it("test_float_double", () => {
    const result = TypeCodec.encodeBinary(3.14);
    const val = result!.readDoubleBE(0);
    expect(Math.abs(val - 3.14)).toBeLessThan(1e-10);
  });

  it("test_float_real", () => {
    const result = TypeCodec.encodeBinary(3.14, oid("real"));
    const val = result!.readFloatBE(0);
    expect(Math.abs(val - 3.14)).toBeLessThan(0.01);
  });

  it("test_str", () => {
    expect(TypeCodec.encodeBinary("hello")).toEqual(Buffer.from("hello"));
  });

  it("test_bytes", () => {
    expect(TypeCodec.encodeBinary(Buffer.from([0x01, 0x02]))).toEqual(
      Buffer.from([0x01, 0x02]),
    );
  });

  it("test_date", () => {
    const d = new Date(Date.UTC(2024, 0, 15));
    const result = TypeCodec.encodeBinary(d);
    // JS Date is always a datetime; encodeBinary encodes as timestamp
    // (microseconds since PG epoch 2000-01-01)
    const usec = result!.readBigInt64BE(0);
    const pgEpochMs = Date.UTC(2000, 0, 1);
    const targetMs = Date.UTC(2024, 0, 15);
    const expectedUsec = BigInt(targetMs - pgEpochMs) * BigInt(1000);
    expect(usec).toBe(expectedUsec);
  });

  it("test_datetime", () => {
    const dt = new Date(Date.UTC(2024, 0, 15, 10, 30, 0));
    const result = TypeCodec.encodeBinary(dt);
    const usec = result!.readBigInt64BE(0);
    expect(usec > BigInt(0)).toBe(true);
  });
});

describe("TestDecodeText", () => {
  it("test_bool_true", () => {
    expect(TypeCodec.decodeText(Buffer.from("t"), oid("boolean"))).toBe(true);
  });

  it("test_bool_false", () => {
    expect(TypeCodec.decodeText(Buffer.from("f"), oid("boolean"))).toBe(false);
  });

  it("test_int", () => {
    expect(TypeCodec.decodeText(Buffer.from("42"), oid("integer"))).toBe(42);
  });

  it("test_bigint", () => {
    const big = 2 ** 40;
    expect(TypeCodec.decodeText(Buffer.from(String(big)), oid("bigint"))).toBe(big);
  });

  it("test_float", () => {
    const val = TypeCodec.decodeText(
      Buffer.from("3.14"),
      oid("double precision"),
    ) as number;
    expect(Math.abs(val - 3.14)).toBeLessThan(1e-10);
  });

  it("test_numeric", () => {
    const val = TypeCodec.decodeText(Buffer.from("3.14"), oid("numeric"));
    // In JS, numeric may come back as string or number
    expect(String(val)).toBe("3.14");
  });

  it("test_uuid", () => {
    const text = Buffer.from("12345678-1234-5678-1234-567812345678");
    const val = TypeCodec.decodeText(text, oid("uuid"));
    expect(String(val)).toBe("12345678-1234-5678-1234-567812345678");
  });

  it("test_text", () => {
    expect(TypeCodec.decodeText(Buffer.from("hello"), oid("text"))).toBe("hello");
  });

  it("test_bytea_hex", () => {
    const val = TypeCodec.decodeText(Buffer.from("\\x0102"), oid("bytea"));
    expect(val).toEqual(Buffer.from([0x01, 0x02]));
  });
});

describe("TestDecodeBinary", () => {
  it("test_bool", () => {
    expect(TypeCodec.decodeBinary(Buffer.from([0x01]), oid("boolean"))).toBe(true);
    expect(TypeCodec.decodeBinary(Buffer.from([0x00]), oid("boolean"))).toBe(false);
  });

  it("test_int4", () => {
    const data = Buffer.alloc(4);
    data.writeInt32BE(42);
    expect(TypeCodec.decodeBinary(data, oid("integer"))).toBe(42);
  });

  it("test_int8", () => {
    const data = Buffer.alloc(8);
    data.writeBigInt64BE(BigInt(2 ** 40));
    expect(TypeCodec.decodeBinary(data, oid("bigint"))).toBe(2 ** 40);
  });

  it("test_float4", () => {
    const data = Buffer.alloc(4);
    data.writeFloatBE(3.14);
    const val = TypeCodec.decodeBinary(data, oid("real")) as number;
    expect(Math.abs(val - 3.14)).toBeLessThan(0.01);
  });

  it("test_float8", () => {
    const data = Buffer.alloc(8);
    data.writeDoubleBE(3.14);
    const val = TypeCodec.decodeBinary(data, oid("double precision")) as number;
    expect(Math.abs(val - 3.14)).toBeLessThan(1e-10);
  });

  it("test_uuid", () => {
    // UUID as 16 bytes
    const hexStr = "12345678123456781234567812345678";
    const uuidBytes = Buffer.from(hexStr, "hex");
    const val = TypeCodec.decodeBinary(uuidBytes, oid("uuid"));
    expect(String(val)).toBe("12345678-1234-5678-1234-567812345678");
  });

  it("test_date", () => {
    // Days since 2000-01-01 for 2024-01-15
    const epoch = Date.UTC(2000, 0, 1);
    const target = Date.UTC(2024, 0, 15);
    const days = Math.floor((target - epoch) / (86400 * 1000));
    const data = Buffer.alloc(4);
    data.writeInt32BE(days);
    const val = TypeCodec.decodeBinary(data, oid("date"));
    // The decoded date should represent 2024-01-15
    if (val instanceof Date) {
      expect(val.getUTCFullYear()).toBe(2024);
      expect(val.getUTCMonth()).toBe(0);
      expect(val.getUTCDate()).toBe(15);
    } else {
      // If it returns a string, just check it contains the date
      expect(String(val)).toContain("2024-01-15");
    }
  });

  it("test_text", () => {
    expect(TypeCodec.decodeBinary(Buffer.from("hello"), oid("text"))).toBe("hello");
  });
});

describe("TestInferTypeOid", () => {
  it("test_none", () => {
    expect(TypeCodec.inferTypeOid(null)).toBe(oid("text"));
  });

  it("test_bool", () => {
    expect(TypeCodec.inferTypeOid(true)).toBe(oid("boolean"));
  });

  it("test_int", () => {
    expect(TypeCodec.inferTypeOid(42)).toBe(oid("integer"));
  });

  it("test_big_int", () => {
    expect(TypeCodec.inferTypeOid(2 ** 40)).toBe(oid("bigint"));
  });

  it("test_float", () => {
    expect(TypeCodec.inferTypeOid(3.14)).toBe(oid("double precision"));
  });

  it("test_str", () => {
    expect(TypeCodec.inferTypeOid("hello")).toBe(oid("text"));
  });

  it("test_bytes", () => {
    expect(TypeCodec.inferTypeOid(Buffer.from([0x01]))).toBe(oid("bytea"));
  });

  it("test_datetime", () => {
    // JS Date always carries timezone info, so inferTypeOid returns timestamptz
    expect(TypeCodec.inferTypeOid(new Date())).toBe(oid("timestamptz"));
  });

  it("test_date", () => {
    // In JS, Date objects are always datetime; there is no separate date type
    // inferTypeOid for a Date should return timestamp or timestamptz
    const inferred = TypeCodec.inferTypeOid(new Date());
    expect(inferred === oid("timestamp") || inferred === oid("timestamptz")).toBe(true);
  });
});
