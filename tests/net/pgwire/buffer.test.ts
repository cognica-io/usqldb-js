//
// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
//
// Copyright (c) 2023-2026 Cognica, Inc.
//

// Unit tests for ReadBuffer and WriteBuffer.

import { describe, it, expect } from "vitest";
import { ReadBuffer } from "../../../src/net/pgwire/read-buffer.js";
import { WriteBuffer } from "../../../src/net/pgwire/write-buffer.js";

describe("TestReadBuffer", () => {
  it("test_read_byte", () => {
    const buf = new ReadBuffer(Buffer.from([0x42]));
    expect(buf.readByte()).toBe(0x42);
  });

  it("test_read_int16", () => {
    const data = Buffer.alloc(2);
    data.writeInt16BE(-1234);
    const buf = new ReadBuffer(data);
    expect(buf.readInt16()).toBe(-1234);
  });

  it("test_read_uint16", () => {
    const data = Buffer.alloc(2);
    data.writeUInt16BE(65000);
    const buf = new ReadBuffer(data);
    expect(buf.readUint16()).toBe(65000);
  });

  it("test_read_int32", () => {
    const data = Buffer.alloc(4);
    data.writeInt32BE(-100000);
    const buf = new ReadBuffer(data);
    expect(buf.readInt32()).toBe(-100000);
  });

  it("test_read_uint32", () => {
    const data = Buffer.alloc(4);
    data.writeUInt32BE(3000000000);
    const buf = new ReadBuffer(data);
    expect(buf.readUint32()).toBe(3000000000);
  });

  it("test_read_string", () => {
    const buf = new ReadBuffer(Buffer.from("hello\x00world\x00"));
    expect(buf.readString()).toBe("hello");
    expect(buf.readString()).toBe("world");
  });

  it("test_read_empty_string", () => {
    const buf = new ReadBuffer(Buffer.from([0x00]));
    expect(buf.readString()).toBe("");
  });

  it("test_read_bytes", () => {
    const buf = new ReadBuffer(Buffer.from([0x01, 0x02, 0x03, 0x04, 0x05]));
    expect(buf.readBytes(3)).toEqual(Buffer.from([0x01, 0x02, 0x03]));
    expect(buf.readBytes(2)).toEqual(Buffer.from([0x04, 0x05]));
  });

  it("test_read_remaining", () => {
    const buf = new ReadBuffer(Buffer.from([0x01, 0x02, 0x03, 0x04, 0x05]));
    buf.readBytes(2);
    expect(buf.readRemaining()).toEqual(Buffer.from([0x03, 0x04, 0x05]));
  });

  it("test_remaining_property", () => {
    const buf = new ReadBuffer(Buffer.from([0x01, 0x02, 0x03]));
    expect(buf.remaining).toBe(3);
    buf.readByte();
    expect(buf.remaining).toBe(2);
  });

  it("test_sequential_reads", () => {
    // struct.pack("!ih", 42, 7) + b"test\x00"
    const data = Buffer.alloc(4 + 2 + 5);
    data.writeInt32BE(42, 0);
    data.writeInt16BE(7, 4);
    Buffer.from("test\x00").copy(data, 6);
    const buf = new ReadBuffer(data);
    expect(buf.readInt32()).toBe(42);
    expect(buf.readInt16()).toBe(7);
    expect(buf.readString()).toBe("test");
    expect(buf.remaining).toBe(0);
  });
});

describe("TestWriteBuffer", () => {
  it("test_write_byte", () => {
    const buf = new WriteBuffer();
    buf.writeByte(0x42);
    const msg = buf.finish("T".charCodeAt(0));
    // type(1) + length(4) + payload(1) = 6 bytes
    expect(msg[0]).toBe("T".charCodeAt(0));
    const length = msg.readInt32BE(1);
    expect(length).toBe(5); // 4 (self) + 1 byte
    expect(msg[5]).toBe(0x42);
  });

  it("test_write_int16", () => {
    const buf = new WriteBuffer();
    buf.writeInt16(-1234);
    const msg = buf.finish("T".charCodeAt(0));
    const payload = msg.subarray(5);
    expect(payload.readInt16BE(0)).toBe(-1234);
  });

  it("test_write_int32", () => {
    const buf = new WriteBuffer();
    buf.writeInt32(-100000);
    const msg = buf.finish("T".charCodeAt(0));
    const payload = msg.subarray(5);
    expect(payload.readInt32BE(0)).toBe(-100000);
  });

  it("test_write_string", () => {
    const buf = new WriteBuffer();
    buf.writeString("hello");
    const msg = buf.finish("T".charCodeAt(0));
    const payload = msg.subarray(5);
    expect(payload).toEqual(Buffer.from("hello\x00"));
  });

  it("test_write_bytes", () => {
    const buf = new WriteBuffer();
    buf.writeBytes(Buffer.from([0x01, 0x02, 0x03]));
    const msg = buf.finish("T".charCodeAt(0));
    const payload = msg.subarray(5);
    expect(payload).toEqual(Buffer.from([0x01, 0x02, 0x03]));
  });

  it("test_finish_no_type", () => {
    const buf = new WriteBuffer();
    buf.writeInt32(196608);
    const msg = buf.finishNoType();
    // length(4) + payload(4) = 8 bytes
    const length = msg.readUInt32BE(0);
    expect(length).toBe(8);
    const version = msg.readUInt32BE(4);
    expect(version).toBe(196608);
  });

  it("test_complex_message", () => {
    // Test building a complete RowDescription-like message.
    const buf = new WriteBuffer();
    buf.writeInt16(1); // column count
    buf.writeString("id"); // column name
    buf.writeInt32(0); // table OID
    buf.writeInt16(1); // column number
    buf.writeInt32(23); // type OID (int4)
    buf.writeInt16(4); // type size
    buf.writeInt32(-1); // type modifier
    buf.writeInt16(0); // format code (text)
    const msg = buf.finish("T".charCodeAt(0));
    expect(msg[0]).toBe("T".charCodeAt(0));
  });
});

describe("TestBufferRoundTrip", () => {
  it("test_int32_round_trip", () => {
    for (const val of [0, 1, -1, 2 ** 31 - 1, -(2 ** 31)]) {
      const wbuf = new WriteBuffer();
      wbuf.writeInt32(val);
      const raw = wbuf.finish("X".charCodeAt(0));
      const rbuf = new ReadBuffer(raw.subarray(5)); // skip type + length
      expect(rbuf.readInt32()).toBe(val);
    }
  });

  it("test_string_round_trip", () => {
    for (const val of ["", "hello", "test with spaces", "unicode: abc"]) {
      const wbuf = new WriteBuffer();
      wbuf.writeString(val);
      const raw = wbuf.finish("X".charCodeAt(0));
      const rbuf = new ReadBuffer(raw.subarray(5));
      expect(rbuf.readString()).toBe(val);
    }
  });
});
