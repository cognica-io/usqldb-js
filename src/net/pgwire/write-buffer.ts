// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
// Copyright (c) 2023-2026 Cognica, Inc.

// Low-level write buffer for PostgreSQL wire protocol messages.
//
// WriteBuffer accumulates payload bytes and finalises them into a complete
// PostgreSQL wire message with the type byte and 4-byte length prefix.

export class WriteBuffer {
  private _buf: number[];

  constructor() {
    this._buf = [];
  }

  /** Append a single unsigned byte. */
  writeByte(val: number): void {
    this._buf.push(val & 0xff);
  }

  /** Append a signed 16-bit big-endian integer. */
  writeInt16(val: number): void {
    const b = Buffer.alloc(2);
    b.writeInt16BE(val);
    this._buf.push(b[0]!, b[1]!);
  }

  /** Append an unsigned 16-bit big-endian integer. */
  writeUint16(val: number): void {
    const b = Buffer.alloc(2);
    b.writeUInt16BE(val);
    this._buf.push(b[0]!, b[1]!);
  }

  /** Append a signed 32-bit big-endian integer. */
  writeInt32(val: number): void {
    const b = Buffer.alloc(4);
    b.writeInt32BE(val);
    this._buf.push(b[0]!, b[1]!, b[2]!, b[3]!);
  }

  /** Append an unsigned 32-bit big-endian integer. */
  writeUint32(val: number): void {
    const b = Buffer.alloc(4);
    b.writeUInt32BE(val);
    this._buf.push(b[0]!, b[1]!, b[2]!, b[3]!);
  }

  /** Append a null-terminated UTF-8 string. */
  writeString(val: string): void {
    const encoded = Buffer.from(val, "utf-8");
    for (let i = 0; i < encoded.length; i++) {
      this._buf.push(encoded[i]!);
    }
    this._buf.push(0);
  }

  /** Append raw bytes (no length prefix, no terminator). */
  writeBytes(val: Buffer): void {
    for (let i = 0; i < val.length; i++) {
      this._buf.push(val[i]!);
    }
  }

  /**
   * Finalise the message with a type byte and length prefix.
   *
   * The length field includes itself (4 bytes) but not the type byte,
   * matching the PostgreSQL wire protocol convention.
   */
  finish(msgType: number): Buffer {
    const payload = Buffer.from(this._buf);
    const length = payload.length + 4; // length includes itself
    const header = Buffer.alloc(5);
    header[0] = msgType;
    header.writeInt32BE(length, 1);
    return Buffer.concat([header, payload]);
  }

  /**
   * Finalise without a type byte (used for startup responses).
   *
   * The returned bytes are just the 4-byte length prefix followed
   * by the payload.  This is used for messages that do not have a
   * leading type byte, such as the server's SSL response.
   */
  finishNoType(): Buffer {
    const payload = Buffer.from(this._buf);
    const length = payload.length + 4;
    const header = Buffer.alloc(4);
    header.writeInt32BE(length);
    return Buffer.concat([header, payload]);
  }
}
