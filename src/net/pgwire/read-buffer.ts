// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
// Copyright (c) 2023-2026 Cognica, Inc.

// Low-level read buffer for PostgreSQL wire protocol messages.
//
// ReadBuffer wraps a Node.js Buffer and provides sequential typed reads
// (int16, int32, null-terminated string, etc.).

export class ReadBuffer {
  private _data: Buffer;
  private _pos: number;

  constructor(data: Buffer) {
    this._data = data;
    this._pos = 0;
  }

  /** Number of unread bytes. */
  get remaining(): number {
    return this._data.length - this._pos;
  }

  /** Read a single unsigned byte. */
  readByte(): number {
    const val = this._data[this._pos]!;
    this._pos += 1;
    return val;
  }

  /** Read a signed 16-bit big-endian integer. */
  readInt16(): number {
    const val = this._data.readInt16BE(this._pos);
    this._pos += 2;
    return val;
  }

  /** Read an unsigned 16-bit big-endian integer. */
  readUint16(): number {
    const val = this._data.readUInt16BE(this._pos);
    this._pos += 2;
    return val;
  }

  /** Read a signed 32-bit big-endian integer. */
  readInt32(): number {
    const val = this._data.readInt32BE(this._pos);
    this._pos += 4;
    return val;
  }

  /** Read an unsigned 32-bit big-endian integer. */
  readUint32(): number {
    const val = this._data.readUInt32BE(this._pos);
    this._pos += 4;
    return val;
  }

  /** Read a null-terminated UTF-8 string. */
  readString(): string {
    const end = this._data.indexOf(0, this._pos);
    const val = this._data.subarray(this._pos, end).toString("utf-8");
    this._pos = end + 1; // skip the null terminator
    return val;
  }

  /** Read exactly n raw bytes. */
  readBytes(n: number): Buffer {
    const val = this._data.subarray(this._pos, this._pos + n);
    this._pos += n;
    return Buffer.from(val);
  }

  /** Read all remaining bytes. */
  readRemaining(): Buffer {
    const val = this._data.subarray(this._pos);
    this._pos = this._data.length;
    return Buffer.from(val);
  }
}
