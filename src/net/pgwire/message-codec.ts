// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
// Copyright (c) 2023-2026 Cognica, Inc.

// Encode and decode PostgreSQL v3 wire protocol messages.
//
// The MessageCodec exposes only static methods and carries no
// state.  Decoding methods convert raw Buffers into the frontend
// message interfaces defined in messages.ts.  Encoding methods
// produce Buffers ready for writing to the transport.

import { ReadBuffer } from "./read-buffer.js";
import { WriteBuffer } from "./write-buffer.js";
import {
  AUTH_CLEARTEXT,
  AUTH_MD5,
  AUTH_OK,
  AUTH_SASL,
  AUTH_SASL_CONTINUE,
  AUTH_SASL_FINAL,
  CANCEL_REQUEST_CODE,
  COMMAND_COMPLETE,
  COPY_IN_RESPONSE,
  COPY_OUT_RESPONSE,
  DATA_ROW,
  ERROR_RESPONSE,
  GSSENC_REQUEST_CODE,
  NOTICE_RESPONSE,
  NOTIFICATION,
  PARAMETER_DESCRIPTION,
  PARAMETER_STATUS,
  ROW_DESCRIPTION,
  SSL_REQUEST_CODE,
} from "./constants.js";
import type {
  Bind,
  CancelRequest,
  Close,
  ColumnDescription,
  CopyData,
  CopyDone,
  CopyFail,
  Describe,
  Execute,
  Flush,
  FrontendMessage,
  FunctionCall,
  GSSENCRequest,
  Parse,
  PasswordMessage,
  Query,
  SASLInitialResponse,
  SASLResponse,
  SSLRequest,
  StartupMessage,
  StartupPhaseMessage,
  Sync,
  Terminate,
} from "./messages.js";

// eslint-disable-next-line @typescript-eslint/no-extraneous-class
export class MessageCodec {
  // ==================================================================
  // Decoding -- frontend messages
  // ==================================================================

  /**
   * Decode an un-typed startup-phase message.
   *
   * The caller has already read the 4-byte length and the full
   * payload; data is the payload after the length field.
   */
  static decodeStartup(data: Buffer): StartupPhaseMessage {
    const buf = new ReadBuffer(data);
    const code = buf.readUint32();

    if (code === SSL_REQUEST_CODE) {
      return { kind: "SSLRequest" } as SSLRequest;
    }
    if (code === GSSENC_REQUEST_CODE) {
      return { kind: "GSSENCRequest" } as GSSENCRequest;
    }
    if (code === CANCEL_REQUEST_CODE) {
      const processId = buf.readInt32();
      const secretKey = buf.readInt32();
      return { kind: "CancelRequest", processId, secretKey } as CancelRequest;
    }

    // Regular startup message: code is the protocol version.
    const params: Record<string, string> = {};
    while (buf.remaining > 1) {
      const key = buf.readString();
      if (!key) {
        break;
      }
      const value = buf.readString();
      params[key] = value;
    }
    return {
      kind: "StartupMessage",
      protocolVersion: code,
      parameters: params,
    } as StartupMessage;
  }

  /** Decode a typed frontend message. */
  static decodeFrontend(msgType: number, payload: Buffer): FrontendMessage {
    const buf = new ReadBuffer(payload);

    if (msgType === 0x51) {
      // 'Q'
      return { kind: "Query", sql: buf.readString() } as Query;
    }

    if (msgType === 0x50) {
      // 'P'
      const name = buf.readString();
      const query = buf.readString();
      const nParams = buf.readInt16();
      const oids: number[] = [];
      for (let i = 0; i < nParams; i++) {
        oids.push(buf.readInt32());
      }
      return {
        kind: "Parse",
        statementName: name,
        query,
        paramTypeOids: oids,
      } as Parse;
    }

    if (msgType === 0x42) {
      // 'B'
      const portal = buf.readString();
      const statement = buf.readString();
      const nParamFmt = buf.readInt16();
      const paramFmts: number[] = [];
      for (let i = 0; i < nParamFmt; i++) {
        paramFmts.push(buf.readInt16());
      }
      const nParams = buf.readInt16();
      const values: (Buffer | null)[] = [];
      for (let i = 0; i < nParams; i++) {
        const length = buf.readInt32();
        if (length === -1) {
          values.push(null);
        } else {
          values.push(buf.readBytes(length));
        }
      }
      const nResultFmt = buf.readInt16();
      const resultFmts: number[] = [];
      for (let i = 0; i < nResultFmt; i++) {
        resultFmts.push(buf.readInt16());
      }
      return {
        kind: "Bind",
        portalName: portal,
        statementName: statement,
        paramFormatCodes: paramFmts,
        paramValues: values,
        resultFormatCodes: resultFmts,
      } as Bind;
    }

    if (msgType === 0x44) {
      // 'D'
      const target = String.fromCharCode(buf.readByte());
      const name = buf.readString();
      return { kind: "Describe", target, name } as Describe;
    }

    if (msgType === 0x45) {
      // 'E'
      const portal = buf.readString();
      const maxRows = buf.readInt32();
      return { kind: "Execute", portalName: portal, maxRows } as Execute;
    }

    if (msgType === 0x43) {
      // 'C'
      const target = String.fromCharCode(buf.readByte());
      const name = buf.readString();
      return { kind: "Close", target, name } as Close;
    }

    if (msgType === 0x53) {
      // 'S'
      return { kind: "Sync" } as Sync;
    }

    if (msgType === 0x48) {
      // 'H'
      return { kind: "Flush" } as Flush;
    }

    if (msgType === 0x58) {
      // 'X'
      return { kind: "Terminate" } as Terminate;
    }

    if (msgType === 0x64) {
      // 'd'
      return { kind: "CopyData", data: buf.readRemaining() } as CopyData;
    }

    if (msgType === 0x63) {
      // 'c'
      return { kind: "CopyDone" } as CopyDone;
    }

    if (msgType === 0x66) {
      // 'f'
      return { kind: "CopyFail", message: buf.readString() } as CopyFail;
    }

    if (msgType === 0x70) {
      // 'p'
      // Could be PasswordMessage, SASLInitialResponse, or SASLResponse.
      // The caller determines which based on the auth state.
      // We return a PasswordMessage by default; the auth handler
      // re-parses if needed.
      const password = payload.subarray(0, payload.length - 1).toString("utf-8");
      return { kind: "PasswordMessage", password } as PasswordMessage;
    }

    if (msgType === 0x46) {
      // 'F'
      const oid = buf.readInt32();
      const nArgFmt = buf.readInt16();
      const argFmts: number[] = [];
      for (let i = 0; i < nArgFmt; i++) {
        argFmts.push(buf.readInt16());
      }
      const nArgs = buf.readInt16();
      const args: (Buffer | null)[] = [];
      for (let i = 0; i < nArgs; i++) {
        const length = buf.readInt32();
        if (length === -1) {
          args.push(null);
        } else {
          args.push(buf.readBytes(length));
        }
      }
      const resultFmt = buf.readInt16();
      return {
        kind: "FunctionCall",
        functionOid: oid,
        argFormatCodes: argFmts,
        arguments: args,
        resultFormat: resultFmt,
      } as FunctionCall;
    }

    // Unknown message type -- the connection handler should send an error.
    throw new Error(`Unknown frontend message type: '${String.fromCharCode(msgType)}'`);
  }

  /** Re-parse a 'p' message payload as SASLInitialResponse. */
  static decodeSASLInitialResponse(payload: Buffer): SASLInitialResponse {
    const buf = new ReadBuffer(payload);
    const mechanism = buf.readString();
    const length = buf.readInt32();
    const data = length >= 0 ? buf.readBytes(length) : Buffer.alloc(0);
    return { kind: "SASLInitialResponse", mechanism, data } as SASLInitialResponse;
  }

  /** Re-parse a 'p' message payload as SASLResponse. */
  static decodeSASLResponse(payload: Buffer): SASLResponse {
    return { kind: "SASLResponse", data: payload } as SASLResponse;
  }

  // ==================================================================
  // Encoding -- backend messages
  // ==================================================================

  /** AuthenticationOk (R, type=0). */
  static encodeAuthOk(): Buffer {
    const buf = Buffer.alloc(9);
    buf[0] = 0x52; // 'R'
    buf.writeInt32BE(8, 1);
    buf.writeInt32BE(AUTH_OK, 5);
    return buf;
  }

  /** AuthenticationCleartextPassword (R, type=3). */
  static encodeAuthCleartext(): Buffer {
    const buf = Buffer.alloc(9);
    buf[0] = 0x52; // 'R'
    buf.writeInt32BE(8, 1);
    buf.writeInt32BE(AUTH_CLEARTEXT, 5);
    return buf;
  }

  /** AuthenticationMD5Password (R, type=5) with 4-byte salt. */
  static encodeAuthMD5(salt: Buffer): Buffer {
    const buf = Buffer.alloc(13);
    buf[0] = 0x52; // 'R'
    buf.writeInt32BE(12, 1);
    buf.writeInt32BE(AUTH_MD5, 5);
    salt.copy(buf, 9);
    return buf;
  }

  /** AuthenticationSASL (R, type=10) listing mechanisms. */
  static encodeAuthSASL(mechanisms: string[]): Buffer {
    const parts: Buffer[] = [];
    for (const mech of mechanisms) {
      const encoded = Buffer.from(mech, "ascii");
      parts.push(encoded, Buffer.from([0]));
    }
    parts.push(Buffer.from([0])); // empty string terminator
    const body = Buffer.concat(parts);
    const length = 4 + 4 + body.length;
    const header = Buffer.alloc(9);
    header[0] = 0x52; // 'R'
    header.writeInt32BE(length, 1);
    header.writeInt32BE(AUTH_SASL, 5);
    return Buffer.concat([header, body]);
  }

  /** AuthenticationSASLContinue (R, type=11). */
  static encodeAuthSASLContinue(data: Buffer): Buffer {
    const length = 4 + 4 + data.length;
    const header = Buffer.alloc(9);
    header[0] = 0x52; // 'R'
    header.writeInt32BE(length, 1);
    header.writeInt32BE(AUTH_SASL_CONTINUE, 5);
    return Buffer.concat([header, data]);
  }

  /** AuthenticationSASLFinal (R, type=12). */
  static encodeAuthSASLFinal(data: Buffer): Buffer {
    const length = 4 + 4 + data.length;
    const header = Buffer.alloc(9);
    header[0] = 0x52; // 'R'
    header.writeInt32BE(length, 1);
    header.writeInt32BE(AUTH_SASL_FINAL, 5);
    return Buffer.concat([header, data]);
  }

  /** ParameterStatus (S). */
  static encodeParameterStatus(name: string, value: string): Buffer {
    const wb = new WriteBuffer();
    wb.writeString(name);
    wb.writeString(value);
    return wb.finish(PARAMETER_STATUS);
  }

  /** BackendKeyData (K). */
  static encodeBackendKeyData(pid: number, secret: number): Buffer {
    const buf = Buffer.alloc(13);
    buf[0] = 0x4b; // 'K'
    buf.writeInt32BE(12, 1);
    buf.writeInt32BE(pid, 5);
    buf.writeInt32BE(secret, 9);
    return buf;
  }

  /** ReadyForQuery (Z) with transaction status byte. */
  static encodeReadyForQuery(txStatus: number): Buffer {
    const buf = Buffer.alloc(6);
    buf[0] = 0x5a; // 'Z'
    buf.writeInt32BE(5, 1);
    buf[5] = txStatus;
    return buf;
  }

  /** RowDescription (T) with full column metadata. */
  static encodeRowDescription(columns: ColumnDescription[]): Buffer {
    const wb = new WriteBuffer();
    wb.writeInt16(columns.length);
    for (const col of columns) {
      wb.writeString(col.name);
      wb.writeInt32(col.tableOid);
      wb.writeInt16(col.columnNumber);
      wb.writeInt32(col.typeOid);
      wb.writeInt16(col.typeSize);
      wb.writeInt32(col.typeModifier);
      wb.writeInt16(col.formatCode);
    }
    return wb.finish(ROW_DESCRIPTION);
  }

  /** DataRow (D) with column values. */
  static encodeDataRow(values: (Buffer | null)[]): Buffer {
    const wb = new WriteBuffer();
    wb.writeInt16(values.length);
    for (const val of values) {
      if (val === null) {
        wb.writeInt32(-1);
      } else {
        wb.writeInt32(val.length);
        wb.writeBytes(val);
      }
    }
    return wb.finish(DATA_ROW);
  }

  /** CommandComplete (C) with command tag string. */
  static encodeCommandComplete(tag: string): Buffer {
    const wb = new WriteBuffer();
    wb.writeString(tag);
    return wb.finish(COMMAND_COMPLETE);
  }

  /** EmptyQueryResponse (I). */
  static encodeEmptyQueryResponse(): Buffer {
    const buf = Buffer.alloc(5);
    buf[0] = 0x49; // 'I'
    buf.writeInt32BE(4, 1);
    return buf;
  }

  /** ErrorResponse (E) with typed fields. */
  static encodeErrorResponse(fields: Map<number, string>): Buffer {
    const wb = new WriteBuffer();
    for (const [code, value] of fields) {
      wb.writeByte(code);
      wb.writeString(value);
    }
    wb.writeByte(0); // terminator
    return wb.finish(ERROR_RESPONSE);
  }

  /** NoticeResponse (N) with typed fields. */
  static encodeNoticeResponse(fields: Map<number, string>): Buffer {
    const wb = new WriteBuffer();
    for (const [code, value] of fields) {
      wb.writeByte(code);
      wb.writeString(value);
    }
    wb.writeByte(0);
    return wb.finish(NOTICE_RESPONSE);
  }

  /** ParseComplete (1). */
  static encodeParseComplete(): Buffer {
    const buf = Buffer.alloc(5);
    buf[0] = 0x31; // '1'
    buf.writeInt32BE(4, 1);
    return buf;
  }

  /** BindComplete (2). */
  static encodeBindComplete(): Buffer {
    const buf = Buffer.alloc(5);
    buf[0] = 0x32; // '2'
    buf.writeInt32BE(4, 1);
    return buf;
  }

  /** CloseComplete (3). */
  static encodeCloseComplete(): Buffer {
    const buf = Buffer.alloc(5);
    buf[0] = 0x33; // '3'
    buf.writeInt32BE(4, 1);
    return buf;
  }

  /** NoData (n). */
  static encodeNoData(): Buffer {
    const buf = Buffer.alloc(5);
    buf[0] = 0x6e; // 'n'
    buf.writeInt32BE(4, 1);
    return buf;
  }

  /** ParameterDescription (t). */
  static encodeParameterDescription(oids: readonly number[]): Buffer {
    const wb = new WriteBuffer();
    wb.writeInt16(oids.length);
    for (const oid of oids) {
      wb.writeInt32(oid);
    }
    return wb.finish(PARAMETER_DESCRIPTION);
  }

  /** PortalSuspended (s). */
  static encodePortalSuspended(): Buffer {
    const buf = Buffer.alloc(5);
    buf[0] = 0x73; // 's'
    buf.writeInt32BE(4, 1);
    return buf;
  }

  /** CopyInResponse (G). */
  static encodeCopyInResponse(overallFormat: number, columnFormats: number[]): Buffer {
    const wb = new WriteBuffer();
    wb.writeByte(overallFormat);
    wb.writeInt16(columnFormats.length);
    for (const fmt of columnFormats) {
      wb.writeInt16(fmt);
    }
    return wb.finish(COPY_IN_RESPONSE);
  }

  /** CopyOutResponse (H). */
  static encodeCopyOutResponse(overallFormat: number, columnFormats: number[]): Buffer {
    const wb = new WriteBuffer();
    wb.writeByte(overallFormat);
    wb.writeInt16(columnFormats.length);
    for (const fmt of columnFormats) {
      wb.writeInt16(fmt);
    }
    return wb.finish(COPY_OUT_RESPONSE);
  }

  /** NotificationResponse (A). */
  static encodeNotification(pid: number, channel: string, payload: string): Buffer {
    const wb = new WriteBuffer();
    wb.writeInt32(pid);
    wb.writeString(channel);
    wb.writeString(payload);
    return wb.finish(NOTIFICATION);
  }
}
