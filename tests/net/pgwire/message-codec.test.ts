//
// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
//
// Copyright (c) 2023-2026 Cognica, Inc.
//

// Unit tests for MessageCodec.

import { describe, it, expect } from "vitest";
import { MessageCodec } from "../../../src/net/pgwire/message-codec.js";
import {
  AUTH_CLEARTEXT,
  AUTH_MD5,
  AUTH_OK,
  AUTH_SASL,
  FIELD_MESSAGE,
  FIELD_SEVERITY,
  FIELD_SQLSTATE,
  TX_IDLE,
} from "../../../src/net/pgwire/constants.js";

describe("TestDecodeStartup", () => {
  it("test_decode_ssl_request", () => {
    const data = Buffer.alloc(4);
    data.writeUInt32BE(80877103);
    const msg = MessageCodec.decodeStartup(data);
    expect(msg.kind).toBe("SSLRequest");
  });

  it("test_decode_gssenc_request", () => {
    const data = Buffer.alloc(4);
    data.writeUInt32BE(80877104);
    const msg = MessageCodec.decodeStartup(data);
    expect(msg.kind).toBe("GSSENCRequest");
  });

  it("test_decode_cancel_request", () => {
    const data = Buffer.alloc(12);
    data.writeUInt32BE(80877102, 0);
    data.writeInt32BE(42, 4);
    data.writeInt32BE(12345, 8);
    const msg = MessageCodec.decodeStartup(data);
    expect(msg.kind).toBe("CancelRequest");
    if (msg.kind === "CancelRequest") {
      expect(msg.processId).toBe(42);
      expect(msg.secretKey).toBe(12345);
    }
  });

  it("test_decode_startup_message", () => {
    const parts: Buffer[] = [];
    const version = Buffer.alloc(4);
    version.writeUInt32BE(196608);
    parts.push(version);
    parts.push(Buffer.from("user\x00alice\x00"));
    parts.push(Buffer.from("database\x00mydb\x00"));
    parts.push(Buffer.from([0x00]));
    const payload = Buffer.concat(parts);
    const msg = MessageCodec.decodeStartup(payload);
    expect(msg.kind).toBe("StartupMessage");
    if (msg.kind === "StartupMessage") {
      expect(msg.protocolVersion).toBe(196608);
      expect(msg.parameters["user"]).toBe("alice");
      expect(msg.parameters["database"]).toBe("mydb");
    }
  });
});

describe("TestDecodeFrontend", () => {
  it("test_decode_query", () => {
    const payload = Buffer.from("SELECT 1\x00");
    const msg = MessageCodec.decodeFrontend("Q".charCodeAt(0), payload);
    expect(msg.kind).toBe("Query");
    if (msg.kind === "Query") {
      expect(msg.sql).toBe("SELECT 1");
    }
  });

  it("test_decode_parse", () => {
    const parts: Buffer[] = [];
    parts.push(Buffer.from("stmt1\x00SELECT $1\x00"));
    const paramCount = Buffer.alloc(2);
    paramCount.writeInt16BE(1);
    parts.push(paramCount);
    const paramType = Buffer.alloc(4);
    paramType.writeInt32BE(23);
    parts.push(paramType);
    const payload = Buffer.concat(parts);
    const msg = MessageCodec.decodeFrontend("P".charCodeAt(0), payload);
    expect(msg.kind).toBe("Parse");
    if (msg.kind === "Parse") {
      expect(msg.statementName).toBe("stmt1");
      expect(msg.query).toBe("SELECT $1");
      expect(msg.paramTypeOids).toEqual([23]);
    }
  });

  it("test_decode_bind", () => {
    const parts: Buffer[] = [];
    parts.push(Buffer.from("\x00")); // portal name (unnamed)
    parts.push(Buffer.from("stmt1\x00")); // statement name
    const paramFormatCount = Buffer.alloc(2);
    paramFormatCount.writeInt16BE(1);
    parts.push(paramFormatCount);
    const paramFormat = Buffer.alloc(2);
    paramFormat.writeInt16BE(0); // text format
    parts.push(paramFormat);
    const paramValueCount = Buffer.alloc(2);
    paramValueCount.writeInt16BE(1);
    parts.push(paramValueCount);
    const valueLen = Buffer.alloc(4);
    valueLen.writeInt32BE(5);
    parts.push(valueLen);
    parts.push(Buffer.from("hello"));
    const resultFormatCount = Buffer.alloc(2);
    resultFormatCount.writeInt16BE(1);
    parts.push(resultFormatCount);
    const resultFormat = Buffer.alloc(2);
    resultFormat.writeInt16BE(0); // text format
    parts.push(resultFormat);
    const payload = Buffer.concat(parts);
    const msg = MessageCodec.decodeFrontend("B".charCodeAt(0), payload);
    expect(msg.kind).toBe("Bind");
    if (msg.kind === "Bind") {
      expect(msg.portalName).toBe("");
      expect(msg.statementName).toBe("stmt1");
      expect(msg.paramFormatCodes).toEqual([0]);
      expect(msg.paramValues).toEqual([Buffer.from("hello")]);
      expect(msg.resultFormatCodes).toEqual([0]);
    }
  });

  it("test_decode_bind_with_null", () => {
    const parts: Buffer[] = [];
    parts.push(Buffer.from("\x00\x00")); // unnamed portal, unnamed stmt
    const paramFormatCount = Buffer.alloc(2);
    paramFormatCount.writeInt16BE(0); // no param format codes
    parts.push(paramFormatCount);
    const paramValueCount = Buffer.alloc(2);
    paramValueCount.writeInt16BE(1); // 1 param value
    parts.push(paramValueCount);
    const nullLen = Buffer.alloc(4);
    nullLen.writeInt32BE(-1); // NULL
    parts.push(nullLen);
    const resultFormatCount = Buffer.alloc(2);
    resultFormatCount.writeInt16BE(0); // no result format codes
    parts.push(resultFormatCount);
    const payload = Buffer.concat(parts);
    const msg = MessageCodec.decodeFrontend("B".charCodeAt(0), payload);
    expect(msg.kind).toBe("Bind");
    if (msg.kind === "Bind") {
      expect(msg.paramValues).toEqual([null]);
    }
  });

  it("test_decode_describe_statement", () => {
    const payload = Buffer.concat([Buffer.from("S"), Buffer.from("stmt1\x00")]);
    const msg = MessageCodec.decodeFrontend("D".charCodeAt(0), payload);
    expect(msg.kind).toBe("Describe");
    if (msg.kind === "Describe") {
      expect(msg.target).toBe("S");
      expect(msg.name).toBe("stmt1");
    }
  });

  it("test_decode_describe_portal", () => {
    const payload = Buffer.concat([Buffer.from("P"), Buffer.from("\x00")]);
    const msg = MessageCodec.decodeFrontend("D".charCodeAt(0), payload);
    expect(msg.kind).toBe("Describe");
    if (msg.kind === "Describe") {
      expect(msg.target).toBe("P");
      expect(msg.name).toBe("");
    }
  });

  it("test_decode_execute", () => {
    const parts: Buffer[] = [];
    parts.push(Buffer.from("\x00")); // unnamed portal
    const maxRows = Buffer.alloc(4);
    maxRows.writeInt32BE(100);
    parts.push(maxRows);
    const payload = Buffer.concat(parts);
    const msg = MessageCodec.decodeFrontend("E".charCodeAt(0), payload);
    expect(msg.kind).toBe("Execute");
    if (msg.kind === "Execute") {
      expect(msg.portalName).toBe("");
      expect(msg.maxRows).toBe(100);
    }
  });

  it("test_decode_close", () => {
    const payload = Buffer.concat([Buffer.from("S"), Buffer.from("stmt1\x00")]);
    const msg = MessageCodec.decodeFrontend("C".charCodeAt(0), payload);
    expect(msg.kind).toBe("Close");
    if (msg.kind === "Close") {
      expect(msg.target).toBe("S");
      expect(msg.name).toBe("stmt1");
    }
  });

  it("test_decode_sync", () => {
    const msg = MessageCodec.decodeFrontend("S".charCodeAt(0), Buffer.alloc(0));
    expect(msg.kind).toBe("Sync");
  });

  it("test_decode_terminate", () => {
    const msg = MessageCodec.decodeFrontend("X".charCodeAt(0), Buffer.alloc(0));
    expect(msg.kind).toBe("Terminate");
  });

  it("test_decode_password", () => {
    const payload = Buffer.from("mysecret\x00");
    const msg = MessageCodec.decodeFrontend("p".charCodeAt(0), payload);
    expect(msg.kind).toBe("PasswordMessage");
    if (msg.kind === "PasswordMessage") {
      expect(msg.password).toBe("mysecret");
    }
  });

  it("test_decode_unknown_raises", () => {
    expect(() =>
      MessageCodec.decodeFrontend("~".charCodeAt(0), Buffer.alloc(0)),
    ).toThrow(/[Uu]nknown/);
  });
});

describe("TestEncodeBackend", () => {
  it("test_encode_auth_ok", () => {
    const data = MessageCodec.encodeAuthOk();
    expect(data[0]).toBe("R".charCodeAt(0));
    const length = data.readInt32BE(1);
    expect(length).toBe(8);
    const authType = data.readInt32BE(5);
    expect(authType).toBe(AUTH_OK);
  });

  it("test_encode_auth_cleartext", () => {
    const data = MessageCodec.encodeAuthCleartext();
    expect(data[0]).toBe("R".charCodeAt(0));
    const authType = data.readInt32BE(5);
    expect(authType).toBe(AUTH_CLEARTEXT);
  });

  it("test_encode_auth_md5", () => {
    const salt = Buffer.from([0x01, 0x02, 0x03, 0x04]);
    const data = MessageCodec.encodeAuthMD5(salt);
    expect(data[0]).toBe("R".charCodeAt(0));
    const authType = data.readInt32BE(5);
    expect(authType).toBe(AUTH_MD5);
    expect(data.subarray(9, 13)).toEqual(salt);
  });

  it("test_encode_auth_sasl", () => {
    const data = MessageCodec.encodeAuthSASL(["SCRAM-SHA-256"]);
    expect(data[0]).toBe("R".charCodeAt(0));
    const authType = data.readInt32BE(5);
    expect(authType).toBe(AUTH_SASL);
    // Should contain the mechanism name followed by double null.
    expect(data.includes(Buffer.from("SCRAM-SHA-256\x00\x00"))).toBe(true);
  });

  it("test_encode_parameter_status", () => {
    const data = MessageCodec.encodeParameterStatus("server_version", "17.0");
    expect(data[0]).toBe("S".charCodeAt(0));
    expect(data.includes(Buffer.from("server_version\x00"))).toBe(true);
    expect(data.includes(Buffer.from("17.0\x00"))).toBe(true);
  });

  it("test_encode_backend_key_data", () => {
    const data = MessageCodec.encodeBackendKeyData(42, 12345);
    expect(data[0]).toBe("K".charCodeAt(0));
    const length = data.readInt32BE(1);
    expect(length).toBe(12);
    const pid = data.readInt32BE(5);
    const secret = data.readInt32BE(9);
    expect(pid).toBe(42);
    expect(secret).toBe(12345);
  });

  it("test_encode_ready_for_query", () => {
    const data = MessageCodec.encodeReadyForQuery(TX_IDLE);
    expect(data[0]).toBe("Z".charCodeAt(0));
    const length = data.readInt32BE(1);
    expect(length).toBe(5);
    expect(data[5]).toBe("I".charCodeAt(0));
  });

  it("test_encode_row_description", () => {
    const cols = [
      {
        name: "id",
        tableOid: 0,
        columnNumber: 1,
        typeOid: 23,
        typeSize: 4,
        typeModifier: -1,
        formatCode: 0,
      },
      {
        name: "name",
        tableOid: 0,
        columnNumber: 2,
        typeOid: 25,
        typeSize: -1,
        typeModifier: -1,
        formatCode: 0,
      },
    ];
    const data = MessageCodec.encodeRowDescription(cols);
    expect(data[0]).toBe("T".charCodeAt(0));
    // Parse column count.
    const colCount = data.readInt16BE(5);
    expect(colCount).toBe(2);
  });

  it("test_encode_data_row", () => {
    const values: (Buffer | null)[] = [Buffer.from("42"), Buffer.from("hello"), null];
    const data = MessageCodec.encodeDataRow(values);
    expect(data[0]).toBe("D".charCodeAt(0));
    const colCount = data.readInt16BE(5);
    expect(colCount).toBe(3);
    // First value: length 2, "42"
    let pos = 7;
    const l1 = data.readInt32BE(pos);
    expect(l1).toBe(2);
    expect(data.subarray(pos + 4, pos + 6)).toEqual(Buffer.from("42"));
    pos += 4 + l1;
    // Second value: length 5, "hello"
    const l2 = data.readInt32BE(pos);
    expect(l2).toBe(5);
    pos += 4 + l2;
    // Third value: NULL (-1)
    const l3 = data.readInt32BE(pos);
    expect(l3).toBe(-1);
  });

  it("test_encode_command_complete", () => {
    const data = MessageCodec.encodeCommandComplete("SELECT 5");
    expect(data[0]).toBe("C".charCodeAt(0));
    expect(data.includes(Buffer.from("SELECT 5\x00"))).toBe(true);
  });

  it("test_encode_empty_query_response", () => {
    const data = MessageCodec.encodeEmptyQueryResponse();
    expect(data[0]).toBe("I".charCodeAt(0));
    const length = data.readInt32BE(1);
    expect(length).toBe(4);
  });

  it("test_encode_error_response", () => {
    const fields = new Map<number, string>([
      [FIELD_SEVERITY, "ERROR"],
      [FIELD_SQLSTATE, "42601"],
      [FIELD_MESSAGE, "syntax error"],
    ]);
    const data = MessageCodec.encodeErrorResponse(fields);
    expect(data[0]).toBe("E".charCodeAt(0));
    // Should end with a null terminator.
    expect(data[data.length - 1]).toBe(0);
  });

  it("test_encode_parse_complete", () => {
    const data = MessageCodec.encodeParseComplete();
    expect(data[0]).toBe("1".charCodeAt(0));
    expect(data.readInt32BE(1)).toBe(4);
  });

  it("test_encode_bind_complete", () => {
    const data = MessageCodec.encodeBindComplete();
    expect(data[0]).toBe("2".charCodeAt(0));
  });

  it("test_encode_close_complete", () => {
    const data = MessageCodec.encodeCloseComplete();
    expect(data[0]).toBe("3".charCodeAt(0));
  });

  it("test_encode_no_data", () => {
    const data = MessageCodec.encodeNoData();
    expect(data[0]).toBe("n".charCodeAt(0));
  });

  it("test_encode_parameter_description", () => {
    const data = MessageCodec.encodeParameterDescription([23, 25]);
    expect(data[0]).toBe("t".charCodeAt(0));
    const n = data.readInt16BE(5);
    expect(n).toBe(2);
  });

  it("test_encode_portal_suspended", () => {
    const data = MessageCodec.encodePortalSuspended();
    expect(data[0]).toBe("s".charCodeAt(0));
  });

  it("test_encode_notification", () => {
    const data = MessageCodec.encodeNotification(1, "channel", "payload");
    expect(data[0]).toBe("A".charCodeAt(0));
    expect(data.includes(Buffer.from("channel\x00"))).toBe(true);
    expect(data.includes(Buffer.from("payload\x00"))).toBe(true);
  });
});
