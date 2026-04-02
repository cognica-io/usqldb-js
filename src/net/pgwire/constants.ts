// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
// Copyright (c) 2023-2026 Cognica, Inc.

// PostgreSQL v3 wire protocol constants.
//
// All magic numbers, message type codes, authentication sub-types,
// error field identifiers, and default server parameters are collected
// here so that the rest of the pgwire package can import a single module.
//
// References:
//     https://www.postgresql.org/docs/17/protocol-message-formats.html
//     https://www.postgresql.org/docs/17/protocol-error-fields.html

// ======================================================================
// Protocol version and special request codes
// ======================================================================

export const PROTOCOL_VERSION = 196608; // 3.0 = (3 << 16) | 0
export const SSL_REQUEST_CODE = 80877103;
export const CANCEL_REQUEST_CODE = 80877102;
export const GSSENC_REQUEST_CODE = 80877104;

// ======================================================================
// Frontend message type codes (client -> server)
// ======================================================================

export const QUERY = "Q".charCodeAt(0);
export const PARSE = "P".charCodeAt(0);
export const BIND = "B".charCodeAt(0);
export const DESCRIBE = "D".charCodeAt(0);
export const EXECUTE = "E".charCodeAt(0);
export const CLOSE = "C".charCodeAt(0);
export const SYNC = "S".charCodeAt(0);
export const FLUSH = "H".charCodeAt(0);
export const TERMINATE = "X".charCodeAt(0);
export const COPY_DATA = "d".charCodeAt(0);
export const COPY_DONE = "c".charCodeAt(0);
export const COPY_FAIL = "f".charCodeAt(0);
export const PASSWORD = "p".charCodeAt(0);
export const FUNCTION_CALL = "F".charCodeAt(0);

// ======================================================================
// Backend message type codes (server -> client)
// ======================================================================

export const AUTH = "R".charCodeAt(0);
export const PARAMETER_STATUS = "S".charCodeAt(0);
export const BACKEND_KEY_DATA = "K".charCodeAt(0);
export const READY_FOR_QUERY = "Z".charCodeAt(0);
export const ROW_DESCRIPTION = "T".charCodeAt(0);
export const DATA_ROW = "D".charCodeAt(0);
export const COMMAND_COMPLETE = "C".charCodeAt(0);
export const ERROR_RESPONSE = "E".charCodeAt(0);
export const NOTICE_RESPONSE = "N".charCodeAt(0);
export const EMPTY_QUERY = "I".charCodeAt(0);
export const PARSE_COMPLETE = "1".charCodeAt(0);
export const BIND_COMPLETE = "2".charCodeAt(0);
export const CLOSE_COMPLETE = "3".charCodeAt(0);
export const NO_DATA = "n".charCodeAt(0);
export const PARAMETER_DESCRIPTION = "t".charCodeAt(0);
export const PORTAL_SUSPENDED = "s".charCodeAt(0);
export const COPY_IN_RESPONSE = "G".charCodeAt(0);
export const COPY_OUT_RESPONSE = "H".charCodeAt(0);
export const NOTIFICATION = "A".charCodeAt(0);

// ======================================================================
// Authentication sub-type codes (inside 'R' messages)
// ======================================================================

export const AUTH_OK = 0;
export const AUTH_KERBEROS_V5 = 2;
export const AUTH_CLEARTEXT = 3;
export const AUTH_MD5 = 5;
export const AUTH_SCM_CREDENTIAL = 6;
export const AUTH_GSS = 7;
export const AUTH_GSS_CONTINUE = 8;
export const AUTH_SSPI = 9;
export const AUTH_SASL = 10;
export const AUTH_SASL_CONTINUE = 11;
export const AUTH_SASL_FINAL = 12;

// ======================================================================
// Error / Notice field codes
// ======================================================================

export const FIELD_SEVERITY = "S".charCodeAt(0);
export const FIELD_SEVERITY_V = "V".charCodeAt(0); // non-localized severity
export const FIELD_SQLSTATE = "C".charCodeAt(0);
export const FIELD_MESSAGE = "M".charCodeAt(0);
export const FIELD_DETAIL = "D".charCodeAt(0);
export const FIELD_HINT = "H".charCodeAt(0);
export const FIELD_POSITION = "P".charCodeAt(0);
export const FIELD_INTERNAL_POSITION = "p".charCodeAt(0);
export const FIELD_INTERNAL_QUERY = "q".charCodeAt(0);
export const FIELD_WHERE = "W".charCodeAt(0);
export const FIELD_SCHEMA = "s".charCodeAt(0);
export const FIELD_TABLE = "t".charCodeAt(0);
export const FIELD_COLUMN = "c".charCodeAt(0);
export const FIELD_DATA_TYPE = "d".charCodeAt(0);
export const FIELD_CONSTRAINT = "n".charCodeAt(0);
export const FIELD_FILE = "F".charCodeAt(0);
export const FIELD_LINE = "L".charCodeAt(0);
export const FIELD_ROUTINE = "R".charCodeAt(0);

// ======================================================================
// Transaction status indicators (inside 'Z' ReadyForQuery)
// ======================================================================

export const TX_IDLE = "I".charCodeAt(0);
export const TX_IN_TRANSACTION = "T".charCodeAt(0);
export const TX_FAILED = "E".charCodeAt(0);

// ======================================================================
// Format codes
// ======================================================================

export const FORMAT_TEXT = 0;
export const FORMAT_BINARY = 1;

// ======================================================================
// Default server parameters sent during startup
// ======================================================================

export const DEFAULT_SERVER_PARAMS: Readonly<Record<string, string>> = {
  server_version: "17.0",
  server_encoding: "UTF8",
  client_encoding: "UTF8",
  DateStyle: "ISO, MDY",
  TimeZone: "UTC",
  integer_datetimes: "on",
  standard_conforming_strings: "on",
  is_superuser: "on",
  session_authorization: "uqa",
  IntervalStyle: "postgres",
  application_name: "",
  default_transaction_read_only: "off",
  in_hot_standby: "off",
};
