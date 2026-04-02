// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
// Copyright (c) 2023-2026 Cognica, Inc.

// Frontend (client -> server) message types and shared types.
//
// Every message that a PostgreSQL client can send is modelled as an
// interface.  Backend (server -> client) messages are not modelled
// here -- they are encoded directly by MessageCodec to avoid an
// unnecessary intermediate representation.
//
// The ColumnDescription interface is used by both the codec
// (for RowDescription encoding) and the query executor (for building
// column metadata).

// ======================================================================
// Shared types
// ======================================================================

/** Metadata for a single column in a RowDescription message. */
export interface ColumnDescription {
  readonly name: string;
  readonly tableOid: number;
  readonly columnNumber: number;
  readonly typeOid: number;
  readonly typeSize: number;
  readonly typeModifier: number;
  readonly formatCode: number;
}

/** Create a ColumnDescription with a replaced formatCode. */
export function columnDescriptionWithFormatCode(
  col: ColumnDescription,
  formatCode: number,
): ColumnDescription {
  return {
    name: col.name,
    tableOid: col.tableOid,
    columnNumber: col.columnNumber,
    typeOid: col.typeOid,
    typeSize: col.typeSize,
    typeModifier: col.typeModifier,
    formatCode,
  };
}

// ======================================================================
// Startup-phase messages (no type byte prefix)
// ======================================================================

/** Initial connection handshake with protocol version and parameters. */
export interface StartupMessage {
  readonly kind: "StartupMessage";
  readonly protocolVersion: number;
  readonly parameters: Readonly<Record<string, string>>;
}

/** Client requests SSL/TLS upgrade. */
export interface SSLRequest {
  readonly kind: "SSLRequest";
}

/** Client requests GSSAPI encryption. */
export interface GSSENCRequest {
  readonly kind: "GSSENCRequest";
}

/** Client requests cancellation of a running query. */
export interface CancelRequest {
  readonly kind: "CancelRequest";
  readonly processId: number;
  readonly secretKey: number;
}

// ======================================================================
// Authentication messages
// ======================================================================

/** Cleartext or MD5-hashed password from client. */
export interface PasswordMessage {
  readonly kind: "PasswordMessage";
  readonly password: string;
}

/** First SASL message from client (mechanism selection + data). */
export interface SASLInitialResponse {
  readonly kind: "SASLInitialResponse";
  readonly mechanism: string;
  readonly data: Buffer;
}

/** Subsequent SASL message from client. */
export interface SASLResponse {
  readonly kind: "SASLResponse";
  readonly data: Buffer;
}

// ======================================================================
// Simple query protocol
// ======================================================================

/** Simple query containing one or more SQL statements. */
export interface Query {
  readonly kind: "Query";
  readonly sql: string;
}

// ======================================================================
// Extended query protocol
// ======================================================================

/** Create a prepared statement. */
export interface Parse {
  readonly kind: "Parse";
  readonly statementName: string;
  readonly query: string;
  readonly paramTypeOids: readonly number[];
}

/** Bind parameters to a prepared statement, creating a portal. */
export interface Bind {
  readonly kind: "Bind";
  readonly portalName: string;
  readonly statementName: string;
  readonly paramFormatCodes: readonly number[];
  readonly paramValues: readonly (Buffer | null)[];
  readonly resultFormatCodes: readonly number[];
}

/** Request description of a statement ('S') or portal ('P'). */
export interface Describe {
  readonly kind: "Describe";
  readonly target: string; // 'S' for statement, 'P' for portal
  readonly name: string;
}

/** Execute a named portal. */
export interface Execute {
  readonly kind: "Execute";
  readonly portalName: string;
  readonly maxRows: number;
}

/** Close a prepared statement ('S') or portal ('P'). */
export interface Close {
  readonly kind: "Close";
  readonly target: string; // 'S' or 'P'
  readonly name: string;
}

/** End of an extended-query batch -- server must send ReadyForQuery. */
export interface Sync {
  readonly kind: "Sync";
}

/** Request the server to flush its output buffer. */
export interface Flush {
  readonly kind: "Flush";
}

// ======================================================================
// COPY protocol
// ======================================================================

/** A chunk of COPY data from the client. */
export interface CopyData {
  readonly kind: "CopyData";
  readonly data: Buffer;
}

/** Client signals completion of COPY IN data. */
export interface CopyDone {
  readonly kind: "CopyDone";
}

/** Client signals failure of COPY IN. */
export interface CopyFail {
  readonly kind: "CopyFail";
  readonly message: string;
}

// ======================================================================
// Other
// ======================================================================

/** Client requests graceful connection close. */
export interface Terminate {
  readonly kind: "Terminate";
}

/** Deprecated function call protocol (PostgreSQL 7.3+). */
export interface FunctionCall {
  readonly kind: "FunctionCall";
  readonly functionOid: number;
  readonly argFormatCodes: readonly number[];
  readonly arguments: readonly (Buffer | null)[];
  readonly resultFormat: number;
}

// ======================================================================
// Type unions
// ======================================================================

/** Union of all frontend messages (after startup). */
export type FrontendMessage =
  | Query
  | Parse
  | Bind
  | Describe
  | Execute
  | Close
  | Sync
  | Flush
  | Terminate
  | CopyData
  | CopyDone
  | CopyFail
  | PasswordMessage
  | SASLInitialResponse
  | SASLResponse
  | FunctionCall;

/** Union of all startup-phase messages. */
export type StartupPhaseMessage =
  | StartupMessage
  | SSLRequest
  | GSSENCRequest
  | CancelRequest;
