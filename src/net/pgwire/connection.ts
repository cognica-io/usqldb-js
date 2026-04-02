// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
// Copyright (c) 2023-2026 Cognica, Inc.

// Per-connection state machine for the PostgreSQL wire protocol.
//
// Each TCP connection spawns one PGWireConnection which drives
// the full protocol lifecycle: startup negotiation, authentication,
// simple query, extended query, and graceful termination.

import type * as net from "node:net";
import * as tls from "node:tls";

import { AuthMethod, createAuthenticator } from "./auth.js";
import type { ScramSHA256Authenticator } from "./auth.js";
import { MessageCodec } from "./message-codec.js";
import {
  DEFAULT_SERVER_PARAMS,
  FORMAT_BINARY,
  FORMAT_TEXT,
  PROTOCOL_VERSION,
  TX_FAILED,
  TX_IDLE,
  TX_IN_TRANSACTION,
} from "./constants.js";
import {
  FeatureNotSupported,
  InFailedSQLTransaction,
  PGWireError,
  ProtocolViolation,
  mapEngineException,
} from "./errors.js";
import type {
  Bind,
  CancelRequest,
  Close,
  ColumnDescription,
  Describe,
  Execute,
  Parse,
  Query,
  StartupMessage,
} from "./messages.js";
import { columnDescriptionWithFormatCode } from "./messages.js";
import { QueryExecutor, QueryResult } from "./query-executor.js";
import { TypeCodec } from "./type-codec.js";

import type { USQLEngine } from "../../core/engine.js";

// ======================================================================
// SocketReader -- buffered reading from a TCP socket
// ======================================================================

export class SocketReader {
  private _socket: net.Socket;
  private _buffer: Buffer;
  private _pendingResolve: ((buf: Buffer) => void) | null;
  private _pendingReject: ((err: Error) => void) | null;
  private _pendingBytes: number;
  private _closed: boolean;
  private _error: Error | null;

  constructor(socket: net.Socket) {
    this._socket = socket;
    this._buffer = Buffer.alloc(0);
    this._pendingResolve = null;
    this._pendingReject = null;
    this._pendingBytes = 0;
    this._closed = false;
    this._error = null;

    this._socket.on("data", (chunk: Buffer) => {
      this._buffer = Buffer.concat([this._buffer, chunk]);
      this._tryResolve();
    });

    this._socket.on("end", () => {
      this._closed = true;
      if (this._pendingReject) {
        const reject = this._pendingReject;
        this._pendingResolve = null;
        this._pendingReject = null;
        reject(new IncompleteReadError("Connection closed"));
      }
    });

    this._socket.on("error", (err: Error) => {
      this._error = err;
      this._closed = true;
      if (this._pendingReject) {
        const reject = this._pendingReject;
        this._pendingResolve = null;
        this._pendingReject = null;
        reject(err);
      }
    });
  }

  /** Replace the underlying socket (e.g. after TLS upgrade). */
  replaceSocket(socket: net.Socket): void {
    // Remove old listeners -- but keep existing buffer
    this._socket.removeAllListeners("data");
    this._socket.removeAllListeners("end");
    this._socket.removeAllListeners("error");
    this._socket = socket;
    this._closed = false;
    this._error = null;

    this._socket.on("data", (chunk: Buffer) => {
      this._buffer = Buffer.concat([this._buffer, chunk]);
      this._tryResolve();
    });

    this._socket.on("end", () => {
      this._closed = true;
      if (this._pendingReject) {
        const reject = this._pendingReject;
        this._pendingResolve = null;
        this._pendingReject = null;
        reject(new IncompleteReadError("Connection closed"));
      }
    });

    this._socket.on("error", (err: Error) => {
      this._error = err;
      this._closed = true;
      if (this._pendingReject) {
        const reject = this._pendingReject;
        this._pendingResolve = null;
        this._pendingReject = null;
        reject(err);
      }
    });
  }

  /** Read exactly n bytes, returning a Promise that resolves when enough data is available. */
  readExactly(n: number): Promise<Buffer> {
    if (this._error) {
      return Promise.reject(this._error);
    }
    if (this._buffer.length >= n) {
      const result = this._buffer.subarray(0, n);
      this._buffer = this._buffer.subarray(n);
      return Promise.resolve(Buffer.from(result));
    }
    if (this._closed) {
      return Promise.reject(new IncompleteReadError("Connection closed"));
    }
    return new Promise<Buffer>((resolve, reject) => {
      this._pendingResolve = resolve;
      this._pendingReject = reject;
      this._pendingBytes = n;
    });
  }

  private _tryResolve(): void {
    if (this._pendingResolve !== null && this._buffer.length >= this._pendingBytes) {
      const n = this._pendingBytes;
      const result = this._buffer.subarray(0, n);
      this._buffer = this._buffer.subarray(n);
      const resolve = this._pendingResolve;
      this._pendingResolve = null;
      this._pendingReject = null;
      this._pendingBytes = 0;
      resolve(Buffer.from(result));
    }
  }
}

class IncompleteReadError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "IncompleteReadError";
  }
}

// ======================================================================
// Prepared statement / Portal
// ======================================================================

class PreparedStatement {
  readonly name: string;
  readonly query: string;
  readonly paramTypeOids: readonly number[];
  columnDescriptions: ColumnDescription[] | null;

  constructor(name: string, query: string, paramTypeOids: readonly number[]) {
    this.name = name;
    this.query = query;
    this.paramTypeOids = paramTypeOids;
    this.columnDescriptions = null;
  }
}

class Portal {
  readonly name: string;
  readonly statement: PreparedStatement;
  readonly paramValues: unknown[];
  readonly resultFormatCodes: readonly number[];
  resultCache: QueryResult | null;
  rowIndex: number;

  constructor(
    name: string,
    statement: PreparedStatement,
    paramValues: unknown[],
    resultFormatCodes: readonly number[],
  ) {
    this.name = name;
    this.statement = statement;
    this.paramValues = paramValues;
    this.resultFormatCodes = resultFormatCodes;
    this.resultCache = null;
    this.rowIndex = 0;
  }
}

// ======================================================================
// Text parameter coercion
// ======================================================================

function coerceTextParam(text: string): unknown {
  if (text === "true" || text === "t") return true;
  if (text === "false" || text === "f") return false;
  if (text === "") return text;
  if (/^-?\d+$/.test(text)) {
    const n = Number(text);
    if (Number.isSafeInteger(n)) return n;
  }
  if (/^-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?$/.test(text)) return Number(text);
  return text;
}

// ======================================================================
// Connection state machine
// ======================================================================

export class PGWireConnection {
  private _reader: SocketReader;
  private _socket: net.Socket;
  private readonly _engine: USQLEngine;
  private readonly _executor: QueryExecutor;
  private readonly _authMethod: string;
  private readonly _credentials: Record<string, string> | null;
  private readonly _processId: number;
  private readonly _secretKey: number;
  private readonly _cancelCallback:
    | ((processId: number, secretKey: number) => void)
    | null;
  private readonly _secureContext: tls.SecureContext | null;

  private _txStatus: number;
  private _statements: Map<string, PreparedStatement>;
  private _portals: Map<string, Portal>;
  private _sessionParams: Record<string, string>;
  private _username: string;
  private _database: string;
  private _closed: boolean;
  private _canceled: boolean;

  constructor(
    socket: net.Socket,
    engine: USQLEngine,
    options?: {
      authMethod?: string;
      credentials?: Record<string, string> | null;
      processId?: number;
      secretKey?: number;
      cancelCallback?: ((processId: number, secretKey: number) => void) | null;
      secureContext?: tls.SecureContext | null;
    },
  ) {
    this._socket = socket;
    this._reader = new SocketReader(socket);
    this._engine = engine;
    this._executor = new QueryExecutor(engine);
    this._authMethod = options?.authMethod ?? AuthMethod.TRUST;
    this._credentials = options?.credentials ?? null;
    this._processId = options?.processId ?? 0;
    this._secretKey = options?.secretKey ?? 0;
    this._cancelCallback = options?.cancelCallback ?? null;
    this._secureContext = options?.secureContext ?? null;

    this._txStatus = TX_IDLE;
    this._statements = new Map();
    this._portals = new Map();
    this._sessionParams = { ...DEFAULT_SERVER_PARAMS };
    this._username = "";
    this._database = "";
    this._closed = false;
    this._canceled = false;
  }

  get processId(): number {
    return this._processId;
  }

  get secretKey(): number {
    return this._secretKey;
  }

  /** Mark this connection's current query as canceled. */
  cancel(): void {
    this._canceled = true;
  }

  // ==================================================================
  // Main lifecycle
  // ==================================================================

  async run(): Promise<void> {
    try {
      const startup = await this._handleStartup();
      if (startup === null) {
        return; // SSL/Cancel handled, connection closed.
      }

      await this._authenticate(startup);
      await this._sendStartupParameters(startup);
      await this._mainLoop();
    } catch (err) {
      if (err instanceof IncompleteReadError) {
        // Client disconnected
      } else if (err instanceof Error && err.message.includes("ECONNRESET")) {
        // Connection reset
      } else {
        // Connection error -- log in production
      }
    } finally {
      this._close();
    }
  }

  // ==================================================================
  // Startup phase
  // ==================================================================

  private async _handleStartup(): Promise<StartupMessage | null> {
    while (true) {
      // Startup messages have no type byte: 4-byte length + payload.
      const rawLen = await this._reader.readExactly(4);
      const length = rawLen.readUInt32BE(0);
      const payload = await this._reader.readExactly(length - 4);

      const msg = MessageCodec.decodeStartup(payload);

      if (msg.kind === "SSLRequest") {
        if (this._secureContext !== null) {
          this._socket.write(Buffer.from([0x53])); // 'S'
          await this._drain();
          // Upgrade to TLS
          const tlsSocket = new tls.TLSSocket(this._socket, {
            secureContext: this._secureContext,
            isServer: true,
          });
          this._socket = tlsSocket;
          this._reader.replaceSocket(tlsSocket);
        } else {
          this._socket.write(Buffer.from([0x4e])); // 'N'
          await this._drain();
        }
        continue;
      }

      if (msg.kind === "GSSENCRequest") {
        this._socket.write(Buffer.from([0x4e])); // 'N'
        await this._drain();
        continue;
      }

      if (msg.kind === "CancelRequest") {
        const cancelMsg = msg as CancelRequest;
        if (this._cancelCallback !== null) {
          this._cancelCallback(cancelMsg.processId, cancelMsg.secretKey);
        }
        return null;
      }

      if (msg.kind === "StartupMessage") {
        const startupMsg = msg as StartupMessage;
        if (startupMsg.protocolVersion !== PROTOCOL_VERSION) {
          const err = new ProtocolViolation(
            `Unsupported protocol version: ` +
              `${startupMsg.protocolVersion >> 16}.` +
              `${startupMsg.protocolVersion & 0xffff}`,
          );
          await this._sendError(err);
          return null;
        }
        return startupMsg;
      }

      return null;
    }
  }

  // ==================================================================
  // Authentication
  // ==================================================================

  private async _authenticate(startup: StartupMessage): Promise<void> {
    this._username = startup.parameters["user"] ?? "";
    this._database = startup.parameters["database"] ?? this._username;

    const auth = createAuthenticator(
      this._authMethod,
      this._username,
      this._credentials,
    );
    let [response, done] = auth.initial();
    if (response.length > 0) {
      this._socket.write(response);
      await this._drain();
    }

    while (!done) {
      const [msgType, payload] = await this._readMessage();
      if (msgType !== 0x70) {
        // 'p'
        throw new ProtocolViolation(
          "Expected password/SASL message during authentication",
        );
      }

      // For SCRAM, we need to re-parse the 'p' message.
      if (this._authMethod === AuthMethod.SCRAM_SHA_256) {
        const scramAuth = auth as ScramSHA256Authenticator;
        if (scramAuth._phase === 0) {
          const sasl = MessageCodec.decodeSASLInitialResponse(payload);
          [response, done] = auth.step(sasl.data);
        } else {
          const saslR = MessageCodec.decodeSASLResponse(payload);
          [response, done] = auth.step(saslR.data);
        }
      } else {
        [response, done] = auth.step(payload);
      }

      if (response.length > 0) {
        this._socket.write(response);
        await this._drain();
      }
    }

    // Send AuthenticationOk.
    this._socket.write(MessageCodec.encodeAuthOk());
    await this._drain();
  }

  private async _sendStartupParameters(startup: StartupMessage): Promise<void> {
    // Merge client-requested parameters.
    if ("application_name" in startup.parameters) {
      this._sessionParams["application_name"] = startup.parameters["application_name"]!;
    }
    if ("client_encoding" in startup.parameters) {
      this._sessionParams["client_encoding"] = startup.parameters["client_encoding"]!;
    }

    // Send all parameter statuses.
    const parts: Buffer[] = [];
    for (const [name, value] of Object.entries(this._sessionParams)) {
      parts.push(MessageCodec.encodeParameterStatus(name, value));
    }

    // BackendKeyData.
    parts.push(MessageCodec.encodeBackendKeyData(this._processId, this._secretKey));

    // ReadyForQuery.
    parts.push(MessageCodec.encodeReadyForQuery(TX_IDLE));

    this._socket.write(Buffer.concat(parts));
    await this._drain();
  }

  // ==================================================================
  // Main message loop
  // ==================================================================

  private async _mainLoop(): Promise<void> {
    while (!this._closed) {
      const [msgType, payload] = await this._readMessage();
      const msg = MessageCodec.decodeFrontend(msgType, payload);

      switch (msg.kind) {
        case "Query":
          await this._handleQuery(msg as Query);
          break;
        case "Parse":
          await this._handleParse(msg as Parse);
          break;
        case "Bind":
          await this._handleBind(msg as Bind);
          break;
        case "Describe":
          await this._handleDescribe(msg as Describe);
          break;
        case "Execute":
          await this._handleExecute(msg as Execute);
          break;
        case "Close":
          await this._handleClose(msg as Close);
          break;
        case "Sync":
          await this._handleSync();
          break;
        case "Flush":
          await this._handleFlush();
          break;
        case "Terminate":
          this._closed = true;
          break;
        case "CopyData":
        case "CopyDone":
        case "CopyFail":
          // Handled within COPY context
          break;
        case "FunctionCall":
          await this._sendError(
            new FeatureNotSupported("Function call protocol is not supported"),
          );
          await this._sendReadyForQuery();
          break;
        default:
          await this._sendError(
            new ProtocolViolation(`Unexpected message type: ${msg.kind}`),
          );
          break;
      }
    }
  }

  // ==================================================================
  // Simple query protocol
  // ==================================================================

  private async _handleQuery(msg: Query): Promise<void> {
    const sql = msg.sql.trim();
    if (!sql) {
      this._socket.write(MessageCodec.encodeEmptyQueryResponse());
      await this._sendReadyForQuery();
      return;
    }

    const statements = QueryExecutor.splitStatements(sql);
    if (statements.length === 0) {
      this._socket.write(MessageCodec.encodeEmptyQueryResponse());
      await this._sendReadyForQuery();
      return;
    }

    for (const rawStmt of statements) {
      const stmt = rawStmt.trim();
      if (!stmt) {
        continue;
      }

      if (this._txStatus === TX_FAILED) {
        // In a failed transaction, reject all commands except
        // ROLLBACK / COMMIT.
        const upper = stmt.trim().toUpperCase();
        if (
          !upper.startsWith("ROLLBACK") &&
          !upper.startsWith("COMMIT") &&
          !upper.startsWith("END")
        ) {
          await this._sendError(
            new InFailedSQLTransaction(
              "current transaction is aborted, " +
                "commands ignored until end of " +
                "transaction block",
            ),
          );
          continue;
        }
      }

      try {
        const result = await this._executor.execute(stmt);
        await this._sendQueryResult(result);
      } catch (exc) {
        if (exc instanceof PGWireError) {
          await this._sendError(exc);
        } else {
          await this._sendError(mapEngineException(exc));
        }
        if (this._txStatus === TX_IN_TRANSACTION) {
          this._txStatus = TX_FAILED;
        }
        break;
      }
    }

    await this._sendReadyForQuery();
  }

  private async _sendQueryResult(result: QueryResult): Promise<void> {
    const parts: Buffer[] = [];

    if (result.isSelect && result.columns.length > 0) {
      parts.push(MessageCodec.encodeRowDescription(result.columns));

      for (const row of result.rows) {
        const values: (Buffer | null)[] = [];
        for (const colDesc of result.columns) {
          const val = row[colDesc.name];
          const encoded = TypeCodec.encodeText(val, colDesc.typeOid);
          values.push(encoded);
        }
        parts.push(MessageCodec.encodeDataRow(values));
      }
    }

    parts.push(MessageCodec.encodeCommandComplete(result.commandTag));
    this._socket.write(Buffer.concat(parts));
  }

  // ==================================================================
  // Extended query protocol
  // ==================================================================

  private async _handleParse(msg: Parse): Promise<void> {
    try {
      const stmt = new PreparedStatement(
        msg.statementName,
        msg.query,
        msg.paramTypeOids,
      );

      // Replace unnamed statement and invalidate dependent portals.
      if (msg.statementName === "") {
        this._invalidatePortalsForStatement("");
      }
      this._statements.set(msg.statementName, stmt);

      this._socket.write(MessageCodec.encodeParseComplete());
    } catch (exc) {
      if (exc instanceof PGWireError) {
        await this._sendError(exc);
      } else {
        await this._sendError(mapEngineException(exc));
      }
    }
  }

  private async _handleBind(msg: Bind): Promise<void> {
    try {
      const stmt = this._statements.get(msg.statementName);
      if (stmt === undefined) {
        throw new PGWireError(
          `prepared statement "${msg.statementName}" does not exist`,
        );
      }

      // Decode parameter values.
      const decodedParams: unknown[] = [];
      for (let i = 0; i < msg.paramValues.length; i++) {
        const rawVal = msg.paramValues[i]!;
        if (rawVal === null) {
          decodedParams.push(null);
          continue;
        }

        // Determine parameter format.
        let fmt: number;
        if (msg.paramFormatCodes.length > 0) {
          if (msg.paramFormatCodes.length === 1) {
            fmt = msg.paramFormatCodes[0]!;
          } else if (i < msg.paramFormatCodes.length) {
            fmt = msg.paramFormatCodes[i]!;
          } else {
            fmt = FORMAT_TEXT;
          }
        } else {
          fmt = FORMAT_TEXT;
        }

        // Determine type OID.
        let oid: number;
        if (i < stmt.paramTypeOids.length) {
          oid = stmt.paramTypeOids[i]!;
        } else {
          oid = 0; // unspecified
        }

        if (fmt === FORMAT_BINARY && oid !== 0) {
          decodedParams.push(TypeCodec.decodeBinary(rawVal, oid));
        } else {
          if (oid !== 0) {
            decodedParams.push(TypeCodec.decodeText(rawVal, oid));
          } else {
            // OID is unspecified and format is text -- auto-coerce
            const text = rawVal.toString("utf-8");
            decodedParams.push(coerceTextParam(text));
          }
        }
      }

      // Replace unnamed portal.
      if (msg.portalName === "") {
        this._portals.delete("");
      }

      const portal = new Portal(
        msg.portalName,
        stmt,
        decodedParams,
        msg.resultFormatCodes,
      );
      this._portals.set(msg.portalName, portal);

      this._socket.write(MessageCodec.encodeBindComplete());
    } catch (exc) {
      if (exc instanceof PGWireError) {
        await this._sendError(exc);
      } else {
        await this._sendError(mapEngineException(exc));
      }
    }
  }

  private async _handleDescribe(msg: Describe): Promise<void> {
    try {
      if (msg.target === "S") {
        const stmt = this._statements.get(msg.name);
        if (stmt === undefined) {
          throw new PGWireError(`prepared statement "${msg.name}" does not exist`);
        }
        // Send ParameterDescription.
        this._socket.write(MessageCodec.encodeParameterDescription(stmt.paramTypeOids));

        // Execute to get column descriptions if not cached.
        if (stmt.columnDescriptions === null) {
          try {
            const result = await this._executor.execute(stmt.query);
            stmt.columnDescriptions = result.columns;
          } catch {
            stmt.columnDescriptions = [];
          }
        }

        if (stmt.columnDescriptions.length > 0) {
          this._socket.write(
            MessageCodec.encodeRowDescription(stmt.columnDescriptions),
          );
        } else {
          this._socket.write(MessageCodec.encodeNoData());
        }
      } else if (msg.target === "P") {
        const portal = this._portals.get(msg.name);
        if (portal === undefined) {
          throw new PGWireError(`portal "${msg.name}" does not exist`);
        }

        // Execute to get column descriptions if not cached.
        if (portal.resultCache === null) {
          try {
            const result = await this._executor.execute(
              portal.statement.query,
              portal.paramValues.length > 0 ? portal.paramValues : null,
            );
            portal.resultCache = result;
          } catch {
            portal.resultCache = new QueryResult([], [], "");
          }
        }

        if (portal.resultCache.columns.length > 0) {
          // Apply result format codes.
          const cols = PGWireConnection._applyFormatCodes(
            portal.resultCache.columns,
            portal.resultFormatCodes,
          );
          this._socket.write(MessageCodec.encodeRowDescription(cols));
        } else {
          this._socket.write(MessageCodec.encodeNoData());
        }
      } else {
        throw new ProtocolViolation(`Invalid Describe kind: '${msg.target}'`);
      }
    } catch (exc) {
      if (exc instanceof PGWireError) {
        await this._sendError(exc);
      } else {
        await this._sendError(mapEngineException(exc));
      }
    }
  }

  private async _handleExecute(msg: Execute): Promise<void> {
    try {
      const portal = this._portals.get(msg.portalName);
      if (portal === undefined) {
        throw new PGWireError(`portal "${msg.portalName}" does not exist`);
      }

      // Check transaction state.
      if (this._txStatus === TX_FAILED) {
        throw new InFailedSQLTransaction(
          "current transaction is aborted, " +
            "commands ignored until end of transaction block",
        );
      }

      // Execute if not cached.
      if (portal.resultCache === null) {
        const result = await this._executor.execute(
          portal.statement.query,
          portal.paramValues.length > 0 ? portal.paramValues : null,
        );
        portal.resultCache = result;
      }

      const result = portal.resultCache;

      if (result.isSelect && result.columns.length > 0) {
        // Apply result format codes.
        const cols = PGWireConnection._applyFormatCodes(
          result.columns,
          portal.resultFormatCodes,
        );

        // Determine how many rows to send.
        const remaining = result.rows.slice(portal.rowIndex);
        let batch: Record<string, unknown>[];
        let suspended: boolean;
        if (msg.maxRows > 0 && remaining.length > msg.maxRows) {
          batch = remaining.slice(0, msg.maxRows);
          portal.rowIndex += msg.maxRows;
          suspended = true;
        } else {
          batch = remaining;
          portal.rowIndex += batch.length;
          suspended = false;
        }

        const parts: Buffer[] = [];
        for (const row of batch) {
          const values: (Buffer | null)[] = [];
          for (const colDesc of cols) {
            const val = row[colDesc.name];
            let encoded: Buffer | null;
            if (colDesc.formatCode === FORMAT_BINARY) {
              encoded = TypeCodec.encodeBinary(val, colDesc.typeOid);
            } else {
              encoded = TypeCodec.encodeText(val, colDesc.typeOid);
            }
            values.push(encoded);
          }
          parts.push(MessageCodec.encodeDataRow(values));
        }

        if (suspended) {
          parts.push(MessageCodec.encodePortalSuspended());
        } else {
          parts.push(MessageCodec.encodeCommandComplete(result.commandTag));
        }
        this._socket.write(Buffer.concat(parts));
      } else {
        this._socket.write(MessageCodec.encodeCommandComplete(result.commandTag));
      }
    } catch (exc) {
      if (exc instanceof PGWireError) {
        await this._sendError(exc);
      } else {
        await this._sendError(mapEngineException(exc));
      }
      if (this._txStatus === TX_IN_TRANSACTION) {
        this._txStatus = TX_FAILED;
      }
    }
  }

  private async _handleClose(msg: Close): Promise<void> {
    if (msg.target === "S") {
      this._statements.delete(msg.name);
      this._invalidatePortalsForStatement(msg.name);
    } else if (msg.target === "P") {
      this._portals.delete(msg.name);
    }
    this._socket.write(MessageCodec.encodeCloseComplete());
  }

  private async _handleSync(): Promise<void> {
    await this._sendReadyForQuery();
  }

  private async _handleFlush(): Promise<void> {
    await this._drain();
  }

  // ==================================================================
  // Helpers
  // ==================================================================

  private async _readMessage(): Promise<[number, Buffer]> {
    const header = await this._reader.readExactly(5);
    const msgType = header[0]!;
    const length = header.readUInt32BE(1);
    if (length < 4) {
      throw new ProtocolViolation(`Invalid message length: ${length}`);
    }
    let payload: Buffer = Buffer.alloc(0);
    if (length > 4) {
      payload = await this._reader.readExactly(length - 4);
    }
    return [msgType, payload];
  }

  private async _sendError(error: PGWireError): Promise<void> {
    this._socket.write(MessageCodec.encodeErrorResponse(error.toFields()));
    await this._drain();
  }

  private async _sendReadyForQuery(): Promise<void> {
    this._socket.write(MessageCodec.encodeReadyForQuery(this._txStatus));
    await this._drain();
  }

  private _invalidatePortalsForStatement(stmtName: string): void {
    const toRemove: string[] = [];
    for (const [name, portal] of this._portals) {
      if (portal.statement.name === stmtName) {
        toRemove.push(name);
      }
    }
    for (const name of toRemove) {
      this._portals.delete(name);
    }
  }

  private static _applyFormatCodes(
    columns: ColumnDescription[],
    formatCodes: readonly number[],
  ): ColumnDescription[] {
    if (formatCodes.length === 0) {
      return columns;
    }

    const result: ColumnDescription[] = [];
    for (let i = 0; i < columns.length; i++) {
      const col = columns[i]!;
      let fmt: number;
      if (formatCodes.length === 1) {
        fmt = formatCodes[0]!;
      } else if (i < formatCodes.length) {
        fmt = formatCodes[i]!;
      } else {
        fmt = FORMAT_TEXT;
      }
      result.push(columnDescriptionWithFormatCode(col, fmt));
    }
    return result;
  }

  /** Await the socket 'drain' event when backpressure occurs. */
  private _drain(): Promise<void> {
    return new Promise<void>((resolve) => {
      if (this._socket.writableNeedDrain) {
        this._socket.once("drain", resolve);
      } else {
        resolve();
      }
    });
  }

  private _close(): void {
    this._closed = true;
    try {
      if (!this._socket.destroyed) {
        this._socket.destroy();
      }
    } catch {
      // Ignore cleanup errors
    }
  }
}
