// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
// Copyright (c) 2023-2026 Cognica, Inc.

// Bridge between the pgwire protocol layer and USQLEngine.
//
// The QueryExecutor translates wire-protocol requests into
// USQLEngine.sql() calls, builds ColumnDescription metadata,
// generates PostgreSQL CommandComplete tags, and intercepts statements
// that UQA does not handle natively (SET, SHOW, RESET,
// DISCARD, BEGIN, COMMIT, ROLLBACK).

import { FORMAT_TEXT } from "./constants.js";
import { mapEngineException } from "./errors.js";
import type { ColumnDescription } from "./messages.js";
import { TypeCodec } from "./type-codec.js";
import { TYPE_LENGTHS, typeOid } from "../../pg-compat/oid.js";
import { DEFAULT_SERVER_PARAMS } from "./constants.js";

import type { USQLEngine } from "../../core/engine.js";

// Internal columns that should not appear in wire-protocol results.
const INTERNAL_COLUMNS = new Set(["_doc_id", "_score"]);

// Regex for SET statements: SET name = value / SET name TO value
const SET_RE = new RegExp(
  "^\\s*SET\\s+(?:SESSION\\s+|LOCAL\\s+)?" +
    "(\\w+(?:\\.\\w+)?)\\s*(?:=|TO)\\s*(.+?)\\s*;?\\s*$",
  "i",
);

// Regex for SHOW statements: SHOW name
const SHOW_RE = new RegExp("^\\s*SHOW\\s+(\\w+(?:\\.\\w+)?)\\s*;?\\s*$", "i");

// Regex for RESET: RESET name / RESET ALL
const RESET_RE = new RegExp("^\\s*RESET\\s+(ALL|\\w+(?:\\.\\w+)?)\\s*;?\\s*$", "i");

// Regex for DISCARD: DISCARD ALL / DISCARD PLANS / etc.
const DISCARD_RE = new RegExp("^\\s*DISCARD\\s+(\\w+)\\s*;?\\s*$", "i");

// Transaction commands
const TX_BEGIN_RE = new RegExp("^\\s*(BEGIN|START\\s+TRANSACTION)\\b", "i");
const TX_COMMIT_RE = new RegExp("^\\s*(COMMIT|END)\\b", "i");
const TX_ROLLBACK_RE = new RegExp("^\\s*ROLLBACK\\b", "i");

// DEALLOCATE for closing prepared statements via SQL
const DEALLOCATE_RE = new RegExp(
  "^\\s*DEALLOCATE\\s+(?:PREPARE\\s+)?(?:ALL|(\\w+))\\s*;?\\s*$",
  "i",
);

// LISTEN / UNLISTEN / NOTIFY
const LISTEN_RE = new RegExp("^\\s*LISTEN\\s+", "i");
const UNLISTEN_RE = new RegExp("^\\s*UNLISTEN\\s+", "i");
const NOTIFY_RE = new RegExp("^\\s*NOTIFY\\s+", "i");

// Regex-based command type detection (replaces pglast parse_sql)
const CMD_TYPE_PATTERNS: [RegExp, string][] = [
  [/^\s*SELECT\b/i, "SELECT"],
  [/^\s*INSERT\b/i, "INSERT"],
  [/^\s*UPDATE\b/i, "UPDATE"],
  [/^\s*DELETE\b/i, "DELETE"],
  [/^\s*CREATE\s+TABLE\b/i, "CREATE TABLE"],
  [/^\s*CREATE\s+INDEX\b/i, "CREATE INDEX"],
  [/^\s*CREATE\s+VIEW\b/i, "CREATE VIEW"],
  [/^\s*CREATE\s+SEQUENCE\b/i, "CREATE SEQUENCE"],
  [/^\s*CREATE\s+SCHEMA\b/i, "CREATE SCHEMA"],
  [/^\s*CREATE\s+FOREIGN\s+TABLE\b/i, "CREATE FOREIGN TABLE"],
  [/^\s*CREATE\s+SERVER\b/i, "CREATE SERVER"],
  [/^\s*CREATE\s+FOREIGN\s+DATA\s+WRAPPER\b/i, "CREATE FOREIGN DATA WRAPPER"],
  [/^\s*DROP\s+TABLE\b/i, "DROP TABLE"],
  [/^\s*DROP\s+INDEX\b/i, "DROP INDEX"],
  [/^\s*DROP\s+VIEW\b/i, "DROP VIEW"],
  [/^\s*DROP\s+SCHEMA\b/i, "DROP SCHEMA"],
  [/^\s*ALTER\s+TABLE\b/i, "ALTER TABLE"],
  [/^\s*ALTER\s+SEQUENCE\b/i, "ALTER SEQUENCE"],
  [/^\s*TRUNCATE\b/i, "TRUNCATE TABLE"],
  [/^\s*EXPLAIN\b/i, "EXPLAIN"],
  [/^\s*COPY\b/i, "COPY"],
  [/^\s*SET\b/i, "SET"],
  [/^\s*SHOW\b/i, "SHOW"],
  [/^\s*(BEGIN|START\s+TRANSACTION)\b/i, "BEGIN"],
  [/^\s*(COMMIT|END)\b/i, "COMMIT"],
  [/^\s*ROLLBACK\b/i, "ROLLBACK"],
];

export class QueryResult {
  readonly columns: ColumnDescription[];
  readonly rows: Record<string, unknown>[];
  readonly commandTag: string;
  readonly isSelect: boolean;

  constructor(
    columns: ColumnDescription[],
    rows: Record<string, unknown>[],
    commandTag: string,
    options?: { isSelect?: boolean },
  ) {
    this.columns = columns;
    this.rows = rows;
    this.commandTag = commandTag;
    this.isSelect = options?.isSelect ?? false;
  }
}

export class QueryExecutor {
  private readonly _engine: USQLEngine;
  private _sessionParams: Record<string, string>;

  constructor(engine: USQLEngine) {
    this._engine = engine;
    this._sessionParams = {};
  }

  get sessionParams(): Record<string, string> {
    return this._sessionParams;
  }

  // ==================================================================
  // Main execution entry point
  // ==================================================================

  async execute(query: string, params?: unknown[] | null): Promise<QueryResult> {
    // Try intercepting connection-level commands first.
    const intercepted = this._tryIntercept(query);
    if (intercepted !== null) {
      return intercepted;
    }

    // Execute via engine (async in JS).
    try {
      const result = await this._engine.sql(query, params ?? undefined);
      return this._buildResult(query, result);
    } catch (exc) {
      throw mapEngineException(exc);
    }
  }

  // ==================================================================
  // Multi-statement splitting
  // ==================================================================

  /**
   * Split a SQL string into individual statements.
   * Semicolon-aware splitter that respects string literals and
   * dollar-quoted strings.
   */
  static splitStatements(sql: string): string[] {
    const stripped = sql.trim();
    if (!stripped) {
      return [];
    }

    const statements: string[] = [];
    let current = "";
    let i = 0;

    while (i < stripped.length) {
      const ch = stripped[i]!;

      // Single-quoted string literal
      if (ch === "'") {
        current += ch;
        i++;
        while (i < stripped.length) {
          const c = stripped[i]!;
          current += c;
          i++;
          if (c === "'" && i < stripped.length && stripped[i] === "'") {
            // Escaped quote
            current += "'";
            i++;
          } else if (c === "'") {
            break;
          }
        }
        continue;
      }

      // Double-quoted identifier
      if (ch === '"') {
        current += ch;
        i++;
        while (i < stripped.length) {
          const c = stripped[i]!;
          current += c;
          i++;
          if (c === '"') {
            break;
          }
        }
        continue;
      }

      // Dollar-quoted string ($$...$$, $tag$...$tag$)
      if (ch === "$") {
        let tag = "$";
        let j = i + 1;
        while (j < stripped.length && stripped[j] !== "$") {
          if (/[a-zA-Z0-9_]/.test(stripped[j]!)) {
            tag += stripped[j]!;
            j++;
          } else {
            break;
          }
        }
        if (j < stripped.length && stripped[j] === "$") {
          tag += "$";
          j++;
          current += tag;
          i = j;
          // Find matching end tag
          const endIdx = stripped.indexOf(tag, i);
          if (endIdx !== -1) {
            current += stripped.slice(i, endIdx + tag.length);
            i = endIdx + tag.length;
          } else {
            // No matching end tag -- consume the rest
            current += stripped.slice(i);
            i = stripped.length;
          }
          continue;
        }
        // Not a dollar-quote
        current += ch;
        i++;
        continue;
      }

      // Line comment
      if (ch === "-" && i + 1 < stripped.length && stripped[i + 1] === "-") {
        const nlIdx = stripped.indexOf("\n", i);
        if (nlIdx === -1) {
          i = stripped.length;
        } else {
          current += " ";
          i = nlIdx + 1;
        }
        continue;
      }

      // Block comment
      if (ch === "/" && i + 1 < stripped.length && stripped[i + 1] === "*") {
        let depth = 1;
        i += 2;
        while (i < stripped.length && depth > 0) {
          if (
            stripped[i] === "/" &&
            i + 1 < stripped.length &&
            stripped[i + 1] === "*"
          ) {
            depth++;
            i += 2;
          } else if (
            stripped[i] === "*" &&
            i + 1 < stripped.length &&
            stripped[i + 1] === "/"
          ) {
            depth--;
            i += 2;
          } else {
            i++;
          }
        }
        current += " ";
        continue;
      }

      // Statement separator
      if (ch === ";") {
        const trimmed = current.trim();
        if (trimmed) {
          statements.push(trimmed);
        }
        current = "";
        i++;
        continue;
      }

      current += ch;
      i++;
    }

    const trimmed = current.trim();
    if (trimmed) {
      statements.push(trimmed);
    }

    return statements;
  }

  // ==================================================================
  // Command interception
  // ==================================================================

  private _tryIntercept(query: string): QueryResult | null {
    let m = SET_RE.exec(query);
    if (m) {
      return this._handleSet(m[1]!, m[2]!);
    }

    m = SHOW_RE.exec(query);
    if (m) {
      return this._handleShow(m[1]!);
    }

    m = RESET_RE.exec(query);
    if (m) {
      return this._handleReset(m[1]!);
    }

    m = DISCARD_RE.exec(query);
    if (m) {
      return this._handleDiscard(m[1]!);
    }

    if (TX_BEGIN_RE.test(query)) {
      return this._handleTransaction("BEGIN");
    }
    if (TX_COMMIT_RE.test(query)) {
      return this._handleTransaction("COMMIT");
    }
    if (TX_ROLLBACK_RE.test(query)) {
      return this._handleTransaction("ROLLBACK");
    }

    if (DEALLOCATE_RE.test(query)) {
      return new QueryResult([], [], "DEALLOCATE");
    }

    if (LISTEN_RE.test(query)) {
      return new QueryResult([], [], "LISTEN");
    }
    if (UNLISTEN_RE.test(query)) {
      return new QueryResult([], [], "UNLISTEN");
    }
    if (NOTIFY_RE.test(query)) {
      return new QueryResult([], [], "NOTIFY");
    }

    return null;
  }

  private _handleSet(name: string, value: string): QueryResult {
    // Strip quotes from value.
    const cleaned = value.trim().replace(/^['"]|['"]$/g, "");
    this._sessionParams[name.toLowerCase()] = cleaned;
    return new QueryResult([], [], "SET");
  }

  private _handleShow(name: string): QueryResult {
    const key = name.toLowerCase();
    let value = this._sessionParams[key];
    if (value === undefined) {
      value = DEFAULT_SERVER_PARAMS[key] ?? "";
    }

    // SHOW returns a single row with the parameter name as column.
    const col: ColumnDescription = {
      name: key,
      tableOid: 0,
      columnNumber: 0,
      typeOid: 25, // text
      typeSize: -1,
      typeModifier: -1,
      formatCode: FORMAT_TEXT,
    };
    return new QueryResult([col], [{ [key]: value }], "SHOW", { isSelect: true });
  }

  private _handleReset(name: string): QueryResult {
    if (name.toUpperCase() === "ALL") {
      this._sessionParams = {};
    } else {
      const key = name.toLowerCase();
      this._sessionParams = Object.fromEntries(
        Object.entries(this._sessionParams).filter(([k]) => k !== key),
      );
    }
    return new QueryResult([], [], "RESET");
  }

  private _handleDiscard(what: string): QueryResult {
    if (what.toUpperCase() === "ALL") {
      this._sessionParams = {};
    }
    return new QueryResult([], [], "DISCARD ALL");
  }

  private _handleTransaction(cmd: string): QueryResult {
    return new QueryResult([], [], cmd);
  }

  // ==================================================================
  // Result building
  // ==================================================================

  private _buildResult(query: string, result: unknown): QueryResult {
    const res = result as {
      columns?: string[];
      rows?: Record<string, unknown>[];
    } | null;

    const columnsRaw: string[] = res?.columns ?? [];
    const rowsRaw: Record<string, unknown>[] = res?.rows ?? [];

    // Detect command type for the tag.
    const cmdType = this._detectCommandType(query);

    // For DML results, build the appropriate tag.
    if (cmdType === "INSERT") {
      const count =
        rowsRaw.length > 0 ? ((rowsRaw[0]?.["inserted"] as number) ?? 0) : 0;
      return new QueryResult([], [], `INSERT 0 ${count}`);
    }

    if (cmdType === "UPDATE") {
      const count = rowsRaw.length > 0 ? ((rowsRaw[0]?.["updated"] as number) ?? 0) : 0;
      return new QueryResult([], [], `UPDATE ${count}`);
    }

    if (cmdType === "DELETE") {
      const count = rowsRaw.length > 0 ? ((rowsRaw[0]?.["deleted"] as number) ?? 0) : 0;
      return new QueryResult([], [], `DELETE ${count}`);
    }

    // DDL: no rows
    if (columnsRaw.length === 0) {
      return new QueryResult([], [], cmdType);
    }

    // SELECT: filter internal columns and build descriptions.
    const visibleColumns = columnsRaw.filter((c) => !INTERNAL_COLUMNS.has(c));

    const colDescs = this._buildColumnDescriptions(visibleColumns, rowsRaw);

    // Filter rows to only visible columns.
    const filteredRows = rowsRaw.map((row) => {
      const filtered: Record<string, unknown> = {};
      for (const [k, v] of Object.entries(row)) {
        if (!INTERNAL_COLUMNS.has(k)) {
          filtered[k] = v;
        }
      }
      return filtered;
    });

    const tag = `SELECT ${filteredRows.length}`;
    return new QueryResult(colDescs, filteredRows, tag, { isSelect: true });
  }

  private _buildColumnDescriptions(
    columns: string[],
    rows: Record<string, unknown>[],
  ): ColumnDescription[] {
    const descriptions: ColumnDescription[] = [];
    const engine = this._engine as unknown as Record<string, unknown>;
    const tables =
      (engine["_tables"] as Map<string, Record<string, unknown>> | undefined) ??
      new Map<string, Record<string, unknown>>();

    for (const colName of columns) {
      let oid = 25; // default: text
      const tableOid = 0;
      let colNumber = 0;
      let typeSize = -1;
      const typeMod = -1;

      // Try to resolve from table metadata.
      for (const [, table] of tables) {
        const tableCols = table["columns"] as
          | Record<string, { type_name?: string; typeName?: string }>
          | undefined;
        if (tableCols && colName in tableCols) {
          const colDef = tableCols[colName]!;
          const typeName = colDef.typeName ?? colDef.type_name ?? "text";
          oid = typeOid(typeName);
          typeSize = TYPE_LENGTHS[oid] ?? -1;
          const colKeys = Object.keys(tableCols);
          colNumber = colKeys.indexOf(colName) + 1;
          break;
        }
      }

      // Fall back to value-based inference.
      if (oid === 25 && rows.length > 0) {
        let firstVal: unknown = undefined;
        for (const row of rows) {
          const val = row[colName];
          if (val !== null && val !== undefined) {
            firstVal = val;
            break;
          }
        }
        if (firstVal !== undefined) {
          oid = TypeCodec.inferTypeOid(firstVal);
          typeSize = TYPE_LENGTHS[oid] ?? -1;
        }
      }

      descriptions.push({
        name: colName,
        tableOid,
        columnNumber: colNumber,
        typeOid: oid,
        typeSize,
        typeModifier: typeMod,
        formatCode: FORMAT_TEXT,
      });
    }

    return descriptions;
  }

  private _detectCommandType(query: string): string {
    for (const [pattern, tag] of CMD_TYPE_PATTERNS) {
      if (pattern.test(query)) {
        return tag;
      }
    }

    // Fallback: first word(s).
    const words = query.trim().split(/\s+/);
    const firstWord = words[0]?.toUpperCase() ?? "";
    if (firstWord === "CREATE" && words.length >= 2) {
      return `CREATE ${words[1]!.toUpperCase()}`;
    }
    if (firstWord === "DROP" && words.length >= 2) {
      return `DROP ${words[1]!.toUpperCase()}`;
    }
    return firstWord;
  }
}
