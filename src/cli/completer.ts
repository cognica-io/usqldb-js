// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
// Copyright (c) 2023-2026 Cognica, Inc.

// Context-aware SQL completer for the usqldb interactive shell.
//
// Provides tab-completion for SQL keywords, table/view/column names,
// schema-qualified names, and backslash commands.

import type { USQLEngine } from "../core/engine.js";

// SQL keywords (uppercase) -- covers DDL, DML, DQL, and UQA extensions
const SQL_KEYWORDS: string[] = [
  // DDL
  "CREATE",
  "TABLE",
  "DROP",
  "IF",
  "EXISTS",
  "PRIMARY",
  "KEY",
  "NOT",
  "NULL",
  "DEFAULT",
  "SERIAL",
  "BIGSERIAL",
  "ALTER",
  "ADD",
  "COLUMN",
  "RENAME",
  "TO",
  "SET",
  "TRUNCATE",
  "UNIQUE",
  "CHECK",
  "CONSTRAINT",
  "REFERENCES",
  "FOREIGN",
  "CASCADE",
  "RESTRICT",
  "TEMPORARY",
  "TEMP",
  "VIEW",
  "INDEX",
  "SEQUENCE",
  "USING",
  // Types
  "INTEGER",
  "INT",
  "BIGINT",
  "SMALLINT",
  "TEXT",
  "VARCHAR",
  "REAL",
  "FLOAT",
  "DOUBLE",
  "PRECISION",
  "NUMERIC",
  "DECIMAL",
  "BOOLEAN",
  "BOOL",
  "CHAR",
  "CHARACTER",
  "JSON",
  "JSONB",
  "UUID",
  "BYTEA",
  "DATE",
  "TIMESTAMP",
  "TIMESTAMPTZ",
  "INTERVAL",
  "POINT",
  "VECTOR",
  // DML
  "INSERT",
  "INTO",
  "VALUES",
  "UPDATE",
  "DELETE",
  "RETURNING",
  "ON",
  "CONFLICT",
  "DO",
  "NOTHING",
  "EXCLUDED",
  // DQL
  "SELECT",
  "FROM",
  "WHERE",
  "AND",
  "OR",
  "IN",
  "BETWEEN",
  "ORDER",
  "BY",
  "ASC",
  "DESC",
  "LIMIT",
  "OFFSET",
  "AS",
  "DISTINCT",
  "GROUP",
  "HAVING",
  "LIKE",
  "ILIKE",
  "IS",
  "CASE",
  "WHEN",
  "THEN",
  "ELSE",
  "END",
  "CAST",
  "COALESCE",
  "NULLIF",
  "UNION",
  "ALL",
  "EXCEPT",
  "INTERSECT",
  "TRUE",
  "FALSE",
  // Joins
  "JOIN",
  "INNER",
  "LEFT",
  "RIGHT",
  "FULL",
  "CROSS",
  "OUTER",
  // Subqueries / CTE
  "WITH",
  "RECURSIVE",
  // Aggregates
  "COUNT",
  "SUM",
  "AVG",
  "MIN",
  "MAX",
  "ARRAY_AGG",
  "STRING_AGG",
  "BOOL_AND",
  "BOOL_OR",
  "FILTER",
  // Window functions
  "OVER",
  "PARTITION",
  "WINDOW",
  "ROWS",
  "RANGE",
  "UNBOUNDED",
  "PRECEDING",
  "FOLLOWING",
  "CURRENT",
  "ROW",
  "ROW_NUMBER",
  "RANK",
  "DENSE_RANK",
  "NTILE",
  "LAG",
  "LEAD",
  "FIRST_VALUE",
  "LAST_VALUE",
  "NTH_VALUE",
  "PERCENT_RANK",
  "CUME_DIST",
  // FDW
  "SERVER",
  "DATA",
  "WRAPPER",
  "OPTIONS",
  "IMPORT",
  // Transaction
  "BEGIN",
  "COMMIT",
  "ROLLBACK",
  "SAVEPOINT",
  "RELEASE",
  // Utility
  "EXPLAIN",
  "ANALYZE",
  "PREPARE",
  "EXECUTE",
  "DEALLOCATE",
  "GENERATE_SERIES",
  // UQA extensions
  "text_match",
  "bayesian_match",
  "knn_match",
  "traverse",
  "rpq",
  "traverse_match",
  "fuse_log_odds",
  "fuse_prob_and",
  "fuse_prob_or",
  "fuse_prob_not",
  "fuse_attention",
  "fuse_learned",
  "spatial_within",
  "multi_field_match",
  "bayesian_knn_match",
  "deep_fusion",
  "deep_learn",
  "deep_predict",
  // Schema names
  "information_schema",
  "pg_catalog",
];

const BACKSLASH_COMMANDS: [string, string][] = [
  ["\\d", "Describe table/view/index or list relations"],
  ["\\dt", "List tables"],
  ["\\di", "List indexes"],
  ["\\dv", "List views"],
  ["\\ds", "List sequences"],
  ["\\df", "List functions"],
  ["\\dn", "List schemas"],
  ["\\du", "List roles"],
  ["\\l", "List databases"],
  ["\\det", "List foreign tables"],
  ["\\des", "List foreign servers"],
  ["\\dew", "List foreign data wrappers"],
  ["\\dG", "List named graphs"],
  ["\\x", "Toggle expanded display"],
  ["\\timing", "Toggle timing"],
  ["\\o", "Redirect output to file"],
  ["\\i", "Execute commands from file"],
  ["\\e", "Edit query in external editor"],
  ["\\conninfo", "Display connection info"],
  ["\\encoding", "Show client encoding"],
  ["\\!", "Execute shell command"],
  ["\\?", "Show help"],
  ["\\q", "Quit"],
];

// Keywords that typically precede a table name
const TABLE_PRECEDING = new Set([
  "FROM",
  "JOIN",
  "INTO",
  "TABLE",
  "ANALYZE",
  "UPDATE",
  "DELETE",
  "INNER",
  "LEFT",
  "RIGHT",
  "FULL",
  "CROSS",
]);

/**
 * Context-aware SQL completer with dynamic table/column names.
 *
 * Adapts to Node.js readline's completer interface, returning
 * [completions, original] tuples.
 */
export class Completer {
  private readonly _engine: USQLEngine;

  constructor(engine: USQLEngine) {
    this._engine = engine;
  }

  /**
   * Node.js readline-compatible completer function.
   *
   * Returns [matchingCompletions, originalSubstring].
   */
  complete(line: string): [string[], string] {
    const trimmed = line.trimStart();

    // -- Backslash commands -------------------------------------------
    if (trimmed.startsWith("\\")) {
      const matches: string[] = [];
      for (const [cmd] of BACKSLASH_COMMANDS) {
        if (cmd.startsWith(trimmed)) {
          matches.push(cmd);
        }
      }
      // After backslash command, complete table names
      const parts = trimmed.split(/\s+/);
      if (parts.length >= 2) {
        const word = parts[parts.length - 1] ?? "";
        const tableMatches = this._tableCompletions(word);
        return [tableMatches, word];
      }
      return [matches, trimmed];
    }

    const word = this._getWordBeforeCursor(line);
    if (!word) {
      return [[], ""];
    }

    const upper = word.toUpperCase();

    // Detect if previous keyword expects a table name
    const before = line
      .slice(0, line.length - word.length)
      .toUpperCase()
      .trimEnd();
    let afterTableKw = false;
    for (const kw of TABLE_PRECEDING) {
      if (before.endsWith(kw)) {
        afterTableKw = true;
        break;
      }
    }

    const candidates: [string, string, number][] = [];

    // SQL keywords
    for (const kw of SQL_KEYWORDS) {
      if (kw.toUpperCase().startsWith(upper)) {
        candidates.push([kw, "keyword", 0]);
      }
    }

    // Table names (regular + foreign)
    const compiler = this._getCompiler();
    const tables = this._getMap(compiler, "_tables");
    const foreignTables = this._getMap(compiler, "_foreignTables");
    const views = this._getMap(compiler, "_views");

    for (const name of tables.keys()) {
      if (name.toUpperCase().startsWith(upper)) {
        candidates.push([name, "table", 0]);
      }
    }
    for (const name of foreignTables.keys()) {
      if (name.toUpperCase().startsWith(upper)) {
        candidates.push([name, "foreign table", 0]);
      }
    }

    // View names
    for (const name of views.keys()) {
      if (name.toUpperCase().startsWith(upper)) {
        candidates.push([name, "view", 0]);
      }
    }

    // Column names (only when not after a table keyword)
    if (!afterTableKw) {
      const seen = new Set<string>();
      const colNames = this._getColumnNames(tables);
      for (const colName of colNames) {
        if (!seen.has(colName) && colName.toUpperCase().startsWith(upper)) {
          seen.add(colName);
          candidates.push([colName, "column", 0]);
        }
      }
      const ftColNames = this._getColumnNames(foreignTables);
      for (const colName of ftColNames) {
        if (!seen.has(colName) && colName.toUpperCase().startsWith(upper)) {
          seen.add(colName);
          candidates.push([colName, "column", 0]);
        }
      }
    }

    // Sort: tables first after FROM/JOIN, keywords first otherwise
    const orderMap: Record<string, Record<string, number>> = {
      table: {
        table: 0,
        view: 1,
        "foreign table": 2,
        keyword: 3,
        column: 4,
      },
      keyword: {
        keyword: 0,
        column: 1,
        table: 2,
        view: 3,
        "foreign table": 4,
      },
    };
    const order = afterTableKw ? orderMap["table"]! : orderMap["keyword"]!;

    candidates.sort((a, b) => {
      const oa = order[a[1]] ?? 9;
      const ob = order[b[1]] ?? 9;
      if (oa !== ob) return oa - ob;
      return a[0].toLowerCase().localeCompare(b[0].toLowerCase());
    });

    const matches = candidates.map((c) => c[0]);
    return [matches, word];
  }

  private _tableCompletions(prefix: string): string[] {
    const upper = prefix.toUpperCase();
    const result: string[] = [];
    const compiler = this._getCompiler();
    const tables = this._getMap(compiler, "_tables");
    const views = this._getMap(compiler, "_views");
    const foreignTables = this._getMap(compiler, "_foreignTables");

    for (const name of [...tables.keys()].sort()) {
      if (name.toUpperCase().startsWith(upper)) {
        result.push(name);
      }
    }
    for (const name of [...views.keys()].sort()) {
      if (name.toUpperCase().startsWith(upper)) {
        result.push(name);
      }
    }
    for (const name of [...foreignTables.keys()].sort()) {
      if (name.toUpperCase().startsWith(upper)) {
        result.push(name);
      }
    }
    return result;
  }

  private _getWordBeforeCursor(line: string): string {
    const match = line.match(/(\S+)$/);
    return match ? match[1]! : "";
  }

  private _getCompiler(): Record<string, unknown> {
    return (this._engine as unknown as Record<string, unknown>)["_compiler"] as Record<
      string,
      unknown
    >;
  }

  private _getMap(
    compiler: Record<string, unknown>,
    field: string,
  ): Map<string, unknown> {
    const value = compiler[field];
    if (value instanceof Map) {
      return value;
    }
    // SchemaAwareTableStore or similar Map-like object
    if (
      value &&
      typeof value === "object" &&
      typeof (value as { keys: unknown })["keys"] === "function"
    ) {
      return value as Map<string, unknown>;
    }
    return new Map();
  }

  private _getColumnNames(tables: Map<string, unknown>): string[] {
    const names: string[] = [];
    for (const table of tables.values()) {
      if (table && typeof table === "object") {
        const columns = (table as Record<string, unknown>)["columns"];
        if (columns instanceof Map) {
          for (const colName of columns.keys()) {
            names.push(colName);
          }
        }
      }
    }
    return names;
  }
}
