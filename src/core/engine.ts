//
// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
//
// Copyright (c) 2023-2026 Cognica, Inc.
//

// PostgreSQL 17-compatible UQA Engine.
//
// Extends the standard UQA Engine so that every SQL query uses
// comprehensive information_schema and pg_catalog providers instead
// of the minimal built-in ones.  This provides full PostgreSQL 17
// system catalog compatibility without changing any other Engine behavior.

import { Engine } from "@jaepil/uqa";
import type { SQLResult } from "@jaepil/uqa";
import { OIDAllocator } from "../pg-compat/oid.js";
import { InformationSchemaProvider } from "../pg-compat/information-schema.js";
import { PGCatalogProvider } from "../pg-compat/pg-catalog.js";

type Row = Record<string, unknown>;

function normalizeRows(rows: Row[]): Row[] {
  if (rows.length === 0) return rows;
  const normalized: Row[] = [];
  for (const row of rows) {
    const newRow: Row = {};
    for (const [key, value] of Object.entries(row)) {
      if (typeof value === "boolean") {
        newRow[key] = value ? 1 : 0;
      } else if (
        typeof value === "number" &&
        (Number.isNaN(value) || !Number.isFinite(value))
      ) {
        newRow[key] = null;
      } else {
        newRow[key] = value;
      }
    }
    normalized.push(newRow);
  }
  return normalized;
}

// Build a composite state object from the compiler's live data and
// the engine's metadata.  In the JS UQA engine, SQL DDL populates
// the compiler's Maps, not the Engine's.  The providers need the
// compiler's data for tables/views/sequences/FDW, and the engine's
// data for _tempTables and _indexManager.
function buildCompilerState(
  compiler: Record<string, unknown>,
  engine: Engine,
): unknown {
  const eng = engine as unknown as Record<string, unknown>;
  return {
    _tables: compiler["_tables"],
    _views: compiler["_views"],
    _sequences: compiler["_sequences"],
    _foreignServers: compiler["_foreignServers"],
    _foreignTables: compiler["_foreignTables"],
    _tempTables: eng["_tempTables"] ?? new Set<string>(),
    _indexManager: eng["_indexManager"] ?? null,
  };
}

export class USQLEngine extends Engine {
  private _oidAllocator: OIDAllocator | null = null;

  constructor(opts?: {
    dbPath?: string;
    parallelWorkers?: number;
    spillThreshold?: number;
  }) {
    super(opts);
    this._patchCompiler();
  }

  private _patchCompiler(): void {
    // Access the private _compiler field at runtime.
    // Engine stores its SQLCompiler as `_compiler` (private in TypeScript,
    // but a regular property at JavaScript runtime).
    const compiler = (this as unknown as Record<string, unknown>)[
      "_compiler"
    ] as Record<string, unknown>;
    const engine = this;

    // Access the compiler's _applyAlias method for alias handling
    const applyAlias = compiler["_applyAlias"] as (
      rows: Row[],
      alias: string | null,
    ) => Row[];

    // Replace the minimal _buildInformationSchemaTable with our full PG17 version.
    // Using `function` (not arrow) so `this` refers to the compiler instance.
    compiler["_buildInformationSchemaTable"] = function (
      viewName: string,
      alias: string,
    ): Row[] {
      const state = buildCompilerState(
        this as unknown as Record<string, unknown>,
        engine,
      );
      const oids = engine._getOids(state);
      const [, rows] = InformationSchemaProvider.build(
        viewName,
        state as Engine,
        oids,
      );
      const normalized = normalizeRows(rows);
      return applyAlias.call(this, normalized, alias);
    };

    // Replace the minimal _buildPgCatalogTable with our full PG17 version
    compiler["_buildPgCatalogTable"] = function (
      viewName: string,
      alias: string,
    ): Row[] {
      const state = buildCompilerState(
        this as unknown as Record<string, unknown>,
        engine,
      );
      const oids = engine._getOids(state);
      const [, rows] = PGCatalogProvider.build(
        viewName,
        state as Engine,
        oids,
      );
      const normalized = normalizeRows(rows);
      return applyAlias.call(this, normalized, alias);
    };
  }

  private _getOids(state?: unknown): OIDAllocator {
    if (this._oidAllocator === null) {
      // Pass the composite state (with compiler tables) to OIDAllocator
      this._oidAllocator = new OIDAllocator(
        (state ?? this) as Engine,
      );
    }
    return this._oidAllocator;
  }

  override async sql(
    query: string,
    params?: unknown[],
  ): Promise<SQLResult | null> {
    // Reset OID allocator for each query to ensure consistent OIDs
    // within a single query execution while allowing for schema changes
    // between queries.
    this._oidAllocator = null;
    return super.sql(query, params);
  }
}
