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
//
// Architecture note:
//
//   In Python, USQLEngine.sql() creates a NEW USQLCompiler per query.
//   In JS, Engine has a single _compiler (private) created in the
//   constructor and reused across queries.  We monkey-patch three
//   methods on the compiler instance to inject catalog support:
//
//     _resolveFromItem  -- intercepts catalog table references
//     _projectColumns   -- preserves columns for empty catalog tables
//     _walkAstForTables -- excludes catalog names from table refs
//
//   Each catalog table is materialized through _resultToTable,
//   registered in _tables, and tracked in _shadowedTables /
//   _expandedViews -- exactly as the Python USQLCompiler does.

import { Engine } from "@jaepil/uqa";
import type { SQLResult } from "@jaepil/uqa";
import { normalizeRows } from "./compiler.js";
import { OIDAllocator } from "../pg-compat/oid.js";
import { InformationSchemaProvider } from "../pg-compat/information-schema.js";
import { PGCatalogProvider } from "../pg-compat/pg-catalog.js";

type Row = Record<string, unknown>;

// pg_catalog tables that can be referenced without schema prefix,
// matching PostgreSQL's implicit pg_catalog search_path behavior.
const PG_CATALOG_NAMES: ReadonlySet<string> = new Set(
  PGCatalogProvider.supportedTables(),
);

// information_schema views that can be referenced without prefix.
const INFO_SCHEMA_NAMES: ReadonlySet<string> = new Set(
  InformationSchemaProvider.supportedViews(),
);

// Build a composite state object from the compiler's live data and
// the engine's metadata.  In the JS UQA engine, SQL DDL populates
// the compiler's Maps, not the Engine's.  The providers need the
// compiler's data for tables/views/sequences/FDW, and the engine's
// data for _tempTables and _indexManager.
function buildCompilerState(
  compiler: Record<string, unknown>,
  eng: Record<string, unknown>,
): unknown {
  return {
    _tables: compiler["_tables"],
    _views: compiler["_views"],
    _sequences: compiler["_sequences"],
    _foreignServers: compiler["_foreignServers"],
    _foreignTables: compiler["_foreignTables"],
    _tempTables: (eng["_tempTables"] as Set<string> | undefined) ?? new Set<string>(),
    _indexManager: (eng["_indexManager"] as unknown) ?? null,
  };
}

// Detect A_Star in SELECT targets.
// A SELECT * AST node looks like:
//   {"ResTarget":{"val":{"ColumnRef":{"fields":[{"A_Star":{}}]}}}}
function isSelectStar(targets: unknown[]): boolean {
  if (targets.length === 0) return true;
  if (targets.length !== 1) return false;
  try {
    // The nested access will throw (caught below) when the shape
    // does not match, so non-null assertions are safe here.
    const t = targets[0] as Record<string, Record<string, unknown>>;
    const val = t["ResTarget"]!["val"] as Record<string, Record<string, unknown[]>>;
    const fields = val["ColumnRef"]!["fields"]!;
    return "A_Star" in (fields[0] as Record<string, unknown>);
  } catch {
    return false;
  }
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
    const getOids = this._getOids.bind(this);
    const eng = this as unknown as Record<string, unknown>;

    // Capture original methods from the compiler instance.
    const origApplyAlias = compiler["_applyAlias"] as (
      rows: Row[],
      alias: string | null,
    ) => Row[];
    const origResultToTable = compiler["_resultToTable"] as (
      name: string,
      result: { columns: string[]; rows: Row[] },
    ) => unknown;

    // ------------------------------------------------------------------
    // Helper: materialize a catalog result as a Table
    // ------------------------------------------------------------------
    // Mirrors Python's _build_information_schema_table / _build_pg_catalog_table:
    //   1. normalize rows
    //   2. call _resultToTable to create a Table
    //   3. register in _tables (saving any shadowed entry)
    //   4. track the internal name in _expandedViews
    //   5. return the internal name
    function materializeCatalog(
      compilerCtx: Record<string, unknown>,
      internalName: string,
      columns: string[],
      rows: Row[],
    ): string {
      const normalized = normalizeRows(rows);
      const table = origResultToTable.call(compilerCtx, internalName, {
        columns,
        rows: normalized,
      });

      const tables = compilerCtx["_tables"] as Map<string, unknown>;
      const shadowedTables = compilerCtx["_shadowedTables"] as Map<string, unknown>;
      const expandedViews = compilerCtx["_expandedViews"] as string[];

      const existing = tables.get(internalName);
      if (existing !== undefined && !shadowedTables.has(internalName)) {
        shadowedTables.set(internalName, existing);
      }
      tables.set(internalName, table);
      expandedViews.push(internalName);

      return internalName;
    }

    // Helper: build and materialize an information_schema view.
    function buildInfoSchemaTable(
      compilerCtx: Record<string, unknown>,
      viewName: string,
    ): string {
      const state = buildCompilerState(compilerCtx, eng);
      const oids = getOids(state);
      const [columns, rows] = InformationSchemaProvider.build(
        viewName,
        state as Engine,
        oids,
      );
      return materializeCatalog(compilerCtx, `_info_schema_${viewName}`, columns, rows);
    }

    // Helper: build and materialize a pg_catalog table.
    function buildPgCatalogTable(
      compilerCtx: Record<string, unknown>,
      tableName: string,
    ): string {
      const state = buildCompilerState(compilerCtx, eng);
      const oids = getOids(state);
      const [columns, rows] = PGCatalogProvider.build(tableName, state as Engine, oids);
      return materializeCatalog(compilerCtx, `_pg_${tableName}`, columns, rows);
    }

    // Helper: read rows back from a materialized table's documentStore.
    function readTableRows(
      compilerCtx: Record<string, unknown>,
      internalName: string,
      alias: string,
    ): Row[] {
      const tables = compilerCtx["_tables"] as Map<string, Record<string, unknown>>;
      const table = tables.get(internalName);
      if (!table) return [];

      const docStore = table["documentStore"] as {
        iterAll(): Iterable<[unknown, Record<string, unknown>]>;
      };

      const rows: Row[] = [];
      for (const [, doc] of docStore.iterAll()) {
        rows.push({ ...doc });
      }
      return origApplyAlias.call(compilerCtx, rows, alias);
    }

    // ------------------------------------------------------------------
    // Override: _resolveFromItem
    // ------------------------------------------------------------------
    // Handles qualified (information_schema.xxx, pg_catalog.xxx) and
    // unqualified catalog name resolution, matching Python's
    // _resolve_from_single logic.
    const origResolveFromItem = compiler["_resolveFromItem"] as (
      node: unknown,
      ctx: unknown,
    ) => Row[];

    compiler["_resolveFromItem"] = function (
      this: Record<string, unknown>,
      node: unknown,
      ctx: unknown,
    ): Row[] {
      const rangeVar = (node as Record<string, unknown> | null)?.["RangeVar"] as
        | Record<string, unknown>
        | undefined;

      if (rangeVar) {
        const schemaName = (rangeVar["schemaname"] as string | null) || null;
        const relName = rangeVar["relname"] as string;
        const aliasNode = rangeVar["alias"] as Record<string, string> | null;
        const alias = (aliasNode && aliasNode["aliasname"]) || relName;

        if (schemaName === "information_schema") {
          const internalName = buildInfoSchemaTable(
            this as Record<string, unknown>,
            relName,
          );
          return readTableRows(this as Record<string, unknown>, internalName, alias);
        }

        if (schemaName === "pg_catalog") {
          const internalName = buildPgCatalogTable(
            this as Record<string, unknown>,
            relName,
          );
          return readTableRows(this as Record<string, unknown>, internalName, alias);
        }

        // Unqualified name: check if it matches a known catalog object
        // that is not a user-defined table/view/foreign-table/CTE.
        if (schemaName === null && relName) {
          const tables = this["_tables"] as Map<string, unknown>;
          const views = this["_views"] as Map<string, unknown>;
          const foreignTables = this["_foreignTables"] as Map<string, unknown>;
          const inlinedCTEs = this["_inlinedCTEs"] as Map<string, unknown>;

          if (
            !tables.has(relName) &&
            !views.has(relName) &&
            !foreignTables.has(relName) &&
            !inlinedCTEs.has(relName)
          ) {
            if (PG_CATALOG_NAMES.has(relName)) {
              const internalName = buildPgCatalogTable(
                this as Record<string, unknown>,
                relName,
              );
              return readTableRows(
                this as Record<string, unknown>,
                internalName,
                alias,
              );
            }
            if (INFO_SCHEMA_NAMES.has(relName)) {
              const internalName = buildInfoSchemaTable(
                this as Record<string, unknown>,
                relName,
              );
              return readTableRows(
                this as Record<string, unknown>,
                internalName,
                alias,
              );
            }
          }
        }
      }

      return origResolveFromItem.call(this, node, ctx);
    };

    // ------------------------------------------------------------------
    // Override: _projectColumns
    // ------------------------------------------------------------------
    // When rows are empty and the query is SELECT *, the base
    // _projectColumns infers columns from Object.keys(rows[0]) which
    // returns [[], []] -- column metadata is lost.  We check
    // _expandedViews for registered catalog tables and read their
    // column metadata from the Table's columns Map.
    const origProjectColumns = compiler["_projectColumns"] as (
      targets: unknown[],
      rows: Row[],
      ctx: unknown,
    ) => [string[], Row[]];

    compiler["_projectColumns"] = function (
      this: Record<string, unknown>,
      targets: unknown[],
      rows: Row[],
      ctx: unknown,
    ): [string[], Row[]] {
      if (rows.length === 0 && isSelectStar(targets)) {
        const expandedViews = this["_expandedViews"] as string[];
        const tables = this["_tables"] as Map<string, Record<string, unknown>>;

        // Walk _expandedViews in reverse to find the most recently
        // registered catalog table.
        for (let i = expandedViews.length - 1; i >= 0; i--) {
          const name = expandedViews[i]!;
          if (name.startsWith("_pg_") || name.startsWith("_info_schema_")) {
            const table = tables.get(name);
            if (table) {
              const columnsMap = table["columns"] as Map<string, unknown>;
              if (columnsMap.size > 0) {
                return [[...columnsMap.keys()], []];
              }
            }
          }
        }
      }

      return origProjectColumns.call(this, targets, rows, ctx);
    };

    // ------------------------------------------------------------------
    // Override static: _walkAstForTables
    // ------------------------------------------------------------------
    // Exclude qualified information_schema / pg_catalog names (same as
    // the base implementation) and ALSO exclude unqualified names that
    // match known catalog objects.  This prevents the compiler from
    // treating catalog tables as missing user tables.
    const compilerCtor = compiler.constructor as unknown as Record<string, unknown>;

    compilerCtor["_walkAstForTables"] = function walkAstForTables(
      node: unknown,
      refs: Set<string>,
    ): void {
      if (node == null || typeof node !== "object") return;

      if (Array.isArray(node)) {
        for (const item of node) {
          walkAstForTables(item, refs);
        }
        return;
      }

      const obj = node as Record<string, unknown>;
      const rangeVar = obj["RangeVar"] as Record<string, unknown> | undefined;

      if (rangeVar !== undefined) {
        const schemaName = rangeVar["schemaname"] as string | undefined;
        if (schemaName === "information_schema" || schemaName === "pg_catalog") {
          return;
        }
        const relName = rangeVar["relname"] as string | undefined;
        // Skip unqualified names that match known catalog objects.
        if (
          !schemaName &&
          relName &&
          (PG_CATALOG_NAMES.has(relName) || INFO_SCHEMA_NAMES.has(relName))
        ) {
          return;
        }
        if (relName) {
          refs.add(relName);
        }
        return;
      }

      // Handle non-RangeVar nodes that have relname directly
      // (e.g., bare table references in some AST positions).
      const relName = obj["relname"];
      if (typeof relName === "string" && relName) {
        const schemaName = obj["schemaname"] as string | undefined;
        if (
          schemaName === "information_schema" ||
          schemaName === "pg_catalog" ||
          (!schemaName &&
            (PG_CATALOG_NAMES.has(relName) || INFO_SCHEMA_NAMES.has(relName)))
        ) {
          // Skip catalog references.
        } else {
          refs.add(relName);
        }
      }

      for (const value of Object.values(obj)) {
        if (value !== null && typeof value === "object") {
          walkAstForTables(value as unknown, refs);
        }
      }
    };
  }

  private _getOids(state?: unknown): OIDAllocator {
    if (this._oidAllocator === null) {
      // Pass the composite state (with compiler tables) to OIDAllocator.
      this._oidAllocator = new OIDAllocator((state ?? this) as Engine);
    }
    return this._oidAllocator;
  }

  override async sql(query: string, params?: unknown[]): Promise<SQLResult | null> {
    // Reset OID allocator for each query to ensure consistent OIDs
    // within a single query execution while allowing for schema changes
    // between queries.  This mirrors Python's approach of creating a
    // new USQLCompiler (and thus new OIDAllocator) per query.
    this._oidAllocator = null;
    // Lazy-initialize persistence on the first SQL call when dbPath is set.
    // Engine.init() is idempotent -- safe to call every time.
    await this.init();
    return super.sql(query, params);
  }
}
