//
// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
//
// Copyright (c) 2023-2026 Cognica, Inc.
//

// PostgreSQL OID allocation and type mapping.
//
// PostgreSQL assigns a unique OID (Object Identifier) to every database
// object: types, tables, schemas, indexes, constraints, functions, etc.
// Tools that inspect the catalog rely on OIDs being consistent across
// JOINs (e.g. pg_class.oid = pg_attribute.attrelid).
//
// OID ranges:
//     0-16383     Reserved for system objects (built-in types, schemas)
//     16384+      User-defined objects (tables, indexes, constraints)

import type { Engine } from "@jaepil/uqa";
import type { Table } from "@jaepil/uqa";

// ======================================================================
// Built-in type OIDs (matching PostgreSQL 17)
// ======================================================================

export const TYPE_OIDS: Readonly<Record<string, number>> = {
  boolean: 16,
  bool: 16,
  bytea: 17,
  name: 19,
  bigint: 20,
  int8: 20,
  smallint: 21,
  int2: 21,
  integer: 23,
  int: 23,
  int4: 23,
  oid: 26,
  text: 25,
  json: 114,
  xml: 142,
  point: 600,
  real: 700,
  float: 700,
  float4: 700,
  "double precision": 701,
  float8: 701,
  character: 1042,
  char: 1042,
  "character varying": 1043,
  varchar: 1043,
  date: 1082,
  time: 1083,
  timestamp: 1114,
  "timestamp without time zone": 1114,
  timestamptz: 1184,
  "timestamp with time zone": 1184,
  interval: 1186,
  numeric: 1700,
  decimal: 1700,
  uuid: 2950,
  jsonb: 3802,
  serial: 23,
  bigserial: 20,
  vector: 16385,
};

// Array type OIDs: element_type_oid -> array_type_oid
export const ARRAY_TYPE_OIDS: Readonly<Record<number, number>> = {
  16: 1000, // bool[]
  17: 1001, // bytea[]
  20: 1016, // int8[]
  21: 1005, // int2[]
  23: 1007, // int4[]
  25: 1009, // text[]
  26: 1028, // oid[]
  114: 199, // json[]
  700: 1021, // float4[]
  701: 1022, // float8[]
  1042: 1014, // bpchar[]
  1043: 1015, // varchar[]
  1082: 1182, // date[]
  1083: 1183, // time[]
  1114: 1115, // timestamp[]
  1184: 1185, // timestamptz[]
  1700: 1231, // numeric[]
  2950: 2951, // uuid[]
  3802: 3807, // jsonb[]
};

// Schema OIDs
export const SCHEMA_OIDS: Readonly<Record<string, number>> = {
  pg_catalog: 11,
  public: 2200,
  information_schema: 13182,
  pg_toast: 99,
};

// Database OID
export const DATABASE_OID = 1;

// Superuser role OID
export const ROLE_OID = 10;

// Access method OIDs
export const AM_BTREE = 403;
export const AM_HASH = 405;
export const AM_GIST = 783;
export const AM_GIN = 2742;
export const AM_BRIN = 3580;
export const AM_HEAP = 2;
export const AM_HNSW = 16386;
export const AM_IVF = 16387;

// pg_class OIDs for system catalogs
export const CLASS_PG_CLASS = 1259;
export const CLASS_PG_TYPE = 1247;
export const CLASS_PG_NAMESPACE = 2615;
export const CLASS_PG_CONSTRAINT = 2606;
export const CLASS_PG_INDEX = 2610;
export const CLASS_PG_ATTRDEF = 2604;
export const CLASS_PG_AM = 2601;
export const CLASS_PG_PROC = 1255;

// Canonical type name mapping (UQA type_name -> PostgreSQL canonical name)
export const CANONICAL_TYPE_NAMES: Readonly<Record<string, string>> = {
  int: "integer",
  int2: "smallint",
  int4: "integer",
  int8: "bigint",
  float: "real",
  float4: "real",
  float8: "double precision",
  bool: "boolean",
  serial: "integer",
  bigserial: "bigint",
  decimal: "numeric",
  char: "character",
  "character varying": "character varying",
  varchar: "character varying",
  name: "name",
  "timestamp without time zone": "timestamp without time zone",
  "timestamp with time zone": "timestamp with time zone",
};

// Type length in bytes (-1 = variable, -2 = null-terminated C string)
export const TYPE_LENGTHS: Readonly<Record<number, number>> = {
  16: 1, // bool
  17: -1, // bytea
  19: 64, // name
  20: 8, // int8
  21: 2, // int2
  23: 4, // int4
  25: -1, // text
  26: 4, // oid
  114: -1, // json
  142: -1, // xml
  600: 16, // point
  700: 4, // float4
  701: 8, // float8
  1042: -1, // bpchar
  1043: -1, // varchar
  1082: 4, // date
  1083: 8, // time
  1114: 8, // timestamp
  1184: 8, // timestamptz
  1186: 16, // interval
  1700: -1, // numeric
  2950: 16, // uuid
  3802: -1, // jsonb
  16385: -1, // vector
};

// Type category (single character, PostgreSQL convention)
export const TYPE_CATEGORIES: Readonly<Record<number, string>> = {
  16: "B", // Boolean
  17: "U", // User-defined (bytea)
  19: "S", // String
  20: "N", // Numeric
  21: "N", // Numeric
  23: "N", // Numeric
  25: "S", // String
  26: "N", // Numeric (oid)
  114: "U", // User-defined (json)
  142: "U", // User-defined (xml)
  600: "G", // Geometric
  700: "N", // Numeric
  701: "N", // Numeric
  1042: "S", // String
  1043: "S", // String
  1082: "D", // Date/Time
  1083: "D", // Date/Time
  1114: "D", // Date/Time
  1184: "D", // Date/Time
  1186: "T", // Timespan
  1700: "N", // Numeric
  2950: "U", // User-defined (uuid)
  3802: "U", // User-defined (jsonb)
  16385: "U", // User-defined (vector)
};

// Type by-value flag (passed by value vs by reference)
export const TYPE_BYVAL: Readonly<Record<number, boolean>> = {
  16: true, // bool
  21: true, // int2
  23: true, // int4
  26: true, // oid
  700: true, // float4
};

// Type alignment
export const TYPE_ALIGN: Readonly<Record<number, string>> = {
  16: "c", // char alignment
  17: "i", // int alignment
  19: "c", // char
  20: "d", // double
  21: "s", // short
  23: "i", // int
  25: "i", // int
  26: "i", // int
  700: "i", // int
  701: "d", // double
  1042: "i", // int
  1043: "i", // int
  1082: "i", // int
  1083: "d", // double
  1114: "d", // double
  1184: "d", // double
  1186: "d", // double
  1700: "i", // int
  2950: "c", // char
  3802: "i", // int
};

// Type storage strategy
export const TYPE_STORAGE: Readonly<Record<number, string>> = {
  16: "p", // plain
  17: "x", // extended
  19: "p", // plain
  20: "p", // plain
  21: "p", // plain
  23: "p", // plain
  25: "x", // extended
  26: "p", // plain
  114: "x", // extended
  700: "p", // plain
  701: "p", // plain
  1042: "x", // extended
  1043: "x", // extended
  1082: "p", // plain
  1114: "p", // plain
  1184: "p", // plain
  1700: "m", // main
  2950: "p", // plain
  3802: "x", // extended
};

// ======================================================================
// Type helper functions
// ======================================================================

export function typeOid(typeName: string): number {
  // Handle array types (e.g. "text[]", "integer[]")
  if (typeName.endsWith("[]")) {
    const base = typeName.slice(0, -2);
    const baseOid = TYPE_OIDS[base] ?? 25; // default to text
    return ARRAY_TYPE_OIDS[baseOid] ?? 1009; // default to text[]
  }
  return TYPE_OIDS[typeName] ?? 25; // default to text
}

export function canonicalTypeName(typeName: string): string {
  if (typeName.endsWith("[]")) {
    const base = typeName.slice(0, -2);
    const baseCanonical = CANONICAL_TYPE_NAMES[base] ?? base;
    return `${baseCanonical}[]`;
  }
  return CANONICAL_TYPE_NAMES[typeName] ?? typeName;
}

export function typeLength(typeName: string): number {
  const oid = typeOid(typeName);
  return TYPE_LENGTHS[oid] ?? -1;
}

export function numericPrecision(typeName: string): number | null {
  const oid = typeOid(typeName);
  const precisions: Record<number, number> = {
    21: 16, // int2
    23: 32, // int4
    20: 64, // int8
    700: 24, // float4
    701: 53, // float8
  };
  return precisions[oid] ?? null;
}

export function numericScale(typeName: string): number | null {
  const oid = typeOid(typeName);
  if (oid === 21 || oid === 23 || oid === 20) return 0;
  return null;
}

export function numericPrecisionRadix(typeName: string): number | null {
  const oid = typeOid(typeName);
  if (oid === 21 || oid === 23 || oid === 20 || oid === 1700) return 10;
  if (oid === 700 || oid === 701) return 2;
  return null;
}

export function characterMaximumLength(_typeName: string): number | null {
  // UQA text types are unbounded
  return null;
}

export function characterOctetLength(typeName: string): number | null {
  const oid = typeOid(typeName);
  if (oid === 25 || oid === 1042 || oid === 1043) {
    return 1073741824; // 1 GB (PostgreSQL default)
  }
  return null;
}

// ======================================================================
// OID Allocator
// ======================================================================

// Helper to access engine internals via Engine's public underscore-prefixed fields
interface EngineInternals {
  _tables: Map<string, TableInternals>;
  _views: Map<string, unknown>;
  _sequences: Map<string, Record<string, number>>;
  _tempTables: Set<string>;
  _foreignServers: Map<string, ForeignServerInternals>;
  _foreignTables: Map<string, ForeignTableInternals>;
}

interface TableInternals {
  primaryKey: string | null;
  columns: Map<string, ColumnDefInternals>;
  foreignKeys: ForeignKeyInternals[];
  checkConstraints: [string, string][];
  rowCount: number;
  _stats: unknown;
}

interface ColumnDefInternals {
  typeName: string;
  notNull: boolean;
  primaryKey: boolean;
  unique: boolean;
  defaultValue: unknown;
  autoIncrement: boolean;
  numericPrecision: number | null;
  numericScale: number | null;
}

interface ForeignKeyInternals {
  column: string;
  refTable: string;
  refColumn: string;
}

interface ForeignServerInternals {
  name: string;
  fdwType: string;
  options: Record<string, string>;
}

interface ForeignTableInternals {
  serverName: string;
  columns: Map<string, ColumnDefInternals>;
  options: Record<string, string>;
}

export class OIDAllocator {
  private _map: Map<string, number>;
  private _next: number;

  constructor(engine: Engine) {
    this._map = new Map();
    this._next = 16384;
    this._build(engine as unknown as EngineInternals);
  }

  private _alloc(): number {
    const oid = this._next;
    this._next += 1;
    return oid;
  }

  private _build(engine: EngineInternals): void {
    // -- Tables --
    for (const name of [...engine._tables.keys()].sort()) {
      this._map.set(`table:${name}`, this._alloc());
      // Each table has an implicit composite type
      this._map.set(`table_type:${name}`, this._alloc());
      // Each table has an implicit TOAST table OID (placeholder)
      this._map.set(`toast:${name}`, this._alloc());
    }

    // -- Views --
    for (const name of [...engine._views.keys()].sort()) {
      this._map.set(`view:${name}`, this._alloc());
    }

    // -- Sequences --
    for (const name of [...engine._sequences.keys()].sort()) {
      this._map.set(`sequence:${name}`, this._alloc());
    }

    // -- Foreign tables --
    for (const name of [...engine._foreignTables.keys()].sort()) {
      this._map.set(`foreign_table:${name}`, this._alloc());
    }

    // -- Foreign servers --
    for (const name of [...engine._foreignServers.keys()].sort()) {
      this._map.set(`foreign_server:${name}`, this._alloc());
    }

    // -- FDW wrappers --
    const fdwTypes = new Set<string>();
    for (const srv of engine._foreignServers.values()) {
      fdwTypes.add(srv.fdwType);
    }
    for (const fdwType of [...fdwTypes].sort()) {
      this._map.set(`fdw:${fdwType}`, this._alloc());
    }

    // -- Indexes (explicit) --
    const indexManager = (engine as unknown as Record<string, unknown>)[
      "_indexManager"
    ] as { _indexes?: Map<string, unknown> } | null;
    if (indexManager !== null && indexManager !== undefined) {
      const indexes = indexManager._indexes;
      if (indexes !== undefined) {
        for (const name of [...indexes.keys()].sort()) {
          this._map.set(`index:${name}`, this._alloc());
        }
      }
    }

    // -- Constraints --
    for (const tname of [...engine._tables.keys()].sort()) {
      const table = engine._tables.get(tname)!;

      if (table.primaryKey) {
        this._map.set(`constraint:${tname}_pkey`, this._alloc());
        // PK also has a backing index
        this._map.set(`index:${tname}_pkey`, this._alloc());
      }

      for (const [cname, cdef] of table.columns) {
        if (cdef.unique && !cdef.primaryKey) {
          this._map.set(`constraint:${tname}_${cname}_key`, this._alloc());
          this._map.set(`index:${tname}_${cname}_key`, this._alloc());
        }
      }

      for (const fk of table.foreignKeys) {
        this._map.set(`constraint:${tname}_${fk.column}_fkey`, this._alloc());
      }

      for (const [checkName] of table.checkConstraints) {
        this._map.set(
          `constraint:${tname}_${checkName}_check`,
          this._alloc(),
        );
      }
    }
  }

  get(category: string, name: string): number | null {
    return this._map.get(`${category}:${name}`) ?? null;
  }

  getOrAlloc(category: string, name: string): number {
    const key = `${category}:${name}`;
    const existing = this._map.get(key);
    if (existing !== undefined) return existing;
    const oid = this._alloc();
    this._map.set(key, oid);
    return oid;
  }

  relationOid(name: string, _engine: Engine): number | null {
    for (const category of [
      "table",
      "view",
      "foreign_table",
      "sequence",
    ] as const) {
      const oid = this._map.get(`${category}:${name}`);
      if (oid !== undefined) return oid;
    }
    return null;
  }

  allByCategory(category: string): Map<string, number> {
    const prefix = `${category}:`;
    const result = new Map<string, number>();
    for (const [key, oid] of this._map) {
      if (key.startsWith(prefix)) {
        result.set(key.slice(prefix.length), oid);
      }
    }
    return result;
  }
}
