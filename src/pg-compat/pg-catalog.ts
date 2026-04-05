// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
// Copyright (c) 2023-2026 Cognica, Inc.

// PostgreSQL 17 pg_catalog table provider.
//
// Generates virtual rows for pg_catalog system tables by inspecting
// UQA Engine state.  Each builder returns a [columns, rows] tuple.
//
// The pg_catalog tables are the real system catalog in PostgreSQL.
// The information_schema views are SQL-standard wrappers built on top
// of pg_catalog.  Tools like psql, SQLAlchemy, DBeaver query pg_catalog
// directly for features beyond the SQL standard.
//
// Reference: PostgreSQL 17 documentation, Chapter 53 -- System Catalogs
// https://www.postgresql.org/docs/17/catalogs.html

import type { Engine } from "@jaepil/uqa";
import {
  AM_BTREE,
  AM_HASH,
  AM_HEAP,
  AM_HNSW,
  AM_IVF,
  ARRAY_TYPE_OIDS,
  DATABASE_OID,
  ROLE_OID,
  SCHEMA_OIDS,
  TYPE_ALIGN,
  TYPE_BYVAL,
  TYPE_LENGTHS,
  TYPE_STORAGE,
  typeOid,
} from "./oid.js";
import type { OIDAllocator } from "./oid.js";
import { getAllConnections } from "./connection-registry.js";

type Row = Record<string, unknown>;
type BuildResult = [string[], Row[]];

// Helper to access engine internals
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

// The JS UQA engine maps NUMERIC/DECIMAL to typeName="float" internally.
// When numericScale is set, the original SQL type was NUMERIC(p,s).
function effectiveTypeName(cdef: ColumnDefInternals): string {
  if (cdef.typeName === "float" && cdef.numericScale !== null) {
    return "numeric";
  }
  return cdef.typeName;
}

interface IndexDefInternals {
  tableName: string;
  columns: string[];
  unique: boolean;
}

interface IndexObjectInternals {
  indexDef: IndexDefInternals;
}

// Database name used as catalog
const CATALOG_NAME = "uqa";
const SCHEMA = "public";
const OWNER = "uqa";
const ENCODING_UTF8 = 6;

// Pre-resolved namespace OIDs (avoids undefined index lookups)
const NS_PG_CATALOG = SCHEMA_OIDS["pg_catalog"] as number;
const NS_PUBLIC = SCHEMA_OIDS["public"] as number;
const NS_INFORMATION_SCHEMA = SCHEMA_OIDS["information_schema"] as number;

// ======================================================================
// Helpers
// ======================================================================

function pgClassRow(options: {
  oid: number;
  relname: string;
  relnamespace: number;
  reltype?: number;
  reloftype?: number;
  relowner?: number;
  relam?: number;
  reltuples?: number;
  relhasindex?: boolean;
  relkind?: string;
  relnatts?: number;
  relchecks?: number;
  relhasrules?: boolean;
}): Row {
  const reltuples = options.reltuples ?? -1;
  return {
    oid: options.oid,
    relname: options.relname,
    relnamespace: options.relnamespace,
    reltype: options.reltype ?? 0,
    reloftype: options.reloftype ?? 0,
    relowner: options.relowner ?? ROLE_OID,
    relam: options.relam ?? 0,
    relfilenode: options.oid,
    reltablespace: 0,
    relpages: reltuples > 0 ? Math.max(1, Math.floor(reltuples / 100)) : 0,
    reltuples,
    relallvisible: 0,
    reltoastrelid: 0,
    relhasindex: options.relhasindex ?? false,
    relisshared: false,
    relpersistence: "p",
    relkind: options.relkind ?? "r",
    relnatts: options.relnatts ?? 0,
    relchecks: options.relchecks ?? 0,
    relhasrules: options.relhasrules ?? false,
    relhastriggers: false,
    relhassubclass: false,
    relrowsecurity: false,
    relforcerowsecurity: false,
    relispopulated: true,
    relreplident: "d",
    relispartition: false,
    relrewrite: 0,
    relfrozenxid: 0,
    relminmxid: 1,
    relacl: null,
    reloptions: null,
    relpartbound: null,
  };
}

// Helper to get the index manager from engine internals
function getIndexManager(
  engine: EngineInternals,
): Map<string, IndexObjectInternals> | null {
  const indexManager = (engine as unknown as Record<string, unknown>)[
    "_indexManager"
  ] as { _indexes?: Map<string, IndexObjectInternals> } | null | undefined;
  if (indexManager === null || indexManager === undefined) return null;
  const indexes = indexManager._indexes;
  if (indexes === undefined) return null;
  return indexes;
}

type TableBuilder = (engine: EngineInternals, oids: OIDAllocator) => BuildResult;

// eslint-disable-next-line @typescript-eslint/no-extraneous-class
export class PGCatalogProvider {
  private static readonly _TABLES: Readonly<Record<string, TableBuilder>> = {
    pg_namespace: PGCatalogProvider._buildPgNamespace,
    pg_class: PGCatalogProvider._buildPgClass,
    pg_attribute: PGCatalogProvider._buildPgAttribute,
    pg_type: PGCatalogProvider._buildPgType,
    pg_constraint: PGCatalogProvider._buildPgConstraint,
    pg_index: PGCatalogProvider._buildPgIndex,
    pg_attrdef: PGCatalogProvider._buildPgAttrdef,
    pg_am: PGCatalogProvider._buildPgAm,
    pg_database: PGCatalogProvider._buildPgDatabase,
    pg_roles: PGCatalogProvider._buildPgRoles,
    pg_user: PGCatalogProvider._buildPgUser,
    pg_tables: PGCatalogProvider._buildPgTables,
    pg_views: PGCatalogProvider._buildPgViews,
    pg_indexes: PGCatalogProvider._buildPgIndexes,
    pg_matviews: PGCatalogProvider._buildPgMatviews,
    pg_sequences: PGCatalogProvider._buildPgSequences,
    pg_settings: PGCatalogProvider._buildPgSettings,
    pg_foreign_server: PGCatalogProvider._buildPgForeignServer,
    pg_foreign_table: PGCatalogProvider._buildPgForeignTable,
    pg_foreign_data_wrapper: PGCatalogProvider._buildPgForeignDataWrapper,
    pg_description: PGCatalogProvider._buildPgDescription,
    pg_depend: PGCatalogProvider._buildPgDepend,
    pg_stat_user_tables: PGCatalogProvider._buildPgStatUserTables,
    pg_stat_user_indexes: PGCatalogProvider._buildPgStatUserIndexes,
    pg_stat_activity: PGCatalogProvider._buildPgStatActivity,
    pg_proc: PGCatalogProvider._buildPgProc,
    pg_extension: PGCatalogProvider._buildPgExtension,
    pg_collation: PGCatalogProvider._buildPgCollation,
    pg_enum: PGCatalogProvider._buildPgEnum,
    pg_inherits: PGCatalogProvider._buildPgInherits,
    pg_trigger: PGCatalogProvider._buildPgTrigger,
    pg_statio_user_tables: PGCatalogProvider._buildPgStatioUserTables,
    pg_auth_members: PGCatalogProvider._buildPgAuthMembers,
    pg_available_extensions: PGCatalogProvider._buildPgAvailableExtensions,
    pg_stat_all_tables: PGCatalogProvider._buildPgStatUserTables,
  };

  static supportedTables(): string[] {
    return Object.keys(PGCatalogProvider._TABLES);
  }

  static build(tableName: string, engine: Engine, oids: OIDAllocator): BuildResult {
    const builder = PGCatalogProvider._TABLES[tableName];
    if (builder === undefined) {
      throw new Error(`Unknown pg_catalog table: '${tableName}'`);
    }
    return builder(engine as unknown as EngineInternals, oids);
  }

  // ==================================================================
  // pg_namespace -- schemas
  // ==================================================================

  private static _buildPgNamespace(
    _engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = ["oid", "nspname", "nspowner", "nspacl"];
    const rows: Row[] = [
      {
        oid: NS_PG_CATALOG,
        nspname: "pg_catalog",
        nspowner: ROLE_OID,
        nspacl: null,
      },
      {
        oid: NS_PUBLIC,
        nspname: "public",
        nspowner: ROLE_OID,
        nspacl: null,
      },
      {
        oid: NS_INFORMATION_SCHEMA,
        nspname: "information_schema",
        nspowner: ROLE_OID,
        nspacl: null,
      },
    ];
    return [columns, rows];
  }

  // ==================================================================
  // pg_class -- all relations (tables, views, indexes, sequences, etc.)
  // ==================================================================

  private static _buildPgClass(
    engine: EngineInternals,
    oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "oid",
      "relname",
      "relnamespace",
      "reltype",
      "reloftype",
      "relowner",
      "relam",
      "relfilenode",
      "reltablespace",
      "relpages",
      "reltuples",
      "relallvisible",
      "reltoastrelid",
      "relhasindex",
      "relisshared",
      "relpersistence",
      "relkind",
      "relnatts",
      "relchecks",
      "relhasrules",
      "relhastriggers",
      "relhassubclass",
      "relrowsecurity",
      "relforcerowsecurity",
      "relispopulated",
      "relreplident",
      "relispartition",
      "relrewrite",
      "relfrozenxid",
      "relminmxid",
      "relacl",
      "reloptions",
      "relpartbound",
    ];
    const rows: Row[] = [];
    const nsPublic = NS_PUBLIC;

    // -- Regular tables -----------------------------------------------
    for (const tname of [...engine._tables.keys()].sort()) {
      const table = engine._tables.get(tname)!;
      const tableOid = oids.get("table", tname) ?? 0;
      const typeOidVal = oids.get("table_type", tname) ?? 0;
      const hasIndex =
        table.primaryKey !== null || [...table.columns.values()].some((c) => c.unique);
      const nChecks = table.checkConstraints.length;
      const reltuples = table.rowCount;

      rows.push(
        pgClassRow({
          oid: tableOid,
          relname: tname,
          relnamespace: nsPublic,
          reltype: typeOidVal,
          relam: AM_HEAP,
          reltuples,
          relhasindex: hasIndex,
          relkind: "r",
          relnatts: table.columns.size,
          relchecks: nChecks,
        }),
      );
    }

    // -- Views --------------------------------------------------------
    for (const vname of [...engine._views.keys()].sort()) {
      const viewOid = oids.get("view", vname) ?? 0;
      rows.push(
        pgClassRow({
          oid: viewOid,
          relname: vname,
          relnamespace: nsPublic,
          relkind: "v",
          relhasrules: true,
        }),
      );
    }

    // -- Sequences ----------------------------------------------------
    for (const sname of [...engine._sequences.keys()].sort()) {
      const seqOid = oids.get("sequence", sname) ?? 0;
      rows.push(
        pgClassRow({
          oid: seqOid,
          relname: sname,
          relnamespace: nsPublic,
          relkind: "S",
          relnatts: 3,
        }),
      );
    }

    // -- Foreign tables -----------------------------------------------
    for (const ftname of [...engine._foreignTables.keys()].sort()) {
      const ftOid = oids.get("foreign_table", ftname) ?? 0;
      const ft = engine._foreignTables.get(ftname)!;
      rows.push(
        pgClassRow({
          oid: ftOid,
          relname: ftname,
          relnamespace: nsPublic,
          relkind: "f",
          relnatts: ft.columns.size,
        }),
      );
    }

    // -- Indexes (explicit) -------------------------------------------
    const indexes = getIndexManager(engine);
    if (indexes !== null) {
      for (const idxName of [...indexes.keys()].sort()) {
        const idxObj = indexes.get(idxName)!;
        const idxOid = oids.get("index", idxName) ?? 0;
        const idxDef = idxObj.indexDef;
        rows.push(
          pgClassRow({
            oid: idxOid,
            relname: idxName,
            relnamespace: nsPublic,
            relam: AM_BTREE,
            relkind: "i",
            relnatts: idxDef.columns.length,
          }),
        );
      }
    }

    // -- Implicit PK/UNIQUE indexes -----------------------------------
    for (const tname of [...engine._tables.keys()].sort()) {
      const table = engine._tables.get(tname)!;
      if (table.primaryKey) {
        const pkIdxName = `${tname}_pkey`;
        const pkIdxOid = oids.get("index", pkIdxName) ?? 0;
        rows.push(
          pgClassRow({
            oid: pkIdxOid,
            relname: pkIdxName,
            relnamespace: nsPublic,
            relam: AM_BTREE,
            relkind: "i",
            relnatts: 1,
          }),
        );
      }
      for (const [cname, cdef] of table.columns) {
        if (cdef.unique && !cdef.primaryKey) {
          const uqIdxName = `${tname}_${cname}_key`;
          const uqIdxOid = oids.get("index", uqIdxName) ?? 0;
          rows.push(
            pgClassRow({
              oid: uqIdxOid,
              relname: uqIdxName,
              relnamespace: nsPublic,
              relam: AM_BTREE,
              relkind: "i",
              relnatts: 1,
            }),
          );
        }
      }
    }

    return [columns, rows];
  }

  // ==================================================================
  // pg_attribute -- columns of all relations
  // ==================================================================

  private static _buildPgAttribute(
    engine: EngineInternals,
    oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "attrelid",
      "attname",
      "atttypid",
      "attstattarget",
      "attlen",
      "attnum",
      "attndims",
      "attcacheoff",
      "atttypmod",
      "attbyval",
      "attalign",
      "attstorage",
      "attcompression",
      "attnotnull",
      "atthasdef",
      "atthasmissing",
      "attidentity",
      "attgenerated",
      "attisdropped",
      "attislocal",
      "attinhcount",
      "attcollation",
      "attacl",
      "attoptions",
      "attfdwoptions",
      "attmissingval",
    ];
    const rows: Row[] = [];

    // System columns present in every table (hidden, negative attnum)
    const systemCols: [string, number, number, number][] = [
      ["tableoid", 26, 4, -6],
      ["cmax", 29, 4, -5],
      ["xmax", 28, 4, -4],
      ["cmin", 29, 4, -3],
      ["xmin", 28, 4, -2],
      ["ctid", 27, 6, -1],
    ];

    for (const tname of [...engine._tables.keys()].sort()) {
      const table = engine._tables.get(tname)!;
      const tableOid = oids.get("table", tname) ?? 0;

      // User columns
      let attnum = 0;
      for (const [cname, cdef] of table.columns) {
        attnum++;
        const colTypeOid = typeOid(effectiveTypeName(cdef));
        const attlen = TYPE_LENGTHS[colTypeOid] ?? -1;
        const byval = TYPE_BYVAL[colTypeOid] ?? false;
        const align = TYPE_ALIGN[colTypeOid] ?? "i";
        const storage = TYPE_STORAGE[colTypeOid] ?? "p";
        const ndims = cdef.typeName.endsWith("[]") ? 1 : 0;
        const hasDefault =
          (cdef.defaultValue !== null && cdef.defaultValue !== undefined) ||
          cdef.autoIncrement;
        const identity = cdef.autoIncrement ? "d" : "";

        // String types use default collation
        const collation =
          colTypeOid === 25 || colTypeOid === 1042 || colTypeOid === 1043 ? 100 : 0;

        rows.push({
          attrelid: tableOid,
          attname: cname,
          atttypid: colTypeOid,
          attstattarget: -1,
          attlen,
          attnum,
          attndims: ndims,
          attcacheoff: -1,
          atttypmod: -1,
          attbyval: byval,
          attalign: align,
          attstorage: storage,
          attcompression: "",
          attnotnull: cdef.notNull || cdef.primaryKey,
          atthasdef: hasDefault,
          atthasmissing: false,
          attidentity: identity,
          attgenerated: "",
          attisdropped: false,
          attislocal: true,
          attinhcount: 0,
          attcollation: collation,
          attacl: null,
          attoptions: null,
          attfdwoptions: null,
          attmissingval: null,
        });
      }

      // System columns
      for (const [sysName, sysType, sysLen, sysNum] of systemCols) {
        rows.push({
          attrelid: tableOid,
          attname: sysName,
          atttypid: sysType,
          attstattarget: 0,
          attlen: sysLen,
          attnum: sysNum,
          attndims: 0,
          attcacheoff: -1,
          atttypmod: -1,
          attbyval: true,
          attalign: sysLen === 4 ? "i" : "s",
          attstorage: "p",
          attcompression: "",
          attnotnull: true,
          atthasdef: false,
          atthasmissing: false,
          attidentity: "",
          attgenerated: "",
          attisdropped: false,
          attislocal: true,
          attinhcount: 0,
          attcollation: 0,
          attacl: null,
          attoptions: null,
          attfdwoptions: null,
          attmissingval: null,
        });
      }
    }

    // Foreign table columns
    for (const ftname of [...engine._foreignTables.keys()].sort()) {
      const ft = engine._foreignTables.get(ftname)!;
      const ftOid = oids.get("foreign_table", ftname) ?? 0;
      let attnum = 0;
      for (const [cname, cdef] of ft.columns) {
        attnum++;
        const colTypeOid = typeOid(effectiveTypeName(cdef));
        const attlen = TYPE_LENGTHS[colTypeOid] ?? -1;
        rows.push({
          attrelid: ftOid,
          attname: cname,
          atttypid: colTypeOid,
          attstattarget: -1,
          attlen,
          attnum,
          attndims: 0,
          attcacheoff: -1,
          atttypmod: -1,
          attbyval: TYPE_BYVAL[colTypeOid] ?? false,
          attalign: TYPE_ALIGN[colTypeOid] ?? "i",
          attstorage: TYPE_STORAGE[colTypeOid] ?? "p",
          attcompression: "",
          attnotnull: false,
          atthasdef: false,
          atthasmissing: false,
          attidentity: "",
          attgenerated: "",
          attisdropped: false,
          attislocal: true,
          attinhcount: 0,
          attcollation:
            colTypeOid === 25 || colTypeOid === 1042 || colTypeOid === 1043 ? 100 : 0,
          attacl: null,
          attoptions: null,
          attfdwoptions: null,
          attmissingval: null,
        });
      }
    }

    return [columns, rows];
  }

  // ==================================================================
  // pg_type -- data types
  // ==================================================================

  private static _buildPgType(
    engine: EngineInternals,
    oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "oid",
      "typname",
      "typnamespace",
      "typowner",
      "typlen",
      "typbyval",
      "typtype",
      "typcategory",
      "typispreferred",
      "typisdefined",
      "typdelim",
      "typrelid",
      "typsubscript",
      "typelem",
      "typarray",
      "typinput",
      "typoutput",
      "typreceive",
      "typsend",
      "typmodin",
      "typmodout",
      "typanalyze",
      "typalign",
      "typstorage",
      "typnotnull",
      "typbasetype",
      "typtypmod",
      "typndims",
      "typcollation",
      "typdefaultbin",
      "typdefault",
      "typacl",
    ];
    const nsPgCatalog = NS_PG_CATALOG;
    const rows: Row[] = [];

    // All built-in base types
    // [oid, typname, typlen, byval, category, preferred]
    const baseTypes: [number, string, number, boolean, string, boolean][] = [
      [16, "bool", 1, true, "B", true],
      [17, "bytea", -1, false, "U", false],
      [18, "char", 1, true, "Z", false],
      [19, "name", 64, false, "S", false],
      [20, "int8", 8, true, "N", false],
      [21, "int2", 2, true, "N", false],
      [23, "int4", 4, true, "N", false],
      [25, "text", -1, false, "S", true],
      [26, "oid", 4, true, "N", false],
      [27, "tid", 6, false, "U", false],
      [28, "xid", 4, true, "U", false],
      [29, "cid", 4, true, "U", false],
      [114, "json", -1, false, "U", false],
      [142, "xml", -1, false, "U", false],
      [600, "point", 16, false, "G", false],
      [700, "float4", 4, true, "N", false],
      [701, "float8", 8, true, "N", true],
      [1042, "bpchar", -1, false, "S", false],
      [1043, "varchar", -1, false, "S", false],
      [1082, "date", 4, true, "D", false],
      [1083, "time", 8, true, "D", false],
      [1114, "timestamp", 8, true, "D", false],
      [1184, "timestamptz", 8, true, "D", true],
      [1186, "interval", 16, false, "T", true],
      [1700, "numeric", -1, false, "N", false],
      [2205, "regclass", 4, true, "N", false],
      [2249, "record", -1, false, "P", false],
      [2278, "void", 4, true, "P", false],
      [2950, "uuid", 16, false, "U", false],
      [3802, "jsonb", -1, false, "U", false],
      [16385, "vector", -1, false, "U", false],
    ];

    for (const [typeOidVal, typname, typlen, byval, cat, preferred] of baseTypes) {
      const arrayOid = ARRAY_TYPE_OIDS[typeOidVal] ?? 0;
      const align = TYPE_ALIGN[typeOidVal] ?? "i";
      const storage = TYPE_STORAGE[typeOidVal] ?? "p";
      const collation = cat === "S" ? 100 : 0;

      rows.push({
        oid: typeOidVal,
        typname,
        typnamespace: nsPgCatalog,
        typowner: ROLE_OID,
        typlen,
        typbyval: byval,
        typtype: "b",
        typcategory: cat,
        typispreferred: preferred,
        typisdefined: true,
        typdelim: ",",
        typrelid: 0,
        typsubscript: "",
        typelem: 0,
        typarray: arrayOid,
        typinput: `${typname}in`,
        typoutput: `${typname}out`,
        typreceive: `${typname}recv`,
        typsend: `${typname}send`,
        typmodin: "",
        typmodout: "",
        typanalyze: "",
        typalign: align,
        typstorage: storage,
        typnotnull: false,
        typbasetype: 0,
        typtypmod: -1,
        typndims: 0,
        typcollation: collation,
        typdefaultbin: null,
        typdefault: null,
        typacl: null,
      });
    }

    // Array types
    const sortedArrayOids = Object.entries(ARRAY_TYPE_OIDS)
      .map(([k, v]) => [Number(k), v] as [number, number])
      .sort((a, b) => a[0] - b[0]);

    for (const [elemOid, arrOid] of sortedArrayOids) {
      // Find the element type name
      let elemName = "";
      for (const [btOid, btName] of baseTypes) {
        if (btOid === elemOid) {
          elemName = btName;
          break;
        }
      }
      if (!elemName) continue;

      rows.push({
        oid: arrOid,
        typname: `_${elemName}`,
        typnamespace: nsPgCatalog,
        typowner: ROLE_OID,
        typlen: -1,
        typbyval: false,
        typtype: "b",
        typcategory: "A",
        typispreferred: false,
        typisdefined: true,
        typdelim: ",",
        typrelid: 0,
        typsubscript: "array_subscript_handler",
        typelem: elemOid,
        typarray: 0,
        typinput: "array_in",
        typoutput: "array_out",
        typreceive: "array_recv",
        typsend: "array_send",
        typmodin: "",
        typmodout: "",
        typanalyze: "",
        typalign: "i",
        typstorage: "x",
        typnotnull: false,
        typbasetype: 0,
        typtypmod: -1,
        typndims: 0,
        typcollation: 0,
        typdefaultbin: null,
        typdefault: null,
        typacl: null,
      });
    }

    // Composite types for user tables
    for (const tname of [...engine._tables.keys()].sort()) {
      const compOid = oids.get("table_type", tname) ?? 0;
      const tableOid = oids.get("table", tname) ?? 0;
      rows.push({
        oid: compOid,
        typname: tname,
        typnamespace: NS_PUBLIC,
        typowner: ROLE_OID,
        typlen: -1,
        typbyval: false,
        typtype: "c",
        typcategory: "C",
        typispreferred: false,
        typisdefined: true,
        typdelim: ",",
        typrelid: tableOid,
        typsubscript: "",
        typelem: 0,
        typarray: 0,
        typinput: "record_in",
        typoutput: "record_out",
        typreceive: "record_recv",
        typsend: "record_send",
        typmodin: "",
        typmodout: "",
        typanalyze: "",
        typalign: "d",
        typstorage: "x",
        typnotnull: false,
        typbasetype: 0,
        typtypmod: -1,
        typndims: 0,
        typcollation: 0,
        typdefaultbin: null,
        typdefault: null,
        typacl: null,
      });
    }

    return [columns, rows];
  }

  // ==================================================================
  // pg_constraint
  // ==================================================================

  private static _buildPgConstraint(
    engine: EngineInternals,
    oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "oid",
      "conname",
      "connamespace",
      "contype",
      "condeferrable",
      "condeferred",
      "convalidated",
      "conrelid",
      "contypid",
      "conindid",
      "conparentid",
      "confrelid",
      "confupdtype",
      "confdeltype",
      "confmatchtype",
      "conislocal",
      "coninhcount",
      "connoinherit",
      "conkey",
      "confkey",
      "conpfeqop",
      "conppeqop",
      "conffeqop",
      "conexclop",
      "conbin",
    ];
    const rows: Row[] = [];
    const nsPublic = NS_PUBLIC;

    for (const tname of [...engine._tables.keys()].sort()) {
      const table = engine._tables.get(tname)!;
      const tableOid = oids.get("table", tname) ?? 0;
      const colNums = new Map<string, number>();
      let num = 0;
      for (const cname of table.columns.keys()) {
        num++;
        colNums.set(cname, num);
      }

      // PRIMARY KEY
      if (table.primaryKey) {
        const conName = `${tname}_pkey`;
        const conOid = oids.get("constraint", conName) ?? 0;
        const idxOid = oids.get("index", conName) ?? 0;
        const pkAttnum = colNums.get(table.primaryKey) ?? 1;
        rows.push({
          oid: conOid,
          conname: conName,
          connamespace: nsPublic,
          contype: "p",
          condeferrable: false,
          condeferred: false,
          convalidated: true,
          conrelid: tableOid,
          contypid: 0,
          conindid: idxOid,
          conparentid: 0,
          confrelid: 0,
          confupdtype: " ",
          confdeltype: " ",
          confmatchtype: " ",
          conislocal: true,
          coninhcount: 0,
          connoinherit: true,
          conkey: `{${pkAttnum}}`,
          confkey: null,
          conpfeqop: null,
          conppeqop: null,
          conffeqop: null,
          conexclop: null,
          conbin: null,
        });
      }

      // UNIQUE constraints
      for (const [cname, cdef] of table.columns) {
        if (cdef.unique && !cdef.primaryKey) {
          const conName = `${tname}_${cname}_key`;
          const conOid = oids.get("constraint", conName) ?? 0;
          const idxOid = oids.get("index", conName) ?? 0;
          const attnum = colNums.get(cname) ?? 1;
          rows.push({
            oid: conOid,
            conname: conName,
            connamespace: nsPublic,
            contype: "u",
            condeferrable: false,
            condeferred: false,
            convalidated: true,
            conrelid: tableOid,
            contypid: 0,
            conindid: idxOid,
            conparentid: 0,
            confrelid: 0,
            confupdtype: " ",
            confdeltype: " ",
            confmatchtype: " ",
            conislocal: true,
            coninhcount: 0,
            connoinherit: true,
            conkey: `{${attnum}}`,
            confkey: null,
            conpfeqop: null,
            conppeqop: null,
            conffeqop: null,
            conexclop: null,
            conbin: null,
          });
        }
      }

      // FOREIGN KEY constraints
      for (const fk of table.foreignKeys) {
        const conName = `${tname}_${fk.column}_fkey`;
        const conOid = oids.get("constraint", conName) ?? 0;
        const fkAttnum = colNums.get(fk.column) ?? 1;
        const refTableOid = oids.get("table", fk.refTable) ?? 0;
        const refTableObj = engine._tables.get(fk.refTable);
        let refAttnum = 1;
        if (refTableObj !== undefined) {
          const refColNums = new Map<string, number>();
          let refNum = 0;
          for (const cn of refTableObj.columns.keys()) {
            refNum++;
            refColNums.set(cn, refNum);
          }
          refAttnum = refColNums.get(fk.refColumn) ?? 1;
        }

        rows.push({
          oid: conOid,
          conname: conName,
          connamespace: nsPublic,
          contype: "f",
          condeferrable: false,
          condeferred: false,
          convalidated: true,
          conrelid: tableOid,
          contypid: 0,
          conindid: 0,
          conparentid: 0,
          confrelid: refTableOid,
          confupdtype: "a",
          confdeltype: "a",
          confmatchtype: "s",
          conislocal: true,
          coninhcount: 0,
          connoinherit: true,
          conkey: `{${fkAttnum}}`,
          confkey: `{${refAttnum}}`,
          conpfeqop: null,
          conppeqop: null,
          conffeqop: null,
          conexclop: null,
          conbin: null,
        });
      }

      // CHECK constraints
      for (const [checkName] of table.checkConstraints) {
        const conName = `${tname}_${checkName}_check`;
        const conOid = oids.get("constraint", conName) ?? 0;
        rows.push({
          oid: conOid,
          conname: conName,
          connamespace: nsPublic,
          contype: "c",
          condeferrable: false,
          condeferred: false,
          convalidated: true,
          conrelid: tableOid,
          contypid: 0,
          conindid: 0,
          conparentid: 0,
          confrelid: 0,
          confupdtype: " ",
          confdeltype: " ",
          confmatchtype: " ",
          conislocal: true,
          coninhcount: 0,
          connoinherit: true,
          conkey: null,
          confkey: null,
          conpfeqop: null,
          conppeqop: null,
          conffeqop: null,
          conexclop: null,
          conbin: null,
        });
      }
    }

    return [columns, rows];
  }

  // ==================================================================
  // pg_index
  // ==================================================================

  private static _buildPgIndex(
    engine: EngineInternals,
    oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "indexrelid",
      "indrelid",
      "indnatts",
      "indnkeyatts",
      "indisunique",
      "indisprimary",
      "indisexclusion",
      "indimmediate",
      "indisclustered",
      "indisvalid",
      "indcheckxmin",
      "indisready",
      "indislive",
      "indisreplident",
      "indkey",
      "indcollation",
      "indclass",
      "indoption",
      "indexprs",
      "indpred",
    ];
    const rows: Row[] = [];

    // Explicit indexes from IndexManager
    const indexes = getIndexManager(engine);
    if (indexes !== null) {
      for (const idxName of [...indexes.keys()].sort()) {
        const idxObj = indexes.get(idxName)!;
        const idxOid = oids.get("index", idxName) ?? 0;
        const idxDef = idxObj.indexDef;
        const tableOid = oids.get("table", idxDef.tableName) ?? 0;
        const tableObj = engine._tables.get(idxDef.tableName);
        const nAtts = idxDef.columns.length;

        const indkeyParts: string[] = [];
        if (tableObj !== undefined) {
          const colNums = new Map<string, number>();
          let n = 0;
          for (const cn of tableObj.columns.keys()) {
            n++;
            colNums.set(cn, n);
          }
          for (const col of idxDef.columns) {
            indkeyParts.push(String(colNums.get(col) ?? 0));
          }
        }
        const indkey = indkeyParts.length > 0 ? indkeyParts.join(" ") : "0";

        const isUnique = idxDef.unique ?? false;

        rows.push({
          indexrelid: idxOid,
          indrelid: tableOid,
          indnatts: nAtts,
          indnkeyatts: nAtts,
          indisunique: isUnique,
          indisprimary: false,
          indisexclusion: false,
          indimmediate: true,
          indisclustered: false,
          indisvalid: true,
          indcheckxmin: false,
          indisready: true,
          indislive: true,
          indisreplident: false,
          indkey,
          indcollation: "",
          indclass: "",
          indoption: "",
          indexprs: null,
          indpred: null,
        });
      }
    }

    // Implicit PK/UNIQUE indexes
    for (const tname of [...engine._tables.keys()].sort()) {
      const table = engine._tables.get(tname)!;
      const tableOid = oids.get("table", tname) ?? 0;
      const colNums = new Map<string, number>();
      let n = 0;
      for (const cn of table.columns.keys()) {
        n++;
        colNums.set(cn, n);
      }

      if (table.primaryKey) {
        const pkIdxName = `${tname}_pkey`;
        const pkIdxOid = oids.get("index", pkIdxName) ?? 0;
        const pkAttnum = colNums.get(table.primaryKey) ?? 0;
        rows.push({
          indexrelid: pkIdxOid,
          indrelid: tableOid,
          indnatts: 1,
          indnkeyatts: 1,
          indisunique: true,
          indisprimary: true,
          indisexclusion: false,
          indimmediate: true,
          indisclustered: false,
          indisvalid: true,
          indcheckxmin: false,
          indisready: true,
          indislive: true,
          indisreplident: false,
          indkey: String(pkAttnum),
          indcollation: "",
          indclass: "",
          indoption: "",
          indexprs: null,
          indpred: null,
        });
      }

      for (const [cname, cdef] of table.columns) {
        if (cdef.unique && !cdef.primaryKey) {
          const uqIdxName = `${tname}_${cname}_key`;
          const uqIdxOid = oids.get("index", uqIdxName) ?? 0;
          const attnum = colNums.get(cname) ?? 0;
          rows.push({
            indexrelid: uqIdxOid,
            indrelid: tableOid,
            indnatts: 1,
            indnkeyatts: 1,
            indisunique: true,
            indisprimary: false,
            indisexclusion: false,
            indimmediate: true,
            indisclustered: false,
            indisvalid: true,
            indcheckxmin: false,
            indisready: true,
            indislive: true,
            indisreplident: false,
            indkey: String(attnum),
            indcollation: "",
            indclass: "",
            indoption: "",
            indexprs: null,
            indpred: null,
          });
        }
      }
    }

    return [columns, rows];
  }

  // ==================================================================
  // pg_attrdef -- column defaults
  // ==================================================================

  private static _buildPgAttrdef(
    engine: EngineInternals,
    oids: OIDAllocator,
  ): BuildResult {
    const columns = ["oid", "adrelid", "adnum", "adbin"];
    const rows: Row[] = [];

    for (const tname of [...engine._tables.keys()].sort()) {
      const table = engine._tables.get(tname)!;
      const tableOid = oids.get("table", tname) ?? 0;
      let attnum = 0;
      for (const [cname, cdef] of table.columns) {
        attnum++;
        if (
          (cdef.defaultValue !== null && cdef.defaultValue !== undefined) ||
          cdef.autoIncrement
        ) {
          const defOid = oids.getOrAlloc("attrdef", `${tname}.${cname}`);
          let adbin: string;
          if (cdef.autoIncrement) {
            adbin = `nextval('${tname}_${cname}_seq'::regclass)`;
          } else {
            adbin = String(cdef.defaultValue);
          }
          rows.push({
            oid: defOid,
            adrelid: tableOid,
            adnum: attnum,
            adbin,
          });
        }
      }
    }

    return [columns, rows];
  }

  // ==================================================================
  // pg_am -- access methods
  // ==================================================================

  private static _buildPgAm(
    _engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = ["oid", "amname", "amhandler", "amtype"];
    const rows: Row[] = [
      {
        oid: AM_HEAP,
        amname: "heap",
        amhandler: "heap_tableam_handler",
        amtype: "t",
      },
      {
        oid: AM_BTREE,
        amname: "btree",
        amhandler: "bthandler",
        amtype: "i",
      },
      {
        oid: AM_HASH,
        amname: "hash",
        amhandler: "hashhandler",
        amtype: "i",
      },
      {
        oid: AM_HNSW,
        amname: "hnsw",
        amhandler: "hnsw_handler",
        amtype: "i",
      },
      {
        oid: AM_IVF,
        amname: "ivf",
        amhandler: "ivf_handler",
        amtype: "i",
      },
    ];
    return [columns, rows];
  }

  // ==================================================================
  // pg_database
  // ==================================================================

  private static _buildPgDatabase(
    _engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "oid",
      "datname",
      "datdba",
      "encoding",
      "datlocprovider",
      "datistemplate",
      "datallowconn",
      "datconnlimit",
      "datfrozenxid",
      "datminmxid",
      "dattablespace",
      "datcollate",
      "datctype",
      "datlocale",
      "datcollversion",
      "datacl",
    ];
    const rows: Row[] = [
      {
        oid: DATABASE_OID,
        datname: CATALOG_NAME,
        datdba: ROLE_OID,
        encoding: ENCODING_UTF8,
        datlocprovider: "c",
        datistemplate: false,
        datallowconn: true,
        datconnlimit: -1,
        datfrozenxid: 0,
        datminmxid: 1,
        dattablespace: 1663,
        datcollate: "en_US.UTF-8",
        datctype: "en_US.UTF-8",
        datlocale: null,
        datcollversion: null,
        datacl: null,
      },
    ];
    return [columns, rows];
  }

  // ==================================================================
  // pg_roles
  // ==================================================================

  private static _buildPgRoles(
    _engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "oid",
      "rolname",
      "rolsuper",
      "rolinherit",
      "rolcreaterole",
      "rolcreatedb",
      "rolcanlogin",
      "rolreplication",
      "rolconnlimit",
      "rolpassword",
      "rolvaliduntil",
      "rolbypassrls",
      "rolconfig",
    ];
    const rows: Row[] = [
      {
        oid: ROLE_OID,
        rolname: OWNER,
        rolsuper: true,
        rolinherit: true,
        rolcreaterole: true,
        rolcreatedb: true,
        rolcanlogin: true,
        rolreplication: true,
        rolconnlimit: -1,
        rolpassword: null,
        rolvaliduntil: null,
        rolbypassrls: true,
        rolconfig: null,
      },
    ];
    return [columns, rows];
  }

  // ==================================================================
  // pg_user -- simplified view over pg_roles
  // ==================================================================

  private static _buildPgUser(
    _engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "usename",
      "usesysid",
      "usecreatedb",
      "usesuper",
      "userepl",
      "usebypassrls",
      "passwd",
      "valuntil",
      "useconfig",
    ];
    const rows: Row[] = [
      {
        usename: OWNER,
        usesysid: ROLE_OID,
        usecreatedb: true,
        usesuper: true,
        userepl: true,
        usebypassrls: true,
        passwd: null,
        valuntil: null,
        useconfig: null,
      },
    ];
    return [columns, rows];
  }

  // ==================================================================
  // pg_tables -- convenience view
  // ==================================================================

  private static _buildPgTables(
    engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "schemaname",
      "tablename",
      "tableowner",
      "tablespace",
      "hasindexes",
      "hasrules",
      "hastriggers",
      "rowsecurity",
    ];
    const rows: Row[] = [];
    for (const tname of [...engine._tables.keys()].sort()) {
      const table = engine._tables.get(tname)!;
      const hasIdx =
        table.primaryKey !== null || [...table.columns.values()].some((c) => c.unique);
      rows.push({
        schemaname: SCHEMA,
        tablename: tname,
        tableowner: OWNER,
        tablespace: null,
        hasindexes: hasIdx,
        hasrules: false,
        hastriggers: false,
        rowsecurity: false,
      });
    }
    return [columns, rows];
  }

  // ==================================================================
  // pg_views -- convenience view
  // ==================================================================

  private static _buildPgViews(
    engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = ["schemaname", "viewname", "viewowner", "definition"];
    const rows: Row[] = [];
    for (const vname of [...engine._views.keys()].sort()) {
      rows.push({
        schemaname: SCHEMA,
        viewname: vname,
        viewowner: OWNER,
        definition: "",
      });
    }
    return [columns, rows];
  }

  // ==================================================================
  // pg_indexes -- convenience view
  // ==================================================================

  private static _buildPgIndexes(
    engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = ["schemaname", "tablename", "indexname", "tablespace", "indexdef"];
    const rows: Row[] = [];

    const indexes = getIndexManager(engine);
    if (indexes !== null) {
      for (const idxName of [...indexes.keys()].sort()) {
        const idxObj = indexes.get(idxName)!;
        const idxDef = idxObj.indexDef;
        const colsStr = idxDef.columns.join(", ");
        rows.push({
          schemaname: SCHEMA,
          tablename: idxDef.tableName,
          indexname: idxName,
          tablespace: null,
          indexdef: `CREATE INDEX ${idxName} ON ${idxDef.tableName} (${colsStr})`,
        });
      }
    }

    // Implicit PK/UNIQUE indexes
    for (const tname of [...engine._tables.keys()].sort()) {
      const table = engine._tables.get(tname)!;
      if (table.primaryKey) {
        const pkName = `${tname}_pkey`;
        rows.push({
          schemaname: SCHEMA,
          tablename: tname,
          indexname: pkName,
          tablespace: null,
          indexdef: `CREATE UNIQUE INDEX ${pkName} ON ${tname} (${table.primaryKey})`,
        });
      }
      for (const [cname, cdef] of table.columns) {
        if (cdef.unique && !cdef.primaryKey) {
          const uqName = `${tname}_${cname}_key`;
          rows.push({
            schemaname: SCHEMA,
            tablename: tname,
            indexname: uqName,
            tablespace: null,
            indexdef: `CREATE UNIQUE INDEX ${uqName} ON ${tname} (${cname})`,
          });
        }
      }
    }

    return [columns, rows];
  }

  // ==================================================================
  // pg_matviews -- materialized views (empty, UQA has none)
  // ==================================================================

  private static _buildPgMatviews(
    _engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "schemaname",
      "matviewname",
      "matviewowner",
      "tablespace",
      "hasindexes",
      "ispopulated",
      "definition",
    ];
    return [columns, []];
  }

  // ==================================================================
  // pg_sequences
  // ==================================================================

  private static _buildPgSequences(
    engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "schemaname",
      "sequencename",
      "sequenceowner",
      "data_type",
      "start_value",
      "min_value",
      "max_value",
      "increment_by",
      "cycle",
      "cache_size",
      "last_value",
    ];
    const rows: Row[] = [];
    for (const sname of [...engine._sequences.keys()].sort()) {
      const seq = engine._sequences.get(sname)!;
      rows.push({
        schemaname: SCHEMA,
        sequencename: sname,
        sequenceowner: OWNER,
        data_type: "bigint",
        start_value: seq["start"] ?? 1,
        min_value: 1,
        max_value: 9223372036854775807,
        increment_by: seq["increment"] ?? 1,
        cycle: false,
        cache_size: 1,
        last_value: seq["current"] ?? seq["start"] ?? 1,
      });
    }
    return [columns, rows];
  }

  // ==================================================================
  // pg_settings -- runtime parameters (GUC)
  // ==================================================================

  private static _buildPgSettings(
    _engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "name",
      "setting",
      "unit",
      "category",
      "short_desc",
      "extra_desc",
      "context",
      "vartype",
      "source",
      "min_val",
      "max_val",
      "enumvals",
      "boot_val",
      "reset_val",
      "sourcefile",
      "sourceline",
      "pending_restart",
    ];

    // [name, setting, unit, category, short_desc, extra_desc,
    //  context, vartype, source, min_val, max_val, enumvals,
    //  boot_val, reset_val, sourcefile, sourceline, pending_restart]
    const settings: [
      string,
      string,
      string | null,
      string,
      string,
      string | null,
      string,
      string,
      string,
      string | null,
      string | null,
      string | null,
      string,
      string,
      string | null,
      number | null,
      boolean,
    ][] = [
      [
        "server_version",
        "17.0",
        null,
        "Preset Options",
        "Shows the server version.",
        null,
        "internal",
        "string",
        "default",
        null,
        null,
        null,
        "17.0",
        "17.0",
        null,
        null,
        false,
      ],
      [
        "server_version_num",
        "170000",
        null,
        "Preset Options",
        "Shows the server version as an integer.",
        null,
        "internal",
        "integer",
        "default",
        null,
        null,
        null,
        "170000",
        "170000",
        null,
        null,
        false,
      ],
      [
        "server_encoding",
        "UTF8",
        null,
        "Preset Options",
        "Shows the server encoding.",
        null,
        "internal",
        "string",
        "default",
        null,
        null,
        null,
        "UTF8",
        "UTF8",
        null,
        null,
        false,
      ],
      [
        "client_encoding",
        "UTF8",
        null,
        "Client Connection Defaults",
        "Sets the client encoding.",
        null,
        "user",
        "string",
        "default",
        null,
        null,
        null,
        "UTF8",
        "UTF8",
        null,
        null,
        false,
      ],
      [
        "lc_collate",
        "en_US.UTF-8",
        null,
        "Preset Options",
        "Shows the collation order locale.",
        null,
        "internal",
        "string",
        "default",
        null,
        null,
        null,
        "en_US.UTF-8",
        "en_US.UTF-8",
        null,
        null,
        false,
      ],
      [
        "lc_ctype",
        "en_US.UTF-8",
        null,
        "Preset Options",
        "Shows the character classification locale.",
        null,
        "internal",
        "string",
        "default",
        null,
        null,
        null,
        "en_US.UTF-8",
        "en_US.UTF-8",
        null,
        null,
        false,
      ],
      [
        "DateStyle",
        "ISO, MDY",
        null,
        "Client Connection Defaults",
        "Sets the display format for date and time.",
        null,
        "user",
        "string",
        "default",
        null,
        null,
        null,
        "ISO, MDY",
        "ISO, MDY",
        null,
        null,
        false,
      ],
      [
        "TimeZone",
        "UTC",
        null,
        "Client Connection Defaults",
        "Sets the time zone.",
        null,
        "user",
        "string",
        "default",
        null,
        null,
        null,
        "UTC",
        "UTC",
        null,
        null,
        false,
      ],
      [
        "standard_conforming_strings",
        "on",
        null,
        "Client Connection Defaults",
        "Causes strings to treat backslashes literally.",
        null,
        "user",
        "bool",
        "default",
        null,
        null,
        null,
        "on",
        "on",
        null,
        null,
        false,
      ],
      [
        "search_path",
        '"$user", public',
        null,
        "Client Connection Defaults",
        "Sets the schema search order.",
        null,
        "user",
        "string",
        "default",
        null,
        null,
        null,
        '"$user", public',
        '"$user", public',
        null,
        null,
        false,
      ],
      [
        "default_transaction_isolation",
        "read committed",
        null,
        "Client Connection Defaults",
        "Sets the default transaction isolation level.",
        null,
        "user",
        "enum",
        "default",
        null,
        null,
        "serializable,repeatable read,read committed,read uncommitted",
        "read committed",
        "read committed",
        null,
        null,
        false,
      ],
      [
        "max_connections",
        "100",
        null,
        "Connections and Authentication",
        "Sets the maximum number of concurrent connections.",
        null,
        "postmaster",
        "integer",
        "default",
        "1",
        "262143",
        null,
        "100",
        "100",
        null,
        null,
        false,
      ],
      [
        "shared_buffers",
        "16384",
        "8kB",
        "Resource Usage / Memory",
        "Sets the number of shared memory buffers.",
        null,
        "postmaster",
        "integer",
        "default",
        "16",
        "1073741823",
        null,
        "16384",
        "16384",
        null,
        null,
        false,
      ],
      [
        "work_mem",
        "4096",
        "kB",
        "Resource Usage / Memory",
        "Sets the maximum memory for query operations.",
        null,
        "user",
        "integer",
        "default",
        "64",
        "2147483647",
        null,
        "4096",
        "4096",
        null,
        null,
        false,
      ],
      [
        "is_superuser",
        "on",
        null,
        "Preset Options",
        "Shows whether the current user is a superuser.",
        null,
        "internal",
        "bool",
        "default",
        null,
        null,
        null,
        "on",
        "on",
        null,
        null,
        false,
      ],
      [
        "transaction_isolation",
        "read committed",
        null,
        "Client Connection Defaults",
        "Shows the current transaction isolation level.",
        null,
        "user",
        "string",
        "override",
        null,
        null,
        null,
        "read committed",
        "read committed",
        null,
        null,
        false,
      ],
      [
        "integer_datetimes",
        "on",
        null,
        "Preset Options",
        "Shows if datetimes are stored as 64-bit integers.",
        null,
        "internal",
        "bool",
        "default",
        null,
        null,
        null,
        "on",
        "on",
        null,
        null,
        false,
      ],
    ];

    const rows: Row[] = [];
    for (const s of settings) {
      rows.push({
        name: s[0],
        setting: s[1],
        unit: s[2],
        category: s[3],
        short_desc: s[4],
        extra_desc: s[5],
        context: s[6],
        vartype: s[7],
        source: s[8],
        min_val: s[9],
        max_val: s[10],
        enumvals: s[11],
        boot_val: s[12],
        reset_val: s[13],
        sourcefile: s[14],
        sourceline: s[15],
        pending_restart: s[16],
      });
    }
    return [columns, rows];
  }

  // ==================================================================
  // pg_foreign_server
  // ==================================================================

  private static _buildPgForeignServer(
    engine: EngineInternals,
    oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "oid",
      "srvname",
      "srvowner",
      "srvfdw",
      "srvtype",
      "srvversion",
      "srvacl",
      "srvoptions",
    ];
    const rows: Row[] = [];
    for (const sname of [...engine._foreignServers.keys()].sort()) {
      const srv = engine._foreignServers.get(sname)!;
      const srvOid = oids.get("foreign_server", sname) ?? 0;
      const fdwOid = oids.get("fdw", srv.fdwType) ?? 0;
      const opts = Object.entries(srv.options)
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([k, v]) => `${k}=${v}`);
      rows.push({
        oid: srvOid,
        srvname: sname,
        srvowner: ROLE_OID,
        srvfdw: fdwOid,
        srvtype: null,
        srvversion: null,
        srvacl: null,
        srvoptions: opts.length > 0 ? `{${opts.join(",")}}` : null,
      });
    }
    return [columns, rows];
  }

  // ==================================================================
  // pg_foreign_table
  // ==================================================================

  private static _buildPgForeignTable(
    engine: EngineInternals,
    oids: OIDAllocator,
  ): BuildResult {
    const columns = ["ftrelid", "ftserver", "ftoptions"];
    const rows: Row[] = [];
    for (const ftname of [...engine._foreignTables.keys()].sort()) {
      const ft = engine._foreignTables.get(ftname)!;
      const ftOid = oids.get("foreign_table", ftname) ?? 0;
      const srvOid = oids.get("foreign_server", ft.serverName) ?? 0;
      const opts = Object.entries(ft.options)
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([k, v]) => `${k}=${v}`);
      rows.push({
        ftrelid: ftOid,
        ftserver: srvOid,
        ftoptions: opts.length > 0 ? `{${opts.join(",")}}` : null,
      });
    }
    return [columns, rows];
  }

  // ==================================================================
  // pg_foreign_data_wrapper
  // ==================================================================

  private static _buildPgForeignDataWrapper(
    engine: EngineInternals,
    oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "oid",
      "fdwname",
      "fdwowner",
      "fdwhandler",
      "fdwvalidator",
      "fdwacl",
      "fdwoptions",
    ];
    const rows: Row[] = [];
    const seen = new Set<string>();
    for (const sname of [...engine._foreignServers.keys()].sort()) {
      const srv = engine._foreignServers.get(sname)!;
      if (seen.has(srv.fdwType)) continue;
      seen.add(srv.fdwType);
      const fdwOid = oids.get("fdw", srv.fdwType) ?? 0;
      rows.push({
        oid: fdwOid,
        fdwname: srv.fdwType,
        fdwowner: ROLE_OID,
        fdwhandler: 0,
        fdwvalidator: 0,
        fdwacl: null,
        fdwoptions: null,
      });
    }
    return [columns, rows];
  }

  // ==================================================================
  // pg_description -- object comments
  // ==================================================================

  private static _buildPgDescription(
    _engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = ["objoid", "classoid", "objsubid", "description"];
    // UQA does not support COMMENT ON, return empty
    return [columns, []];
  }

  // ==================================================================
  // pg_depend -- object dependencies
  // ==================================================================

  private static _buildPgDepend(
    _engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "classid",
      "objid",
      "objsubid",
      "refclassid",
      "refobjid",
      "refobjsubid",
      "deptype",
    ];
    // Basic dependency tracking: FK constraints depend on their
    // referenced tables.
    const rows: Row[] = [];
    return [columns, rows];
  }

  // ==================================================================
  // pg_stat_user_tables (also used for pg_stat_all_tables)
  // ==================================================================

  private static _buildPgStatUserTables(
    engine: EngineInternals,
    oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "relid",
      "schemaname",
      "relname",
      "seq_scan",
      "seq_tup_read",
      "idx_scan",
      "idx_tup_fetch",
      "n_tup_ins",
      "n_tup_upd",
      "n_tup_del",
      "n_tup_hot_upd",
      "n_live_tup",
      "n_dead_tup",
      "n_mod_since_analyze",
      "n_ins_since_vacuum",
      "last_vacuum",
      "last_autovacuum",
      "last_analyze",
      "last_autoanalyze",
      "vacuum_count",
      "autovacuum_count",
      "analyze_count",
      "autoanalyze_count",
    ];
    const rows: Row[] = [];
    for (const tname of [...engine._tables.keys()].sort()) {
      const table = engine._tables.get(tname)!;
      const tableOid = oids.get("table", tname) ?? 0;
      const nLive = table.rowCount;
      const hasStats = Boolean(table._stats);
      rows.push({
        relid: tableOid,
        schemaname: SCHEMA,
        relname: tname,
        seq_scan: 0,
        seq_tup_read: 0,
        idx_scan: 0,
        idx_tup_fetch: 0,
        n_tup_ins: nLive,
        n_tup_upd: 0,
        n_tup_del: 0,
        n_tup_hot_upd: 0,
        n_live_tup: nLive,
        n_dead_tup: 0,
        n_mod_since_analyze: hasStats ? 0 : nLive,
        n_ins_since_vacuum: nLive,
        last_vacuum: null,
        last_autovacuum: null,
        last_analyze: null,
        last_autoanalyze: null,
        vacuum_count: 0,
        autovacuum_count: 0,
        analyze_count: hasStats ? 1 : 0,
        autoanalyze_count: 0,
      });
    }
    return [columns, rows];
  }

  // ==================================================================
  // pg_stat_user_indexes
  // ==================================================================

  private static _buildPgStatUserIndexes(
    engine: EngineInternals,
    oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "relid",
      "indexrelid",
      "schemaname",
      "relname",
      "indexrelname",
      "idx_scan",
      "idx_tup_read",
      "idx_tup_fetch",
    ];
    const rows: Row[] = [];

    for (const tname of [...engine._tables.keys()].sort()) {
      const table = engine._tables.get(tname)!;
      const tableOid = oids.get("table", tname) ?? 0;
      if (table.primaryKey) {
        const pkName = `${tname}_pkey`;
        const pkOid = oids.get("index", pkName) ?? 0;
        rows.push({
          relid: tableOid,
          indexrelid: pkOid,
          schemaname: SCHEMA,
          relname: tname,
          indexrelname: pkName,
          idx_scan: 0,
          idx_tup_read: 0,
          idx_tup_fetch: 0,
        });
      }
      for (const [cname, cdef] of table.columns) {
        if (cdef.unique && !cdef.primaryKey) {
          const uqName = `${tname}_${cname}_key`;
          const uqOid = oids.get("index", uqName) ?? 0;
          rows.push({
            relid: tableOid,
            indexrelid: uqOid,
            schemaname: SCHEMA,
            relname: tname,
            indexrelname: uqName,
            idx_scan: 0,
            idx_tup_read: 0,
            idx_tup_fetch: 0,
          });
        }
      }
    }

    return [columns, rows];
  }

  // ==================================================================
  // pg_stat_activity -- active sessions
  // ==================================================================

  private static _buildPgStatActivity(
    _engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "datid",
      "datname",
      "pid",
      "leader_pid",
      "usesysid",
      "usename",
      "application_name",
      "client_addr",
      "client_hostname",
      "client_port",
      "backend_start",
      "xact_start",
      "query_start",
      "state_change",
      "wait_event_type",
      "wait_event",
      "state",
      "backend_xid",
      "backend_xmin",
      "query_id",
      "query",
      "backend_type",
    ];

    const liveConnections = getAllConnections();
    let rows: Row[];

    if (liveConnections.length > 0) {
      rows = liveConnections.map((conn) => ({
        datid: DATABASE_OID,
        datname: conn.database || CATALOG_NAME,
        pid: conn.pid,
        leader_pid: null,
        usesysid: ROLE_OID,
        usename: conn.username || OWNER,
        application_name: conn.applicationName,
        client_addr: conn.clientAddr,
        client_hostname: null,
        client_port: conn.clientPort,
        backend_start: conn.backendStart?.toISOString() ?? null,
        xact_start: conn.xactStart?.toISOString() ?? null,
        query_start: conn.queryStart?.toISOString() ?? null,
        state_change: conn.stateChange?.toISOString() ?? null,
        wait_event_type: null,
        wait_event: null,
        state: conn.state,
        backend_xid: null,
        backend_xmin: null,
        query_id: null,
        query: conn.query,
        backend_type: conn.backendType,
      }));
    } else {
      rows = [
        {
          datid: DATABASE_OID,
          datname: CATALOG_NAME,
          pid: process.pid,
          leader_pid: null,
          usesysid: ROLE_OID,
          usename: OWNER,
          application_name: "usqldb",
          client_addr: null,
          client_hostname: null,
          client_port: -1,
          backend_start: null,
          xact_start: null,
          query_start: null,
          state_change: null,
          wait_event_type: null,
          wait_event: null,
          state: "active",
          backend_xid: null,
          backend_xmin: null,
          query_id: null,
          query: "",
          backend_type: "client backend",
        },
      ];
    }

    return [columns, rows];
  }

  // ==================================================================
  // pg_proc -- functions/procedures
  // ==================================================================

  private static _buildPgProc(
    _engine: EngineInternals,
    oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "oid",
      "proname",
      "pronamespace",
      "proowner",
      "prolang",
      "procost",
      "prorows",
      "provariadic",
      "prosupport",
      "prokind",
      "prosecdef",
      "proleakproof",
      "proisstrict",
      "proretset",
      "provolatile",
      "proparallel",
      "pronargs",
      "pronargdefaults",
      "prorettype",
      "proargtypes",
      "proallargtypes",
      "proargmodes",
      "proargnames",
      "proargdefaults",
      "protrftypes",
      "prosrc",
      "probin",
      "prosqlbody",
      "proconfig",
      "proacl",
    ];

    // UQA built-in extended SQL functions
    // [name, nargs, rettype, argtypes]
    const uqaFunctions: [string, number, number, string][] = [
      ["text_match", 2, 25, "25 25"],
      ["bayesian_match", 2, 25, "25 25"],
      ["knn_match", 3, 25, "25 2277 23"],
      ["traverse_match", 3, 25, "23 25 23"],
      ["fuse_log_odds", 0, 25, ""],
      ["fuse_prob_and", 0, 25, ""],
      ["fuse_prob_or", 0, 25, ""],
      ["fuse_prob_not", 1, 25, "25"],
      ["spatial_within", 4, 25, "25 600 600 701"],
    ];

    const rows: Row[] = [];
    const nsPublic = NS_PUBLIC;
    for (const [fname, nargs, rettype, argtypes] of uqaFunctions) {
      const funcOid = oids.getOrAlloc("function", fname);
      rows.push({
        oid: funcOid,
        proname: fname,
        pronamespace: nsPublic,
        proowner: ROLE_OID,
        prolang: 14, // SQL
        procost: 100,
        prorows: 0,
        provariadic: 0,
        prosupport: "",
        prokind: "f",
        prosecdef: false,
        proleakproof: false,
        proisstrict: false,
        proretset: false,
        provolatile: "v",
        proparallel: "u",
        pronargs: nargs,
        pronargdefaults: 0,
        prorettype: rettype,
        proargtypes: argtypes,
        proallargtypes: null,
        proargmodes: null,
        proargnames: null,
        proargdefaults: null,
        protrftypes: null,
        prosrc: fname,
        probin: null,
        prosqlbody: null,
        proconfig: null,
        proacl: null,
      });
    }
    return [columns, rows];
  }

  // ==================================================================
  // pg_extension
  // ==================================================================

  private static _buildPgExtension(
    _engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "oid",
      "extname",
      "extowner",
      "extnamespace",
      "extrelocatable",
      "extversion",
      "extconfig",
      "extcondition",
    ];
    const rows: Row[] = [
      {
        oid: 13181,
        extname: "plpgsql",
        extowner: ROLE_OID,
        extnamespace: NS_PG_CATALOG,
        extrelocatable: false,
        extversion: "1.0",
        extconfig: null,
        extcondition: null,
      },
    ];
    return [columns, rows];
  }

  // ==================================================================
  // pg_collation
  // ==================================================================

  private static _buildPgCollation(
    _engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "oid",
      "collname",
      "collnamespace",
      "collowner",
      "collprovider",
      "collisdeterministic",
      "collencoding",
      "collcollate",
      "collctype",
      "colliculocale",
      "collicurules",
      "collversion",
    ];
    const rows: Row[] = [
      {
        oid: 100,
        collname: "default",
        collnamespace: NS_PG_CATALOG,
        collowner: ROLE_OID,
        collprovider: "d",
        collisdeterministic: true,
        collencoding: -1,
        collcollate: "",
        collctype: "",
        colliculocale: null,
        collicurules: null,
        collversion: null,
      },
      {
        oid: 950,
        collname: "C",
        collnamespace: NS_PG_CATALOG,
        collowner: ROLE_OID,
        collprovider: "c",
        collisdeterministic: true,
        collencoding: -1,
        collcollate: "C",
        collctype: "C",
        colliculocale: null,
        collicurules: null,
        collversion: null,
      },
      {
        oid: 951,
        collname: "POSIX",
        collnamespace: NS_PG_CATALOG,
        collowner: ROLE_OID,
        collprovider: "c",
        collisdeterministic: true,
        collencoding: -1,
        collcollate: "POSIX",
        collctype: "POSIX",
        colliculocale: null,
        collicurules: null,
        collversion: null,
      },
    ];
    return [columns, rows];
  }

  // ==================================================================
  // pg_enum (empty -- UQA has no enum types)
  // ==================================================================

  private static _buildPgEnum(
    _engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = ["oid", "enumtypid", "enumsortorder", "enumlabel"];
    return [columns, []];
  }

  // ==================================================================
  // pg_inherits (empty -- no inheritance)
  // ==================================================================

  private static _buildPgInherits(
    _engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = ["inhrelid", "inhparent", "inhseqno", "inhdetachpending"];
    return [columns, []];
  }

  // ==================================================================
  // pg_trigger (empty -- no triggers)
  // ==================================================================

  private static _buildPgTrigger(
    _engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "oid",
      "tgrelid",
      "tgparentid",
      "tgname",
      "tgfoid",
      "tgtype",
      "tgenabled",
      "tgisinternal",
      "tgconstrrelid",
      "tgconstrindid",
      "tgconstraint",
      "tgdeferrable",
      "tginitdeferred",
      "tgnargs",
      "tgattr",
      "tgargs",
      "tgqual",
      "tgoldtable",
      "tgnewtable",
    ];
    return [columns, []];
  }

  // ==================================================================
  // pg_statio_user_tables
  // ==================================================================

  private static _buildPgStatioUserTables(
    engine: EngineInternals,
    oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "relid",
      "schemaname",
      "relname",
      "heap_blks_read",
      "heap_blks_hit",
      "idx_blks_read",
      "idx_blks_hit",
      "toast_blks_read",
      "toast_blks_hit",
      "tidx_blks_read",
      "tidx_blks_hit",
    ];
    const rows: Row[] = [];
    for (const tname of [...engine._tables.keys()].sort()) {
      const tableOid = oids.get("table", tname) ?? 0;
      rows.push({
        relid: tableOid,
        schemaname: SCHEMA,
        relname: tname,
        heap_blks_read: 0,
        heap_blks_hit: 0,
        idx_blks_read: 0,
        idx_blks_hit: 0,
        toast_blks_read: 0,
        toast_blks_hit: 0,
        tidx_blks_read: 0,
        tidx_blks_hit: 0,
      });
    }
    return [columns, rows];
  }

  // ==================================================================
  // pg_auth_members (empty -- single user)
  // ==================================================================

  private static _buildPgAuthMembers(
    _engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "oid",
      "roleid",
      "member",
      "grantor",
      "admin_option",
      "inherit_option",
      "set_option",
    ];
    return [columns, []];
  }

  // ==================================================================
  // pg_available_extensions
  // ==================================================================

  private static _buildPgAvailableExtensions(
    _engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = ["name", "default_version", "installed_version", "comment"];
    const rows: Row[] = [
      {
        name: "plpgsql",
        default_version: "1.0",
        installed_version: "1.0",
        comment: "PL/pgSQL procedural language",
      },
    ];
    return [columns, rows];
  }
}
