//
// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
//
// Copyright (c) 2023-2026 Cognica, Inc.
//

// PostgreSQL 17 information_schema view provider.
//
// Generates virtual rows for all standard information_schema views by
// inspecting UQA Engine state.  Each builder returns a
// [columns, rows] tuple suitable for conversion into virtual rows.
//
// Reference: PostgreSQL 17 documentation, Chapter 37 -- The Information Schema
// https://www.postgresql.org/docs/17/information-schema.html

import type { Engine } from "@jaepil/uqa";
import type { OIDAllocator } from "./oid.js";
import {
  canonicalTypeName,
  characterMaximumLength,
  characterOctetLength,
  numericPrecision,
  numericPrecisionRadix,
  numericScale,
} from "./oid.js";

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
  fdwType: string;
  options: Record<string, string>;
}

interface ForeignTableInternals {
  serverName: string;
  columns: Map<string, ColumnDefInternals>;
  options: Record<string, string>;
}

// Database name used as table_catalog
const CATALOG = "uqa";
const SCHEMA = "public";
const OWNER = "uqa";

// UDT name mapping
function udtName(typeName: string): string {
  const mapping: Record<string, string> = {
    integer: "int4",
    int: "int4",
    int4: "int4",
    bigint: "int8",
    int8: "int8",
    smallint: "int2",
    int2: "int2",
    serial: "int4",
    bigserial: "int8",
    text: "text",
    varchar: "varchar",
    "character varying": "varchar",
    character: "bpchar",
    char: "bpchar",
    name: "name",
    boolean: "bool",
    bool: "bool",
    real: "float4",
    float: "float4",
    float4: "float4",
    "double precision": "float8",
    float8: "float8",
    numeric: "numeric",
    decimal: "numeric",
    date: "date",
    timestamp: "timestamp",
    "timestamp without time zone": "timestamp",
    timestamptz: "timestamptz",
    "timestamp with time zone": "timestamptz",
    json: "json",
    jsonb: "jsonb",
    uuid: "uuid",
    bytea: "bytea",
    point: "point",
    vector: "vector",
  };
  if (typeName.endsWith("[]")) {
    const base = typeName.slice(0, -2);
    const baseUdt = mapping[base] ?? base;
    return `_${baseUdt}`;
  }
  return mapping[typeName] ?? typeName;
}

function formatDefault(value: unknown): string {
  if (typeof value === "string") {
    const escaped = value.replace(/'/g, "''");
    return `'${escaped}'::text`;
  }
  if (typeof value === "boolean") {
    return value ? "true" : "false";
  }
  return String(value);
}

// The JS UQA engine maps NUMERIC/DECIMAL to typeName="float" internally.
// When numericScale is set, the original SQL type was NUMERIC(p,s).
// This helper resolves the effective type name for catalog display.
function effectiveTypeName(cdef: ColumnDefInternals): string {
  if (cdef.typeName === "float" && cdef.numericScale !== null) {
    return "numeric";
  }
  return cdef.typeName;
}

type ViewBuilder = (
  engine: EngineInternals,
  oids: OIDAllocator,
) => BuildResult;

export class InformationSchemaProvider {
  private static readonly _VIEWS: Readonly<Record<string, ViewBuilder>> = {
    schemata: InformationSchemaProvider._buildSchemata,
    tables: InformationSchemaProvider._buildTables,
    columns: InformationSchemaProvider._buildColumns,
    table_constraints: InformationSchemaProvider._buildTableConstraints,
    key_column_usage: InformationSchemaProvider._buildKeyColumnUsage,
    referential_constraints:
      InformationSchemaProvider._buildReferentialConstraints,
    constraint_column_usage:
      InformationSchemaProvider._buildConstraintColumnUsage,
    check_constraints: InformationSchemaProvider._buildCheckConstraints,
    views: InformationSchemaProvider._buildViews,
    sequences: InformationSchemaProvider._buildSequences,
    routines: InformationSchemaProvider._buildRoutines,
    parameters: InformationSchemaProvider._buildParameters,
    foreign_tables: InformationSchemaProvider._buildForeignTables,
    foreign_servers: InformationSchemaProvider._buildForeignServers,
    foreign_server_options:
      InformationSchemaProvider._buildForeignServerOptions,
    foreign_table_options: InformationSchemaProvider._buildForeignTableOptions,
    enabled_roles: InformationSchemaProvider._buildEnabledRoles,
    applicable_roles: InformationSchemaProvider._buildApplicableRoles,
    character_sets: InformationSchemaProvider._buildCharacterSets,
    collations: InformationSchemaProvider._buildCollations,
    domains: InformationSchemaProvider._buildDomains,
    element_types: InformationSchemaProvider._buildElementTypes,
    triggers: InformationSchemaProvider._buildTriggers,
  };

  static supportedViews(): string[] {
    return Object.keys(InformationSchemaProvider._VIEWS);
  }

  static build(
    viewName: string,
    engine: Engine,
    oids: OIDAllocator,
  ): BuildResult {
    const builder = InformationSchemaProvider._VIEWS[viewName];
    if (builder === undefined) {
      throw new Error(`Unknown information_schema view: '${viewName}'`);
    }
    return builder(engine as unknown as EngineInternals, oids);
  }

  // ==================================================================
  // information_schema.schemata
  // ==================================================================

  private static _buildSchemata(
    _engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "catalog_name",
      "schema_name",
      "schema_owner",
      "default_character_set_catalog",
      "default_character_set_schema",
      "default_character_set_name",
      "sql_path",
    ];
    const rows: Row[] = [
      {
        catalog_name: CATALOG,
        schema_name: "public",
        schema_owner: OWNER,
        default_character_set_catalog: null,
        default_character_set_schema: null,
        default_character_set_name: null,
        sql_path: null,
      },
      {
        catalog_name: CATALOG,
        schema_name: "information_schema",
        schema_owner: OWNER,
        default_character_set_catalog: null,
        default_character_set_schema: null,
        default_character_set_name: null,
        sql_path: null,
      },
      {
        catalog_name: CATALOG,
        schema_name: "pg_catalog",
        schema_owner: OWNER,
        default_character_set_catalog: null,
        default_character_set_schema: null,
        default_character_set_name: null,
        sql_path: null,
      },
    ];
    return [columns, rows];
  }

  // ==================================================================
  // information_schema.tables
  // ==================================================================

  private static _buildTables(
    engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "table_catalog",
      "table_schema",
      "table_name",
      "table_type",
      "self_referencing_column_name",
      "reference_generation",
      "user_defined_type_catalog",
      "user_defined_type_schema",
      "user_defined_type_name",
      "is_insertable_into",
      "is_typed",
      "commit_action",
    ];
    const rows: Row[] = [];

    for (const tname of [...engine._tables.keys()].sort()) {
      const isTemp = engine._tempTables.has(tname);
      rows.push({
        table_catalog: CATALOG,
        table_schema: SCHEMA,
        table_name: tname,
        table_type: isTemp ? "LOCAL TEMPORARY" : "BASE TABLE",
        self_referencing_column_name: null,
        reference_generation: null,
        user_defined_type_catalog: null,
        user_defined_type_schema: null,
        user_defined_type_name: null,
        is_insertable_into: "YES",
        is_typed: "NO",
        commit_action: null,
      });
    }

    for (const vname of [...engine._views.keys()].sort()) {
      rows.push({
        table_catalog: CATALOG,
        table_schema: SCHEMA,
        table_name: vname,
        table_type: "VIEW",
        self_referencing_column_name: null,
        reference_generation: null,
        user_defined_type_catalog: null,
        user_defined_type_schema: null,
        user_defined_type_name: null,
        is_insertable_into: "NO",
        is_typed: "NO",
        commit_action: null,
      });
    }

    for (const ftname of [...engine._foreignTables.keys()].sort()) {
      rows.push({
        table_catalog: CATALOG,
        table_schema: SCHEMA,
        table_name: ftname,
        table_type: "FOREIGN",
        self_referencing_column_name: null,
        reference_generation: null,
        user_defined_type_catalog: null,
        user_defined_type_schema: null,
        user_defined_type_name: null,
        is_insertable_into: "NO",
        is_typed: "NO",
        commit_action: null,
      });
    }

    return [columns, rows];
  }

  // ==================================================================
  // information_schema.columns
  // ==================================================================

  private static _buildColumns(
    engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "table_catalog",
      "table_schema",
      "table_name",
      "column_name",
      "ordinal_position",
      "column_default",
      "is_nullable",
      "data_type",
      "character_maximum_length",
      "character_octet_length",
      "numeric_precision",
      "numeric_precision_radix",
      "numeric_scale",
      "datetime_precision",
      "interval_type",
      "interval_precision",
      "character_set_catalog",
      "character_set_schema",
      "character_set_name",
      "collation_catalog",
      "collation_schema",
      "collation_name",
      "domain_catalog",
      "domain_schema",
      "domain_name",
      "udt_catalog",
      "udt_schema",
      "udt_name",
      "scope_catalog",
      "scope_schema",
      "scope_name",
      "maximum_cardinality",
      "dtd_identifier",
      "is_self_referencing",
      "is_identity",
      "identity_generation",
      "identity_start",
      "identity_increment",
      "identity_maximum",
      "identity_minimum",
      "identity_cycle",
      "is_generated",
      "generation_expression",
      "is_updatable",
    ];
    const rows: Row[] = [];

    for (const tname of [...engine._tables.keys()].sort()) {
      const table = engine._tables.get(tname)!;
      let pos = 0;
      for (const [cname, cdef] of table.columns) {
        pos++;
        const effType = effectiveTypeName(cdef);
        const display = canonicalTypeName(effType);
        const udt = udtName(effType);

        let defaultStr: string | null = null;
        if (cdef.autoIncrement) {
          defaultStr = `nextval('${tname}_${cname}_seq'::regclass)`;
        } else if (cdef.defaultValue !== null && cdef.defaultValue !== undefined) {
          defaultStr = formatDefault(cdef.defaultValue);
        }

        const isIdentity = cdef.autoIncrement ? "YES" : "NO";
        const identityGen = cdef.autoIncrement ? "BY DEFAULT" : null;

        let dtPrecision: number | null = null;
        if (
          [
            "timestamp",
            "timestamptz",
            "timestamp without time zone",
            "timestamp with time zone",
            "date",
            "time",
          ].includes(effType)
        ) {
          dtPrecision = 6; // microsecond precision
        }

        rows.push({
          table_catalog: CATALOG,
          table_schema: SCHEMA,
          table_name: tname,
          column_name: cname,
          ordinal_position: pos,
          column_default: defaultStr,
          is_nullable: cdef.notNull ? "NO" : "YES",
          data_type: display,
          character_maximum_length: characterMaximumLength(effType),
          character_octet_length: characterOctetLength(effType),
          numeric_precision:
            cdef.numericPrecision !== null
              ? cdef.numericPrecision
              : numericPrecision(effType),
          numeric_precision_radix: numericPrecisionRadix(effType),
          numeric_scale:
            cdef.numericScale !== null
              ? cdef.numericScale
              : numericScale(effType),
          datetime_precision: dtPrecision,
          interval_type: null,
          interval_precision: null,
          character_set_catalog: null,
          character_set_schema: null,
          character_set_name: null,
          collation_catalog: null,
          collation_schema: null,
          collation_name: null,
          domain_catalog: null,
          domain_schema: null,
          domain_name: null,
          udt_catalog: CATALOG,
          udt_schema: "pg_catalog",
          udt_name: udt,
          scope_catalog: null,
          scope_schema: null,
          scope_name: null,
          maximum_cardinality: null,
          dtd_identifier: String(pos),
          is_self_referencing: "NO",
          is_identity: isIdentity,
          identity_generation: identityGen,
          identity_start: cdef.autoIncrement ? "1" : null,
          identity_increment: cdef.autoIncrement ? "1" : null,
          identity_maximum: null,
          identity_minimum: null,
          identity_cycle: cdef.autoIncrement ? "NO" : null,
          is_generated: "NEVER",
          generation_expression: null,
          is_updatable: "YES",
        });
      }
    }

    // Foreign table columns
    for (const ftname of [...engine._foreignTables.keys()].sort()) {
      const ft = engine._foreignTables.get(ftname)!;
      let pos = 0;
      for (const [cname, cdef] of ft.columns) {
        pos++;
        const effType = effectiveTypeName(cdef);
        const display = canonicalTypeName(effType);
        const udt = udtName(effType);
        rows.push({
          table_catalog: CATALOG,
          table_schema: SCHEMA,
          table_name: ftname,
          column_name: cname,
          ordinal_position: pos,
          column_default: null,
          is_nullable: "YES",
          data_type: display,
          character_maximum_length: null,
          character_octet_length: null,
          numeric_precision: numericPrecision(effType),
          numeric_precision_radix: numericPrecisionRadix(effType),
          numeric_scale: numericScale(effType),
          datetime_precision: null,
          interval_type: null,
          interval_precision: null,
          character_set_catalog: null,
          character_set_schema: null,
          character_set_name: null,
          collation_catalog: null,
          collation_schema: null,
          collation_name: null,
          domain_catalog: null,
          domain_schema: null,
          domain_name: null,
          udt_catalog: CATALOG,
          udt_schema: "pg_catalog",
          udt_name: udt,
          scope_catalog: null,
          scope_schema: null,
          scope_name: null,
          maximum_cardinality: null,
          dtd_identifier: String(pos),
          is_self_referencing: "NO",
          is_identity: "NO",
          identity_generation: null,
          identity_start: null,
          identity_increment: null,
          identity_maximum: null,
          identity_minimum: null,
          identity_cycle: null,
          is_generated: "NEVER",
          generation_expression: null,
          is_updatable: "NO",
        });
      }
    }

    return [columns, rows];
  }

  // ==================================================================
  // information_schema.table_constraints
  // ==================================================================

  private static _buildTableConstraints(
    engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "constraint_catalog",
      "constraint_schema",
      "constraint_name",
      "table_catalog",
      "table_schema",
      "table_name",
      "constraint_type",
      "is_deferrable",
      "initially_deferred",
      "enforced",
      "nulls_distinct",
    ];
    const rows: Row[] = [];

    for (const tname of [...engine._tables.keys()].sort()) {
      const table = engine._tables.get(tname)!;

      // PRIMARY KEY
      if (table.primaryKey) {
        rows.push({
          constraint_catalog: CATALOG,
          constraint_schema: SCHEMA,
          constraint_name: `${tname}_pkey`,
          table_catalog: CATALOG,
          table_schema: SCHEMA,
          table_name: tname,
          constraint_type: "PRIMARY KEY",
          is_deferrable: "NO",
          initially_deferred: "NO",
          enforced: "YES",
          nulls_distinct: null,
        });
      }

      // UNIQUE constraints
      for (const [cname, cdef] of table.columns) {
        if (cdef.unique && !cdef.primaryKey) {
          rows.push({
            constraint_catalog: CATALOG,
            constraint_schema: SCHEMA,
            constraint_name: `${tname}_${cname}_key`,
            table_catalog: CATALOG,
            table_schema: SCHEMA,
            table_name: tname,
            constraint_type: "UNIQUE",
            is_deferrable: "NO",
            initially_deferred: "NO",
            enforced: "YES",
            nulls_distinct: "YES",
          });
        }
      }

      // FOREIGN KEY constraints
      for (const fk of table.foreignKeys) {
        rows.push({
          constraint_catalog: CATALOG,
          constraint_schema: SCHEMA,
          constraint_name: `${tname}_${fk.column}_fkey`,
          table_catalog: CATALOG,
          table_schema: SCHEMA,
          table_name: tname,
          constraint_type: "FOREIGN KEY",
          is_deferrable: "NO",
          initially_deferred: "NO",
          enforced: "YES",
          nulls_distinct: null,
        });
      }

      // CHECK constraints
      for (const [checkName] of table.checkConstraints) {
        rows.push({
          constraint_catalog: CATALOG,
          constraint_schema: SCHEMA,
          constraint_name: `${tname}_${checkName}_check`,
          table_catalog: CATALOG,
          table_schema: SCHEMA,
          table_name: tname,
          constraint_type: "CHECK",
          is_deferrable: "NO",
          initially_deferred: "NO",
          enforced: "YES",
          nulls_distinct: null,
        });
      }
    }

    return [columns, rows];
  }

  // ==================================================================
  // information_schema.key_column_usage
  // ==================================================================

  private static _buildKeyColumnUsage(
    engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "constraint_catalog",
      "constraint_schema",
      "constraint_name",
      "table_catalog",
      "table_schema",
      "table_name",
      "column_name",
      "ordinal_position",
      "position_in_unique_constraint",
    ];
    const rows: Row[] = [];

    for (const tname of [...engine._tables.keys()].sort()) {
      const table = engine._tables.get(tname)!;

      if (table.primaryKey) {
        rows.push({
          constraint_catalog: CATALOG,
          constraint_schema: SCHEMA,
          constraint_name: `${tname}_pkey`,
          table_catalog: CATALOG,
          table_schema: SCHEMA,
          table_name: tname,
          column_name: table.primaryKey,
          ordinal_position: 1,
          position_in_unique_constraint: null,
        });
      }

      for (const [cname, cdef] of table.columns) {
        if (cdef.unique && !cdef.primaryKey) {
          rows.push({
            constraint_catalog: CATALOG,
            constraint_schema: SCHEMA,
            constraint_name: `${tname}_${cname}_key`,
            table_catalog: CATALOG,
            table_schema: SCHEMA,
            table_name: tname,
            column_name: cname,
            ordinal_position: 1,
            position_in_unique_constraint: null,
          });
        }
      }

      for (const fk of table.foreignKeys) {
        const refTable = engine._tables.get(fk.refTable);
        let refPos: number | null = null;
        if (refTable !== undefined) {
          const refCols = [...refTable.columns.keys()];
          if (refCols.includes(fk.refColumn)) {
            refPos = 1; // single-column PK/UNIQUE
          }
        }

        rows.push({
          constraint_catalog: CATALOG,
          constraint_schema: SCHEMA,
          constraint_name: `${tname}_${fk.column}_fkey`,
          table_catalog: CATALOG,
          table_schema: SCHEMA,
          table_name: tname,
          column_name: fk.column,
          ordinal_position: 1,
          position_in_unique_constraint: refPos,
        });
      }
    }

    return [columns, rows];
  }

  // ==================================================================
  // information_schema.referential_constraints
  // ==================================================================

  private static _buildReferentialConstraints(
    engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "constraint_catalog",
      "constraint_schema",
      "constraint_name",
      "unique_constraint_catalog",
      "unique_constraint_schema",
      "unique_constraint_name",
      "match_option",
      "update_rule",
      "delete_rule",
    ];
    const rows: Row[] = [];

    for (const tname of [...engine._tables.keys()].sort()) {
      const table = engine._tables.get(tname)!;
      for (const fk of table.foreignKeys) {
        const refTable = engine._tables.get(fk.refTable);
        let refConstraint: string | null = null;
        if (refTable !== undefined) {
          if (refTable.primaryKey === fk.refColumn) {
            refConstraint = `${fk.refTable}_pkey`;
          } else {
            refConstraint = `${fk.refTable}_${fk.refColumn}_key`;
          }
        }

        rows.push({
          constraint_catalog: CATALOG,
          constraint_schema: SCHEMA,
          constraint_name: `${tname}_${fk.column}_fkey`,
          unique_constraint_catalog: CATALOG,
          unique_constraint_schema: SCHEMA,
          unique_constraint_name: refConstraint,
          match_option: "NONE",
          update_rule: "NO ACTION",
          delete_rule: "NO ACTION",
        });
      }
    }

    return [columns, rows];
  }

  // ==================================================================
  // information_schema.constraint_column_usage
  // ==================================================================

  private static _buildConstraintColumnUsage(
    engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "table_catalog",
      "table_schema",
      "table_name",
      "column_name",
      "constraint_catalog",
      "constraint_schema",
      "constraint_name",
    ];
    const rows: Row[] = [];

    for (const tname of [...engine._tables.keys()].sort()) {
      const table = engine._tables.get(tname)!;

      if (table.primaryKey) {
        rows.push({
          table_catalog: CATALOG,
          table_schema: SCHEMA,
          table_name: tname,
          column_name: table.primaryKey,
          constraint_catalog: CATALOG,
          constraint_schema: SCHEMA,
          constraint_name: `${tname}_pkey`,
        });
      }

      for (const [cname, cdef] of table.columns) {
        if (cdef.unique && !cdef.primaryKey) {
          rows.push({
            table_catalog: CATALOG,
            table_schema: SCHEMA,
            table_name: tname,
            column_name: cname,
            constraint_catalog: CATALOG,
            constraint_schema: SCHEMA,
            constraint_name: `${tname}_${cname}_key`,
          });
        }
      }

      // FK: the referenced columns
      for (const fk of table.foreignKeys) {
        rows.push({
          table_catalog: CATALOG,
          table_schema: SCHEMA,
          table_name: fk.refTable,
          column_name: fk.refColumn,
          constraint_catalog: CATALOG,
          constraint_schema: SCHEMA,
          constraint_name: `${tname}_${fk.column}_fkey`,
        });
      }
    }

    return [columns, rows];
  }

  // ==================================================================
  // information_schema.check_constraints
  // ==================================================================

  private static _buildCheckConstraints(
    engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "constraint_catalog",
      "constraint_schema",
      "constraint_name",
      "check_clause",
    ];
    const rows: Row[] = [];

    for (const tname of [...engine._tables.keys()].sort()) {
      const table = engine._tables.get(tname)!;

      // NOT NULL constraints (PostgreSQL exposes these as CHECK)
      for (const [cname, cdef] of table.columns) {
        if (cdef.notNull && !cdef.primaryKey) {
          rows.push({
            constraint_catalog: CATALOG,
            constraint_schema: SCHEMA,
            constraint_name: `${tname}_${cname}_not_null`,
            check_clause: `${cname} IS NOT NULL`,
          });
        }
      }

      // Explicit CHECK constraints
      for (const [checkName] of table.checkConstraints) {
        rows.push({
          constraint_catalog: CATALOG,
          constraint_schema: SCHEMA,
          constraint_name: `${tname}_${checkName}_check`,
          check_clause: checkName,
        });
      }
    }

    return [columns, rows];
  }

  // ==================================================================
  // information_schema.views
  // ==================================================================

  private static _buildViews(
    engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "table_catalog",
      "table_schema",
      "table_name",
      "view_definition",
      "check_option",
      "is_updatable",
      "is_insertable_into",
      "is_trigger_updatable",
      "is_trigger_deletable",
      "is_trigger_insertable_into",
    ];
    const rows: Row[] = [];

    for (const vname of [...engine._views.keys()].sort()) {
      rows.push({
        table_catalog: CATALOG,
        table_schema: SCHEMA,
        table_name: vname,
        view_definition: "",
        check_option: "NONE",
        is_updatable: "NO",
        is_insertable_into: "NO",
        is_trigger_updatable: "NO",
        is_trigger_deletable: "NO",
        is_trigger_insertable_into: "NO",
      });
    }

    return [columns, rows];
  }

  // ==================================================================
  // information_schema.sequences
  // ==================================================================

  private static _buildSequences(
    engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "sequence_catalog",
      "sequence_schema",
      "sequence_name",
      "data_type",
      "numeric_precision",
      "numeric_precision_radix",
      "numeric_scale",
      "start_value",
      "minimum_value",
      "maximum_value",
      "increment",
      "cycle_option",
    ];
    const rows: Row[] = [];

    for (const sname of [...engine._sequences.keys()].sort()) {
      const seq = engine._sequences.get(sname)!;
      rows.push({
        sequence_catalog: CATALOG,
        sequence_schema: SCHEMA,
        sequence_name: sname,
        data_type: "bigint",
        numeric_precision: 64,
        numeric_precision_radix: 2,
        numeric_scale: 0,
        start_value: String(seq["start"] ?? 1),
        minimum_value: "1",
        maximum_value: "9223372036854775807",
        increment: String(seq["increment"] ?? 1),
        cycle_option: "NO",
      });
    }

    return [columns, rows];
  }

  // ==================================================================
  // information_schema.routines (empty)
  // ==================================================================

  private static _buildRoutines(
    _engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "specific_catalog",
      "specific_schema",
      "specific_name",
      "routine_catalog",
      "routine_schema",
      "routine_name",
      "routine_type",
      "data_type",
      "type_udt_catalog",
      "type_udt_schema",
      "type_udt_name",
      "routine_definition",
      "external_language",
      "is_deterministic",
      "security_type",
    ];
    return [columns, []];
  }

  // ==================================================================
  // information_schema.parameters (empty)
  // ==================================================================

  private static _buildParameters(
    _engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "specific_catalog",
      "specific_schema",
      "specific_name",
      "ordinal_position",
      "parameter_mode",
      "is_result",
      "as_locator",
      "parameter_name",
      "data_type",
      "parameter_default",
    ];
    return [columns, []];
  }

  // ==================================================================
  // information_schema.foreign_tables
  // ==================================================================

  private static _buildForeignTables(
    engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "foreign_table_catalog",
      "foreign_table_schema",
      "foreign_table_name",
      "foreign_server_catalog",
      "foreign_server_name",
    ];
    const rows: Row[] = [];

    for (const ftname of [...engine._foreignTables.keys()].sort()) {
      const ft = engine._foreignTables.get(ftname)!;
      rows.push({
        foreign_table_catalog: CATALOG,
        foreign_table_schema: SCHEMA,
        foreign_table_name: ftname,
        foreign_server_catalog: CATALOG,
        foreign_server_name: ft.serverName,
      });
    }

    return [columns, rows];
  }

  // ==================================================================
  // information_schema.foreign_servers
  // ==================================================================

  private static _buildForeignServers(
    engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "foreign_server_catalog",
      "foreign_server_name",
      "foreign_data_wrapper_catalog",
      "foreign_data_wrapper_name",
      "foreign_server_type",
      "foreign_server_version",
      "authorization_identifier",
    ];
    const rows: Row[] = [];

    for (const sname of [...engine._foreignServers.keys()].sort()) {
      const srv = engine._foreignServers.get(sname)!;
      rows.push({
        foreign_server_catalog: CATALOG,
        foreign_server_name: sname,
        foreign_data_wrapper_catalog: CATALOG,
        foreign_data_wrapper_name: srv.fdwType,
        foreign_server_type: null,
        foreign_server_version: null,
        authorization_identifier: OWNER,
      });
    }

    return [columns, rows];
  }

  // ==================================================================
  // information_schema.foreign_server_options
  // ==================================================================

  private static _buildForeignServerOptions(
    engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "foreign_server_catalog",
      "foreign_server_name",
      "option_name",
      "option_value",
    ];
    const rows: Row[] = [];

    for (const sname of [...engine._foreignServers.keys()].sort()) {
      const srv = engine._foreignServers.get(sname)!;
      for (const optName of Object.keys(srv.options).sort()) {
        rows.push({
          foreign_server_catalog: CATALOG,
          foreign_server_name: sname,
          option_name: optName,
          option_value: srv.options[optName],
        });
      }
    }

    return [columns, rows];
  }

  // ==================================================================
  // information_schema.foreign_table_options
  // ==================================================================

  private static _buildForeignTableOptions(
    engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "foreign_table_catalog",
      "foreign_table_schema",
      "foreign_table_name",
      "option_name",
      "option_value",
    ];
    const rows: Row[] = [];

    for (const ftname of [...engine._foreignTables.keys()].sort()) {
      const ft = engine._foreignTables.get(ftname)!;
      for (const optName of Object.keys(ft.options).sort()) {
        rows.push({
          foreign_table_catalog: CATALOG,
          foreign_table_schema: SCHEMA,
          foreign_table_name: ftname,
          option_name: optName,
          option_value: ft.options[optName],
        });
      }
    }

    return [columns, rows];
  }

  // ==================================================================
  // information_schema.enabled_roles
  // ==================================================================

  private static _buildEnabledRoles(
    _engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    return [["role_name"], [{ role_name: OWNER }]];
  }

  // ==================================================================
  // information_schema.applicable_roles
  // ==================================================================

  private static _buildApplicableRoles(
    _engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    return [
      ["grantee", "role_name", "is_grantable"],
      [{ grantee: OWNER, role_name: OWNER, is_grantable: "YES" }],
    ];
  }

  // ==================================================================
  // information_schema.character_sets
  // ==================================================================

  private static _buildCharacterSets(
    _engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "character_set_catalog",
      "character_set_schema",
      "character_set_name",
      "character_repertoire",
      "form_of_use",
      "default_collate_catalog",
      "default_collate_schema",
      "default_collate_name",
    ];
    return [
      columns,
      [
        {
          character_set_catalog: null,
          character_set_schema: null,
          character_set_name: "UTF8",
          character_repertoire: "UCS",
          form_of_use: "UTF8",
          default_collate_catalog: CATALOG,
          default_collate_schema: "pg_catalog",
          default_collate_name: "en_US.utf8",
        },
      ],
    ];
  }

  // ==================================================================
  // information_schema.collations
  // ==================================================================

  private static _buildCollations(
    _engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "collation_catalog",
      "collation_schema",
      "collation_name",
      "pad_attribute",
    ];
    return [
      columns,
      [
        {
          collation_catalog: CATALOG,
          collation_schema: "pg_catalog",
          collation_name: "en_US.utf8",
          pad_attribute: "NO PAD",
        },
      ],
    ];
  }

  // ==================================================================
  // information_schema.domains (empty)
  // ==================================================================

  private static _buildDomains(
    _engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "domain_catalog",
      "domain_schema",
      "domain_name",
      "data_type",
      "character_maximum_length",
      "numeric_precision",
      "domain_default",
    ];
    return [columns, []];
  }

  // ==================================================================
  // information_schema.element_types
  // ==================================================================

  private static _buildElementTypes(
    engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "object_catalog",
      "object_schema",
      "object_name",
      "object_type",
      "collection_type_identifier",
      "data_type",
      "character_maximum_length",
      "numeric_precision",
      "numeric_precision_radix",
      "numeric_scale",
      "dtd_identifier",
    ];
    const rows: Row[] = [];

    for (const tname of [...engine._tables.keys()].sort()) {
      const table = engine._tables.get(tname)!;
      let pos = 0;
      for (const [, cdef] of table.columns) {
        pos++;
        if (cdef.typeName.endsWith("[]")) {
          const base = cdef.typeName.slice(0, -2);
          const display = canonicalTypeName(base);
          rows.push({
            object_catalog: CATALOG,
            object_schema: SCHEMA,
            object_name: tname,
            object_type: "TABLE",
            collection_type_identifier: String(pos),
            data_type: display,
            character_maximum_length: null,
            numeric_precision: numericPrecision(base),
            numeric_precision_radix: numericPrecisionRadix(base),
            numeric_scale: numericScale(base),
            dtd_identifier: String(pos),
          });
        }
      }
    }

    return [columns, rows];
  }

  // ==================================================================
  // information_schema.triggers (empty)
  // ==================================================================

  private static _buildTriggers(
    _engine: EngineInternals,
    _oids: OIDAllocator,
  ): BuildResult {
    const columns = [
      "trigger_catalog",
      "trigger_schema",
      "trigger_name",
      "event_manipulation",
      "event_object_catalog",
      "event_object_schema",
      "event_object_table",
      "action_order",
      "action_condition",
      "action_statement",
      "action_orientation",
      "action_timing",
    ];
    return [columns, []];
  }
}
