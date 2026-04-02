//
// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
//
// Copyright (c) 2023-2026 Cognica, Inc.
//

// Tests for PostgreSQL 17-compatible information_schema and pg_catalog.

import { describe, it, expect, beforeEach } from "vitest";
import { USQLEngine } from "../src/core/engine.js";

let engine: USQLEngine;

async function createTestEngine(): Promise<USQLEngine> {
  const e = new USQLEngine();
  await e.sql(
    "CREATE TABLE departments (  id SERIAL PRIMARY KEY,  name TEXT NOT NULL UNIQUE)",
  );
  await e.sql(
    "CREATE TABLE employees (" +
      "  id SERIAL PRIMARY KEY," +
      "  dept_id INTEGER REFERENCES departments(id)," +
      "  name TEXT NOT NULL," +
      "  email TEXT UNIQUE," +
      "  salary NUMERIC(10,2)," +
      "  is_active BOOLEAN DEFAULT true" +
      ")",
  );
  await e.sql("CREATE VIEW dept_summary AS SELECT name FROM departments");
  await e.sql("CREATE SEQUENCE invoice_seq START 1000 INCREMENT 5");
  await e.sql("INSERT INTO departments (name) VALUES ('Engineering')");
  await e.sql("INSERT INTO departments (name) VALUES ('Sales')");
  await e.sql(
    "INSERT INTO employees (dept_id, name, email, salary, is_active) " +
      "VALUES (1, 'Alice', 'alice@example.com', 150000.50, true)",
  );
  return e;
}

beforeEach(async () => {
  engine = await createTestEngine();
});

// ======================================================================
// information_schema tests
// ======================================================================

describe("TestInformationSchemaSchemata", () => {
  it("test_lists_all_schemas", async () => {
    const r = await engine.sql("SELECT schema_name FROM information_schema.schemata");
    const names = new Set(r!.rows.map((row) => row["schema_name"]));
    expect(names).toEqual(new Set(["public", "information_schema", "pg_catalog"]));
  });

  it("test_catalog_name", async () => {
    const r = await engine.sql(
      "SELECT catalog_name FROM information_schema.schemata " +
        "WHERE schema_name = 'public'",
    );
    expect(r!.rows[0]!["catalog_name"]).toBe("uqa");
  });
});

describe("TestInformationSchemaTables", () => {
  it("test_base_tables", async () => {
    const r = await engine.sql(
      "SELECT table_name, table_type " +
        "FROM information_schema.tables " +
        "WHERE table_schema = 'public' AND table_type = 'BASE TABLE' " +
        "ORDER BY table_name",
    );
    const names = r!.rows.map((row) => row["table_name"]);
    expect(names).toContain("departments");
    expect(names).toContain("employees");
  });

  it("test_views", async () => {
    const r = await engine.sql(
      "SELECT table_name, table_type " +
        "FROM information_schema.tables " +
        "WHERE table_type = 'VIEW'",
    );
    expect(r!.rows.length).toBeGreaterThanOrEqual(1);
    const names = new Set(r!.rows.map((row) => row["table_name"]));
    expect(names).toContain("dept_summary");
  });

  it("test_is_insertable", async () => {
    const r = await engine.sql(
      "SELECT table_name, is_insertable_into " +
        "FROM information_schema.tables " +
        "WHERE table_name = 'departments'",
    );
    expect(r!.rows[0]!["is_insertable_into"]).toBe("YES");
  });
});

describe("TestInformationSchemaColumns", () => {
  it("test_column_count", async () => {
    const r = await engine.sql(
      "SELECT column_name FROM information_schema.columns " +
        "WHERE table_name = 'employees'",
    );
    expect(r!.rows.length).toBe(6);
  });

  it("test_ordinal_position", async () => {
    const r = await engine.sql(
      "SELECT column_name, ordinal_position " +
        "FROM information_schema.columns " +
        "WHERE table_name = 'employees' " +
        "ORDER BY ordinal_position",
    );
    const cols = r!.rows.map((row) => row["column_name"]);
    expect(cols).toEqual(["id", "dept_id", "name", "email", "salary", "is_active"]);
  });

  it("test_data_types", async () => {
    const r = await engine.sql(
      "SELECT column_name, data_type " +
        "FROM information_schema.columns " +
        "WHERE table_name = 'employees' " +
        "ORDER BY ordinal_position",
    );
    const types: Record<string, unknown> = {};
    for (const row of r!.rows) {
      types[row["column_name"] as string] = row["data_type"];
    }
    expect(types["id"]).toBe("integer");
    expect(types["name"]).toBe("text");
    // JS UQA engine maps NUMERIC to float internally
    expect(types["salary"]).toBe("real");
    expect(types["is_active"]).toBe("boolean");
  });

  it("test_nullable", async () => {
    const r = await engine.sql(
      "SELECT column_name, is_nullable " +
        "FROM information_schema.columns " +
        "WHERE table_name = 'employees'",
    );
    const nullable: Record<string, unknown> = {};
    for (const row of r!.rows) {
      nullable[row["column_name"] as string] = row["is_nullable"];
    }
    expect(nullable["id"]).toBe("NO"); // PK is NOT NULL
    expect(nullable["name"]).toBe("NO");
    expect(nullable["email"]).toBe("YES");
  });

  it("test_defaults", async () => {
    const r = await engine.sql(
      "SELECT column_name, column_default " +
        "FROM information_schema.columns " +
        "WHERE table_name = 'employees' AND column_default IS NOT NULL",
    );
    const defaults: Record<string, string> = {};
    for (const row of r!.rows) {
      defaults[row["column_name"] as string] = row["column_default"] as string;
    }
    expect(defaults["id"]).toContain("nextval");
    expect(defaults["is_active"]).toBe("true");
  });

  it("test_numeric_precision_scale", async () => {
    const r = await engine.sql(
      "SELECT numeric_precision, numeric_scale " +
        "FROM information_schema.columns " +
        "WHERE table_name = 'employees' AND column_name = 'salary'",
    );
    // JS UQA engine maps NUMERIC to float internally
    expect(r!.rows[0]!["numeric_precision"]).toBe(24);
    expect(r!.rows[0]!["numeric_scale"]).toBe(null);
  });

  it("test_udt_name", async () => {
    const r = await engine.sql(
      "SELECT column_name, udt_name " +
        "FROM information_schema.columns " +
        "WHERE table_name = 'employees' " +
        "ORDER BY ordinal_position",
    );
    const udt: Record<string, unknown> = {};
    for (const row of r!.rows) {
      udt[row["column_name"] as string] = row["udt_name"];
    }
    expect(udt["id"]).toBe("int4");
    expect(udt["name"]).toBe("text");
    // JS UQA engine maps NUMERIC to float internally
    expect(udt["salary"]).toBe("float4");
    expect(udt["is_active"]).toBe("bool");
  });

  it("test_identity", async () => {
    const r = await engine.sql(
      "SELECT column_name, is_identity, identity_generation " +
        "FROM information_schema.columns " +
        "WHERE table_name = 'employees' AND is_identity = 'YES'",
    );
    expect(r!.rows.length).toBe(1);
    expect(r!.rows[0]!["column_name"]).toBe("id");
    expect(r!.rows[0]!["identity_generation"]).toBe("BY DEFAULT");
  });
});

describe("TestInformationSchemaConstraints", () => {
  it("test_primary_keys", async () => {
    const r = await engine.sql(
      "SELECT constraint_name, table_name " +
        "FROM information_schema.table_constraints " +
        "WHERE constraint_type = 'PRIMARY KEY' " +
        "ORDER BY table_name",
    );
    const names: Record<string, unknown> = {};
    for (const row of r!.rows) {
      names[row["table_name"] as string] = row["constraint_name"];
    }
    expect(names["departments"]).toBe("departments_pkey");
    expect(names["employees"]).toBe("employees_pkey");
  });

  it("test_unique_constraints", async () => {
    const r = await engine.sql(
      "SELECT constraint_name, table_name " +
        "FROM information_schema.table_constraints " +
        "WHERE constraint_type = 'UNIQUE'",
    );
    const names = new Set(r!.rows.map((row) => row["constraint_name"]));
    expect(names).toContain("departments_name_key");
    expect(names).toContain("employees_email_key");
  });

  it("test_foreign_keys", async () => {
    const r = await engine.sql(
      "SELECT constraint_name, table_name " +
        "FROM information_schema.table_constraints " +
        "WHERE constraint_type = 'FOREIGN KEY'",
    );
    expect(r!.rows.length).toBe(1);
    expect(r!.rows[0]!["constraint_name"]).toBe("employees_dept_id_fkey");
    expect(r!.rows[0]!["table_name"]).toBe("employees");
  });

  it("test_key_column_usage", async () => {
    const r = await engine.sql(
      "SELECT constraint_name, column_name " +
        "FROM information_schema.key_column_usage " +
        "WHERE table_name = 'employees'",
    );
    const cols: Record<string, unknown> = {};
    for (const row of r!.rows) {
      cols[row["constraint_name"] as string] = row["column_name"];
    }
    expect(cols["employees_pkey"]).toBe("id");
    expect(cols["employees_email_key"]).toBe("email");
    expect(cols["employees_dept_id_fkey"]).toBe("dept_id");
  });

  it("test_referential_constraints", async () => {
    const r = await engine.sql(
      "SELECT constraint_name, unique_constraint_name, " +
        "update_rule, delete_rule " +
        "FROM information_schema.referential_constraints",
    );
    expect(r!.rows.length).toBe(1);
    const row = r!.rows[0]!;
    expect(row["constraint_name"]).toBe("employees_dept_id_fkey");
    expect(row["unique_constraint_name"]).toBe("departments_pkey");
    expect(row["update_rule"]).toBe("NO ACTION");
    expect(row["delete_rule"]).toBe("NO ACTION");
  });

  it("test_constraint_column_usage", async () => {
    const r = await engine.sql(
      "SELECT table_name, column_name, constraint_name " +
        "FROM information_schema.constraint_column_usage " +
        "WHERE constraint_name = 'employees_dept_id_fkey'",
    );
    expect(r!.rows.length).toBe(1);
    expect(r!.rows[0]!["table_name"]).toBe("departments");
    expect(r!.rows[0]!["column_name"]).toBe("id");
  });
});

describe("TestInformationSchemaViews", () => {
  it("test_view_listing", async () => {
    const r = await engine.sql(
      "SELECT table_name, is_updatable FROM information_schema.views",
    );
    expect(r!.rows.length).toBeGreaterThanOrEqual(1);
    const names = new Set(r!.rows.map((row) => row["table_name"]));
    expect(names).toContain("dept_summary");
  });

  it("test_check_option", async () => {
    const r = await engine.sql(
      "SELECT check_option FROM information_schema.views " +
        "WHERE table_name = 'dept_summary'",
    );
    expect(r!.rows[0]!["check_option"]).toBe("NONE");
  });
});

describe("TestInformationSchemaSequences", () => {
  it("test_sequence_listing", async () => {
    const r = await engine.sql(
      "SELECT sequence_name, data_type, start_value, increment " +
        "FROM information_schema.sequences " +
        "WHERE sequence_name = 'invoice_seq'",
    );
    expect(r!.rows.length).toBe(1);
    expect(r!.rows[0]!["sequence_name"]).toBe("invoice_seq");
    expect(r!.rows[0]!["data_type"]).toBe("bigint");
    expect(r!.rows[0]!["start_value"]).toBe("1000");
    expect(r!.rows[0]!["increment"]).toBe("5");
  });
});

describe("TestInformationSchemaOther", () => {
  it("test_enabled_roles", async () => {
    const r = await engine.sql(
      "SELECT role_name FROM information_schema.enabled_roles",
    );
    expect(r!.rows.length).toBe(1);
    expect(r!.rows[0]!["role_name"]).toBe("uqa");
  });

  it("test_character_sets", async () => {
    const r = await engine.sql(
      "SELECT character_set_name FROM information_schema.character_sets",
    );
    expect(r!.rows[0]!["character_set_name"]).toBe("UTF8");
  });

  it("test_empty_domains", async () => {
    const r = await engine.sql("SELECT * FROM information_schema.domains");
    expect(r!.rows.length).toBe(0);
  });

  it("test_empty_triggers", async () => {
    const r = await engine.sql("SELECT * FROM information_schema.triggers");
    expect(r!.rows.length).toBe(0);
  });
});

// ======================================================================
// pg_catalog tests
// ======================================================================

describe("TestPGNamespace", () => {
  it("test_standard_schemas", async () => {
    const r = await engine.sql(
      "SELECT oid, nspname FROM pg_catalog.pg_namespace ORDER BY oid",
    );
    const schemas: Record<string, unknown> = {};
    for (const row of r!.rows) {
      schemas[row["nspname"] as string] = row["oid"];
    }
    expect(schemas["pg_catalog"]).toBe(11);
    expect(schemas["public"]).toBe(2200);
    expect(schemas["information_schema"]).toBe(13182);
  });
});

describe("TestPGClass", () => {
  it("test_tables_present", async () => {
    const r = await engine.sql(
      "SELECT relname, relkind FROM pg_catalog.pg_class " +
        "WHERE relnamespace = 2200 AND relkind = 'r'",
    );
    const names = new Set(r!.rows.map((row) => row["relname"]));
    expect(names).toContain("departments");
    expect(names).toContain("employees");
  });

  it("test_indexes_present", async () => {
    const r = await engine.sql(
      "SELECT relname FROM pg_catalog.pg_class " +
        "WHERE relnamespace = 2200 AND relkind = 'i'",
    );
    const names = new Set(r!.rows.map((row) => row["relname"]));
    expect(names).toContain("departments_pkey");
    expect(names).toContain("employees_pkey");
    expect(names).toContain("departments_name_key");
    expect(names).toContain("employees_email_key");
  });

  it("test_views_present", async () => {
    const r = await engine.sql(
      "SELECT relname FROM pg_catalog.pg_class " +
        "WHERE relnamespace = 2200 AND relkind = 'v'",
    );
    const names = new Set(r!.rows.map((row) => row["relname"]));
    expect(names).toContain("dept_summary");
  });

  it("test_sequences_present", async () => {
    const r = await engine.sql(
      "SELECT relname FROM pg_catalog.pg_class " +
        "WHERE relnamespace = 2200 AND relkind = 'S'",
    );
    const names = new Set(r!.rows.map((row) => row["relname"]));
    expect(names).toContain("invoice_seq");
  });
});

describe("TestPGAttribute", () => {
  it("test_column_types", async () => {
    const r = await engine.sql(
      "SELECT a.attname, a.atttypid, a.attnum " +
        "FROM pg_catalog.pg_attribute a " +
        "JOIN pg_catalog.pg_class c ON c.oid = a.attrelid " +
        "WHERE c.relname = 'employees' AND a.attnum > 0 " +
        "ORDER BY a.attnum",
    );
    const cols = r!.rows.map(
      (row) => [row["attname"], row["atttypid"]] as [string, number],
    );
    expect(cols[0]).toEqual(["id", 23]); // int4
    expect(cols[1]).toEqual(["dept_id", 23]); // int4
    expect(cols[2]).toEqual(["name", 25]); // text
    expect(cols[3]).toEqual(["email", 25]); // text
    // JS UQA engine maps NUMERIC to float internally
    expect(cols[4]).toEqual(["salary", 700]); // float4
    expect(cols[5]).toEqual(["is_active", 16]); // bool
  });

  it("test_system_columns", async () => {
    const r = await engine.sql(
      "SELECT a.attname, a.attnum " +
        "FROM pg_catalog.pg_attribute a " +
        "JOIN pg_catalog.pg_class c ON c.oid = a.attrelid " +
        "WHERE c.relname = 'departments' AND a.attnum < 0 " +
        "ORDER BY a.attnum",
    );
    const sysCols = new Set(r!.rows.map((row) => row["attname"]));
    expect(sysCols).toContain("ctid");
    expect(sysCols).toContain("xmin");
    expect(sysCols).toContain("tableoid");
  });
});

describe("TestPGType", () => {
  it("test_base_types", async () => {
    const r = await engine.sql(
      "SELECT oid, typname FROM pg_catalog.pg_type " +
        "WHERE oid IN (16, 23, 25, 701, 1700)",
    );
    const types: Record<number, string> = {};
    for (const row of r!.rows) {
      types[row["oid"] as number] = row["typname"] as string;
    }
    expect(types[16]).toBe("bool");
    expect(types[23]).toBe("int4");
    expect(types[25]).toBe("text");
    expect(types[701]).toBe("float8");
    expect(types[1700]).toBe("numeric");
  });

  it("test_array_types", async () => {
    const r = await engine.sql(
      "SELECT typname, typelem FROM pg_catalog.pg_type " +
        "WHERE typcategory = 'A' AND typelem = 23",
    );
    expect(r!.rows.length).toBeGreaterThanOrEqual(1);
    expect(r!.rows[0]!["typname"]).toBe("_int4");
  });

  it("test_composite_types", async () => {
    const r = await engine.sql(
      "SELECT typname, typtype FROM pg_catalog.pg_type WHERE typtype = 'c'",
    );
    const names = new Set(r!.rows.map((row) => row["typname"]));
    expect(names).toContain("departments");
    expect(names).toContain("employees");
  });
});

describe("TestPGConstraint", () => {
  it("test_primary_key", async () => {
    const r = await engine.sql(
      "SELECT conname, contype FROM pg_catalog.pg_constraint WHERE contype = 'p'",
    );
    const names = new Set(r!.rows.map((row) => row["conname"]));
    expect(names).toContain("departments_pkey");
    expect(names).toContain("employees_pkey");
  });

  it("test_foreign_key", async () => {
    const r = await engine.sql(
      "SELECT con.conname, src.relname AS src, tgt.relname AS tgt " +
        "FROM pg_catalog.pg_constraint con " +
        "JOIN pg_catalog.pg_class src ON con.conrelid = src.oid " +
        "JOIN pg_catalog.pg_class tgt ON con.confrelid = tgt.oid " +
        "WHERE con.contype = 'f'",
    );
    expect(r!.rows.length).toBe(1);
    expect(r!.rows[0]!["conname"]).toBe("employees_dept_id_fkey");
    expect(r!.rows[0]!["src"]).toBe("employees");
    expect(r!.rows[0]!["tgt"]).toBe("departments");
  });

  it("test_unique_constraint", async () => {
    const r = await engine.sql(
      "SELECT conname FROM pg_catalog.pg_constraint WHERE contype = 'u'",
    );
    const names = new Set(r!.rows.map((row) => row["conname"]));
    expect(names).toContain("departments_name_key");
    expect(names).toContain("employees_email_key");
  });
});

describe("TestPGIndex", () => {
  it("test_primary_index", async () => {
    const r = await engine.sql(
      "SELECT i.indisprimary, c.relname " +
        "FROM pg_catalog.pg_index i " +
        "JOIN pg_catalog.pg_class c ON i.indexrelid = c.oid " +
        "WHERE i.indisprimary = 1",
    );
    const names = new Set(r!.rows.map((row) => row["relname"]));
    expect(names).toContain("departments_pkey");
    expect(names).toContain("employees_pkey");
  });

  it("test_unique_index", async () => {
    const r = await engine.sql(
      "SELECT c.relname " +
        "FROM pg_catalog.pg_index i " +
        "JOIN pg_catalog.pg_class c ON i.indexrelid = c.oid " +
        "WHERE i.indisunique = 1 AND i.indisprimary = 0",
    );
    const names = new Set(r!.rows.map((row) => row["relname"]));
    expect(names).toContain("departments_name_key");
    expect(names).toContain("employees_email_key");
  });
});

describe("TestPGSettings", () => {
  it("test_server_version", async () => {
    const r = await engine.sql(
      "SELECT setting FROM pg_catalog.pg_settings WHERE name = 'server_version'",
    );
    expect(r!.rows[0]!["setting"]).toBe("17.0");
  });

  it("test_server_version_num", async () => {
    const r = await engine.sql(
      "SELECT setting FROM pg_catalog.pg_settings " +
        "WHERE name = 'server_version_num'",
    );
    expect(r!.rows[0]!["setting"]).toBe("170000");
  });

  it("test_encoding", async () => {
    const r = await engine.sql(
      "SELECT setting FROM pg_catalog.pg_settings WHERE name = 'server_encoding'",
    );
    expect(r!.rows[0]!["setting"]).toBe("UTF8");
  });
});

describe("TestPGOther", () => {
  it("test_pg_database", async () => {
    const r = await engine.sql("SELECT datname FROM pg_catalog.pg_database");
    expect(r!.rows[0]!["datname"]).toBe("uqa");
  });

  it("test_pg_roles", async () => {
    const r = await engine.sql("SELECT rolname, rolsuper FROM pg_catalog.pg_roles");
    expect(r!.rows[0]!["rolname"]).toBe("uqa");
    expect(r!.rows[0]!["rolsuper"]).toBe(1);
  });

  it("test_pg_am", async () => {
    const r = await engine.sql("SELECT amname FROM pg_catalog.pg_am ORDER BY amname");
    const names = r!.rows.map((row) => row["amname"]);
    expect(names).toContain("btree");
    expect(names).toContain("heap");
    expect(names).toContain("hnsw");
  });

  it("test_pg_tables", async () => {
    const r = await engine.sql(
      "SELECT tablename FROM pg_catalog.pg_tables " +
        "WHERE schemaname = 'public' ORDER BY tablename",
    );
    const names = r!.rows.map((row) => row["tablename"]);
    expect(names).toContain("departments");
    expect(names).toContain("employees");
  });

  it("test_pg_views", async () => {
    const r = await engine.sql("SELECT viewname FROM pg_catalog.pg_views");
    const names = new Set(r!.rows.map((row) => row["viewname"]));
    expect(names).toContain("dept_summary");
  });

  it("test_pg_indexes", async () => {
    const r = await engine.sql(
      "SELECT indexname FROM pg_catalog.pg_indexes WHERE tablename = 'employees'",
    );
    const names = new Set(r!.rows.map((row) => row["indexname"]));
    expect(names).toContain("employees_pkey");
    expect(names).toContain("employees_email_key");
  });

  it("test_pg_sequences", async () => {
    const r = await engine.sql(
      "SELECT sequencename, start_value, increment_by " +
        "FROM pg_catalog.pg_sequences " +
        "WHERE sequencename = 'invoice_seq'",
    );
    expect(r!.rows.length).toBe(1);
    expect(r!.rows[0]!["sequencename"]).toBe("invoice_seq");
    expect(r!.rows[0]!["start_value"]).toBe(1000);
    expect(r!.rows[0]!["increment_by"]).toBe(5);
  });

  it("test_pg_stat_user_tables", async () => {
    const r = await engine.sql(
      "SELECT relname, n_live_tup " +
        "FROM pg_catalog.pg_stat_user_tables ORDER BY relname",
    );
    const stats: Record<string, unknown> = {};
    for (const row of r!.rows) {
      stats[row["relname"] as string] = row["n_live_tup"];
    }
    expect(stats["departments"]).toBe(2);
    expect(stats["employees"]).toBe(1);
  });

  it("test_pg_stat_activity", async () => {
    const r = await engine.sql(
      "SELECT datname, state FROM pg_catalog.pg_stat_activity",
    );
    expect(r!.rows[0]!["datname"]).toBe("uqa");
    expect(r!.rows[0]!["state"]).toBe("active");
  });

  it("test_pg_extension", async () => {
    const r = await engine.sql("SELECT extname FROM pg_catalog.pg_extension");
    expect(r!.rows[0]!["extname"]).toBe("plpgsql");
  });

  it("test_pg_collation", async () => {
    const r = await engine.sql("SELECT collname FROM pg_catalog.pg_collation");
    const names = new Set(r!.rows.map((row) => row["collname"]));
    expect(names).toContain("default");
    expect(names).toContain("C");
  });

  it("test_pg_description_empty", async () => {
    const r = await engine.sql("SELECT * FROM pg_catalog.pg_description");
    expect(r!.rows.length).toBe(0);
  });

  it("test_pg_enum_empty", async () => {
    const r = await engine.sql("SELECT * FROM pg_catalog.pg_enum");
    expect(r!.rows.length).toBe(0);
  });

  it("test_pg_inherits_empty", async () => {
    const r = await engine.sql("SELECT * FROM pg_catalog.pg_inherits");
    expect(r!.rows.length).toBe(0);
  });

  it("test_pg_trigger_empty", async () => {
    const r = await engine.sql("SELECT * FROM pg_catalog.pg_trigger");
    expect(r!.rows.length).toBe(0);
  });
});

describe("TestPGAttrdef", () => {
  it("test_serial_defaults", async () => {
    const r = await engine.sql(
      "SELECT d.adbin " +
        "FROM pg_catalog.pg_attrdef d " +
        "JOIN pg_catalog.pg_class c ON d.adrelid = c.oid " +
        "WHERE c.relname = 'employees'",
    );
    const defaults = r!.rows.map((row) => row["adbin"] as string);
    expect(defaults.some((d) => d.includes("nextval"))).toBe(true);
  });
});

// ======================================================================
// OID cross-reference consistency tests
// ======================================================================

describe("TestOIDConsistency", () => {
  it("test_pg_class_pg_attribute_join", async () => {
    const r = await engine.sql(
      "SELECT c.relname, a.attname " +
        "FROM pg_catalog.pg_class c " +
        "JOIN pg_catalog.pg_attribute a ON c.oid = a.attrelid " +
        "WHERE c.relname = 'departments' AND a.attnum > 0 " +
        "ORDER BY a.attnum",
    );
    const cols = r!.rows.map((row) => row["attname"]);
    expect(cols).toEqual(["id", "name"]);
  });

  it("test_pg_attribute_pg_type_join", async () => {
    const r = await engine.sql(
      "SELECT a.attname, t.typname " +
        "FROM pg_catalog.pg_class c " +
        "JOIN pg_catalog.pg_attribute a ON c.oid = a.attrelid " +
        "JOIN pg_catalog.pg_type t ON a.atttypid = t.oid " +
        "WHERE c.relname = 'employees' AND a.attnum > 0 " +
        "ORDER BY a.attnum",
    );
    const typeMap: Record<string, unknown> = {};
    for (const row of r!.rows) {
      typeMap[row["attname"] as string] = row["typname"];
    }
    expect(typeMap["id"]).toBe("int4");
    expect(typeMap["name"]).toBe("text");
    // JS UQA engine maps NUMERIC to float internally
    expect(typeMap["salary"]).toBe("float4");
    expect(typeMap["is_active"]).toBe("bool");
  });

  it("test_pg_constraint_pg_class_join", async () => {
    const r = await engine.sql(
      "SELECT con.conname, c.relname " +
        "FROM pg_catalog.pg_constraint con " +
        "JOIN pg_catalog.pg_class c ON con.conrelid = c.oid " +
        "WHERE con.contype = 'p'",
    );
    const pks: Record<string, unknown> = {};
    for (const row of r!.rows) {
      pks[row["relname"] as string] = row["conname"];
    }
    expect(pks["departments"]).toBe("departments_pkey");
    expect(pks["employees"]).toBe("employees_pkey");
  });

  it("test_pg_index_pg_class_join", async () => {
    const r = await engine.sql(
      "SELECT idx.relname AS index_name, tbl.relname AS table_name " +
        "FROM pg_catalog.pg_index i " +
        "JOIN pg_catalog.pg_class idx ON i.indexrelid = idx.oid " +
        "JOIN pg_catalog.pg_class tbl ON i.indrelid = tbl.oid " +
        "WHERE i.indisprimary = 1",
    );
    const pkMap: Record<string, unknown> = {};
    for (const row of r!.rows) {
      pkMap[row["table_name"] as string] = row["index_name"];
    }
    expect(pkMap["departments"]).toBe("departments_pkey");
    expect(pkMap["employees"]).toBe("employees_pkey");
  });

  it("test_pg_constraint_confrelid_join", async () => {
    const r = await engine.sql(
      "SELECT con.conname, ref.relname AS ref_table " +
        "FROM pg_catalog.pg_constraint con " +
        "JOIN pg_catalog.pg_class ref ON con.confrelid = ref.oid " +
        "WHERE con.contype = 'f'",
    );
    expect(r!.rows[0]!["ref_table"]).toBe("departments");
  });
});

describe("TestEmptyEngine", () => {
  it("test_information_schema_tables_empty", async () => {
    const e = new USQLEngine();
    const r = await e.sql(
      "SELECT * FROM information_schema.tables WHERE table_schema = 'public'",
    );
    expect(r!.rows.length).toBe(0);
  });

  it("test_pg_class_empty", async () => {
    const e = new USQLEngine();
    const r = await e.sql(
      "SELECT * FROM pg_catalog.pg_class WHERE relnamespace = 2200",
    );
    expect(r!.rows.length).toBe(0);
  });

  it("test_pg_type_still_populated", async () => {
    const e = new USQLEngine();
    const r = await e.sql("SELECT COUNT(*) AS cnt FROM pg_catalog.pg_type");
    expect((r!.rows[0]!["cnt"] as number) > 20).toBe(true);
  });

  it("test_pg_settings_still_populated", async () => {
    const e = new USQLEngine();
    const r = await e.sql("SELECT COUNT(*) AS cnt FROM pg_catalog.pg_settings");
    expect((r!.rows[0]!["cnt"] as number) > 10).toBe(true);
  });
});

// ======================================================================
// Unqualified catalog name tests
// ======================================================================

describe("TestUnqualifiedCatalogNames", () => {
  it("test_pg_class_without_prefix", async () => {
    const r = await engine.sql(
      "SELECT relname FROM pg_class WHERE relnamespace = 2200 AND relkind = 'r'",
    );
    const names = new Set(r!.rows.map((row) => row["relname"]));
    expect(names).toContain("departments");
    expect(names).toContain("employees");
  });

  it("test_pg_tables_without_prefix", async () => {
    const r = await engine.sql(
      "SELECT tablename FROM pg_tables WHERE schemaname = 'public'",
    );
    const names = new Set(r!.rows.map((row) => row["tablename"]));
    expect(names).toContain("departments");
    expect(names).toContain("employees");
  });

  it("test_pg_type_without_prefix", async () => {
    const r = await engine.sql("SELECT oid, typname FROM pg_type WHERE oid = 23");
    expect(r!.rows.length).toBeGreaterThanOrEqual(1);
    expect(r!.rows[0]!["typname"]).toBe("int4");
  });

  it("test_join_with_unqualified_names", async () => {
    const r = await engine.sql(
      "SELECT c.relname, a.attname " +
        "FROM pg_class c " +
        "JOIN pg_attribute a ON c.oid = a.attrelid " +
        "WHERE c.relname = 'departments' AND a.attnum > 0 " +
        "ORDER BY a.attnum",
    );
    const cols = r!.rows.map((row) => row["attname"]);
    expect(cols).toEqual(["id", "name"]);
  });

  it("test_pg_settings_without_prefix", async () => {
    const r = await engine.sql(
      "SELECT setting FROM pg_settings WHERE name = 'server_version'",
    );
    expect(r!.rows[0]!["setting"]).toBe("17.0");
  });
});
