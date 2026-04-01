//
// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
//
// Copyright (c) 2023-2026 Cognica, Inc.
//

// Tests for PostgreSQL 17-compatible information_schema and pg_catalog.

import { describe, it, expect, beforeEach } from "vitest";
import { USQLEngine } from "../src/index.js";

type Row = Record<string, unknown>;

interface Result {
  rows: Row[];
  columns: string[];
  [Symbol.iterator](): Iterator<Row>;
}

async function query(engine: USQLEngine, sql: string): Promise<Result> {
  const result = await engine.sql(sql);
  return result as unknown as Result;
}

function rows(result: Result): Row[] {
  return result.rows;
}

describe("PostgreSQL 17 catalog compatibility", () => {
  let engine: USQLEngine;

  beforeEach(async () => {
    engine = new USQLEngine();
    await engine.sql(
      "CREATE TABLE departments (id SERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE)",
    );
    await engine.sql(
      "CREATE TABLE employees (" +
        "id SERIAL PRIMARY KEY, " +
        "dept_id INTEGER REFERENCES departments(id), " +
        "name TEXT NOT NULL, " +
        "email TEXT UNIQUE, " +
        "salary NUMERIC(10,2), " +
        "is_active BOOLEAN DEFAULT true" +
        ")",
    );
    await engine.sql("CREATE VIEW dept_summary AS SELECT name FROM departments");
    await engine.sql("CREATE SEQUENCE invoice_seq START 1000 INCREMENT 5");
    await engine.sql("INSERT INTO departments (name) VALUES ('Engineering')");
    await engine.sql("INSERT INTO departments (name) VALUES ('Sales')");
    await engine.sql(
      "INSERT INTO employees (dept_id, name, email, salary, is_active) " +
        "VALUES (1, 'Alice', 'alice@example.com', 150000.50, true)",
    );
  });

  // ==================================================================
  // information_schema tests
  // ==================================================================

  describe("information_schema.schemata", () => {
    it("lists all schemas", async () => {
      const r = await query(
        engine,
        "SELECT schema_name FROM information_schema.schemata",
      );
      const names = new Set(rows(r).map((row) => row["schema_name"]));
      expect(names).toEqual(new Set(["public", "information_schema", "pg_catalog"]));
    });

    it("has catalog name", async () => {
      const r = await query(
        engine,
        "SELECT catalog_name FROM information_schema.schemata WHERE schema_name = 'public'",
      );
      expect(rows(r)[0]!["catalog_name"]).toBe("uqa");
    });
  });

  describe("information_schema.tables", () => {
    it("lists base tables", async () => {
      const r = await query(
        engine,
        "SELECT table_name, table_type FROM information_schema.tables " +
          "WHERE table_schema = 'public' AND table_type = 'BASE TABLE' " +
          "ORDER BY table_name",
      );
      const names = rows(r).map((row) => row["table_name"]);
      expect(names).toContain("departments");
      expect(names).toContain("employees");
    });

    it("lists views", async () => {
      const r = await query(
        engine,
        "SELECT table_name, table_type FROM information_schema.tables " +
          "WHERE table_type = 'VIEW'",
      );
      const names = new Set(rows(r).map((row) => row["table_name"]));
      expect(names).toContain("dept_summary");
    });

    it("shows is_insertable_into", async () => {
      const r = await query(
        engine,
        "SELECT table_name, is_insertable_into FROM information_schema.tables " +
          "WHERE table_name = 'departments'",
      );
      expect(rows(r)[0]!["is_insertable_into"]).toBe("YES");
    });
  });

  describe("information_schema.columns", () => {
    it("counts columns", async () => {
      const r = await query(
        engine,
        "SELECT column_name FROM information_schema.columns " +
          "WHERE table_name = 'employees'",
      );
      expect(rows(r).length).toBe(6);
    });

    it("has correct ordinal positions", async () => {
      const r = await query(
        engine,
        "SELECT column_name, ordinal_position FROM information_schema.columns " +
          "WHERE table_name = 'employees' ORDER BY ordinal_position",
      );
      const cols = rows(r).map((row) => row["column_name"]);
      expect(cols).toEqual(["id", "dept_id", "name", "email", "salary", "is_active"]);
    });

    it("shows correct data types", async () => {
      const r = await query(
        engine,
        "SELECT column_name, data_type FROM information_schema.columns " +
          "WHERE table_name = 'employees' ORDER BY ordinal_position",
      );
      const types: Record<string, unknown> = {};
      for (const row of rows(r)) {
        types[row["column_name"] as string] = row["data_type"];
      }
      expect(types["id"]).toBe("integer");
      expect(types["name"]).toBe("text");
      // JS UQA engine maps NUMERIC to "float" internally, displayed as "real"
      expect(types["salary"]).toBe("real");
      expect(types["is_active"]).toBe("boolean");
    });

    it("shows nullable correctly", async () => {
      const r = await query(
        engine,
        "SELECT column_name, is_nullable FROM information_schema.columns " +
          "WHERE table_name = 'employees'",
      );
      const nullable: Record<string, unknown> = {};
      for (const row of rows(r)) {
        nullable[row["column_name"] as string] = row["is_nullable"];
      }
      expect(nullable["id"]).toBe("NO"); // PK is NOT NULL
      expect(nullable["name"]).toBe("NO");
      expect(nullable["email"]).toBe("YES");
    });

    it("shows defaults", async () => {
      const r = await query(
        engine,
        "SELECT column_name, column_default FROM information_schema.columns " +
          "WHERE table_name = 'employees' AND column_default IS NOT NULL",
      );
      const defaults: Record<string, unknown> = {};
      for (const row of rows(r)) {
        defaults[row["column_name"] as string] = row["column_default"];
      }
      expect(String(defaults["id"])).toContain("nextval");
      expect(defaults["is_active"]).toBe("true");
    });

    it("shows numeric precision and scale", async () => {
      const r = await query(
        engine,
        "SELECT numeric_precision, numeric_scale FROM information_schema.columns " +
          "WHERE table_name = 'employees' AND column_name = 'salary'",
      );
      // JS UQA engine maps NUMERIC to "float" and doesn't preserve precision/scale.
      // The float4 type has precision 24 (from PostgreSQL type metadata).
      expect(rows(r)[0]!["numeric_precision"]).toBe(24);
      expect(rows(r)[0]!["numeric_scale"]).toBe(null);
    });

    it("shows udt_name", async () => {
      const r = await query(
        engine,
        "SELECT column_name, udt_name FROM information_schema.columns " +
          "WHERE table_name = 'employees' ORDER BY ordinal_position",
      );
      const udt: Record<string, unknown> = {};
      for (const row of rows(r)) {
        udt[row["column_name"] as string] = row["udt_name"];
      }
      expect(udt["id"]).toBe("int4");
      expect(udt["name"]).toBe("text");
      // JS UQA engine maps NUMERIC to "float", udt_name is "float4"
      expect(udt["salary"]).toBe("float4");
      expect(udt["is_active"]).toBe("bool");
    });

    it("shows identity columns", async () => {
      const r = await query(
        engine,
        "SELECT column_name, is_identity, identity_generation " +
          "FROM information_schema.columns " +
          "WHERE table_name = 'employees' AND is_identity = 'YES'",
      );
      expect(rows(r).length).toBe(1);
      expect(rows(r)[0]!["column_name"]).toBe("id");
      expect(rows(r)[0]!["identity_generation"]).toBe("BY DEFAULT");
    });
  });

  describe("information_schema.constraints", () => {
    it("shows primary keys", async () => {
      const r = await query(
        engine,
        "SELECT constraint_name, table_name " +
          "FROM information_schema.table_constraints " +
          "WHERE constraint_type = 'PRIMARY KEY' ORDER BY table_name",
      );
      const names: Record<string, unknown> = {};
      for (const row of rows(r)) {
        names[row["table_name"] as string] = row["constraint_name"];
      }
      expect(names["departments"]).toBe("departments_pkey");
      expect(names["employees"]).toBe("employees_pkey");
    });

    it("shows unique constraints", async () => {
      const r = await query(
        engine,
        "SELECT constraint_name, table_name " +
          "FROM information_schema.table_constraints " +
          "WHERE constraint_type = 'UNIQUE'",
      );
      const names = new Set(rows(r).map((row) => row["constraint_name"]));
      expect(names).toContain("departments_name_key");
      expect(names).toContain("employees_email_key");
    });

    it("shows foreign keys", async () => {
      const r = await query(
        engine,
        "SELECT constraint_name, table_name " +
          "FROM information_schema.table_constraints " +
          "WHERE constraint_type = 'FOREIGN KEY'",
      );
      expect(rows(r).length).toBe(1);
      expect(rows(r)[0]!["constraint_name"]).toBe("employees_dept_id_fkey");
      expect(rows(r)[0]!["table_name"]).toBe("employees");
    });

    it("shows key column usage", async () => {
      const r = await query(
        engine,
        "SELECT constraint_name, column_name " +
          "FROM information_schema.key_column_usage " +
          "WHERE table_name = 'employees'",
      );
      const cols: Record<string, unknown> = {};
      for (const row of rows(r)) {
        cols[row["constraint_name"] as string] = row["column_name"];
      }
      expect(cols["employees_pkey"]).toBe("id");
      expect(cols["employees_email_key"]).toBe("email");
      expect(cols["employees_dept_id_fkey"]).toBe("dept_id");
    });

    it("shows referential constraints", async () => {
      const r = await query(
        engine,
        "SELECT constraint_name, unique_constraint_name, " +
          "update_rule, delete_rule " +
          "FROM information_schema.referential_constraints",
      );
      expect(rows(r).length).toBe(1);
      const row = rows(r)[0]!;
      expect(row["constraint_name"]).toBe("employees_dept_id_fkey");
      expect(row["unique_constraint_name"]).toBe("departments_pkey");
      expect(row["update_rule"]).toBe("NO ACTION");
      expect(row["delete_rule"]).toBe("NO ACTION");
    });

    it("shows constraint column usage", async () => {
      const r = await query(
        engine,
        "SELECT table_name, column_name, constraint_name " +
          "FROM information_schema.constraint_column_usage " +
          "WHERE constraint_name = 'employees_dept_id_fkey'",
      );
      expect(rows(r).length).toBe(1);
      expect(rows(r)[0]!["table_name"]).toBe("departments");
      expect(rows(r)[0]!["column_name"]).toBe("id");
    });
  });

  describe("information_schema.views", () => {
    it("lists views", async () => {
      const r = await query(
        engine,
        "SELECT table_name, is_updatable FROM information_schema.views",
      );
      const names = new Set(rows(r).map((row) => row["table_name"]));
      expect(names).toContain("dept_summary");
    });

    it("shows check_option", async () => {
      const r = await query(
        engine,
        "SELECT check_option FROM information_schema.views " +
          "WHERE table_name = 'dept_summary'",
      );
      expect(rows(r)[0]!["check_option"]).toBe("NONE");
    });
  });

  describe("information_schema.sequences", () => {
    it("lists sequences", async () => {
      const r = await query(
        engine,
        "SELECT sequence_name, data_type, start_value, increment " +
          "FROM information_schema.sequences " +
          "WHERE sequence_name = 'invoice_seq'",
      );
      expect(rows(r).length).toBe(1);
      expect(rows(r)[0]!["sequence_name"]).toBe("invoice_seq");
      expect(rows(r)[0]!["data_type"]).toBe("bigint");
      expect(rows(r)[0]!["start_value"]).toBe("1000");
      expect(rows(r)[0]!["increment"]).toBe("5");
    });
  });

  describe("information_schema other views", () => {
    it("shows enabled_roles", async () => {
      const r = await query(
        engine,
        "SELECT role_name FROM information_schema.enabled_roles",
      );
      expect(rows(r).length).toBe(1);
      expect(rows(r)[0]!["role_name"]).toBe("uqa");
    });

    it("shows character_sets", async () => {
      const r = await query(
        engine,
        "SELECT character_set_name FROM information_schema.character_sets",
      );
      expect(rows(r)[0]!["character_set_name"]).toBe("UTF8");
    });

    it("returns empty domains", async () => {
      const r = await query(engine, "SELECT * FROM information_schema.domains");
      expect(rows(r).length).toBe(0);
    });

    it("returns empty triggers", async () => {
      const r = await query(engine, "SELECT * FROM information_schema.triggers");
      expect(rows(r).length).toBe(0);
    });
  });

  // ==================================================================
  // pg_catalog tests
  // ==================================================================

  describe("pg_catalog.pg_namespace", () => {
    it("lists standard schemas", async () => {
      const r = await query(
        engine,
        "SELECT oid, nspname FROM pg_catalog.pg_namespace ORDER BY oid",
      );
      const schemas: Record<string, unknown> = {};
      for (const row of rows(r)) {
        schemas[row["nspname"] as string] = row["oid"];
      }
      expect(schemas["pg_catalog"]).toBe(11);
      expect(schemas["public"]).toBe(2200);
      expect(schemas["information_schema"]).toBe(13182);
    });
  });

  describe("pg_catalog.pg_class", () => {
    it("lists tables", async () => {
      const r = await query(
        engine,
        "SELECT relname, relkind FROM pg_catalog.pg_class " +
          "WHERE relnamespace = 2200 AND relkind = 'r'",
      );
      const names = new Set(rows(r).map((row) => row["relname"]));
      expect(names).toContain("departments");
      expect(names).toContain("employees");
    });

    it("lists indexes", async () => {
      const r = await query(
        engine,
        "SELECT relname FROM pg_catalog.pg_class " +
          "WHERE relnamespace = 2200 AND relkind = 'i'",
      );
      const names = new Set(rows(r).map((row) => row["relname"]));
      expect(names).toContain("departments_pkey");
      expect(names).toContain("employees_pkey");
      expect(names).toContain("departments_name_key");
      expect(names).toContain("employees_email_key");
    });

    it("lists views", async () => {
      const r = await query(
        engine,
        "SELECT relname FROM pg_catalog.pg_class " +
          "WHERE relnamespace = 2200 AND relkind = 'v'",
      );
      const names = new Set(rows(r).map((row) => row["relname"]));
      expect(names).toContain("dept_summary");
    });

    it("lists sequences", async () => {
      const r = await query(
        engine,
        "SELECT relname FROM pg_catalog.pg_class " +
          "WHERE relnamespace = 2200 AND relkind = 'S'",
      );
      const names = new Set(rows(r).map((row) => row["relname"]));
      expect(names).toContain("invoice_seq");
    });
  });

  describe("pg_catalog.pg_attribute", () => {
    it("shows column types via JOIN", async () => {
      const r = await query(
        engine,
        "SELECT a.attname, a.atttypid, a.attnum " +
          "FROM pg_catalog.pg_attribute a " +
          "JOIN pg_catalog.pg_class c ON c.oid = a.attrelid " +
          "WHERE c.relname = 'employees' AND a.attnum > 0 " +
          "ORDER BY a.attnum",
      );
      const cols = rows(r).map((row) => [row["attname"], row["atttypid"]]);
      expect(cols[0]).toEqual(["id", 23]); // int4
      expect(cols[1]).toEqual(["dept_id", 23]); // int4
      expect(cols[2]).toEqual(["name", 25]); // text
      expect(cols[3]).toEqual(["email", 25]); // text
      // JS UQA engine maps NUMERIC to "float" -> type OID 700 (float4)
      expect(cols[4]).toEqual(["salary", 700]); // float4
      expect(cols[5]).toEqual(["is_active", 16]); // bool
    });

    it("includes system columns", async () => {
      const r = await query(
        engine,
        "SELECT a.attname, a.attnum " +
          "FROM pg_catalog.pg_attribute a " +
          "JOIN pg_catalog.pg_class c ON c.oid = a.attrelid " +
          "WHERE c.relname = 'departments' AND a.attnum < 0 " +
          "ORDER BY a.attnum",
      );
      const sysCols = new Set(rows(r).map((row) => row["attname"]));
      expect(sysCols).toContain("ctid");
      expect(sysCols).toContain("xmin");
      expect(sysCols).toContain("tableoid");
    });
  });

  describe("pg_catalog.pg_type", () => {
    it("shows base types", async () => {
      const r = await query(
        engine,
        "SELECT oid, typname FROM pg_catalog.pg_type " +
          "WHERE oid IN (16, 23, 25, 701, 1700)",
      );
      const types: Record<number, unknown> = {};
      for (const row of rows(r)) {
        types[row["oid"] as number] = row["typname"];
      }
      expect(types[16]).toBe("bool");
      expect(types[23]).toBe("int4");
      expect(types[25]).toBe("text");
      expect(types[701]).toBe("float8");
      expect(types[1700]).toBe("numeric");
    });

    it("shows array types", async () => {
      const r = await query(
        engine,
        "SELECT typname, typelem FROM pg_catalog.pg_type " +
          "WHERE typcategory = 'A' AND typelem = 23",
      );
      expect(rows(r).length).toBeGreaterThanOrEqual(1);
      expect(rows(r)[0]!["typname"]).toBe("_int4");
    });

    it("shows composite types for user tables", async () => {
      const r = await query(
        engine,
        "SELECT typname, typtype FROM pg_catalog.pg_type WHERE typtype = 'c'",
      );
      const names = new Set(rows(r).map((row) => row["typname"]));
      expect(names).toContain("departments");
      expect(names).toContain("employees");
    });
  });

  describe("pg_catalog.pg_constraint", () => {
    it("shows primary keys", async () => {
      const r = await query(
        engine,
        "SELECT conname, contype FROM pg_catalog.pg_constraint WHERE contype = 'p'",
      );
      const names = new Set(rows(r).map((row) => row["conname"]));
      expect(names).toContain("departments_pkey");
      expect(names).toContain("employees_pkey");
    });

    it("shows foreign keys with JOIN", async () => {
      const r = await query(
        engine,
        "SELECT con.conname, src.relname AS src, tgt.relname AS tgt " +
          "FROM pg_catalog.pg_constraint con " +
          "JOIN pg_catalog.pg_class src ON con.conrelid = src.oid " +
          "JOIN pg_catalog.pg_class tgt ON con.confrelid = tgt.oid " +
          "WHERE con.contype = 'f'",
      );
      expect(rows(r).length).toBe(1);
      expect(rows(r)[0]!["conname"]).toBe("employees_dept_id_fkey");
      expect(rows(r)[0]!["src"]).toBe("employees");
      expect(rows(r)[0]!["tgt"]).toBe("departments");
    });

    it("shows unique constraints", async () => {
      const r = await query(
        engine,
        "SELECT conname FROM pg_catalog.pg_constraint WHERE contype = 'u'",
      );
      const names = new Set(rows(r).map((row) => row["conname"]));
      expect(names).toContain("departments_name_key");
      expect(names).toContain("employees_email_key");
    });
  });

  describe("pg_catalog.pg_index", () => {
    it("shows primary indexes", async () => {
      const r = await query(
        engine,
        "SELECT i.indisprimary, c.relname " +
          "FROM pg_catalog.pg_index i " +
          "JOIN pg_catalog.pg_class c ON i.indexrelid = c.oid " +
          "WHERE i.indisprimary = 1",
      );
      const names = new Set(rows(r).map((row) => row["relname"]));
      expect(names).toContain("departments_pkey");
      expect(names).toContain("employees_pkey");
    });

    it("shows unique non-primary indexes", async () => {
      const r = await query(
        engine,
        "SELECT c.relname " +
          "FROM pg_catalog.pg_index i " +
          "JOIN pg_catalog.pg_class c ON i.indexrelid = c.oid " +
          "WHERE i.indisunique = 1 AND i.indisprimary = 0",
      );
      const names = new Set(rows(r).map((row) => row["relname"]));
      expect(names).toContain("departments_name_key");
      expect(names).toContain("employees_email_key");
    });
  });

  describe("pg_catalog.pg_settings", () => {
    it("shows server_version", async () => {
      const r = await query(
        engine,
        "SELECT setting FROM pg_catalog.pg_settings WHERE name = 'server_version'",
      );
      expect(rows(r)[0]!["setting"]).toBe("17.0");
    });

    it("shows server_version_num", async () => {
      const r = await query(
        engine,
        "SELECT setting FROM pg_catalog.pg_settings WHERE name = 'server_version_num'",
      );
      expect(rows(r)[0]!["setting"]).toBe("170000");
    });

    it("shows encoding", async () => {
      const r = await query(
        engine,
        "SELECT setting FROM pg_catalog.pg_settings WHERE name = 'server_encoding'",
      );
      expect(rows(r)[0]!["setting"]).toBe("UTF8");
    });
  });

  describe("pg_catalog other tables", () => {
    it("shows pg_database", async () => {
      const r = await query(engine, "SELECT datname FROM pg_catalog.pg_database");
      expect(rows(r)[0]!["datname"]).toBe("uqa");
    });

    it("shows pg_roles", async () => {
      const r = await query(
        engine,
        "SELECT rolname, rolsuper FROM pg_catalog.pg_roles",
      );
      expect(rows(r)[0]!["rolname"]).toBe("uqa");
      expect(rows(r)[0]!["rolsuper"]).toBe(1); // normalized bool
    });

    it("shows pg_am", async () => {
      const r = await query(
        engine,
        "SELECT amname FROM pg_catalog.pg_am ORDER BY amname",
      );
      const names = rows(r).map((row) => row["amname"]);
      expect(names).toContain("btree");
      expect(names).toContain("heap");
      expect(names).toContain("hnsw");
    });

    it("shows pg_tables", async () => {
      const r = await query(
        engine,
        "SELECT tablename FROM pg_catalog.pg_tables " +
          "WHERE schemaname = 'public' ORDER BY tablename",
      );
      const names = rows(r).map((row) => row["tablename"]);
      expect(names).toContain("departments");
      expect(names).toContain("employees");
    });

    it("shows pg_views", async () => {
      const r = await query(engine, "SELECT viewname FROM pg_catalog.pg_views");
      const names = new Set(rows(r).map((row) => row["viewname"]));
      expect(names).toContain("dept_summary");
    });

    it("shows pg_indexes", async () => {
      const r = await query(
        engine,
        "SELECT indexname FROM pg_catalog.pg_indexes WHERE tablename = 'employees'",
      );
      const names = new Set(rows(r).map((row) => row["indexname"]));
      expect(names).toContain("employees_pkey");
      expect(names).toContain("employees_email_key");
    });

    it("shows pg_sequences", async () => {
      const r = await query(
        engine,
        "SELECT sequencename, start_value, increment_by " +
          "FROM pg_catalog.pg_sequences " +
          "WHERE sequencename = 'invoice_seq'",
      );
      expect(rows(r).length).toBe(1);
      expect(rows(r)[0]!["sequencename"]).toBe("invoice_seq");
      expect(rows(r)[0]!["start_value"]).toBe(1000);
      expect(rows(r)[0]!["increment_by"]).toBe(5);
    });

    it("shows pg_stat_user_tables", async () => {
      const r = await query(
        engine,
        "SELECT relname, n_live_tup FROM pg_catalog.pg_stat_user_tables ORDER BY relname",
      );
      const stats: Record<string, unknown> = {};
      for (const row of rows(r)) {
        stats[row["relname"] as string] = row["n_live_tup"];
      }
      expect(stats["departments"]).toBe(2);
      expect(stats["employees"]).toBe(1);
    });

    it("shows pg_stat_activity", async () => {
      const r = await query(
        engine,
        "SELECT datname, state FROM pg_catalog.pg_stat_activity",
      );
      expect(rows(r)[0]!["datname"]).toBe("uqa");
      expect(rows(r)[0]!["state"]).toBe("active");
    });

    it("shows pg_extension", async () => {
      const r = await query(engine, "SELECT extname FROM pg_catalog.pg_extension");
      expect(rows(r)[0]!["extname"]).toBe("plpgsql");
    });

    it("shows pg_collation", async () => {
      const r = await query(engine, "SELECT collname FROM pg_catalog.pg_collation");
      const names = new Set(rows(r).map((row) => row["collname"]));
      expect(names).toContain("default");
      expect(names).toContain("C");
    });

    it("returns empty pg_description", async () => {
      const r = await query(engine, "SELECT * FROM pg_catalog.pg_description");
      expect(rows(r).length).toBe(0);
    });

    it("returns empty pg_enum", async () => {
      const r = await query(engine, "SELECT * FROM pg_catalog.pg_enum");
      expect(rows(r).length).toBe(0);
    });

    it("returns empty pg_inherits", async () => {
      const r = await query(engine, "SELECT * FROM pg_catalog.pg_inherits");
      expect(rows(r).length).toBe(0);
    });

    it("returns empty pg_trigger", async () => {
      const r = await query(engine, "SELECT * FROM pg_catalog.pg_trigger");
      expect(rows(r).length).toBe(0);
    });
  });

  describe("pg_catalog.pg_attrdef", () => {
    it("shows serial defaults", async () => {
      const r = await query(
        engine,
        "SELECT d.adbin " +
          "FROM pg_catalog.pg_attrdef d " +
          "JOIN pg_catalog.pg_class c ON d.adrelid = c.oid " +
          "WHERE c.relname = 'employees'",
      );
      const defaults = rows(r).map((row) => String(row["adbin"]));
      expect(defaults.some((d) => d.includes("nextval"))).toBe(true);
    });
  });

  // ==================================================================
  // OID cross-reference consistency tests
  // ==================================================================

  describe("OID consistency", () => {
    it("pg_class.oid matches pg_attribute.attrelid", async () => {
      const r = await query(
        engine,
        "SELECT c.relname, a.attname " +
          "FROM pg_catalog.pg_class c " +
          "JOIN pg_catalog.pg_attribute a ON c.oid = a.attrelid " +
          "WHERE c.relname = 'departments' AND a.attnum > 0 " +
          "ORDER BY a.attnum",
      );
      const cols = rows(r).map((row) => row["attname"]);
      expect(cols).toEqual(["id", "name"]);
    });

    it("pg_attribute.atttypid matches pg_type.oid", async () => {
      const r = await query(
        engine,
        "SELECT a.attname, t.typname " +
          "FROM pg_catalog.pg_class c " +
          "JOIN pg_catalog.pg_attribute a ON c.oid = a.attrelid " +
          "JOIN pg_catalog.pg_type t ON a.atttypid = t.oid " +
          "WHERE c.relname = 'employees' AND a.attnum > 0 " +
          "ORDER BY a.attnum",
      );
      const typeMap: Record<string, unknown> = {};
      for (const row of rows(r)) {
        typeMap[row["attname"] as string] = row["typname"];
      }
      expect(typeMap["id"]).toBe("int4");
      expect(typeMap["name"]).toBe("text");
      // JS UQA engine maps NUMERIC to "float" -> float4
      expect(typeMap["salary"]).toBe("float4");
      expect(typeMap["is_active"]).toBe("bool");
    });

    it("pg_constraint.conrelid matches pg_class.oid", async () => {
      const r = await query(
        engine,
        "SELECT con.conname, c.relname " +
          "FROM pg_catalog.pg_constraint con " +
          "JOIN pg_catalog.pg_class c ON con.conrelid = c.oid " +
          "WHERE con.contype = 'p'",
      );
      const pks: Record<string, unknown> = {};
      for (const row of rows(r)) {
        pks[row["relname"] as string] = row["conname"];
      }
      expect(pks["departments"]).toBe("departments_pkey");
      expect(pks["employees"]).toBe("employees_pkey");
    });

    it("pg_index matches pg_class for both index and table", async () => {
      const r = await query(
        engine,
        "SELECT idx.relname AS index_name, tbl.relname AS table_name " +
          "FROM pg_catalog.pg_index i " +
          "JOIN pg_catalog.pg_class idx ON i.indexrelid = idx.oid " +
          "JOIN pg_catalog.pg_class tbl ON i.indrelid = tbl.oid " +
          "WHERE i.indisprimary = 1",
      );
      const pkMap: Record<string, unknown> = {};
      for (const row of rows(r)) {
        pkMap[row["table_name"] as string] = row["index_name"];
      }
      expect(pkMap["departments"]).toBe("departments_pkey");
      expect(pkMap["employees"]).toBe("employees_pkey");
    });

    it("FK constraint confrelid matches referenced table", async () => {
      const r = await query(
        engine,
        "SELECT con.conname, ref.relname AS ref_table " +
          "FROM pg_catalog.pg_constraint con " +
          "JOIN pg_catalog.pg_class ref ON con.confrelid = ref.oid " +
          "WHERE con.contype = 'f'",
      );
      expect(rows(r)[0]!["ref_table"]).toBe("departments");
    });
  });

  // ==================================================================
  // Empty engine tests
  // ==================================================================

  describe("empty engine", () => {
    it("information_schema.tables is empty for public schema", async () => {
      const e = new USQLEngine();
      const r = await query(
        e,
        "SELECT * FROM information_schema.tables WHERE table_schema = 'public'",
      );
      expect(rows(r).length).toBe(0);
    });

    it("pg_class is empty for public schema", async () => {
      const e = new USQLEngine();
      const r = await query(
        e,
        "SELECT * FROM pg_catalog.pg_class WHERE relnamespace = 2200",
      );
      expect(rows(r).length).toBe(0);
    });

    it("pg_type is still populated", async () => {
      const e = new USQLEngine();
      const r = await query(e, "SELECT COUNT(*) AS cnt FROM pg_catalog.pg_type");
      expect((rows(r)[0]!["cnt"] as number) > 20).toBe(true);
    });

    it("pg_settings is still populated", async () => {
      const e = new USQLEngine();
      const r = await query(e, "SELECT COUNT(*) AS cnt FROM pg_catalog.pg_settings");
      expect((rows(r)[0]!["cnt"] as number) > 10).toBe(true);
    });
  });
});
