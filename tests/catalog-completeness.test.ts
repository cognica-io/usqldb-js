//
// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
//
// Copyright (c) 2023-2026 Cognica, Inc.
//

// Tests that every pg_catalog table and information_schema view returns
// correct columns (even when empty).

import { describe, it, expect, beforeAll } from "vitest";
import { USQLEngine } from "../src/core/engine.js";
import { PGCatalogProvider } from "../src/pg-compat/pg-catalog.js";
import { InformationSchemaProvider } from "../src/pg-compat/information-schema.js";

let engine: USQLEngine;
let engineWithData: USQLEngine;

beforeAll(async () => {
  // Empty engine -- no user objects
  engine = new USQLEngine();

  // Engine with test data
  engineWithData = new USQLEngine();
  await engineWithData.sql(
    "CREATE TABLE departments (id SERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE)",
  );
  await engineWithData.sql(
    "CREATE TABLE employees (" +
      "  id SERIAL PRIMARY KEY," +
      "  dept_id INTEGER REFERENCES departments(id)," +
      "  name TEXT NOT NULL," +
      "  email TEXT UNIQUE" +
      ")",
  );
  await engineWithData.sql("CREATE VIEW dept_summary AS SELECT name FROM departments");
  await engineWithData.sql("CREATE SEQUENCE invoice_seq START 1000 INCREMENT 5");
  await engineWithData.sql("INSERT INTO departments (name) VALUES ('Engineering')");
  await engineWithData.sql(
    "INSERT INTO employees (dept_id, name, email) VALUES (1, 'Alice', 'alice@example.com')",
  );
});

// ======================================================================
// pg_catalog table count
// ======================================================================

describe("PGCatalog table count", () => {
  it("should have at least 35 supported tables", () => {
    const tables = PGCatalogProvider.supportedTables();
    expect(tables.length).toBeGreaterThanOrEqual(35);
  });
});

// ======================================================================
// information_schema view count
// ======================================================================

describe("InformationSchema view count", () => {
  it("should have at least 23 supported views", () => {
    const views = InformationSchemaProvider.supportedViews();
    expect(views.length).toBeGreaterThanOrEqual(23);
  });
});

// ======================================================================
// pg_catalog -- every table returns columns even when empty
// ======================================================================

describe("PGCatalog tables -- column metadata preserved", () => {
  const pgCatalogTables = PGCatalogProvider.supportedTables();

  for (const tableName of pgCatalogTables) {
    it(`SELECT * FROM pg_catalog.${tableName} returns columns`, async () => {
      const r = await engine.sql(`SELECT * FROM pg_catalog.${tableName}`);
      expect(r).not.toBe(null);
      expect(r!.columns.length).toBeGreaterThan(0);
    });
  }
});

// ======================================================================
// information_schema -- every view returns columns even when empty
// ======================================================================

describe("InformationSchema views -- column metadata preserved", () => {
  const infoSchemaViews = InformationSchemaProvider.supportedViews();

  for (const viewName of infoSchemaViews) {
    it(`SELECT * FROM information_schema.${viewName} returns columns`, async () => {
      const r = await engine.sql(`SELECT * FROM information_schema.${viewName}`);
      expect(r).not.toBe(null);
      expect(r!.columns.length).toBeGreaterThan(0);
    });
  }
});

// ======================================================================
// pg_catalog -- tables with data return rows
// ======================================================================

describe("PGCatalog tables with data", () => {
  it("pg_namespace returns rows", async () => {
    const r = await engine.sql("SELECT * FROM pg_catalog.pg_namespace");
    expect(r!.rows.length).toBeGreaterThan(0);
    expect(r!.columns.length).toBeGreaterThan(0);
  });

  it("pg_type returns rows", async () => {
    const r = await engine.sql("SELECT * FROM pg_catalog.pg_type");
    expect(r!.rows.length).toBeGreaterThan(0);
  });

  it("pg_settings returns rows", async () => {
    const r = await engine.sql("SELECT * FROM pg_catalog.pg_settings");
    expect(r!.rows.length).toBeGreaterThan(0);
  });

  it("pg_database returns rows", async () => {
    const r = await engine.sql("SELECT * FROM pg_catalog.pg_database");
    expect(r!.rows.length).toBeGreaterThan(0);
  });

  it("pg_roles returns rows", async () => {
    const r = await engine.sql("SELECT * FROM pg_catalog.pg_roles");
    expect(r!.rows.length).toBeGreaterThan(0);
  });

  it("pg_am returns rows", async () => {
    const r = await engine.sql("SELECT * FROM pg_catalog.pg_am");
    expect(r!.rows.length).toBeGreaterThan(0);
  });

  it("pg_collation returns rows", async () => {
    const r = await engine.sql("SELECT * FROM pg_catalog.pg_collation");
    expect(r!.rows.length).toBeGreaterThan(0);
  });

  it("pg_extension returns rows", async () => {
    const r = await engine.sql("SELECT * FROM pg_catalog.pg_extension");
    expect(r!.rows.length).toBeGreaterThan(0);
  });
});

// ======================================================================
// information_schema -- views with data return rows
// ======================================================================

describe("InformationSchema views with data", () => {
  it("schemata returns rows", async () => {
    const r = await engine.sql("SELECT * FROM information_schema.schemata");
    expect(r!.rows.length).toBeGreaterThan(0);
    expect(r!.columns.length).toBeGreaterThan(0);
  });

  it("enabled_roles returns rows", async () => {
    const r = await engine.sql("SELECT * FROM information_schema.enabled_roles");
    expect(r!.rows.length).toBeGreaterThan(0);
  });

  it("character_sets returns rows", async () => {
    const r = await engine.sql("SELECT * FROM information_schema.character_sets");
    expect(r!.rows.length).toBeGreaterThan(0);
  });
});

// ======================================================================
// pg_catalog with user data -- verify data flows through
// ======================================================================

describe("PGCatalog with user data", () => {
  it("pg_class lists user tables", async () => {
    const r = await engineWithData.sql(
      "SELECT * FROM pg_catalog.pg_class WHERE relnamespace = 2200",
    );
    expect(r!.rows.length).toBeGreaterThan(0);
    expect(r!.columns.length).toBeGreaterThan(0);
  });

  it("pg_attribute lists user columns", async () => {
    const r = await engineWithData.sql(
      "SELECT a.attname FROM pg_catalog.pg_attribute a " +
        "JOIN pg_catalog.pg_class c ON c.oid = a.attrelid " +
        "WHERE c.relname = 'departments' AND a.attnum > 0",
    );
    expect(r!.rows.length).toBeGreaterThan(0);
  });

  it("pg_constraint lists constraints", async () => {
    const r = await engineWithData.sql("SELECT * FROM pg_catalog.pg_constraint");
    expect(r!.rows.length).toBeGreaterThan(0);
  });

  it("pg_index lists indexes", async () => {
    const r = await engineWithData.sql("SELECT * FROM pg_catalog.pg_index");
    expect(r!.rows.length).toBeGreaterThan(0);
  });

  it("pg_stat_user_tables lists stats", async () => {
    const r = await engineWithData.sql("SELECT * FROM pg_catalog.pg_stat_user_tables");
    expect(r!.rows.length).toBeGreaterThan(0);
  });

  it("pg_tables lists user tables", async () => {
    const r = await engineWithData.sql(
      "SELECT * FROM pg_catalog.pg_tables WHERE schemaname = 'public'",
    );
    expect(r!.rows.length).toBeGreaterThan(0);
  });

  it("pg_views lists user views", async () => {
    const r = await engineWithData.sql("SELECT * FROM pg_catalog.pg_views");
    const names = new Set(r!.rows.map((row) => row["viewname"]));
    expect(names).toContain("dept_summary");
  });

  it("pg_sequences lists user sequences", async () => {
    const r = await engineWithData.sql("SELECT * FROM pg_catalog.pg_sequences");
    expect(r!.rows.length).toBeGreaterThan(0);
    const names = new Set(r!.rows.map((row) => row["sequencename"]));
    expect(names).toContain("invoice_seq");
  });
});

// ======================================================================
// information_schema with user data
// ======================================================================

describe("InformationSchema with user data", () => {
  it("tables lists user tables", async () => {
    const r = await engineWithData.sql(
      "SELECT * FROM information_schema.tables WHERE table_schema = 'public'",
    );
    expect(r!.rows.length).toBeGreaterThan(0);
  });

  it("columns lists user columns", async () => {
    const r = await engineWithData.sql(
      "SELECT * FROM information_schema.columns WHERE table_name = 'departments'",
    );
    expect(r!.rows.length).toBeGreaterThan(0);
  });

  it("table_constraints lists constraints", async () => {
    const r = await engineWithData.sql(
      "SELECT * FROM information_schema.table_constraints",
    );
    expect(r!.rows.length).toBeGreaterThan(0);
  });

  it("views lists user views", async () => {
    const r = await engineWithData.sql("SELECT * FROM information_schema.views");
    const names = new Set(r!.rows.map((row) => row["table_name"]));
    expect(names).toContain("dept_summary");
  });

  it("sequences lists user sequences", async () => {
    const r = await engineWithData.sql("SELECT * FROM information_schema.sequences");
    expect(r!.rows.length).toBeGreaterThan(0);
  });
});

// ======================================================================
// Cross-catalog JOINs
// ======================================================================

describe("Cross-catalog JOINs", () => {
  it("JOIN pg_catalog.pg_class with pg_catalog.pg_namespace", async () => {
    const r = await engineWithData.sql(
      "SELECT c.relname, n.nspname " +
        "FROM pg_catalog.pg_class c " +
        "JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid " +
        "WHERE n.nspname = 'public' AND c.relkind = 'r'",
    );
    expect(r!.rows.length).toBeGreaterThan(0);
    const names = new Set(r!.rows.map((row) => row["relname"]));
    expect(names).toContain("departments");
  });

  it("JOIN pg_catalog.pg_attribute with pg_catalog.pg_type", async () => {
    const r = await engineWithData.sql(
      "SELECT a.attname, t.typname " +
        "FROM pg_catalog.pg_attribute a " +
        "JOIN pg_catalog.pg_type t ON a.atttypid = t.oid " +
        "JOIN pg_catalog.pg_class c ON a.attrelid = c.oid " +
        "WHERE c.relname = 'employees' AND a.attnum > 0",
    );
    expect(r!.rows.length).toBeGreaterThan(0);
  });

  it("JOIN pg_catalog.pg_constraint with pg_catalog.pg_class", async () => {
    const r = await engineWithData.sql(
      "SELECT con.conname, c.relname " +
        "FROM pg_catalog.pg_constraint con " +
        "JOIN pg_catalog.pg_class c ON con.conrelid = c.oid",
    );
    expect(r!.rows.length).toBeGreaterThan(0);
  });

  it("JOIN pg_catalog.pg_index with pg_catalog.pg_class", async () => {
    const r = await engineWithData.sql(
      "SELECT idx.relname AS index_name, tbl.relname AS table_name " +
        "FROM pg_catalog.pg_index i " +
        "JOIN pg_catalog.pg_class idx ON i.indexrelid = idx.oid " +
        "JOIN pg_catalog.pg_class tbl ON i.indrelid = tbl.oid",
    );
    expect(r!.rows.length).toBeGreaterThan(0);
  });
});

// ======================================================================
// Empty table column integrity
// ======================================================================

describe("Empty table column integrity", () => {
  it("pg_description has columns when empty", async () => {
    const r = await engine.sql("SELECT * FROM pg_catalog.pg_description");
    expect(r!.rows.length).toBe(0);
    expect(r!.columns.length).toBeGreaterThan(0);
  });

  it("pg_enum has columns when empty", async () => {
    const r = await engine.sql("SELECT * FROM pg_catalog.pg_enum");
    expect(r!.rows.length).toBe(0);
    expect(r!.columns.length).toBeGreaterThan(0);
  });

  it("pg_inherits has columns when empty", async () => {
    const r = await engine.sql("SELECT * FROM pg_catalog.pg_inherits");
    expect(r!.rows.length).toBe(0);
    expect(r!.columns.length).toBeGreaterThan(0);
  });

  it("pg_trigger has columns when empty", async () => {
    const r = await engine.sql("SELECT * FROM pg_catalog.pg_trigger");
    expect(r!.rows.length).toBe(0);
    expect(r!.columns.length).toBeGreaterThan(0);
  });

  it("information_schema.domains has columns when empty", async () => {
    const r = await engine.sql("SELECT * FROM information_schema.domains");
    expect(r!.rows.length).toBe(0);
    expect(r!.columns.length).toBeGreaterThan(0);
  });

  it("information_schema.triggers has columns when empty", async () => {
    const r = await engine.sql("SELECT * FROM information_schema.triggers");
    expect(r!.rows.length).toBe(0);
    expect(r!.columns.length).toBeGreaterThan(0);
  });
});
