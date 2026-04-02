// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
// Copyright (c) 2023-2026 Cognica, Inc.

// PostgreSQL 17 catalog introspection.
//
// Demonstrates querying information_schema and pg_catalog views to
// inspect database structure -- the same queries that tools like
// psql, DBeaver, DataGrip, and SQLAlchemy use internally.

import { USQLEngine } from "../src/core/engine.js";

async function main(): Promise<void> {
  const engine = new USQLEngine();

  // Create a sample schema.
  await engine.sql(`
    CREATE TABLE authors (
      id   SERIAL PRIMARY KEY,
      name TEXT NOT NULL UNIQUE
    )
  `);
  await engine.sql(`
    CREATE TABLE books (
      id        SERIAL PRIMARY KEY,
      title     TEXT NOT NULL,
      author_id INTEGER REFERENCES authors(id),
      isbn      VARCHAR(13) UNIQUE,
      price     NUMERIC,
      published DATE
    )
  `);
  await engine.sql(`
    CREATE VIEW expensive_books AS
    SELECT b.title, b.price, a.name AS author
    FROM books b
    JOIN authors a ON b.author_id = a.id
    WHERE b.price > 30
  `);

  // ---- information_schema.tables --------------------------------------

  console.log("=== information_schema.tables ===");
  const tables = await engine.sql(`
    SELECT table_name, table_type
    FROM information_schema.tables
    WHERE table_schema = 'public'
    ORDER BY table_name
  `);
  for (const row of tables!.rows) {
    console.log(`  ${String(row["table_name"]).padEnd(20)} ${row["table_type"]}`);
  }

  // ---- information_schema.columns -------------------------------------

  console.log("\n=== information_schema.columns (books) ===");
  const columns = await engine.sql(`
    SELECT column_name, data_type, is_nullable, column_default
    FROM information_schema.columns
    WHERE table_name = 'books'
    ORDER BY ordinal_position
  `);
  for (const row of columns!.rows) {
    const nullable = row["is_nullable"] === "YES" ? "NULL" : "NOT NULL";
    const dflt = row["column_default"] ? ` DEFAULT ${row["column_default"]}` : "";
    console.log(
      `  ${String(row["column_name"]).padEnd(15)} `
      + `${String(row["data_type"]).padEnd(20)} ${nullable}${dflt}`,
    );
  }

  // ---- information_schema.table_constraints ---------------------------

  console.log("\n=== information_schema.table_constraints ===");
  const constraints = await engine.sql(`
    SELECT constraint_name, table_name, constraint_type
    FROM information_schema.table_constraints
    WHERE table_schema = 'public'
    ORDER BY table_name, constraint_type
  `);
  for (const row of constraints!.rows) {
    console.log(
      `  ${String(row["constraint_name"]).padEnd(30)} `
      + `${String(row["table_name"]).padEnd(15)} `
      + `${row["constraint_type"]}`,
    );
  }

  // ---- information_schema.key_column_usage -----------------------------

  console.log("\n=== Foreign key details ===");
  const fkeys = await engine.sql(`
    SELECT
      tc.constraint_name,
      kcu.column_name,
      ccu.table_name AS referenced_table,
      ccu.column_name AS referenced_column
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
    JOIN information_schema.constraint_column_usage ccu
      ON tc.constraint_name = ccu.constraint_name
    WHERE tc.constraint_type = 'FOREIGN KEY'
  `);
  for (const row of fkeys!.rows) {
    console.log(
      `  ${row["constraint_name"]}: `
      + `${row["column_name"]} -> `
      + `${row["referenced_table"]}.${row["referenced_column"]}`,
    );
  }

  // ---- pg_catalog.pg_class + pg_attribute + pg_type -------------------

  console.log("\n=== pg_catalog: columns with type OIDs ===");
  const pgcols = await engine.sql(`
    SELECT
      c.relname AS table_name,
      a.attname AS column_name,
      t.typname AS type_name,
      a.atttypid AS type_oid,
      a.attnotnull AS not_null
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_attribute a ON c.oid = a.attrelid
    JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
    WHERE c.relname = 'books' AND a.attnum > 0
    ORDER BY a.attnum
  `);
  for (const row of pgcols!.rows) {
    const nn = row["not_null"] ? "NOT NULL" : "";
    console.log(
      `  ${String(row["column_name"]).padEnd(15)} `
      + `${String(row["type_name"]).padEnd(15)} `
      + `(OID ${row["type_oid"]}) ${nn}`,
    );
  }

  // ---- pg_catalog.pg_indexes ------------------------------------------

  console.log("\n=== pg_catalog.pg_indexes ===");
  const indexes = await engine.sql(`
    SELECT schemaname, tablename, indexname
    FROM pg_catalog.pg_indexes
    WHERE schemaname = 'public'
    ORDER BY tablename, indexname
  `);
  for (const row of indexes!.rows) {
    console.log(`  ${String(row["tablename"]).padEnd(15)} ${row["indexname"]}`);
  }

  // ---- pg_catalog.pg_views --------------------------------------------

  console.log("\n=== pg_catalog.pg_views ===");
  const views = await engine.sql(`
    SELECT viewname, definition
    FROM pg_catalog.pg_views
    WHERE schemaname = 'public'
  `);
  for (const row of views!.rows) {
    const defn = String(row["definition"] ?? "").substring(0, 60);
    console.log(`  ${String(row["viewname"]).padEnd(20)} ${defn}...`);
  }

  // ---- pg_catalog.pg_settings -----------------------------------------

  console.log("\n=== pg_catalog.pg_settings (sample) ===");
  const settings = await engine.sql(`
    SELECT name, setting
    FROM pg_catalog.pg_settings
    WHERE name IN ('server_version', 'server_encoding', 'DateStyle',
                   'TimeZone', 'standard_conforming_strings')
    ORDER BY name
  `);
  for (const row of settings!.rows) {
    console.log(`  ${String(row["name"]).padEnd(35)} ${row["setting"]}`);
  }
}

main();
