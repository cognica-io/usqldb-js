// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
// Copyright (c) 2023-2026 Cognica, Inc.

// Advanced schema features: views, sequences, foreign keys, check constraints.
//
// Demonstrates the full range of DDL features supported by usqldb,
// and how they appear in the PostgreSQL catalog system.

import { USQLEngine } from "../src/core/engine.js";

async function main(): Promise<void> {
  const engine = new USQLEngine();

  // ---- Schema with multiple constraint types --------------------------

  await engine.sql(`
    CREATE TABLE categories (
      id   SERIAL PRIMARY KEY,
      name TEXT NOT NULL UNIQUE,
      slug TEXT NOT NULL UNIQUE
    )
  `);

  await engine.sql(`
    CREATE TABLE products (
      id          SERIAL PRIMARY KEY,
      name        TEXT NOT NULL,
      category_id INTEGER REFERENCES categories(id),
      price       NUMERIC NOT NULL,
      weight_kg   REAL,
      sku         VARCHAR(20) UNIQUE,
      CHECK (price > 0)
    )
  `);

  await engine.sql(`
    CREATE TABLE order_items (
      id         SERIAL PRIMARY KEY,
      product_id INTEGER REFERENCES products(id),
      quantity   INTEGER NOT NULL,
      unit_price NUMERIC NOT NULL,
      CHECK (quantity > 0),
      CHECK (unit_price >= 0)
    )
  `);

  // ---- Views ----------------------------------------------------------

  await engine.sql(`
    CREATE VIEW product_catalog AS
    SELECT
      p.id,
      p.name,
      c.name AS category,
      p.price,
      p.sku
    FROM products p
    JOIN categories c ON p.category_id = c.id
  `);

  await engine.sql(`
    CREATE VIEW recent_orders AS
    SELECT
      oi.id AS order_item_id,
      p.name AS product_name,
      oi.quantity,
      oi.unit_price
    FROM order_items oi
    JOIN products p ON oi.product_id = p.id
  `);

  // ---- Sequences ------------------------------------------------------

  await engine.sql("CREATE SEQUENCE invoice_number_seq START 1000 INCREMENT 1");

  // ---- Populate data --------------------------------------------------

  await engine.sql(
    "INSERT INTO categories (name, slug) VALUES ('Electronics', 'electronics')",
  );
  await engine.sql("INSERT INTO categories (name, slug) VALUES ('Books', 'books')");
  await engine.sql("INSERT INTO categories (name, slug) VALUES ('Clothing', 'clothing')");

  await engine.sql(
    "INSERT INTO products (name, category_id, price, weight_kg, sku) "
    + "VALUES ('Laptop', 1, 999.99, 2.1, 'ELEC-001')",
  );
  await engine.sql(
    "INSERT INTO products (name, category_id, price, weight_kg, sku) "
    + "VALUES ('Python Book', 2, 49.99, 0.8, 'BOOK-001')",
  );
  await engine.sql(
    "INSERT INTO products (name, category_id, price, weight_kg, sku) "
    + "VALUES ('T-Shirt', 3, 19.99, 0.2, 'CLTH-001')",
  );

  await engine.sql(
    "INSERT INTO order_items (product_id, quantity, unit_price) VALUES (1, 2, 999.99)",
  );
  await engine.sql(
    "INSERT INTO order_items (product_id, quantity, unit_price) VALUES (2, 5, 49.99)",
  );
  await engine.sql(
    "INSERT INTO order_items (product_id, quantity, unit_price) VALUES (3, 10, 19.99)",
  );
  await engine.sql(
    "INSERT INTO order_items (product_id, quantity, unit_price) VALUES (1, 1, 899.99)",
  );

  // ---- Query views ----------------------------------------------------

  console.log("=== Product catalog (view) ===");
  const catalog = await engine.sql("SELECT * FROM product_catalog ORDER BY price DESC");
  for (const row of catalog!.rows) {
    const name = String(row["name"]).padEnd(15);
    const category = String(row["category"]).padEnd(12);
    const price = Number(row["price"]).toFixed(2).padStart(8);
    console.log(`  ${name} ${category} $${price}  SKU=${row["sku"]}`);
  }

  console.log("\n=== Recent orders (view) ===");
  const orders = await engine.sql("SELECT * FROM recent_orders ORDER BY order_item_id");
  for (const row of orders!.rows) {
    const productName = String(row["product_name"]).padEnd(15);
    const unitPrice = Number(row["unit_price"]).toFixed(2).padStart(8);
    console.log(
      `  #${row["order_item_id"]}  ${productName} `
      + `qty=${row["quantity"]}  $${unitPrice}`,
    );
  }

  // ---- Introspect constraints via pg_catalog --------------------------

  console.log("\n=== All constraints ===");
  const conResult = await engine.sql(`
    SELECT
      c.conname AS constraint_name,
      cl.relname AS table_name,
      c.contype AS type
    FROM pg_catalog.pg_constraint c
    JOIN pg_catalog.pg_class cl ON c.conrelid = cl.oid
    ORDER BY cl.relname, c.contype, c.conname
  `);
  const typeLabels: Record<string, string> = {
    p: "PRIMARY KEY",
    u: "UNIQUE",
    f: "FOREIGN KEY",
    c: "CHECK",
  };
  for (const row of conResult!.rows) {
    const label = typeLabels[row["type"] as string] ?? String(row["type"]);
    console.log(
      `  ${String(row["table_name"]).padEnd(15)} `
      + `${label.padEnd(15)} `
      + `${row["constraint_name"]}`,
    );
  }

  // ---- Introspect views -----------------------------------------------

  console.log("\n=== Views in information_schema ===");
  const viewsResult = await engine.sql(`
    SELECT table_name, view_definition
    FROM information_schema.views
    WHERE table_schema = 'public'
    ORDER BY table_name
  `);
  for (const row of viewsResult!.rows) {
    const defn = String(row["view_definition"] ?? "").substring(0, 70);
    console.log(`  ${String(row["table_name"]).padEnd(20)} ${defn}...`);
  }

  // ---- Introspect sequences -------------------------------------------

  console.log("\n=== Sequences ===");
  const seqResult = await engine.sql(`
    SELECT sequence_name, start_value, increment
    FROM information_schema.sequences
    WHERE sequence_schema = 'public'
    ORDER BY sequence_name
  `);
  for (const row of seqResult!.rows) {
    console.log(
      `  ${String(row["sequence_name"]).padEnd(25)} `
      + `START ${row["start_value"]}  `
      + `INCREMENT ${row["increment"]}`,
    );
  }
}

main();
