// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
// Copyright (c) 2023-2026 Cognica, Inc.

// Connecting to usqldb with pg (Node.js PostgreSQL client).
//
// Starts an in-process pgwire server and demonstrates using the pg
// module to perform DDL, DML, queries, and catalog introspection --
// the same way you would interact with a real PostgreSQL database.
//
// Requirements:
//     npm install pg @types/pg

import { PGWireServer } from "../src/net/pgwire/server.js";
import { createConfig } from "../src/net/pgwire/config.js";

import pg from "pg";
const { Client } = pg;

async function startServer(): Promise<[PGWireServer, number]> {
  // Start a pgwire server on an ephemeral port.
  const config = createConfig({ host: "127.0.0.1", port: 0 });
  const server = new PGWireServer(config);
  await server.start();
  return [server, server.port];
}

async function main(): Promise<void> {
  const [server, port] = await startServer();
  console.log(`Server running on port ${port}\n`);

  const client = new Client({
    host: "127.0.0.1",
    port,
    user: "uqa",
    database: "uqa",
  });
  await client.connect();

  // ---- DDL ------------------------------------------------------------

  await client.query(`
    CREATE TABLE products (
      id    SERIAL PRIMARY KEY,
      name  TEXT NOT NULL,
      price REAL NOT NULL,
      stock INTEGER DEFAULT 0
    )
  `);
  console.log("Created table: products");

  // ---- INSERT ---------------------------------------------------------

  const products: [string, number, number][] = [
    ["Laptop", 999.99, 50],
    ["Mouse", 29.99, 200],
    ["Keyboard", 79.99, 150],
    ["Monitor", 449.99, 75],
    ["Headphones", 149.99, 120],
  ];
  for (const [name, price, stock] of products) {
    await client.query(
      "INSERT INTO products (name, price, stock) VALUES ($1, $2, $3)",
      [name, price, stock],
    );
  }
  console.log(`Inserted ${products.length} products\n`);

  // ---- SELECT ---------------------------------------------------------

  console.log("=== Products (price > 100) ===");
  const selectResult = await client.query(
    "SELECT name, price, stock FROM products WHERE price > $1 ORDER BY price DESC",
    [100],
  );
  for (const row of selectResult.rows) {
    const name = String(row.name).padEnd(15);
    const price = Number(row.price).toFixed(2).padStart(8);
    console.log(`  ${name} $${price}  stock=${row.stock}`);
  }

  // ---- Aggregation ----------------------------------------------------

  console.log("\n=== Summary ===");
  const aggResult = await client.query(`
    SELECT
      COUNT(*) AS total_products,
      SUM(stock) AS total_stock,
      AVG(price) AS avg_price,
      MIN(price) AS min_price,
      MAX(price) AS max_price
    FROM products
  `);
  const agg = aggResult.rows[0];
  console.log(`  Products: ${agg.total_products}`);
  console.log(`  Total stock: ${agg.total_stock}`);
  console.log(`  Avg price: $${Number(agg.avg_price).toFixed(2)}`);
  console.log(`  Price range: $${Number(agg.min_price).toFixed(2)} - $${Number(agg.max_price).toFixed(2)}`);

  // ---- Catalog introspection via pg -----------------------------------

  console.log("\n=== Column metadata (via information_schema) ===");
  const colResult = await client.query(`
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_name = 'products'
    ORDER BY ordinal_position
  `);
  for (const row of colResult.rows) {
    console.log(
      `  ${String(row.column_name).padEnd(10)} `
      + `${String(row.data_type).padEnd(15)} `
      + `nullable=${row.is_nullable}`,
    );
  }

  // ---- Server version -------------------------------------------------

  const versionResult = await client.query("SHOW server_version");
  console.log(`\nServer version: ${versionResult.rows[0].server_version}`);

  await client.end();
  await server.stop();
  console.log("\nDone.");
}

main();
