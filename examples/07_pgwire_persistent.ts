// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
// Copyright (c) 2023-2026 Cognica, Inc.

// PGWire server with persistent storage.
//
// Starts a pgwire server backed by a file-based database so data
// survives server restarts.
//
// Usage:
//     npx tsx examples/07_pgwire_persistent.ts
//
// Then connect with:
//     psql -h 127.0.0.1 -p 15432 -U uqa -d uqa

import * as os from "node:os";
import * as path from "node:path";

import { PGWireServer } from "../src/net/pgwire/server.js";
import { createConfig } from "../src/net/pgwire/config.js";

const DB_PATH = path.join(os.tmpdir(), "usqldb_server_example.db");

async function main(): Promise<void> {
  const config = createConfig({
    host: "127.0.0.1",
    port: 15432,
    dbPath: DB_PATH,
  });

  const server = new PGWireServer(config);
  await server.start();
  console.log(`Server listening on ${config.host}:${server.port}`);
  console.log(`Database: ${DB_PATH}`);
  console.log();
  console.log("Connect with:  psql -h 127.0.0.1 -p 15432 -U uqa -d uqa");
  console.log();
  console.log("Data persists across server restarts.");
  console.log("Press Ctrl+C to stop.\n");

  await new Promise<void>((resolve) => {
    process.on("SIGINT", () => {
      resolve();
    });
    process.on("SIGTERM", () => {
      resolve();
    });
  });

  await server.stop();
  console.log(`\nDatabase saved to ${DB_PATH}`);
  console.log("\nServer stopped.");
}

main();
