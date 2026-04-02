// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
// Copyright (c) 2023-2026 Cognica, Inc.

// PGWire server with a shared engine across all connections.
//
// By default, each connection gets its own USQLEngine instance.
// This example demonstrates sharing a single engine so all connections
// see the same data -- useful for multi-client scenarios.
//
// Usage:
//     npx tsx examples/08_pgwire_shared_engine.ts
//
// Then connect with multiple psql sessions:
//     psql -h 127.0.0.1 -p 15432 -U uqa -d uqa

import { USQLEngine } from "../src/core/engine.js";
import { PGWireServer } from "../src/net/pgwire/server.js";
import { createConfig } from "../src/net/pgwire/config.js";

async function main(): Promise<void> {
  // Create a single shared engine.
  const sharedEngine = new USQLEngine();

  // Pre-populate with sample data.
  await sharedEngine.sql(`
    CREATE TABLE messages (
      id   SERIAL PRIMARY KEY,
      text TEXT NOT NULL
    )
  `);
  await sharedEngine.sql("INSERT INTO messages (text) VALUES ('Hello from the server!')");

  const config = createConfig({
    host: "127.0.0.1",
    port: 15432,
    engineFactory: () => sharedEngine,
  });

  const server = new PGWireServer(config);
  await server.start();
  console.log(`Server listening on ${config.host}:${server.port}`);
  console.log("All connections share the same engine instance.");
  console.log();
  console.log("Try connecting with multiple psql sessions:");
  console.log("  psql -h 127.0.0.1 -p 15432 -U uqa -d uqa");
  console.log();
  console.log("Insert in one session, query in another -- data is shared.");
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
  console.log("\nServer stopped.");
}

main();
