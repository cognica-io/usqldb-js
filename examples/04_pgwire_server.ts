// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
// Copyright (c) 2023-2026 Cognica, Inc.

// PGWire server: basic startup.
//
// Starts a PostgreSQL-compatible TCP server that accepts connections
// from any standard PostgreSQL client.
//
// Usage:
//     npx tsx examples/04_pgwire_server.ts
//
// Then connect with:
//     psql -h 127.0.0.1 -p 15432 -U uqa -d uqa

import { PGWireServer } from "../src/net/pgwire/server.js";
import { createConfig } from "../src/net/pgwire/config.js";

async function main(): Promise<void> {
  const config = createConfig({
    host: "127.0.0.1",
    port: 15432,
  });

  const server = new PGWireServer(config);
  await server.start();
  console.log(`usqldb pgwire server listening on ${config.host}:${server.port}`);
  console.log("Connect with:  psql -h 127.0.0.1 -p 15432 -U uqa -d uqa");
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
