// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
// Copyright (c) 2023-2026 Cognica, Inc.

// PGWire server with authentication.
//
// Demonstrates all four authentication methods supported by usqldb:
// trust, cleartext password, MD5, and SCRAM-SHA-256.
//
// Usage:
//     npx tsx examples/05_pgwire_auth.ts
//
// Then connect with:
//     psql -h 127.0.0.1 -p 15432 -U admin -d uqa
//     (password: secret123)

import { PGWireServer } from "../src/net/pgwire/server.js";
import { createConfig } from "../src/net/pgwire/config.js";
import { AuthMethod } from "../src/net/pgwire/auth.js";

// User credentials shared across examples.
const CREDENTIALS: Record<string, string> = {
  admin: "secret123",
  reader: "readonly",
};

async function runServer(method: AuthMethod): Promise<void> {
  const config = createConfig({
    host: "127.0.0.1",
    port: 15432,
    authMethod: method,
    credentials: CREDENTIALS,
  });

  const server = new PGWireServer(config);
  await server.start();
  console.log(`Server listening on ${config.host}:${server.port}`);
  console.log(`Auth method: ${method}`);
  console.log(`Users: ${Object.keys(CREDENTIALS).join(", ")}`);
  console.log("Connect with:  psql -h 127.0.0.1 -p 15432 -U admin -d uqa");
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
}

async function main(): Promise<void> {
  console.log("Available authentication methods:");
  for (const m of Object.values(AuthMethod)) {
    console.log(`  - ${m}`);
  }
  console.log();

  // Default to SCRAM-SHA-256 (PostgreSQL 17 default).
  const method = AuthMethod.SCRAM_SHA_256;
  console.log(`Starting server with ${method} authentication...\n`);

  await runServer(method);
  console.log("\nServer stopped.");
}

main();
