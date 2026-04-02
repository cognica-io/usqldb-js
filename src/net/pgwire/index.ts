// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
// Copyright (c) 2023-2026 Cognica, Inc.

// PostgreSQL v3 wire protocol server for usqldb.
//
// This module implements a full PostgreSQL 17-compatible wire protocol
// server that enables standard PostgreSQL clients (psql, psycopg,
// asyncpg, SQLAlchemy, JDBC, DBeaver, DataGrip, Django, etc.) to
// connect to usqldb over TCP.
//
// Quick start:
//
//     import { PGWireServer, createConfig } from "usqldb/net/pgwire";
//
//     const config = createConfig({ host: "0.0.0.0", port: 5432 });
//     const server = new PGWireServer(config);
//     await server.start();
//
// With authentication:
//
//     import { PGWireServer, createConfig, AuthMethod } from "usqldb/net/pgwire";
//
//     const config = createConfig({
//         host: "0.0.0.0",
//         port: 5432,
//         authMethod: AuthMethod.SCRAM_SHA_256,
//         credentials: { admin: "secret123" },
//     });
//     const server = new PGWireServer(config);
//     await server.start();

export { AuthMethod } from "./auth.js";
export type { PGWireConfig } from "./config.js";
export { createConfig } from "./config.js";
export { PGWireServer } from "./server.js";
