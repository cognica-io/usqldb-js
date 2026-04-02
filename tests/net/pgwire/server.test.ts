//
// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
//
// Copyright (c) 2023-2026 Cognica, Inc.
//

// Integration tests for PGWireServer with node-postgres client.

import { describe, it, expect, afterAll, beforeAll } from "vitest";
import pg from "pg";
import { USQLEngine } from "../../../src/core/engine.js";
import { createConfig } from "../../../src/net/pgwire/config.js";
import { PGWireServer } from "../../../src/net/pgwire/server.js";

const { Client } = pg;

// ======================================================================
// Server fixture helpers
// ======================================================================

interface ServerHandle {
  server: PGWireServer;
  port: number;
  connString: string;
}

async function startServer(engineFactory?: () => unknown): Promise<ServerHandle> {
  const config = createConfig({
    host: "127.0.0.1",
    port: 0,
    ...(engineFactory ? { engineFactory } : {}),
  });
  const server = new PGWireServer(config);
  await server.start();
  const port = server.port;
  const connString = `postgresql://uqa@127.0.0.1:${port}/uqa`;
  return { server, port, connString };
}

// ======================================================================
// TestBasicConnection
// ======================================================================

describe("TestBasicConnection", () => {
  let handle: ServerHandle;

  beforeAll(async () => {
    handle = await startServer();
  });

  afterAll(async () => {
    await handle.server.stop();
  }, 15000);

  it("test_connect_and_disconnect", async () => {
    const client = new Client({ connectionString: handle.connString });
    await client.connect();
    const res = await client.query("SELECT 1 AS num");
    expect(res.rows.length).toBeGreaterThanOrEqual(0);
    await client.end();
  });

  it("test_simple_select", async () => {
    const client = new Client({ connectionString: handle.connString });
    await client.connect();
    const res = await client.query("SELECT 1 AS num");
    expect(res.rows[0]).toBeDefined();
    expect(res.rows[0]!.num).toBe(1);
    await client.end();
  });

  it("test_select_multiple_columns", async () => {
    const client = new Client({ connectionString: handle.connString });
    await client.connect();
    const res = await client.query("SELECT 1 AS a, 'hello' AS b, 3.14 AS c");
    const row = res.rows[0]!;
    expect(row.a).toBe(1);
    expect(row.b).toBe("hello");
    await client.end();
  });
});

// ======================================================================
// TestDDLAndDML
// ======================================================================

describe("TestDDLAndDML", () => {
  let handle: ServerHandle;

  beforeAll(async () => {
    handle = await startServer();
  });

  afterAll(async () => {
    await handle.server.stop();
  }, 15000);

  it("test_create_table_and_insert", async () => {
    const client = new Client({ connectionString: handle.connString });
    await client.connect();
    await client.query(
      "CREATE TABLE test_table (  id SERIAL PRIMARY KEY,  name TEXT NOT NULL)",
    );
    await client.query("INSERT INTO test_table (name) VALUES ('Alice')");
    await client.query("INSERT INTO test_table (name) VALUES ('Bob')");

    const res = await client.query("SELECT name FROM test_table ORDER BY id");
    expect(res.rows.length).toBe(2);
    expect(res.rows[0]!.name).toBe("Alice");
    expect(res.rows[1]!.name).toBe("Bob");
    await client.end();
  });

  it("test_update", async () => {
    const client = new Client({ connectionString: handle.connString });
    await client.connect();
    await client.query("CREATE TABLE upd (id INTEGER PRIMARY KEY, val TEXT)");
    await client.query("INSERT INTO upd (id, val) VALUES (1, 'old')");
    await client.query("UPDATE upd SET val = 'new' WHERE id = 1");
    const res = await client.query("SELECT val FROM upd WHERE id = 1");
    expect(res.rows[0]!.val).toBe("new");
    await client.end();
  });

  it("test_delete", async () => {
    const client = new Client({ connectionString: handle.connString });
    await client.connect();
    await client.query("CREATE TABLE del_test (id INTEGER PRIMARY KEY)");
    await client.query("INSERT INTO del_test (id) VALUES (1)");
    await client.query("INSERT INTO del_test (id) VALUES (2)");
    await client.query("DELETE FROM del_test WHERE id = 1");
    const res = await client.query("SELECT COUNT(*) FROM del_test");
    expect(Number(res.rows[0]!.count)).toBe(1);
    await client.end();
  });
});

// ======================================================================
// TestPGCatalog
// ======================================================================

describe("TestPGCatalog", () => {
  let handle: ServerHandle;

  beforeAll(async () => {
    handle = await startServer();
  });

  afterAll(async () => {
    await handle.server.stop();
  }, 15000);

  it("test_pg_tables", async () => {
    const client = new Client({ connectionString: handle.connString });
    await client.connect();
    await client.query("CREATE TABLE catalog_test (id INTEGER PRIMARY KEY)");
    const res = await client.query(
      "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public'",
    );
    const tables = new Set(
      res.rows.map((row: Record<string, unknown>) => row.tablename),
    );
    expect(tables).toContain("catalog_test");
    await client.end();
  });

  it("test_information_schema_columns", async () => {
    const client = new Client({ connectionString: handle.connString });
    await client.connect();
    await client.query("CREATE TABLE col_test (id INTEGER, name TEXT, score REAL)");
    const res = await client.query(
      "SELECT column_name, data_type " +
        "FROM information_schema.columns " +
        "WHERE table_name = 'col_test' " +
        "ORDER BY ordinal_position",
    );
    expect(res.rows.length).toBe(3);
    expect(res.rows[0]!.column_name).toBe("id");
    expect(res.rows[1]!.column_name).toBe("name");
    expect(res.rows[2]!.column_name).toBe("score");
    await client.end();
  });
});

// ======================================================================
// TestSetShow
// ======================================================================

describe("TestSetShow", () => {
  let handle: ServerHandle;

  beforeAll(async () => {
    handle = await startServer();
  });

  afterAll(async () => {
    await handle.server.stop();
  }, 15000);

  it("test_set_and_show", async () => {
    const client = new Client({ connectionString: handle.connString });
    await client.connect();
    await client.query("SET search_path TO public");
    const res = await client.query("SHOW search_path");
    expect(res.rows[0]).toBeDefined();
    expect(res.rows[0]!.search_path).toBe("public");
    await client.end();
  });

  it("test_show_server_version", async () => {
    const client = new Client({ connectionString: handle.connString });
    await client.connect();
    const res = await client.query("SHOW server_version");
    expect(res.rows[0]).toBeDefined();
    expect(String(res.rows[0]!.server_version)).toContain("17");
    await client.end();
  });
});

// ======================================================================
// TestExtendedProtocol
// ======================================================================

describe("TestExtendedProtocol", () => {
  let handle: ServerHandle;

  beforeAll(async () => {
    handle = await startServer();
  });

  afterAll(async () => {
    await handle.server.stop();
  }, 15000);

  it("test_prepared_statement", async () => {
    const client = new Client({ connectionString: handle.connString });
    await client.connect();
    await client.query("CREATE TABLE prep_test (id INTEGER PRIMARY KEY, name TEXT)");
    await client.query("INSERT INTO prep_test (id, name) VALUES (1, 'Alice')");
    await client.query("INSERT INTO prep_test (id, name) VALUES (2, 'Bob')");

    // node-postgres uses extended protocol for parameterized queries.
    const res = await client.query("SELECT name FROM prep_test WHERE id = $1", [1]);
    expect(res.rows[0]).toBeDefined();
    expect(res.rows[0]!.name).toBe("Alice");
    await client.end();
  });
});

// ======================================================================
// TestEmptyQuery
// ======================================================================

describe("TestEmptyQuery", () => {
  let handle: ServerHandle;

  beforeAll(async () => {
    handle = await startServer();
  });

  afterAll(async () => {
    await handle.server.stop();
  }, 15000);

  it("test_empty_string", async () => {
    const client = new Client({ connectionString: handle.connString });
    await client.connect();
    const res = await client.query("");
    // node-postgres returns an empty result for empty queries.
    expect(
      res.command === null ||
        res.command === undefined ||
        res.command === "" ||
        res.rows.length === 0,
    ).toBe(true);
    await client.end();
  });

  it("test_semicolons_only", async () => {
    const client = new Client({ connectionString: handle.connString });
    await client.connect();
    const res = await client.query(";;;");
    expect(
      res.command === null ||
        res.command === undefined ||
        res.command === "" ||
        res.rows.length === 0,
    ).toBe(true);
    await client.end();
  });
});

// ======================================================================
// TestMultipleConnections
// ======================================================================

describe("TestMultipleConnections", () => {
  let handle: ServerHandle;

  beforeAll(async () => {
    handle = await startServer();
  });

  afterAll(async () => {
    await handle.server.stop();
  }, 15000);

  it("test_two_connections", async () => {
    const client1 = new Client({ connectionString: handle.connString });
    const client2 = new Client({ connectionString: handle.connString });

    await client1.connect();
    await client2.connect();

    await client1.query("CREATE TABLE multi_test (id INTEGER)");
    await client1.query("INSERT INTO multi_test (id) VALUES (1)");

    // conn2 has its own engine, so it will not see conn1's table.
    // This is expected with per-connection engines.
    await client1.end();
    await client2.end();
  });
});

// ======================================================================
// TestErrorHandling
// ======================================================================

describe("TestErrorHandling", () => {
  let handle: ServerHandle;

  beforeAll(async () => {
    handle = await startServer();
  });

  afterAll(async () => {
    await handle.server.stop();
  }, 15000);

  it("test_syntax_error", async () => {
    const client = new Client({ connectionString: handle.connString });
    await client.connect();
    await expect(client.query("SELET 1")).rejects.toThrow();
    // Connection should still be usable after error.
    const res = await client.query("SELECT 1");
    expect(Number(res.rows[0]!["?column?"] ?? res.rows[0]!["1"])).toBe(1);
    await client.end();
  });

  it("test_table_not_found", async () => {
    const client = new Client({ connectionString: handle.connString });
    await client.connect();
    await expect(client.query("SELECT * FROM nonexistent_table")).rejects.toThrow();
    await client.end();
  });
});

// ======================================================================
// TestSharedEngine
// ======================================================================

describe("TestSharedEngine", () => {
  let handle: ServerHandle;

  beforeAll(async () => {
    const sharedEngine = new USQLEngine();
    handle = await startServer(() => sharedEngine);
  });

  afterAll(async () => {
    await handle.server.stop();
  }, 15000);

  it("test_shared_engine_visibility", async () => {
    const client1 = new Client({ connectionString: handle.connString });
    await client1.connect();
    await client1.query("CREATE TABLE shared_test (id INTEGER PRIMARY KEY)");
    await client1.query("INSERT INTO shared_test (id) VALUES (1)");
    await client1.end();

    // With a shared engine, a second connection should see the table.
    const client2 = new Client({ connectionString: handle.connString });
    await client2.connect();
    const res = await client2.query("SELECT id FROM shared_test");
    expect(res.rows.length).toBe(1);
    expect(Number(res.rows[0]!.id)).toBe(1);
    await client2.end();
  });
});
