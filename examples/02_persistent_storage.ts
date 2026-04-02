// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
// Copyright (c) 2023-2026 Cognica, Inc.

// Persistent storage with dbPath.
//
// Demonstrates using USQLEngine with a file-based database that survives
// across process restarts.

import * as os from "node:os";
import * as path from "node:path";
import * as fs from "node:fs";

import { USQLEngine } from "../src/core/engine.js";

const DB_PATH = path.join(os.tmpdir(), "usqldb_example.db");

async function createAndPopulate(): Promise<void> {
  // First run: create schema and insert data.
  const engine = new USQLEngine({ dbPath: DB_PATH });

  await engine.sql(`
    CREATE TABLE IF NOT EXISTS notes (
      id      SERIAL PRIMARY KEY,
      title   TEXT NOT NULL,
      body    TEXT
    )
  `);

  await engine.sql("INSERT INTO notes (title, body) VALUES ('Hello', 'First note')");
  await engine.sql("INSERT INTO notes (title, body) VALUES ('TODO', 'Buy groceries')");
  await engine.sql("INSERT INTO notes (title, body) VALUES ('Idea', 'Build something cool')");

  const result = await engine.sql("SELECT COUNT(*) AS cnt FROM notes");
  console.log(`Created ${result!.rows[0]["cnt"]} notes in ${DB_PATH}`);
  engine.close();
}

async function readBack(): Promise<void> {
  // Second run: read data from the existing database.
  const engine = new USQLEngine({ dbPath: DB_PATH });

  const result = await engine.sql("SELECT id, title, body FROM notes ORDER BY id");
  console.log("\nNotes from persistent storage:");
  for (const row of result!.rows) {
    console.log(`  [${row["id"]}] ${row["title"]}: ${row["body"]}`);
  }

  engine.close();
}

async function main(): Promise<void> {
  await createAndPopulate();
  await readBack();

  // Cleanup
  if (fs.existsSync(DB_PATH)) {
    fs.unlinkSync(DB_PATH);
    console.log(`\nCleaned up ${DB_PATH}`);
  }
}

main();
