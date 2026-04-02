// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
// Copyright (c) 2023-2026 Cognica, Inc.

// CLI entry point for the usqldb interactive shell command.
//
// Usage:
//     usqldb                        Start with an in-memory database
//     usqldb --db mydata.db         Start with persistent storage
//     usqldb script.sql             Execute a SQL script then enter REPL
//     usqldb --db mydata.db s.sql   Persistent + script
//     usqldb -c "SELECT 1"          Execute a command string and exit

import * as fs from "node:fs";

import { USQLShell } from "./shell.js";

/**
 * Entry point for the usqldb interactive shell command.
 */
export async function main(argv?: string[]): Promise<void> {
  const args = argv ?? process.argv.slice(2);

  let dbPath: string | undefined;
  let command: string | undefined;
  const scripts: string[] = [];

  // Parse arguments manually
  for (let i = 0; i < args.length; i++) {
    const arg = args[i]!;
    if (arg === "--db") {
      i++;
      dbPath = args[i];
    } else if (arg === "-c") {
      i++;
      command = args[i];
    } else if (arg.startsWith("--db=")) {
      dbPath = arg.slice("--db=".length);
    } else if (arg.startsWith("-c=")) {
      command = arg.slice("-c=".length);
    } else if (arg === "--help" || arg === "-h") {
      console.log(
        "usqldb -- interactive SQL shell (PostgreSQL 17 compatible)\n" +
          "\n" +
          "Usage:\n" +
          "  usqldb [options] [script.sql ...]\n" +
          "\n" +
          "Options:\n" +
          "  --db PATH    SQLite database file for persistent storage\n" +
          "  -c COMMAND   Execute a single SQL command string and exit\n" +
          "  -h, --help   Show this help message",
      );
      return;
    } else {
      scripts.push(arg);
    }
  }

  const shell = new USQLShell({ dbPath });

  // -c: execute command and exit
  if (command !== undefined) {
    try {
      await shell._executeText(command);
    } finally {
      await shell.close();
    }
    return;
  }

  for (const scriptPath of scripts) {
    if (!fs.existsSync(scriptPath)) {
      console.error(`File not found: ${scriptPath}`);
      process.exitCode = 1;
      await shell.close();
      return;
    }
    try {
      await shell.runFileAsync(scriptPath);
    } catch (err) {
      console.error(`Error executing ${scriptPath}: ${err}`);
      process.exitCode = 1;
      await shell.close();
      return;
    }
  }

  try {
    if (process.stdin.isTTY) {
      await shell.run();
    } else if (!scripts.length) {
      await shell.run();
    }
  } finally {
    await shell.close();
  }
}
