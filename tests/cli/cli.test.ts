//
// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
//
// Copyright (c) 2023-2026 Cognica, Inc.
//

// Tests for the usqldb CLI module (formatter, commands, shell).

import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it, expect, beforeEach, vi } from "vitest";
import { Formatter } from "../../src/cli/formatter.js";
import { CommandHandler } from "../../src/cli/command-handler.js";
import { USQLShell } from "../../src/cli/shell.js";
import { USQLEngine } from "../../src/core/engine.js";

// ======================================================================
// Fixtures
// ======================================================================

let engine: USQLEngine;
let shell: USQLShell;

async function createTestEngine(): Promise<USQLEngine> {
  const e = new USQLEngine();
  await e.sql(
    "CREATE TABLE departments (  id SERIAL PRIMARY KEY,  name TEXT NOT NULL UNIQUE)",
  );
  await e.sql(
    "CREATE TABLE employees (" +
      "  id SERIAL PRIMARY KEY," +
      "  dept_id INTEGER REFERENCES departments(id)," +
      "  name TEXT NOT NULL," +
      "  email TEXT UNIQUE," +
      "  salary NUMERIC(10,2)" +
      ")",
  );
  await e.sql("CREATE VIEW dept_summary AS SELECT name FROM departments");
  await e.sql("CREATE SEQUENCE invoice_seq START 1000 INCREMENT 5");
  await e.sql("INSERT INTO departments (name) VALUES ('Engineering')");
  await e.sql("INSERT INTO departments (name) VALUES ('Sales')");
  await e.sql(
    "INSERT INTO employees (dept_id, name, email, salary) " +
      "VALUES (1, 'Alice', 'alice@ex.com', 150000.50)",
  );
  return e;
}

interface CapturedHandler {
  handler: CommandHandler;
  lines: string[];
}

function createCaptured(eng: USQLEngine): CapturedHandler {
  const lines: string[] = [];
  const fmt = new Formatter();
  const handler = new CommandHandler(eng, fmt, (text: string) => {
    lines.push(text);
  });
  return { handler, lines };
}

beforeEach(async () => {
  engine = await createTestEngine();
  shell = new USQLShell();
  // Inject the test engine into the shell.
  // Access internal fields to set the engine.
  const shellAny = shell as unknown as Record<string, unknown>;
  shellAny._engine = engine;
  const commands = shellAny._commands as Record<string, unknown>;
  commands.engine = engine;
});

// ======================================================================
// Formatter tests
// ======================================================================

describe("TestFormatter", () => {
  it("test_aligned_basic", () => {
    const fmt = new Formatter();
    const text = fmt.formatRows(
      ["name", "age"],
      [
        { name: "Alice", age: 30 },
        { name: "Bob", age: 25 },
      ],
    );
    expect(text).toContain("Alice");
    expect(text).toContain("Bob");
    expect(text).toContain("(2 rows)");
  });

  it("test_aligned_title", () => {
    const fmt = new Formatter();
    const text = fmt.formatRows(["x"], [{ x: 1 }], "Test Title");
    expect(text).toContain("Test Title");
  });

  it("test_aligned_empty", () => {
    const fmt = new Formatter();
    const text = fmt.formatRows(["x"], []);
    expect(text).toContain("(0 rows)");
  });

  it("test_aligned_single_row", () => {
    const fmt = new Formatter();
    const text = fmt.formatRows(["x"], [{ x: 1 }]);
    expect(text).toContain("(1 row)");
  });

  it("test_expanded_mode", () => {
    const fmt = new Formatter();
    fmt.expanded = true;
    const text = fmt.formatRows(["name", "age"], [{ name: "Alice", age: 30 }]);
    expect(text).toContain("-[ RECORD 1 ]");
    expect(text).toContain("name");
    expect(text).toContain("Alice");
    expect(text).toContain("(1 row)");
  });

  it("test_null_display", () => {
    const fmt = new Formatter();
    fmt.nullDisplay = "[NULL]";
    const text = fmt.formatRows(["x"], [{ x: null }]);
    expect(text).toContain("[NULL]");
  });

  it("test_float_formatting", () => {
    const fmt = new Formatter();
    const text = fmt.formatRows(["val"], [{ val: 3.14 }]);
    expect(text).toContain("3.1400");
  });

  it("test_format_result", async () => {
    const fmt = new Formatter();
    const result = await engine.sql("SELECT 1 AS num");
    const text = fmt.formatResult(result!);
    expect(text).toContain("num");
    expect(text).toContain("1");
  });
});

// ======================================================================
// Command handler tests
// ======================================================================

describe("TestCommandListTables", () => {
  it("test_dt", async () => {
    const { handler, lines } = createCaptured(engine);
    await handler.handle("\\dt");
    const output = lines.join("\n");
    expect(output).toContain("departments");
    expect(output).toContain("employees");
    expect(output).toContain("table");
  });

  it("test_dt_with_pattern", async () => {
    const { handler, lines } = createCaptured(engine);
    await handler.handle("\\dt emp");
    const output = lines.join("\n");
    expect(output).toContain("employees");
    expect(output).not.toContain("departments");
  });

  it("test_dt_plus", async () => {
    const { handler, lines } = createCaptured(engine);
    await handler.handle("\\dt+");
    const output = lines.join("\n");
    expect(output).toContain("departments");
  });
});

describe("TestCommandListIndexes", () => {
  it("test_di", async () => {
    const { handler, lines } = createCaptured(engine);
    await handler.handle("\\di");
    const output = lines.join("\n");
    expect(output).toContain("departments_pkey");
    expect(output).toContain("employees_email_key");
    expect(output).toContain("index");
  });
});

describe("TestCommandListViews", () => {
  it("test_dv", async () => {
    const { handler, lines } = createCaptured(engine);
    await handler.handle("\\dv");
    const output = lines.join("\n");
    expect(output).toContain("dept_summary");
    expect(output).toContain("view");
  });
});

describe("TestCommandListSequences", () => {
  it("test_ds", async () => {
    const { handler, lines } = createCaptured(engine);
    await handler.handle("\\ds");
    const output = lines.join("\n");
    expect(output).toContain("invoice_seq");
    expect(output).toContain("sequence");
  });
});

describe("TestCommandListSchemas", () => {
  it("test_dn", async () => {
    const { handler, lines } = createCaptured(engine);
    await handler.handle("\\dn");
    const output = lines.join("\n");
    expect(output).toContain("public");
    expect(output).toContain("pg_catalog");
    expect(output).toContain("information_schema");
  });
});

describe("TestCommandListRoles", () => {
  it("test_du", async () => {
    const { handler, lines } = createCaptured(engine);
    await handler.handle("\\du");
    const output = lines.join("\n");
    expect(output).toContain("uqa");
    expect(output).toContain("yes"); // superuser
  });
});

describe("TestCommandListDatabases", () => {
  it("test_l", async () => {
    const { handler, lines } = createCaptured(engine);
    await handler.handle("\\l");
    const output = lines.join("\n");
    expect(output).toContain("uqa");
    expect(output).toContain("UTF8");
  });
});

describe("TestCommandDescribe", () => {
  it("test_d_no_args_lists_all", async () => {
    const { handler, lines } = createCaptured(engine);
    await handler.handle("\\d");
    const output = lines.join("\n");
    expect(output).toContain("departments");
    expect(output).toContain("employees");
    expect(output).toContain("dept_summary");
    expect(output).toContain("invoice_seq");
  });

  it("test_d_table", async () => {
    const { handler, lines } = createCaptured(engine);
    await handler.handle("\\d employees");
    const output = lines.join("\n");
    // Title
    expect(output).toContain('Table "public.employees"');
    // Columns
    expect(output).toContain("id");
    expect(output).toContain("dept_id");
    expect(output).toContain("salary");
    expect(output).toContain("integer");
    expect(output).toContain("not null");
    // Indexes
    expect(output).toContain("employees_pkey");
    expect(output).toContain("PRIMARY KEY");
    expect(output).toContain("employees_email_key");
    expect(output).toContain("UNIQUE CONSTRAINT");
    // FK
    expect(output).toContain("employees_dept_id_fkey");
    expect(output).toContain("FOREIGN KEY");
    expect(output).toContain("REFERENCES departments");
  });

  it("test_d_table_referenced_by", async () => {
    const { handler, lines } = createCaptured(engine);
    await handler.handle("\\d departments");
    const output = lines.join("\n");
    expect(output).toContain("Referenced by:");
    expect(output).toContain("employees");
    expect(output).toContain("employees_dept_id_fkey");
  });

  it("test_d_sequence", async () => {
    const { handler, lines } = createCaptured(engine);
    await handler.handle("\\d invoice_seq");
    const output = lines.join("\n");
    expect(output).toContain('Sequence "public.invoice_seq"');
    expect(output).toContain("1000");
    expect(output).toContain("5");
  });

  it("test_d_index", async () => {
    const { handler, lines } = createCaptured(engine);
    await handler.handle("\\d employees_pkey");
    const output = lines.join("\n");
    expect(output).toContain('Index "public.employees_pkey"');
    expect(output).toContain("employees");
  });

  it("test_d_view", async () => {
    const { handler, lines } = createCaptured(engine);
    await handler.handle("\\d dept_summary");
    const output = lines.join("\n");
    expect(output).toContain('View "public.dept_summary"');
  });

  it("test_d_nonexistent", async () => {
    const { handler, lines } = createCaptured(engine);
    await handler.handle("\\d nonexistent");
    const output = lines.join("\n");
    expect(output).toContain("Did not find");
  });
});

describe("TestCommandToggle", () => {
  it("test_x_toggle", async () => {
    const { handler, lines } = createCaptured(engine);
    expect(handler.formatter.expanded).toBe(false);
    await handler.handle("\\x");
    expect(handler.formatter.expanded).toBe(true);
    expect(lines[lines.length - 1]).toContain("on");
    await handler.handle("\\x");
    expect(handler.formatter.expanded).toBe(false);
    expect(lines[lines.length - 1]).toContain("off");
  });

  it("test_timing_toggle", async () => {
    const { handler, lines } = createCaptured(engine);
    expect(handler.showTiming).toBe(false);
    await handler.handle("\\timing");
    expect(handler.showTiming).toBe(true);
    expect(lines[lines.length - 1]).toContain("on");
  });
});

describe("TestCommandMisc", () => {
  it("test_conninfo", async () => {
    const { handler, lines } = createCaptured(engine);
    await handler.handle("\\conninfo");
    expect(lines[lines.length - 1]).toContain("uqa");
  });

  it("test_encoding", async () => {
    const { handler, lines } = createCaptured(engine);
    await handler.handle("\\encoding");
    expect(lines[lines.length - 1]).toContain("UTF8");
  });

  it("test_help", async () => {
    const { handler, lines } = createCaptured(engine);
    await handler.handle("\\?");
    const output = lines.join("\n");
    expect(output).toContain("\\dt");
    expect(output).toContain("\\d ");
    expect(output).toContain("\\q");
  });

  it("test_quit", async () => {
    const { handler } = createCaptured(engine);
    const shouldQuit = await handler.handle("\\q");
    expect(shouldQuit).toBe(true);
  });

  it("test_invalid_command", async () => {
    const { handler, lines } = createCaptured(engine);
    await handler.handle("\\zzz");
    const lastLine = lines[lines.length - 1]!;
    expect(lastLine.includes("Invalid") || lastLine.includes("Try")).toBe(true);
  });

  it("test_output_redirect", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "usqldb-test-"));
    try {
      const { handler } = createCaptured(engine);
      const outfile = path.join(tmpDir, "out.txt");
      await handler.handle(`\\o ${outfile}`);
      expect(handler.outputFile).toBe(outfile);
      await handler.handle("\\o");
      expect(handler.outputFile).toBe(null);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});

describe("TestCommandListAll", () => {
  it("test_d_excludes_indexes", async () => {
    // \\d without args should not list indexes (psql behavior).
    const { handler, lines } = createCaptured(engine);
    await handler.handle("\\d");
    const output = lines.join("\n");
    // Should show tables, views, sequences but NOT individual indexes
    expect(output).toContain("departments");
    expect(output).not.toContain("employees_pkey"); // index excluded
  });
});

// ======================================================================
// Shell integration tests
// ======================================================================

describe("TestShellExecution", () => {
  it("test_execute_select", async () => {
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    try {
      await shell._executeText("SELECT 1 AS answer");
      const allOutput = logSpy.mock.calls.map((c) => String(c[0])).join("\n");
      expect(allOutput).toContain("answer");
      expect(allOutput).toContain("1");
    } finally {
      logSpy.mockRestore();
    }
  });

  it("test_execute_create_and_query", async () => {
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    try {
      await shell._executeText("SELECT name FROM departments ORDER BY name");
      const allOutput = logSpy.mock.calls.map((c) => String(c[0])).join("\n");
      expect(allOutput).toContain("Engineering");
      expect(allOutput).toContain("Sales");
    } finally {
      logSpy.mockRestore();
    }
  });

  it("test_execute_error", async () => {
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    try {
      await shell._executeText("SELECT * FROM nonexistent_table");
      const allOutput = logSpy.mock.calls.map((c) => String(c[0])).join("\n");
      expect(allOutput).toContain("ERROR");
    } finally {
      logSpy.mockRestore();
    }
  });

  it("test_run_file", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "usqldb-test-"));
    try {
      const sqlFile = path.join(tmpDir, "test.sql");
      fs.writeFileSync(sqlFile, "SELECT 42 AS magic;");
      const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
      try {
        await shell.runFileAsync(sqlFile);
        const allOutput = logSpy.mock.calls.map((c) => String(c[0])).join("\n");
        expect(allOutput).toContain("42");
      } finally {
        logSpy.mockRestore();
      }
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it("test_timing", async () => {
    const shellAny = shell as unknown as Record<string, unknown>;
    const commands = shellAny._commands as Record<string, unknown>;
    commands.showTiming = true;
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    try {
      await shell._executeText("SELECT 1");
      const allOutput = logSpy.mock.calls.map((c) => String(c[0])).join("\n");
      expect(allOutput).toContain("Time:");
      expect(allOutput).toContain("ms");
    } finally {
      logSpy.mockRestore();
    }
  });
});
