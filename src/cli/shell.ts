// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
// Copyright (c) 2023-2026 Cognica, Inc.

// usqldb -- interactive SQL shell with PostgreSQL 17-compatible catalogs.
//
// Usage:
//     usqldb                        Start with an in-memory database
//     usqldb --db mydata.db         Start with persistent SQLite storage
//     usqldb script.sql             Execute a SQL script then enter REPL
//     usqldb --db mydata.db s.sql   Persistent + script
//     usqldb -c "SELECT 1"          Execute a command string and exit
//
// Special commands (backslash):
//     \d [NAME]       Describe table/view or list all relations
//     \dt             List tables
//     \di             List indexes
//     \dv             List views
//     \x              Toggle expanded display
//     \timing         Toggle query timing
//     \?              Show all commands
//     \q              Quit

import * as readline from "node:readline";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";

import { USQLEngine } from "../core/engine.js";
import { CommandHandler } from "./command-handler.js";
import { Completer } from "./completer.js";
import { ANSI, Formatter } from "./formatter.js";

interface SQLResult {
  readonly columns: string[];
  readonly rows: Record<string, unknown>[];
}

/**
 * Interactive SQL shell backed by a USQLEngine.
 */
export class USQLShell {
  private readonly _dbPath: string | null;
  private readonly _engine: USQLEngine;
  private readonly _formatter: Formatter;
  private readonly _commands: CommandHandler;
  private readonly _completer: Completer;
  private readonly _useColor: boolean;
  private _rl: readline.Interface | null = null;

  constructor(opts?: { dbPath?: string }) {
    this._dbPath = opts?.dbPath ?? null;
    this._engine = new USQLEngine({ dbPath: opts?.dbPath });
    this._useColor = process.stdout.isTTY === true;

    this._formatter = new Formatter();
    this._formatter.useColor = this._useColor;
    this._commands = new CommandHandler(this._engine, this._formatter, (text: string) =>
      console.log(text),
    );
    this._commands.dbPath = this._dbPath;
    this._commands.executeFileFn = (p: string) => this.runFile(p);

    this._completer = new Completer(this._engine);
  }

  // ------------------------------------------------------------------
  // Public API
  // ------------------------------------------------------------------

  /**
   * Execute every statement in a SQL script file.
   */
  runFile(filePath: string): void {
    const text = fs.readFileSync(filePath, "utf-8");
    // Use a synchronous-style approach: queue the async work
    // but for the sync API we cannot await. The caller should
    // use runFileAsync for proper async handling.
    void this._executeText(text);
  }

  /**
   * Execute every statement in a SQL script file (async version).
   */
  async runFileAsync(filePath: string): Promise<void> {
    const text = fs.readFileSync(filePath, "utf-8");
    await this._executeText(text);
  }

  /**
   * Enter the read-eval-print loop.
   */
  async run(): Promise<void> {
    const rl = this._ensureReadline();
    this._printBanner();
    let buf = "";

    const prompt = (): string => {
      if (!buf) {
        return "uqa=> ";
      }
      return "uqa-> ";
    };

    return new Promise<void>((resolve) => {
      const askLine = (): void => {
        rl.question(prompt(), async (line: string) => {
          const stripped = line.trim();

          // Empty line with no buffer: skip
          if (!stripped && !buf) {
            askLine();
            return;
          }

          // Backslash commands only at the start (not in multi-line buffer)
          if (!buf && stripped.startsWith("\\")) {
            try {
              const shouldQuit = await this._commands.handle(stripped);
              if (shouldQuit) {
                console.log();
                resolve();
                return;
              }
            } catch (exc) {
              console.log(this._colorError(`ERROR: ${exc}`));
            }
            askLine();
            return;
          }

          buf += line + "\n";

          // Semicolon terminates the statement
          if (!buf.includes(";")) {
            askLine();
            return;
          }

          await this._executeText(buf);
          buf = "";
          askLine();
        });
      };

      rl.on("close", () => {
        console.log();
        resolve();
      });

      rl.on("SIGINT", () => {
        if (buf) {
          buf = "";
          console.log();
        } else {
          console.log();
        }
        askLine();
      });

      askLine();
    });
  }

  /**
   * Close the engine and clean up.
   */
  async close(): Promise<void> {
    if (this._rl) {
      this._rl.close();
      this._rl = null;
    }
    this._engine.close();
  }

  // ------------------------------------------------------------------
  // Statement execution
  // ------------------------------------------------------------------

  async _executeText(text: string): Promise<void> {
    for (const raw of text.split(";")) {
      const stmt = raw.trim();
      if (!stmt) {
        continue;
      }
      // Skip pure comment blocks
      const isAllComments = stmt
        .split("\n")
        .every((ln) => ln.trim().startsWith("--") || !ln.trim());
      if (isAllComments) {
        continue;
      }
      await this._executeSQL(stmt);
    }
  }

  private async _executeSQL(stmt: string): Promise<void> {
    const t0 = performance.now();
    let result: SQLResult | null;
    try {
      result = await this._engine.sql(stmt);
    } catch (exc) {
      console.log(this._colorError(`ERROR: ${exc}`));
      return;
    }
    const elapsed = performance.now() - t0;

    this._printResult(result);
    if (this._commands.showTiming) {
      this._print(this._colorDim(`Time: ${elapsed.toFixed(3)} ms`));
    }
  }

  private _printResult(result: SQLResult | null): void {
    if (!result) {
      return;
    }
    if (!result.columns.length && !result.rows.length) {
      return;
    }
    const text = this._formatter.formatResult(result);
    if (text) {
      this._print(text);
    }
  }

  private _print(text: string): void {
    if (this._commands.outputFile !== null) {
      fs.appendFileSync(this._commands.outputFile, text + "\n");
    } else {
      console.log(text);
    }
  }

  // ------------------------------------------------------------------
  // Color helpers
  // ------------------------------------------------------------------

  private _colorError(text: string): string {
    if (!this._useColor) {
      return text;
    }
    return ANSI.red + text + ANSI.reset;
  }

  private _colorDim(text: string): string {
    if (!this._useColor) {
      return text;
    }
    return ANSI.dim + text + ANSI.reset;
  }

  // ------------------------------------------------------------------
  // Session management
  // ------------------------------------------------------------------

  private static _historyPath(): string {
    const historyDir = path.join(os.homedir(), ".cognica", "usqldb");
    fs.mkdirSync(historyDir, { recursive: true });
    return path.join(historyDir, ".usql_history");
  }

  private _ensureReadline(): readline.Interface {
    if (this._rl === null) {
      const historyPath = USQLShell._historyPath();

      // Load existing history
      let history: string[] = [];
      try {
        const content = fs.readFileSync(historyPath, "utf-8");
        history = content
          .split("\n")
          .filter((line) => line.trim())
          .reverse();
      } catch {
        // No history file yet
      }

      const completer = this._completer;
      this._rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout,
        terminal: true,
        history,
        historySize: 1000,
        completer: (line: string) => completer.complete(line),
      });

      // Save history on close
      this._rl.on("close", () => {
        try {
          const rl = this._rl as unknown as { history?: string[] };
          if (rl.history) {
            const lines = [...rl.history].reverse().join("\n");
            fs.writeFileSync(historyPath, lines + "\n");
          }
        } catch {
          // Ignore history save errors
        }
      });
    }
    return this._rl;
  }

  // ------------------------------------------------------------------
  // UI
  // ------------------------------------------------------------------

  private _printBanner(): void {
    const db = this._dbPath ?? ":memory:";
    console.log("usqldb 0.2.0 (PostgreSQL 17.0 compatible)");
    console.log(`Database: ${db}`);
    console.log('Type "\\?" for help.');
    console.log();
  }
}
