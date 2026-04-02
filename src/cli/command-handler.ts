// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
// Copyright (c) 2023-2026 Cognica, Inc.

// Backslash command handlers for the usqldb interactive shell.
//
// Each command queries pg_catalog / information_schema through the
// USQLEngine, proving the catalog layer works and producing
// psql-compatible output.
//
// Supported commands:
//     \d [NAME]       Describe relation or list all relations
//     \dt[+] [PAT]    List tables
//     \di[+] [PAT]    List indexes
//     \dv[+] [PAT]    List views
//     \ds[+] [PAT]    List sequences
//     \df[+] [PAT]    List functions
//     \dn[+]          List schemas
//     \du             List roles
//     \l              List databases
//     \det            List foreign tables
//     \des            List foreign servers
//     \dew            List foreign data wrappers
//     \dG             List named graphs
//     \x              Toggle expanded display
//     \timing         Toggle timing display
//     \o [FILE]       Send output to file
//     \i FILE         Execute commands from file
//     \e [FILE]       Edit query buffer / file in $EDITOR
//     \conninfo       Display connection info
//     \encoding       Show client encoding
//     \! CMD          Execute shell command
//     \?              Show help
//     \q              Quit

import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import * as child_process from "node:child_process";

import type { Formatter } from "./formatter.js";
import type { USQLEngine } from "../core/engine.js";

type Row = Record<string, unknown>;

interface SQLResult {
  readonly columns: string[];
  readonly rows: Row[];
}

/**
 * Dispatches and executes backslash commands.
 */
export class CommandHandler {
  readonly engine: USQLEngine;
  readonly formatter: Formatter;
  private readonly _output: (text: string) => void;

  // Mutable state
  showTiming: boolean = false;
  outputFile: string | null = null;
  dbPath: string | null = null;

  // Execute-file callback (set by REPL)
  executeFileFn: ((filePath: string) => void) | null = null;

  constructor(
    engine: USQLEngine,
    formatter: Formatter,
    outputFn: (text: string) => void,
  ) {
    this.engine = engine;
    this.formatter = formatter;
    this._output = outputFn;
  }

  // ------------------------------------------------------------------
  // Dispatch
  // ------------------------------------------------------------------

  async handle(cmdLine: string): Promise<boolean> {
    const parts = cmdLine.split(/\s+/);
    const verb = parts[0] ?? "";
    const arg = cmdLine.replace(verb, "").trim();

    if (verb === "\\q" || verb === "\\quit") {
      return true;
    }

    const dispatch: Record<string, (arg: string) => Promise<void> | void> = {
      "\\d": (a) => this._cmdDescribe(a),
      "\\dt": (a) => this._cmdListTables(a),
      "\\dt+": (a) => this._cmdListTablesPlus(a),
      "\\di": (a) => this._cmdListIndexes(a),
      "\\di+": (a) => this._cmdListIndexes(a),
      "\\dv": (a) => this._cmdListViews(a),
      "\\dv+": (a) => this._cmdListViews(a),
      "\\ds": (a) => this._cmdListSequences(a),
      "\\ds+": (a) => this._cmdListSequences(a),
      "\\df": (a) => this._cmdListFunctions(a),
      "\\df+": (a) => this._cmdListFunctions(a),
      "\\dn": (a) => this._cmdListSchemas(a),
      "\\dn+": (a) => this._cmdListSchemas(a),
      "\\du": (a) => this._cmdListRoles(a),
      "\\dg": (a) => this._cmdListRoles(a),
      "\\l": (a) => this._cmdListDatabases(a),
      "\\l+": (a) => this._cmdListDatabases(a),
      "\\det": (a) => this._cmdListForeignTables(a),
      "\\des": (a) => this._cmdListForeignServers(a),
      "\\dew": (a) => this._cmdListForeignDataWrappers(a),
      "\\dG": (a) => this._cmdListGraphs(a),
      "\\x": (a) => this._cmdToggleExpanded(a),
      "\\timing": (a) => this._cmdToggleTiming(a),
      "\\o": (a) => this._cmdOutput(a),
      "\\i": (a) => this._cmdInclude(a),
      "\\e": (a) => this._cmdEdit(a),
      "\\conninfo": (a) => this._cmdConninfo(a),
      "\\encoding": (a) => this._cmdEncoding(a),
      "\\!": (a) => this._cmdShell(a),
      "\\?": (a) => this._cmdHelp(a),
      "\\h": (a) => this._cmdHelp(a),
      "\\help": (a) => this._cmdHelp(a),
    };

    const handler = dispatch[verb];
    if (handler !== undefined) {
      await handler(arg);
      return false;
    }

    // Try prefix match: \d<name> should resolve to \d <name>
    if (verb.startsWith("\\d") && verb.length > 2 && dispatch[verb] === undefined) {
      const name = verb.slice(2) + (arg ? " " + arg : "");
      await this._cmdDescribe(name.trim());
      return false;
    }

    this._output(`Invalid command \\${verb.slice(1)}. Try \\? for help.`);
    return false;
  }

  // ------------------------------------------------------------------
  // Output helpers
  // ------------------------------------------------------------------

  output(text: string): void {
    if (this.outputFile !== null) {
      fs.appendFileSync(this.outputFile, text + "\n");
    } else {
      this._output(text);
    }
  }

  private async _query(sql: string): Promise<SQLResult> {
    const result = await this.engine.sql(sql);
    return result ?? { columns: [], rows: [] };
  }

  private _printRows(columns: string[], rows: Row[], title?: string): void {
    this.output(this.formatter.formatRows(columns, rows, title));
  }

  // ------------------------------------------------------------------
  // \d [NAME] -- describe or list
  // ------------------------------------------------------------------

  private async _cmdDescribe(arg: string): Promise<void> {
    if (!arg) {
      await this._cmdListRelations("");
      return;
    }
    await this._describeRelation(arg);
  }

  private async _cmdListRelations(pattern: string): Promise<void> {
    const r = await this._query(
      'SELECT c.relname AS "Name", ' +
        'n.nspname AS "Schema", ' +
        "c.relkind " +
        "FROM pg_catalog.pg_class c " +
        "JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid " +
        "WHERE n.nspname = 'public' " +
        "ORDER BY c.relname",
    );
    if (!r.rows.length) {
      this.output("No relations found.");
      return;
    }

    const KINDS: Record<string, string> = {
      r: "table",
      v: "view",
      i: "index",
      S: "sequence",
      f: "foreign table",
      m: "materialized view",
    };
    const columns = ["Schema", "Name", "Type", "Owner"];
    const rows: Row[] = [];
    for (const row of r.rows) {
      const kindCode = String(row["relkind"] ?? "r");
      const kindLabel = KINDS[kindCode] ?? kindCode;
      // Filter out indexes for \d (psql behavior)
      if (kindCode === "i") {
        continue;
      }
      if (pattern && !likeMatch(String(row["Name"]), pattern)) {
        continue;
      }
      rows.push({
        Schema: row["Schema"],
        Name: row["Name"],
        Type: kindLabel,
        Owner: "uqa",
      });
    }
    if (!rows.length) {
      this.output("No matching relations found.");
      return;
    }
    this._printRows(columns, rows, "List of relations");
  }

  private async _describeRelation(name: string): Promise<void> {
    const r = await this._query(
      "SELECT c.relkind, n.nspname " +
        "FROM pg_catalog.pg_class c " +
        "JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid " +
        `WHERE c.relname = '${escape(name)}'`,
    );
    if (!r.rows.length) {
      this.output(`Did not find any relation named "${name}".`);
      return;
    }

    const relkind = String(r.rows[0]!["relkind"]);
    const schema = String(r.rows[0]!["nspname"]);

    if (relkind === "r") {
      await this._describeTable(name, schema);
    } else if (relkind === "v") {
      await this._describeView(name, schema);
    } else if (relkind === "i") {
      await this._describeIndex(name, schema);
    } else if (relkind === "S") {
      await this._describeSequence(name, schema);
    } else if (relkind === "f") {
      await this._describeForeignTable(name, schema);
    } else {
      await this._describeTable(name, schema);
    }
  }

  // ------------------------------------------------------------------
  // \d TABLE -- full table description
  // ------------------------------------------------------------------

  private async _describeTable(name: string, schema: string): Promise<void> {
    const lines: string[] = [];

    // -- Title --------------------------------------------------------
    const title = `Table "${schema}.${name}"`;

    // -- Columns ------------------------------------------------------
    const r = await this._query(
      "SELECT column_name, data_type, is_nullable, column_default " +
        "FROM information_schema.columns " +
        `WHERE table_name = '${escape(name)}' ` +
        "ORDER BY ordinal_position",
    );
    const colColumns = ["Column", "Type", "Collation", "Nullable", "Default"];
    const colRows: Row[] = [];
    for (const row of r.rows) {
      const nullable = row["is_nullable"] === "YES" ? "" : "not null";
      const defaultVal = row["column_default"] ?? "";
      colRows.push({
        Column: row["column_name"],
        Type: row["data_type"],
        Collation: "",
        Nullable: nullable,
        Default: String(defaultVal),
      });
    }
    let formatted = this.formatter.formatRows(colColumns, colRows, title);
    // Remove the "(N rows)" footer from column listing
    if (formatted.endsWith(" rows)") || formatted.endsWith(" row)")) {
      const lastNewline = formatted.lastIndexOf("\n");
      if (lastNewline !== -1) {
        formatted = formatted.slice(0, lastNewline);
      }
    }
    lines.push(formatted);

    // -- Indexes ------------------------------------------------------
    const rIdx = await this._query(
      "SELECT indexname, indexdef " +
        "FROM pg_catalog.pg_indexes " +
        `WHERE tablename = '${escape(name)}'`,
    );
    if (rIdx.rows.length) {
      const idxLines: string[] = [];
      // Classify indexes using pg_index
      const rIdxProps = await this._query(
        "SELECT c.relname, i.indisprimary, i.indisunique " +
          "FROM pg_catalog.pg_index i " +
          "JOIN pg_catalog.pg_class c ON i.indexrelid = c.oid " +
          "JOIN pg_catalog.pg_class t ON i.indrelid = t.oid " +
          `WHERE t.relname = '${escape(name)}'`,
      );
      const idxProps: Map<string, { primary: unknown; unique: unknown }> = new Map();
      for (const row of rIdxProps.rows) {
        idxProps.set(String(row["relname"]), {
          primary: row["indisprimary"],
          unique: row["indisunique"],
        });
      }

      for (const row of rIdx.rows) {
        const idxName = String(row["indexname"]);
        const props = idxProps.get(idxName) ?? {};
        // Extract column list from indexdef
        const idxDef = String(row["indexdef"]);
        let colPart = "";
        const openParen = idxDef.indexOf("(");
        const closeParen = idxDef.lastIndexOf(")");
        if (openParen !== -1 && closeParen !== -1) {
          colPart = idxDef.slice(openParen, closeParen + 1);
        }

        const labelParts: string[] = [];
        if ((props as Row)["primary"]) {
          labelParts.push("PRIMARY KEY,");
        } else if ((props as Row)["unique"]) {
          labelParts.push("UNIQUE CONSTRAINT,");
        }
        labelParts.push("btree");

        const desc = labelParts.join(" ");
        idxLines.push(`    "${idxName}" ${desc} ${colPart}`);
      }

      lines.push("Indexes:");
      lines.push(...idxLines);
    }

    // -- Check constraints --------------------------------------------
    const rCheck = await this._query(
      "SELECT constraint_name " +
        "FROM information_schema.table_constraints " +
        `WHERE table_name = '${escape(name)}' ` +
        "AND constraint_type = 'CHECK'",
    );
    if (rCheck.rows.length) {
      lines.push("Check constraints:");
      for (const row of rCheck.rows) {
        lines.push(`    "${row["constraint_name"]}"`);
      }
    }

    // -- Foreign-key constraints --------------------------------------
    const fkLines = await this._buildFKLines(name);
    if (fkLines.length) {
      lines.push("Foreign-key constraints:");
      lines.push(...fkLines);
    }

    // -- Referenced by ------------------------------------------------
    const refLines = await this._buildReferencedBy(name);
    if (refLines.length) {
      lines.push("Referenced by:");
      lines.push(...refLines);
    }

    this.output(lines.join("\n"));
  }

  private async _buildFKLines(tableName: string): Promise<string[]> {
    const r = await this._query(
      "SELECT tc.constraint_name, kcu.column_name " +
        "FROM information_schema.table_constraints tc " +
        "JOIN information_schema.key_column_usage kcu " +
        "  ON tc.constraint_name = kcu.constraint_name " +
        `WHERE tc.table_name = '${escape(tableName)}' ` +
        "  AND tc.constraint_type = 'FOREIGN KEY'",
    );
    if (!r.rows.length) {
      return [];
    }

    const fkMap = new Map<string, string>();
    for (const row of r.rows) {
      fkMap.set(String(row["constraint_name"]), String(row["column_name"]));
    }

    // Get referenced table/column
    const inList = Array.from(fkMap.keys())
      .map((n) => `'${escape(n)}'`)
      .join(",");
    const rRef = await this._query(
      "SELECT rc.constraint_name, " +
        "  rc.unique_constraint_name, " +
        "  ccu.table_name AS ref_table, " +
        "  ccu.column_name AS ref_column " +
        "FROM information_schema.referential_constraints rc " +
        "JOIN information_schema.constraint_column_usage ccu " +
        "  ON rc.constraint_name = ccu.constraint_name " +
        `WHERE rc.constraint_name IN (${inList})`,
    );
    const refMap = new Map<string, [string, string]>();
    for (const row of rRef.rows) {
      refMap.set(String(row["constraint_name"]), [
        String(row["ref_table"]),
        String(row["ref_column"]),
      ]);
    }

    const result: string[] = [];
    const sorted = Array.from(fkMap.entries()).sort((a, b) => a[0].localeCompare(b[0]));
    for (const [conName, colName] of sorted) {
      const [refTable, refCol] = refMap.get(conName) ?? ["?", "?"];
      result.push(
        `    "${conName}" FOREIGN KEY (${colName}) ` +
          `REFERENCES ${refTable}(${refCol})`,
      );
    }
    return result;
  }

  private async _buildReferencedBy(tableName: string): Promise<string[]> {
    const r = await this._query(
      "SELECT ccu.constraint_name, " +
        "  ccu.column_name AS ref_column, " +
        "  tc.table_name AS src_table, " +
        "  kcu.column_name AS src_column " +
        "FROM information_schema.constraint_column_usage ccu " +
        "JOIN information_schema.table_constraints tc " +
        "  ON ccu.constraint_name = tc.constraint_name " +
        "JOIN information_schema.key_column_usage kcu " +
        "  ON tc.constraint_name = kcu.constraint_name " +
        `WHERE ccu.table_name = '${escape(tableName)}' ` +
        "  AND tc.constraint_type = 'FOREIGN KEY' " +
        `  AND tc.table_name != '${escape(tableName)}'`,
    );
    if (!r.rows.length) {
      return [];
    }

    const result: string[] = [];
    for (const row of r.rows) {
      const refCol = String(row["ref_column"] ?? "?");
      result.push(
        `    TABLE "${row["src_table"]}" ` +
          `CONSTRAINT "${row["constraint_name"]}" ` +
          `FOREIGN KEY (${row["src_column"]}) ` +
          `REFERENCES ${tableName}(${refCol})`,
      );
    }
    return result;
  }

  // ------------------------------------------------------------------
  // \d VIEW
  // ------------------------------------------------------------------

  private async _describeView(name: string, schema: string): Promise<void> {
    const lines: string[] = [];
    const title = `View "${schema}.${name}"`;

    // Try information_schema.columns first
    const r = await this._query(
      "SELECT column_name, data_type, is_nullable " +
        "FROM information_schema.columns " +
        `WHERE table_name = '${escape(name)}' ` +
        "ORDER BY ordinal_position",
    );
    const colColumns = ["Column", "Type", "Collation", "Nullable", "Default"];
    const colRows: Row[] = [];
    for (const row of r.rows) {
      const nullable = row["is_nullable"] === "YES" ? "" : "not null";
      colRows.push({
        Column: row["column_name"],
        Type: row["data_type"],
        Collation: "",
        Nullable: nullable,
        Default: "",
      });
    }

    // If no columns from information_schema, try to resolve from
    // the expanded view result
    if (!colRows.length) {
      const eng = this.engine as unknown as Record<string, unknown>;
      const tables = eng["_tables"] as
        | Map<string, { columns: Map<string, { typeName: string }> }>
        | undefined;
      const viewTable = tables?.get(name);
      if (viewTable) {
        for (const [cname, cdef] of viewTable.columns) {
          colRows.push({
            Column: cname,
            Type: cdef.typeName,
            Collation: "",
            Nullable: "",
            Default: "",
          });
        }
      }
    }

    if (colRows.length) {
      let text = this.formatter.formatRows(colColumns, colRows, title);
      text = stripFooter(text);
      lines.push(text);
    } else {
      lines.push(title);
    }

    this.output(lines.join("\n"));
  }

  // ------------------------------------------------------------------
  // \d INDEX
  // ------------------------------------------------------------------

  private async _describeIndex(name: string, schema: string): Promise<void> {
    const r = await this._query(
      "SELECT tablename, indexdef " +
        "FROM pg_catalog.pg_indexes " +
        `WHERE indexname = '${escape(name)}'`,
    );
    if (r.rows.length) {
      const row = r.rows[0]!;
      this.output(`Index "${schema}.${name}"`);
      this.output(`  Table: ${row["tablename"]}`);
      this.output(`  Definition: ${row["indexdef"]}`);
    } else {
      this.output(`Index "${schema}.${name}"`);
    }
  }

  // ------------------------------------------------------------------
  // \d SEQUENCE
  // ------------------------------------------------------------------

  private async _describeSequence(name: string, schema: string): Promise<void> {
    const r = await this._query(
      "SELECT * FROM pg_catalog.pg_sequences " +
        `WHERE sequencename = '${escape(name)}'`,
    );
    if (r.rows.length) {
      const seq = r.rows[0]!;
      this.output(`Sequence "${schema}.${name}"`);
      this.output(`  Type: ${seq["data_type"] ?? "bigint"}`);
      this.output(`  Start: ${seq["start_value"] ?? 1}`);
      this.output(`  Min: ${seq["min_value"] ?? 1}`);
      this.output(`  Max: ${seq["max_value"] ?? ""}`);
      this.output(`  Increment: ${seq["increment_by"] ?? 1}`);
      this.output(`  Cycle: ${seq["cycle"] ? "yes" : "no"}`);
    }
  }

  // ------------------------------------------------------------------
  // \d FOREIGN TABLE
  // ------------------------------------------------------------------

  private async _describeForeignTable(name: string, schema: string): Promise<void> {
    const lines: string[] = [];
    const title = `Foreign table "${schema}.${name}"`;

    const r = await this._query(
      "SELECT column_name, data_type " +
        "FROM information_schema.columns " +
        `WHERE table_name = '${escape(name)}' ` +
        "ORDER BY ordinal_position",
    );
    const colColumns = ["Column", "Type", "Collation", "Nullable", "Default"];
    const colRows: Row[] = [];
    for (const row of r.rows) {
      colRows.push({
        Column: row["column_name"],
        Type: row["data_type"],
        Collation: "",
        Nullable: "",
        Default: "",
      });
    }
    if (colRows.length) {
      let text = this.formatter.formatRows(colColumns, colRows, title);
      text = stripFooter(text);
      lines.push(text);
    } else {
      lines.push(title);
    }

    // Server info
    const rSrv = await this._query(
      "SELECT foreign_server_name " +
        "FROM information_schema.foreign_tables " +
        `WHERE foreign_table_name = '${escape(name)}'`,
    );
    if (rSrv.rows.length) {
      lines.push(`Server: ${rSrv.rows[0]!["foreign_server_name"]}`);
    }

    this.output(lines.join("\n"));
  }

  // ------------------------------------------------------------------
  // \dt -- list tables
  // ------------------------------------------------------------------

  private async _cmdListTables(arg: string): Promise<void> {
    const r = await this._query(
      'SELECT tablename AS "Name", ' +
        'schemaname AS "Schema", ' +
        'tableowner AS "Owner" ' +
        "FROM pg_catalog.pg_tables " +
        "WHERE schemaname = 'public' " +
        "ORDER BY tablename",
    );
    const rows = filterRows(r.rows, "Name", arg);
    const cols = ["Schema", "Name", "Type", "Owner"];
    const out: Row[] = rows.map((row) => ({
      Schema: row["Schema"],
      Name: row["Name"],
      Type: "table",
      Owner: row["Owner"],
    }));
    if (!out.length) {
      this.output("No matching tables found.");
      return;
    }
    this._printRows(cols, out, "List of relations");
  }

  private async _cmdListTablesPlus(arg: string): Promise<void> {
    const r = await this._query(
      'SELECT t.tablename AS "Name", ' +
        't.schemaname AS "Schema", ' +
        't.tableowner AS "Owner", ' +
        's.n_live_tup AS "Rows" ' +
        "FROM pg_catalog.pg_tables t " +
        "LEFT JOIN pg_catalog.pg_stat_user_tables s " +
        "  ON t.tablename = s.relname " +
        "WHERE t.schemaname = 'public' " +
        "ORDER BY t.tablename",
    );
    if (!r.rows.length) {
      // Fallback without JOIN
      await this._cmdListTables(arg);
      return;
    }
    const rows = filterRows(r.rows, "Name", arg);
    const cols = ["Schema", "Name", "Type", "Owner", "Rows"];
    const out: Row[] = rows.map((row) => ({
      Schema: row["Schema"],
      Name: row["Name"],
      Type: "table",
      Owner: row["Owner"],
      Rows: row["Rows"] ?? "",
    }));
    if (!out.length) {
      this.output("No matching tables found.");
      return;
    }
    this._printRows(cols, out, "List of relations");
  }

  // ------------------------------------------------------------------
  // \di -- list indexes
  // ------------------------------------------------------------------

  private async _cmdListIndexes(arg: string): Promise<void> {
    const r = await this._query(
      'SELECT indexname AS "Name", ' +
        'schemaname AS "Schema", ' +
        'tablename AS "Table" ' +
        "FROM pg_catalog.pg_indexes " +
        "WHERE schemaname = 'public' " +
        "ORDER BY indexname",
    );
    const rows = filterRows(r.rows, "Name", arg);
    const cols = ["Schema", "Name", "Type", "Owner", "Table"];
    const out: Row[] = rows.map((row) => ({
      Schema: row["Schema"],
      Name: row["Name"],
      Type: "index",
      Owner: "uqa",
      Table: row["Table"],
    }));
    if (!out.length) {
      this.output("No matching indexes found.");
      return;
    }
    this._printRows(cols, out, "List of relations");
  }

  // ------------------------------------------------------------------
  // \dv -- list views
  // ------------------------------------------------------------------

  private async _cmdListViews(arg: string): Promise<void> {
    const r = await this._query(
      'SELECT viewname AS "Name", ' +
        'schemaname AS "Schema", ' +
        'viewowner AS "Owner" ' +
        "FROM pg_catalog.pg_views " +
        "WHERE schemaname = 'public' " +
        "ORDER BY viewname",
    );
    const rows = filterRows(r.rows, "Name", arg);
    const cols = ["Schema", "Name", "Type", "Owner"];
    const out: Row[] = rows.map((row) => ({
      Schema: row["Schema"],
      Name: row["Name"],
      Type: "view",
      Owner: row["Owner"],
    }));
    if (!out.length) {
      this.output("No matching views found.");
      return;
    }
    this._printRows(cols, out, "List of relations");
  }

  // ------------------------------------------------------------------
  // \ds -- list sequences
  // ------------------------------------------------------------------

  private async _cmdListSequences(arg: string): Promise<void> {
    const r = await this._query(
      'SELECT sequencename AS "Name", ' +
        'schemaname AS "Schema", ' +
        'sequenceowner AS "Owner" ' +
        "FROM pg_catalog.pg_sequences " +
        "WHERE schemaname = 'public' " +
        "ORDER BY sequencename",
    );
    const rows = filterRows(r.rows, "Name", arg);
    const cols = ["Schema", "Name", "Type", "Owner"];
    const out: Row[] = rows.map((row) => ({
      Schema: row["Schema"],
      Name: row["Name"],
      Type: "sequence",
      Owner: row["Owner"],
    }));
    if (!out.length) {
      this.output("No matching sequences found.");
      return;
    }
    this._printRows(cols, out, "List of relations");
  }

  // ------------------------------------------------------------------
  // \df -- list functions
  // ------------------------------------------------------------------

  private async _cmdListFunctions(arg: string): Promise<void> {
    const r = await this._query(
      'SELECT proname AS "Name", ' +
        'pronargs AS "Args" ' +
        "FROM pg_catalog.pg_proc " +
        "ORDER BY proname",
    );
    const rows = filterRows(r.rows, "Name", arg);
    const cols = ["Schema", "Name", "Result data type", "Argument data types"];
    const out: Row[] = rows.map((row) => ({
      Schema: "public",
      Name: row["Name"],
      "Result data type": "text",
      "Argument data types": `(${row["Args"]} args)`,
    }));
    if (!out.length) {
      this.output("No matching functions found.");
      return;
    }
    this._printRows(cols, out, "List of functions");
  }

  // ------------------------------------------------------------------
  // \dn -- list schemas
  // ------------------------------------------------------------------

  private async _cmdListSchemas(_arg: string): Promise<void> {
    const r = await this._query(
      'SELECT nspname AS "Name" FROM pg_catalog.pg_namespace ORDER BY nspname',
    );
    const cols = ["Name", "Owner"];
    const out: Row[] = r.rows.map((row) => ({
      Name: row["Name"],
      Owner: "uqa",
    }));
    this._printRows(cols, out, "List of schemas");
  }

  // ------------------------------------------------------------------
  // \du -- list roles
  // ------------------------------------------------------------------

  private async _cmdListRoles(_arg: string): Promise<void> {
    const r = await this._query(
      'SELECT rolname AS "Name", ' +
        "rolsuper, rolcreaterole, rolcreatedb, " +
        "rolcanlogin, rolreplication, rolconnlimit " +
        "FROM pg_catalog.pg_roles " +
        "ORDER BY rolname",
    );
    const cols = [
      "Role name",
      "Superuser",
      "Create role",
      "Create DB",
      "Login",
      "Replication",
      "Conn limit",
    ];
    const out: Row[] = [];
    for (const row of r.rows) {
      out.push({
        "Role name": row["Name"],
        Superuser: yn(row["rolsuper"]),
        "Create role": yn(row["rolcreaterole"]),
        "Create DB": yn(row["rolcreatedb"]),
        Login: yn(row["rolcanlogin"]),
        Replication: yn(row["rolreplication"]),
        "Conn limit": row["rolconnlimit"] ?? -1,
      });
    }
    this._printRows(cols, out, "List of roles");
  }

  // ------------------------------------------------------------------
  // \l -- list databases
  // ------------------------------------------------------------------

  private async _cmdListDatabases(_arg: string): Promise<void> {
    const r = await this._query(
      'SELECT datname AS "Name", ' +
        'encoding AS "Encoding", ' +
        'datcollate AS "Collate", ' +
        'datctype AS "Ctype" ' +
        "FROM pg_catalog.pg_database",
    );
    const cols = ["Name", "Owner", "Encoding", "Collate", "Ctype"];
    const out: Row[] = r.rows.map((row) => ({
      Name: row["Name"],
      Owner: "uqa",
      Encoding: "UTF8",
      Collate: row["Collate"] ?? "",
      Ctype: row["Ctype"] ?? "",
    }));
    this._printRows(cols, out, "List of databases");
  }

  // ------------------------------------------------------------------
  // \det -- list foreign tables
  // ------------------------------------------------------------------

  private async _cmdListForeignTables(_arg: string): Promise<void> {
    const r = await this._query(
      'SELECT foreign_table_name AS "Name", ' +
        'foreign_table_schema AS "Schema", ' +
        'foreign_server_name AS "Server" ' +
        "FROM information_schema.foreign_tables " +
        "ORDER BY foreign_table_name",
    );
    if (!r.rows.length) {
      this.output("No foreign tables found.");
      return;
    }
    const cols = ["Schema", "Name", "Server"];
    this._printRows(cols, r.rows, "List of foreign tables");
  }

  // ------------------------------------------------------------------
  // \des -- list foreign servers
  // ------------------------------------------------------------------

  private async _cmdListForeignServers(_arg: string): Promise<void> {
    const r = await this._query(
      'SELECT foreign_server_name AS "Name", ' +
        'foreign_data_wrapper_name AS "FDW" ' +
        "FROM information_schema.foreign_servers " +
        "ORDER BY foreign_server_name",
    );
    if (!r.rows.length) {
      this.output("No foreign servers found.");
      return;
    }
    const cols = ["Name", "Owner", "FDW"];
    const out: Row[] = r.rows.map((row) => ({
      Name: row["Name"],
      Owner: "uqa",
      FDW: row["FDW"] ?? "",
    }));
    this._printRows(cols, out, "List of foreign servers");
  }

  // ------------------------------------------------------------------
  // \dew -- list foreign data wrappers
  // ------------------------------------------------------------------

  private async _cmdListForeignDataWrappers(_arg: string): Promise<void> {
    const r = await this._query(
      'SELECT fdwname AS "Name" ' +
        "FROM pg_catalog.pg_foreign_data_wrapper " +
        "ORDER BY fdwname",
    );
    if (!r.rows.length) {
      this.output("No foreign data wrappers found.");
      return;
    }
    const cols = ["Name", "Owner"];
    const out: Row[] = r.rows.map((row) => ({
      Name: row["Name"],
      Owner: "uqa",
    }));
    this._printRows(cols, out, "List of foreign-data wrappers");
  }

  // ------------------------------------------------------------------
  // \dG -- list named graphs (UQA extension)
  // ------------------------------------------------------------------

  private _cmdListGraphs(_arg: string): void {
    const gs = (this.engine as unknown as Record<string, unknown>)["_graphStore"] as {
      graphNames(): string[];
      vertexIdsInGraph(g: string): Set<number>;
      edgesInGraph(g: string): unknown[];
    };
    const names = gs.graphNames();
    if (!names.length) {
      this.output("No named graphs.");
      return;
    }
    const cols = ["Graph", "Vertices", "Edges"];
    const out: Row[] = [];
    for (const name of [...names].sort()) {
      out.push({
        Graph: name,
        Vertices: gs.vertexIdsInGraph(name).size,
        Edges: gs.edgesInGraph(name).length,
      });
    }
    this._printRows(cols, out, "List of named graphs");
  }

  // ------------------------------------------------------------------
  // \x -- toggle expanded display
  // ------------------------------------------------------------------

  private _cmdToggleExpanded(_arg: string): void {
    this.formatter.expanded = !this.formatter.expanded;
    const state = this.formatter.expanded ? "on" : "off";
    this.output(`Expanded display is ${state}.`);
  }

  // ------------------------------------------------------------------
  // \timing -- toggle timing
  // ------------------------------------------------------------------

  private _cmdToggleTiming(_arg: string): void {
    this.showTiming = !this.showTiming;
    const state = this.showTiming ? "on" : "off";
    this.output(`Timing is ${state}.`);
  }

  // ------------------------------------------------------------------
  // \o [FILE] -- output to file
  // ------------------------------------------------------------------

  private _cmdOutput(arg: string): void {
    if (arg) {
      this.outputFile = arg;
      this.output(`Output redirected to: ${arg}`);
    } else {
      if (this.outputFile !== null) {
        this.output(`Output restored to stdout (was: ${this.outputFile}).`);
      }
      this.outputFile = null;
    }
  }

  // ------------------------------------------------------------------
  // \i FILE -- include/execute file
  // ------------------------------------------------------------------

  private _cmdInclude(arg: string): void {
    if (!arg) {
      this.output("Usage: \\i <filename>");
      return;
    }
    if (!fs.existsSync(arg)) {
      this.output(`File not found: ${arg}`);
      return;
    }
    if (this.executeFileFn !== null) {
      this.executeFileFn(arg);
    }
  }

  // ------------------------------------------------------------------
  // \e [FILE] -- edit in $EDITOR
  // ------------------------------------------------------------------

  private _cmdEdit(arg: string): void {
    const editor = process.env["EDITOR"] ?? process.env["VISUAL"] ?? "vi";
    let filePath: string;
    let isTempFile: boolean;

    if (arg) {
      filePath = arg;
      isTempFile = false;
    } else {
      filePath = path.join(os.tmpdir(), `usqldb_${Date.now()}.sql`);
      fs.writeFileSync(filePath, "");
      isTempFile = true;
    }

    try {
      child_process.spawnSync(editor, [filePath], { stdio: "inherit" });
    } catch {
      this.output(`Editor not found: ${editor}`);
      return;
    }

    if (isTempFile && fs.existsSync(filePath)) {
      if (this.executeFileFn !== null) {
        this.executeFileFn(filePath);
      }
      try {
        fs.unlinkSync(filePath);
      } catch {
        // ignore cleanup errors
      }
    }
  }

  // ------------------------------------------------------------------
  // \conninfo -- connection info
  // ------------------------------------------------------------------

  private _cmdConninfo(_arg: string): void {
    const db = this.dbPath ?? ":memory:";
    this.output(`You are connected to database "uqa" via file "${db}".`);
  }

  // ------------------------------------------------------------------
  // \encoding -- client encoding
  // ------------------------------------------------------------------

  private _cmdEncoding(_arg: string): void {
    this.output("UTF8");
  }

  // ------------------------------------------------------------------
  // \! -- shell command
  // ------------------------------------------------------------------

  private _cmdShell(arg: string): void {
    if (arg) {
      child_process.execSync(arg, { stdio: "inherit" });
    } else {
      const shell = process.env["SHELL"] ?? "/bin/sh";
      child_process.spawnSync(shell, [], { stdio: "inherit" });
    }
  }

  // ------------------------------------------------------------------
  // \? -- help
  // ------------------------------------------------------------------

  private _cmdHelp(_arg: string): void {
    this.output(
      "General\n" +
        "  \\q                  Quit\n" +
        "  \\? [commands]       Show help\n" +
        "  \\conninfo           Display connection info\n" +
        "  \\encoding           Show client encoding\n" +
        "  \\! [COMMAND]        Execute shell command\n" +
        "\n" +
        "Informational\n" +
        "  \\d [NAME]           Describe table/view/index or list all\n" +
        "  \\dt[+] [PATTERN]    List tables\n" +
        "  \\di[+] [PATTERN]    List indexes\n" +
        "  \\dv[+] [PATTERN]    List views\n" +
        "  \\ds[+] [PATTERN]    List sequences\n" +
        "  \\df[+] [PATTERN]    List functions\n" +
        "  \\dn[+]              List schemas\n" +
        "  \\du                 List roles\n" +
        "  \\l[+]               List databases\n" +
        "  \\det                List foreign tables\n" +
        "  \\des                List foreign servers\n" +
        "  \\dew                List foreign data wrappers\n" +
        "  \\dG                 List named graphs\n" +
        "\n" +
        "Formatting\n" +
        "  \\x                  Toggle expanded display\n" +
        "  \\timing             Toggle timing of commands\n" +
        "\n" +
        "Input/Output\n" +
        "  \\o [FILE]           Send output to file or stdout\n" +
        "  \\i FILE             Execute commands from file\n" +
        "  \\e [FILE]           Edit query or file with $EDITOR",
    );
  }
}

// ======================================================================
// Helpers
// ======================================================================

function escape(s: string): string {
  return s.replace(/'/g, "''");
}

function likeMatch(value: string, pattern: string): boolean {
  return value.toLowerCase().includes(pattern.toLowerCase());
}

function filterRows(rows: Row[], key: string, pattern: string): Row[] {
  if (!pattern) {
    return rows;
  }
  return rows.filter((r) => likeMatch(String(r[key] ?? ""), pattern));
}

function stripFooter(text: string): string {
  const lines = text.split("\n");
  if (
    lines.length &&
    lines[lines.length - 1]!.startsWith("(") &&
    lines[lines.length - 1]!.endsWith(")")
  ) {
    return lines.slice(0, -1).join("\n");
  }
  return text;
}

function yn(val: unknown): string {
  if (val === true || val === 1 || val === "1" || val === "t" || val === "true") {
    return "yes";
  }
  return "no";
}
