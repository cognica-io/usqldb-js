// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
// Copyright (c) 2023-2026 Cognica, Inc.

// Output formatting for the usqldb interactive shell.
//
// Produces psql-compatible tabular and expanded output.

type Row = Record<string, unknown>;

interface SQLResult {
  readonly columns: string[];
  readonly rows: Row[];
}

/**
 * Stateful output formatter matching psql display conventions.
 *
 * Supports two modes:
 *   - aligned (default): columnar table with | separators
 *   - expanded (\x): vertical, one column per line
 */
export class Formatter {
  expanded: boolean = false;
  nullDisplay: string = "";

  // ------------------------------------------------------------------
  // Public API
  // ------------------------------------------------------------------

  /**
   * Format a SQLResult for terminal display.
   */
  formatResult(result: SQLResult, title?: string): string {
    const { columns, rows } = result;
    if (!columns.length && !rows.length) {
      return "";
    }
    if (this.expanded) {
      return this._formatExpanded(columns, rows);
    }
    return this._formatAligned(columns, rows, title);
  }

  /**
   * Format raw column/row data for terminal display.
   */
  formatRows(columns: string[], rows: Row[], title?: string): string {
    if (this.expanded) {
      return this._formatExpanded(columns, rows);
    }
    return this._formatAligned(columns, rows, title);
  }

  // ------------------------------------------------------------------
  // Aligned (tabular) format
  // ------------------------------------------------------------------

  private _formatAligned(columns: string[], rows: Row[], title?: string): string {
    if (!rows.length && !columns.length) {
      return "(0 rows)";
    }

    // Stringify all values
    const widths: Map<string, number> = new Map();
    for (const col of columns) {
      widths.set(col, col.length);
    }

    const strRows: Map<string, string>[] = [];
    for (const row of rows) {
      const sr = new Map<string, string>();
      for (const col of columns) {
        const s = this._formatValue(row[col]);
        sr.set(col, s);
        const current = widths.get(col) ?? 0;
        widths.set(col, Math.max(current, s.length));
      }
      strRows.push(sr);
    }

    const parts: string[] = [];

    // Optional title centered above the table
    if (title) {
      let tableWidth = 0;
      for (const col of columns) {
        tableWidth += widths.get(col) ?? 0;
      }
      tableWidth += 3 * (columns.length - 1) + 2;
      parts.push(center(title, tableWidth));
    }

    // Header
    const headerCells = columns.map((col) => {
      const w = widths.get(col) ?? 0;
      return center(col, w);
    });
    parts.push(" " + headerCells.join(" | "));

    // Separator
    const sepCells = columns.map((col) => {
      const w = widths.get(col) ?? 0;
      return "-".repeat(w);
    });
    parts.push("-" + sepCells.join("-+-") + "-");

    // Data rows
    for (const sr of strRows) {
      const cells = columns.map((col) => {
        const w = widths.get(col) ?? 0;
        const val = sr.get(col) ?? "";
        return ljust(val, w);
      });
      parts.push(" " + cells.join(" | "));
    }

    // Footer
    const n = strRows.length;
    if (n === 0) {
      parts.push("(0 rows)");
    } else if (n === 1) {
      parts.push("(1 row)");
    } else {
      parts.push(`(${n} rows)`);
    }

    return parts.join("\n");
  }

  // ------------------------------------------------------------------
  // Expanded (vertical) format
  // ------------------------------------------------------------------

  private _formatExpanded(columns: string[], rows: Row[]): string {
    if (!rows.length) {
      return "(0 rows)";
    }

    let colWidth = 0;
    for (const c of columns) {
      colWidth = Math.max(colWidth, c.length);
    }

    const parts: string[] = [];

    for (let i = 0; i < rows.length; i++) {
      const row = rows[i]!;
      // Record header
      const label = `-[ RECORD ${i + 1} ]`;
      const padding = Math.max(0, colWidth + 3 - label.length);
      parts.push(label + "-".repeat(padding));
      // Column values
      for (const col of columns) {
        const val = this._formatValue(row[col]);
        parts.push(`${ljust(col, colWidth)} | ${val}`);
      }
    }

    const n = rows.length;
    if (n === 1) {
      parts.push("(1 row)");
    } else {
      parts.push(`(${n} rows)`);
    }

    return parts.join("\n");
  }

  // ------------------------------------------------------------------
  // Value formatting
  // ------------------------------------------------------------------

  private _formatValue(val: unknown): string {
    if (val === null || val === undefined) {
      return this.nullDisplay;
    }
    if (typeof val === "number" && !Number.isInteger(val)) {
      // Show 4 decimal places for floats (matching UQA convention)
      return val.toFixed(4);
    }
    if (typeof val === "boolean") {
      return val ? "t" : "f";
    }
    return String(val);
  }
}

// ------------------------------------------------------------------
// String helpers
// ------------------------------------------------------------------

function center(s: string, width: number): string {
  if (s.length >= width) return s;
  const total = width - s.length;
  const left = Math.floor(total / 2);
  const right = total - left;
  return " ".repeat(left) + s + " ".repeat(right);
}

function ljust(s: string, width: number): string {
  if (s.length >= width) return s;
  return s + " ".repeat(width - s.length);
}
