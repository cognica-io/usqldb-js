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

export const ANSI = {
  reset: "\x1b[0m",
  bold: "\x1b[1m",
  dim: "\x1b[2m",
  red: "\x1b[31m",
  cyan: "\x1b[36m",
};

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
  useColor: boolean = true;

  private _wrap(code: string, text: string): string {
    if (!this.useColor) {
      return text;
    }
    return code + text + ANSI.reset;
  }

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
      return this._wrap(ANSI.dim, "(0 rows)");
    }

    const widths: Map<string, number> = new Map();
    for (const col of columns) {
      widths.set(col, col.length);
    }

    const cellRows: Map<string, { text: string; original: unknown }>[] = [];
    for (const row of rows) {
      const cr = new Map<string, { text: string; original: unknown }>();
      for (const col of columns) {
        const original = row[col];
        const text = this._formatValue(original);
        cr.set(col, { text, original });
        const current = widths.get(col) ?? 0;
        widths.set(col, Math.max(current, text.length));
      }
      cellRows.push(cr);
    }

    const parts: string[] = [];

    if (title) {
      let tableWidth = 0;
      for (const col of columns) {
        tableWidth += widths.get(col) ?? 0;
      }
      tableWidth += 3 * (columns.length - 1) + 2;
      parts.push(center(title, tableWidth));
    }

    // Header (bold)
    const headerCells = columns.map((col) => {
      const w = widths.get(col) ?? 0;
      return center(col, w);
    });
    parts.push(this._wrap(ANSI.bold, " " + headerCells.join(" | ")));

    // Separator (dim)
    const sepCells = columns.map((col) => {
      const w = widths.get(col) ?? 0;
      return "-".repeat(w);
    });
    parts.push(this._wrap(ANSI.dim, "-" + sepCells.join("-+-") + "-"));

    // Data rows
    for (const cr of cellRows) {
      const cells = columns.map((col) => {
        const w = widths.get(col) ?? 0;
        const cell = cr.get(col)!;
        const padded = ljust(cell.text, w);
        return this._colorizeCell(cell.original, padded);
      });
      parts.push(" " + cells.join(" | "));
    }

    // Footer (dim)
    const n = cellRows.length;
    let footer: string;
    if (n === 0) {
      footer = "(0 rows)";
    } else if (n === 1) {
      footer = "(1 row)";
    } else {
      footer = `(${n} rows)`;
    }
    parts.push(this._wrap(ANSI.dim, footer));

    return parts.join("\n");
  }

  // ------------------------------------------------------------------
  // Expanded (vertical) format
  // ------------------------------------------------------------------

  private _formatExpanded(columns: string[], rows: Row[]): string {
    if (!rows.length) {
      return this._wrap(ANSI.dim, "(0 rows)");
    }

    let colWidth = 0;
    for (const c of columns) {
      colWidth = Math.max(colWidth, c.length);
    }

    const parts: string[] = [];

    for (let i = 0; i < rows.length; i++) {
      const row = rows[i]!;
      const label = `-[ RECORD ${i + 1} ]`;
      const padding = Math.max(0, colWidth + 3 - label.length);
      parts.push(this._wrap(ANSI.bold, label + "-".repeat(padding)));
      for (const col of columns) {
        const original = row[col];
        const val = this._formatValue(original);
        const colorized = this._colorizeCell(original, val);
        parts.push(`${ljust(col, colWidth)} | ${colorized}`);
      }
    }

    const n = rows.length;
    let footer: string;
    if (n === 1) {
      footer = "(1 row)";
    } else {
      footer = `(${n} rows)`;
    }
    parts.push(this._wrap(ANSI.dim, footer));

    return parts.join("\n");
  }

  // ------------------------------------------------------------------
  // Value colorization
  // ------------------------------------------------------------------

  private _colorizeCell(original: unknown, padded: string): string {
    if (!this.useColor) {
      return padded;
    }
    if (original === null || original === undefined) {
      return ANSI.dim + padded + ANSI.reset;
    }
    if (typeof original === "boolean") {
      const code = original ? ANSI.cyan : ANSI.red;
      return code + padded + ANSI.reset;
    }
    return padded;
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
