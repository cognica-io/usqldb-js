//
// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
//
// Copyright (c) 2023-2026 Cognica, Inc.
//

// Row normalization utilities for catalog data.
//
// Converts values for Arrow-compatible storage:
//   - boolean -> number (0/1), matching PostgreSQL's boolean-to-integer casting
//   - NaN / Infinity -> null

type Row = Record<string, unknown>;

export function normalizeRows(rows: Row[]): Row[] {
  if (rows.length === 0) return rows;
  const normalized: Row[] = [];
  for (const row of rows) {
    const newRow: Row = {};
    for (const [key, value] of Object.entries(row)) {
      if (typeof value === "boolean") {
        newRow[key] = value ? 1 : 0;
      } else if (
        typeof value === "number" &&
        (Number.isNaN(value) || !Number.isFinite(value))
      ) {
        newRow[key] = null;
      } else {
        newRow[key] = value;
      }
    }
    normalized.push(newRow);
  }
  return normalized;
}
