//
// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
//
// Copyright (c) 2023-2026 Cognica, Inc.
//

// Unit tests for error hierarchy and exception mapping.

import { describe, it, expect } from "vitest";
import {
  FIELD_MESSAGE,
  FIELD_SEVERITY,
  FIELD_SQLSTATE,
} from "../../../src/net/pgwire/constants.js";
import {
  DuplicateTable,
  DivisionByZero,
  FeatureNotSupported,
  InvalidTransactionState,
  PGWireError,
  SQLSyntaxError,
  UndefinedTable,
  UniqueViolation,
  mapEngineException,
} from "../../../src/net/pgwire/errors.js";

/** Helper: extract sqlstate from a PGWireError via toFields(). */
function sqlstateOf(err: PGWireError): string | undefined {
  return err.toFields().get(FIELD_SQLSTATE);
}

/** Helper: extract severity from a PGWireError via toFields(). */
function severityOf(err: PGWireError): string | undefined {
  return err.toFields().get(FIELD_SEVERITY);
}

describe("TestPGWireError", () => {
  it("test_base_error", () => {
    const err = new PGWireError("something broke");
    expect(err.message).toBe("something broke");
    expect(sqlstateOf(err)).toBe("XX000");
    expect(severityOf(err)).toBe("ERROR");
  });

  it("test_error_with_details", () => {
    const err = new PGWireError("bad stuff", {
      detail: "this is the detail",
      hint: "try this instead",
      position: 42,
    });
    const fields = err.toFields();
    expect(fields.get(FIELD_SEVERITY)).toBe("ERROR");
    expect(fields.get(FIELD_SQLSTATE)).toBe("XX000");
    expect(fields.get(FIELD_MESSAGE)).toBe("bad stuff");
    expect(fields.get("D".charCodeAt(0))).toBe("this is the detail");
    expect(fields.get("H".charCodeAt(0))).toBe("try this instead");
    expect(fields.get("P".charCodeAt(0))).toBe("42");
  });

  it("test_syntax_error", () => {
    const err = new SQLSyntaxError("unexpected token");
    expect(sqlstateOf(err)).toBe("42601");
  });

  it("test_undefined_table", () => {
    const err = new UndefinedTable("table 'foo' does not exist");
    expect(sqlstateOf(err)).toBe("42P01");
  });

  it("test_unique_violation", () => {
    const err = new UniqueViolation("duplicate key");
    expect(sqlstateOf(err)).toBe("23505");
  });
});

describe("TestMapEngineException", () => {
  it("test_table_not_exists", () => {
    const exc = new Error("Table 'users' does not exist");
    const result = mapEngineException(exc);
    expect(result).toBeInstanceOf(UndefinedTable);
    expect(sqlstateOf(result)).toBe("42P01");
  });

  it("test_table_already_exists", () => {
    const exc = new Error("Table 'users' already exists");
    const result = mapEngineException(exc);
    expect(result).toBeInstanceOf(DuplicateTable);
    expect(sqlstateOf(result)).toBe("42P07");
  });

  it("test_unique_violation", () => {
    const exc = new Error("UNIQUE constraint violated on column 'id'");
    const result = mapEngineException(exc);
    expect(result).toBeInstanceOf(UniqueViolation);
  });

  it("test_unsupported_statement", () => {
    const exc = new Error("Unsupported statement: CopyStmt");
    const result = mapEngineException(exc);
    expect(result).toBeInstanceOf(FeatureNotSupported);
    expect(sqlstateOf(result)).toBe("0A000");
  });

  it("test_transaction_error", () => {
    const exc = new Error("Transactions require a persistent engine (db_path)");
    const result = mapEngineException(exc);
    expect(result).toBeInstanceOf(InvalidTransactionState);
  });

  it("test_pglast_parse_error", () => {
    // Simulate a ParseError (we check class name, not type).
    class ParseError extends Error {
      constructor(msg: string) {
        super(msg);
        this.name = "ParseError";
      }
    }
    const exc = new ParseError("syntax error at position 5");
    const result = mapEngineException(exc);
    expect(result).toBeInstanceOf(SQLSyntaxError);
  });

  it("test_generic_value_error", () => {
    const exc = new Error("something unexpected");
    const result = mapEngineException(exc);
    expect(result).toBeInstanceOf(PGWireError);
  });

  it("test_zero_division", () => {
    // In JS, there is no ZeroDivisionError, but mapEngineException
    // should handle the pattern if the message contains "division by zero"
    // or if it is a RangeError. We test with a matching message.
    const exc = new RangeError("division by zero");
    const result = mapEngineException(exc);
    expect(result).toBeInstanceOf(DivisionByZero);
  });
});
