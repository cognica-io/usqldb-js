// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
// Copyright (c) 2023-2026 Cognica, Inc.

// PostgreSQL error hierarchy and SQLSTATE mapping.
//
// Every error that the pgwire server can surface to a client is represented
// as a PGWireError (or subclass) carrying a 5-character SQLSTATE
// code.  The mapEngineException helper converts exceptions raised
// by USQLEngine into the appropriate subclass.
//
// SQLSTATE reference:
//     https://www.postgresql.org/docs/17/errcodes-appendix.html

import {
  FIELD_DETAIL,
  FIELD_HINT,
  FIELD_MESSAGE,
  FIELD_POSITION,
  FIELD_SEVERITY,
  FIELD_SEVERITY_V,
  FIELD_SQLSTATE,
} from "./constants.js";

export class PGWireError extends Error {
  /** Static severity for class-level access. */
  static readonly severity: string = "ERROR";
  /** Static sqlstate for class-level access. */
  static readonly sqlstate: string = "XX000"; // internal_error

  readonly detail: string | null;
  readonly hint: string | null;
  readonly position: number | null;

  constructor(
    message: string,
    options?: {
      detail?: string | null;
      hint?: string | null;
      position?: number | null;
    },
  ) {
    super(message);
    this.name = "PGWireError";
    this.detail = options?.detail ?? null;
    this.hint = options?.hint ?? null;
    this.position = options?.position ?? null;
  }

  /** Instance accessor for severity (delegates to the subclass static). */
  getSeverity(): string {
    return (this.constructor as typeof PGWireError).severity;
  }

  /** Instance accessor for sqlstate (delegates to the subclass static). */
  getSqlstate(): string {
    return (this.constructor as typeof PGWireError).sqlstate;
  }

  /** Build the ErrorResponse field map for wire encoding. */
  toFields(): Map<number, string> {
    const fields = new Map<number, string>();
    fields.set(FIELD_SEVERITY, this.getSeverity());
    fields.set(FIELD_SEVERITY_V, this.getSeverity());
    fields.set(FIELD_SQLSTATE, this.getSqlstate());
    fields.set(FIELD_MESSAGE, this.message);
    if (this.detail !== null) {
      fields.set(FIELD_DETAIL, this.detail);
    }
    if (this.hint !== null) {
      fields.set(FIELD_HINT, this.hint);
    }
    if (this.position !== null) {
      fields.set(FIELD_POSITION, String(this.position));
    }
    return fields;
  }
}

// -- Syntax / Schema errors (Class 42) ------------------------------------

export class SQLSyntaxError extends PGWireError {
  static override readonly sqlstate = "42601";
  constructor(
    message: string,
    options?: {
      detail?: string | null;
      hint?: string | null;
      position?: number | null;
    },
  ) {
    super(message, options);
    this.name = "SQLSyntaxError";
  }
}

export class UndefinedTable extends PGWireError {
  static override readonly sqlstate = "42P01";
  constructor(
    message: string,
    options?: {
      detail?: string | null;
      hint?: string | null;
      position?: number | null;
    },
  ) {
    super(message, options);
    this.name = "UndefinedTable";
  }
}

export class UndefinedColumn extends PGWireError {
  static override readonly sqlstate = "42703";
  constructor(
    message: string,
    options?: {
      detail?: string | null;
      hint?: string | null;
      position?: number | null;
    },
  ) {
    super(message, options);
    this.name = "UndefinedColumn";
  }
}

export class DuplicateTable extends PGWireError {
  static override readonly sqlstate = "42P07";
  constructor(
    message: string,
    options?: {
      detail?: string | null;
      hint?: string | null;
      position?: number | null;
    },
  ) {
    super(message, options);
    this.name = "DuplicateTable";
  }
}

export class DuplicateColumn extends PGWireError {
  static override readonly sqlstate = "42701";
  constructor(
    message: string,
    options?: {
      detail?: string | null;
      hint?: string | null;
      position?: number | null;
    },
  ) {
    super(message, options);
    this.name = "DuplicateColumn";
  }
}

export class UndefinedFunction extends PGWireError {
  static override readonly sqlstate = "42883";
  constructor(
    message: string,
    options?: {
      detail?: string | null;
      hint?: string | null;
      position?: number | null;
    },
  ) {
    super(message, options);
    this.name = "UndefinedFunction";
  }
}

export class InvalidSchemaName extends PGWireError {
  static override readonly sqlstate = "3F000";
  constructor(
    message: string,
    options?: {
      detail?: string | null;
      hint?: string | null;
      position?: number | null;
    },
  ) {
    super(message, options);
    this.name = "InvalidSchemaName";
  }
}

// -- Constraint violations (Class 23) -------------------------------------

export class IntegrityConstraintViolation extends PGWireError {
  static override readonly sqlstate = "23000";
  constructor(
    message: string,
    options?: {
      detail?: string | null;
      hint?: string | null;
      position?: number | null;
    },
  ) {
    super(message, options);
    this.name = "IntegrityConstraintViolation";
  }
}

export class UniqueViolation extends PGWireError {
  static override readonly sqlstate = "23505";
  constructor(
    message: string,
    options?: {
      detail?: string | null;
      hint?: string | null;
      position?: number | null;
    },
  ) {
    super(message, options);
    this.name = "UniqueViolation";
  }
}

export class ForeignKeyViolation extends PGWireError {
  static override readonly sqlstate = "23503";
  constructor(
    message: string,
    options?: {
      detail?: string | null;
      hint?: string | null;
      position?: number | null;
    },
  ) {
    super(message, options);
    this.name = "ForeignKeyViolation";
  }
}

export class NotNullViolation extends PGWireError {
  static override readonly sqlstate = "23502";
  constructor(
    message: string,
    options?: {
      detail?: string | null;
      hint?: string | null;
      position?: number | null;
    },
  ) {
    super(message, options);
    this.name = "NotNullViolation";
  }
}

export class CheckViolation extends PGWireError {
  static override readonly sqlstate = "23514";
  constructor(
    message: string,
    options?: {
      detail?: string | null;
      hint?: string | null;
      position?: number | null;
    },
  ) {
    super(message, options);
    this.name = "CheckViolation";
  }
}

// -- Feature / Data errors ------------------------------------------------

export class FeatureNotSupported extends PGWireError {
  static override readonly sqlstate = "0A000";
  constructor(
    message: string,
    options?: {
      detail?: string | null;
      hint?: string | null;
      position?: number | null;
    },
  ) {
    super(message, options);
    this.name = "FeatureNotSupported";
  }
}

export class InvalidParameterValue extends PGWireError {
  static override readonly sqlstate = "22023";
  constructor(
    message: string,
    options?: {
      detail?: string | null;
      hint?: string | null;
      position?: number | null;
    },
  ) {
    super(message, options);
    this.name = "InvalidParameterValue";
  }
}

export class DivisionByZero extends PGWireError {
  static override readonly sqlstate = "22012";
  constructor(
    message: string,
    options?: {
      detail?: string | null;
      hint?: string | null;
      position?: number | null;
    },
  ) {
    super(message, options);
    this.name = "DivisionByZero";
  }
}

export class InvalidTextRepresentation extends PGWireError {
  static override readonly sqlstate = "22P02";
  constructor(
    message: string,
    options?: {
      detail?: string | null;
      hint?: string | null;
      position?: number | null;
    },
  ) {
    super(message, options);
    this.name = "InvalidTextRepresentation";
  }
}

// -- Connection / Protocol errors ------------------------------------------

export class ProtocolViolation extends PGWireError {
  static override readonly sqlstate = "08P01";
  constructor(
    message: string,
    options?: {
      detail?: string | null;
      hint?: string | null;
      position?: number | null;
    },
  ) {
    super(message, options);
    this.name = "ProtocolViolation";
  }
}

export class InvalidAuthorizationSpecification extends PGWireError {
  static override readonly sqlstate = "28000";
  static override readonly severity = "FATAL";
  constructor(
    message: string,
    options?: {
      detail?: string | null;
      hint?: string | null;
      position?: number | null;
    },
  ) {
    super(message, options);
    this.name = "InvalidAuthorizationSpecification";
  }
}

export class InvalidPassword extends PGWireError {
  static override readonly sqlstate = "28P01";
  static override readonly severity = "FATAL";
  constructor(
    message: string,
    options?: {
      detail?: string | null;
      hint?: string | null;
      position?: number | null;
    },
  ) {
    super(message, options);
    this.name = "InvalidPassword";
  }
}

// -- Operational errors ----------------------------------------------------

export class QueryCanceled extends PGWireError {
  static override readonly sqlstate = "57014";
  constructor(
    message: string,
    options?: {
      detail?: string | null;
      hint?: string | null;
      position?: number | null;
    },
  ) {
    super(message, options);
    this.name = "QueryCanceled";
  }
}

export class AdminShutdown extends PGWireError {
  static override readonly sqlstate = "57P01";
  static override readonly severity = "FATAL";
  constructor(
    message: string,
    options?: {
      detail?: string | null;
      hint?: string | null;
      position?: number | null;
    },
  ) {
    super(message, options);
    this.name = "AdminShutdown";
  }
}

export class InvalidTransactionState extends PGWireError {
  static override readonly sqlstate = "25000";
  constructor(
    message: string,
    options?: {
      detail?: string | null;
      hint?: string | null;
      position?: number | null;
    },
  ) {
    super(message, options);
    this.name = "InvalidTransactionState";
  }
}

export class InFailedSQLTransaction extends PGWireError {
  static override readonly sqlstate = "25P02";
  constructor(
    message: string,
    options?: {
      detail?: string | null;
      hint?: string | null;
      position?: number | null;
    },
  ) {
    super(message, options);
    this.name = "InFailedSQLTransaction";
  }
}

// ======================================================================
// Exception mapper
// ======================================================================

// Patterns matched against the string representation of engine exceptions.
// Order matters: first match wins.
const VALUE_ERROR_PATTERNS: [string, new (msg: string) => PGWireError][] = [
  ["already exists", DuplicateTable],
  ["does not exist", UndefinedTable],
  ["UNIQUE constraint violated", UniqueViolation],
  ["FOREIGN KEY constraint violated", ForeignKeyViolation],
  ["NOT NULL constraint violated", NotNullViolation],
  ["CHECK constraint violated", CheckViolation],
  ["Unsupported statement", FeatureNotSupported],
  ["Transactions require", InvalidTransactionState],
  ["division by zero", DivisionByZero],
  ["Unknown column", UndefinedColumn],
  ["Duplicate column", DuplicateColumn],
  ["Unknown function", UndefinedFunction],
];

/** Convert a USQLEngine exception to a PGWireError. */
export function mapEngineException(exc: unknown): PGWireError {
  const msg = exc instanceof Error ? exc.message : String(exc);

  // Parse errors
  if (exc instanceof Error) {
    const excTypeName = exc.constructor.name;
    if (excTypeName === "ParseError" || excTypeName === "PSqlParseError") {
      return new SQLSyntaxError(msg);
    }
  }

  // ValueError equivalents (generic Error with matching messages)
  for (const [pattern, ErrorClass] of VALUE_ERROR_PATTERNS) {
    if (msg.includes(pattern)) {
      return new ErrorClass(msg);
    }
  }

  if (exc instanceof TypeError) {
    return new InvalidTextRepresentation(msg);
  }

  if (exc instanceof RangeError && msg.includes("division by zero")) {
    return new DivisionByZero(msg);
  }

  return new PGWireError(msg);
}
