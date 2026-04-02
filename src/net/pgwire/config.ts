// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
// Copyright (c) 2023-2026 Cognica, Inc.

// Server configuration for the pgwire protocol server.

/** Configuration for a PGWireServer instance. */
export interface PGWireConfig {
  /** Bind address for the TCP listener. */
  readonly host: string;
  /** Bind port. Use 0 for an OS-assigned ephemeral port. */
  readonly port: number;
  /** Path passed to USQLEngine for persistent storage. null creates an in-memory engine per connection. */
  readonly dbPath: string | null;
  /** Authentication method name: "trust", "password", "md5", or "scram-sha-256". */
  readonly authMethod: string;
  /** Mapping of {username: password} for password-based auth. */
  readonly credentials: Record<string, string> | null;
  /** Path to an SSL certificate file (PEM). When set together with sslKeyfile, the server accepts SSL connections. */
  readonly sslCertfile: string | null;
  /** Path to an SSL private key file (PEM). */
  readonly sslKeyfile: string | null;
  /** Maximum number of concurrent client connections. */
  readonly maxConnections: number;
  /** Optional callable that returns a USQLEngine instance. When provided, this overrides dbPath. */
  readonly engineFactory: (() => unknown) | null;
}

/** Create a PGWireConfig with defaults for omitted fields. */
export function createConfig(overrides?: Partial<PGWireConfig>): PGWireConfig {
  return {
    host: overrides?.host ?? "127.0.0.1",
    port: overrides?.port ?? 5432,
    dbPath: overrides?.dbPath ?? null,
    authMethod: overrides?.authMethod ?? "trust",
    credentials: overrides?.credentials ?? null,
    sslCertfile: overrides?.sslCertfile ?? null,
    sslKeyfile: overrides?.sslKeyfile ?? null,
    maxConnections: overrides?.maxConnections ?? 100,
    engineFactory: overrides?.engineFactory ?? null,
  };
}
