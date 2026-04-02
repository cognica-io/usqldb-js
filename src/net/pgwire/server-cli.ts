// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
// Copyright (c) 2023-2026 Cognica, Inc.

// Command-line entry point for the usqldb pgwire server.
//
// Usage:
//
//     npx usqldb-server                            # in-memory, port 5432
//     npx usqldb-server --port 15432               # custom port
//     npx usqldb-server --db mydata.db             # persistent storage
//     npx usqldb-server --auth scram-sha-256 \
//                       --user admin:secret         # with authentication

import { AuthMethod } from "./auth.js";
import { createConfig } from "./config.js";
import { PGWireServer } from "./server.js";

interface ParsedArgs {
  host: string;
  port: number;
  db: string | null;
  auth: string;
  user: string[];
  maxConnections: number;
  sslCert: string | null;
  sslKey: string | null;
  logLevel: string;
}

function parseArgs(argv: string[]): ParsedArgs {
  const result: ParsedArgs = {
    host: "127.0.0.1",
    port: 5432,
    db: null,
    auth: "trust",
    user: [],
    maxConnections: 100,
    sslCert: null,
    sslKey: null,
    logLevel: "INFO",
  };

  const validAuthMethods = Object.values(AuthMethod) as string[];

  let i = 0;
  while (i < argv.length) {
    const arg = argv[i]!;

    if (arg === "--host" && i + 1 < argv.length) {
      result.host = argv[i + 1]!;
      i += 2;
      continue;
    }

    if (arg === "--port" && i + 1 < argv.length) {
      result.port = parseInt(argv[i + 1]!, 10);
      if (Number.isNaN(result.port)) {
        process.stderr.write(`Invalid port: ${argv[i + 1]}\n`);
        process.exit(1);
      }
      i += 2;
      continue;
    }

    if (arg === "--db" && i + 1 < argv.length) {
      result.db = argv[i + 1]!;
      i += 2;
      continue;
    }

    if (arg === "--auth" && i + 1 < argv.length) {
      const method = argv[i + 1]!;
      if (!validAuthMethods.includes(method)) {
        process.stderr.write(
          `Invalid auth method: ${method}. Valid: ${validAuthMethods.join(", ")}\n`,
        );
        process.exit(1);
      }
      result.auth = method;
      i += 2;
      continue;
    }

    if (arg === "--user" && i + 1 < argv.length) {
      result.user.push(argv[i + 1]!);
      i += 2;
      continue;
    }

    if (arg === "--max-connections" && i + 1 < argv.length) {
      result.maxConnections = parseInt(argv[i + 1]!, 10);
      if (Number.isNaN(result.maxConnections)) {
        process.stderr.write(`Invalid max-connections: ${argv[i + 1]}\n`);
        process.exit(1);
      }
      i += 2;
      continue;
    }

    if (arg === "--ssl-cert" && i + 1 < argv.length) {
      result.sslCert = argv[i + 1]!;
      i += 2;
      continue;
    }

    if (arg === "--ssl-key" && i + 1 < argv.length) {
      result.sslKey = argv[i + 1]!;
      i += 2;
      continue;
    }

    if (arg === "--log-level" && i + 1 < argv.length) {
      const level = argv[i + 1]!.toUpperCase();
      if (!["DEBUG", "INFO", "WARNING", "ERROR"].includes(level)) {
        process.stderr.write(
          `Invalid log level: ${argv[i + 1]}. Valid: DEBUG, INFO, WARNING, ERROR\n`,
        );
        process.exit(1);
      }
      result.logLevel = level;
      i += 2;
      continue;
    }

    if (arg === "--help" || arg === "-h") {
      process.stdout.write(
        "usqldb-server -- PostgreSQL 17-compatible wire protocol server for usqldb\n\n" +
          "Options:\n" +
          "  --host HOST              bind address (default: 127.0.0.1)\n" +
          "  --port PORT              bind port (default: 5432)\n" +
          "  --db PATH                database file for persistent storage (default: in-memory)\n" +
          "  --auth METHOD            authentication method: trust, password, md5, scram-sha-256 (default: trust)\n" +
          "  --user NAME:PASSWORD     add a user credential (repeatable)\n" +
          "  --max-connections N      maximum concurrent connections (default: 100)\n" +
          "  --ssl-cert PATH          SSL certificate file (PEM)\n" +
          "  --ssl-key PATH           SSL private key file (PEM)\n" +
          "  --log-level LEVEL        logging level: DEBUG, INFO, WARNING, ERROR (default: INFO)\n" +
          "  --help, -h               show this help message\n",
      );
      process.exit(0);
    }

    process.stderr.write(`Unknown argument: ${arg}\n`);
    process.exit(1);
  }

  return result;
}

function buildCredentials(userArgs: string[]): Record<string, string> | null {
  if (userArgs.length === 0) {
    return null;
  }
  const creds: Record<string, string> = {};
  for (const entry of userArgs) {
    if (!entry.includes(":")) {
      process.stderr.write(
        `Invalid --user format: '${entry}' (expected NAME:PASSWORD)\n`,
      );
      process.exit(1);
    }
    const colonIdx = entry.indexOf(":");
    const name = entry.slice(0, colonIdx);
    const password = entry.slice(colonIdx + 1);
    creds[name] = password;
  }
  return creds;
}

export function main(argv?: string[]): void {
  const args = parseArgs(argv ?? process.argv.slice(2));

  const credentials = buildCredentials(args.user);

  if (args.auth !== "trust" && !credentials) {
    process.stderr.write(
      `Authentication method '${args.auth}' requires at least one ` +
        `--user NAME:PASSWORD\n`,
    );
    process.exit(1);
  }

  const config = createConfig({
    host: args.host,
    port: args.port,
    dbPath: args.db,
    authMethod: args.auth,
    credentials,
    sslCertfile: args.sslCert,
    sslKeyfile: args.sslKey,
    maxConnections: args.maxConnections,
  });

  const server = new PGWireServer(config);

  let stopping = false;

  const shutdown = (): void => {
    if (stopping) return;
    stopping = true;
    server
      .stop()
      .then(() => {
        process.stdout.write("usqldb-server stopped\n");
        process.exit(0);
      })
      .catch((err) => {
        process.stderr.write(`Error during shutdown: ${err}\n`);
        process.exit(1);
      });
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);

  const dbDesc = args.db ?? "(in-memory)";
  process.stdout.write(
    `usqldb-server starting on ${args.host}:${args.port} ` +
      `[db=${dbDesc}, auth=${args.auth}]\n`,
  );

  server
    .start()
    .then(() => {
      const actualPort = server.port;
      if (actualPort !== args.port) {
        process.stdout.write(`Listening on port ${actualPort}\n`);
      }
      // Keep the process alive
      return server.serveForever();
    })
    .catch((err) => {
      process.stderr.write(`Failed to start server: ${err}\n`);
      process.exit(1);
    });
}

// Allow direct execution
const isMainModule =
  typeof process !== "undefined" &&
  process.argv[1] !== undefined &&
  (process.argv[1].endsWith("server-cli.ts") ||
    process.argv[1].endsWith("server-cli.js"));

if (isMainModule) {
  main();
}
