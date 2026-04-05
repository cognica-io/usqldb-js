# usqldb-js

PostgreSQL 17-compatible layer for [UQA](https://github.com/cognica-io/uqa-js) — system catalogs, psql-style CLI, and wire protocol server.

usqldb-js extends the UQA SQL engine with a comprehensive set of PostgreSQL system catalog views so that standard PostgreSQL tools — psql, SQLAlchemy, DBeaver, DataGrip, Django, and others — can introspect the database as if it were a real PostgreSQL 17 instance.

TypeScript port of [usqldb](https://github.com/cognica-io/usqldb) that runs in **web browsers** and Node.js.

## Features

- **23 information_schema views** — schemata, tables, columns, constraints, views, sequences, routines, foreign tables, triggers, and more.
- **35 pg_catalog tables** — pg_class, pg_attribute, pg_type, pg_constraint, pg_index, pg_proc, pg_settings, statistics views, and more, with consistent OID cross-references across all of them.
- **PostgreSQL v3 wire protocol server** — full Simple Query and Extended Query support with SCRAM-SHA-256 / MD5 / trust authentication. Connect with psql, DBeaver, DataGrip, JDBC, psycopg, asyncpg, node-postgres, and any other PostgreSQL client.
- **Interactive SQL shell** — psql-style REPL with backslash commands (`\d`, `\dt`, `\di`, `\dv`, `\ds`, `\df`, `\dn`, `\du`, `\l`, `\det`, `\des`, `\dew`, `\x`, `\timing`, `\o`, `\i`, `\e`), tab-completion, expanded display, query timing, multi-line editing, and ANSI color output (automatically disabled when piped).
- **Drop-in engine** — `USQLEngine` is a drop-in replacement for `Engine` from `@jaepil/uqa`. Import it, and every query gets full catalog support.
- **Persistent storage** — file-based SQLite databases that survive across process restarts.
- **Browser-compatible** — ESM and UMD builds with no Node.js-specific APIs.

## Requirements

- Node.js 20+ (for development and testing)
- `@jaepil/uqa` >= 0.3.7

## Installation

```bash
npm install @jaepil/usqldb
```

## Quick Start

### As a library

```typescript
import { USQLEngine } from "@jaepil/usqldb";

const engine = new USQLEngine();
await engine.sql("CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT)");
await engine.sql("INSERT INTO users (name) VALUES ('Alice')");

// information_schema
const cols = await engine.sql(
  "SELECT column_name, data_type " +
  "FROM information_schema.columns " +
  "WHERE table_name = 'users'"
);

// pg_catalog with OID joins
const types = await engine.sql(
  "SELECT c.relname, a.attname, t.typname " +
  "FROM pg_catalog.pg_class c " +
  "JOIN pg_catalog.pg_attribute a ON c.oid = a.attrelid " +
  "JOIN pg_catalog.pg_type t ON a.atttypid = t.oid " +
  "WHERE c.relname = 'users' AND a.attnum > 0"
);
```

### Wire protocol server

```typescript
import { PGWireServer, createConfig, AuthMethod } from "@jaepil/usqldb/net/pgwire";

const config = createConfig({
  host: "0.0.0.0",
  port: 5432,
  authMethod: AuthMethod.SCRAM_SHA_256,
  credentials: { admin: "secret123" },
});
const server = new PGWireServer(config);
await server.start();
```

Then connect with any PostgreSQL client:

```bash
psql -h localhost -p 5432 -U admin
```

### As a CLI

```bash
# In-memory database
npx usqldb

# Persistent storage
npx usqldb --db mydata.db

# Execute a single command and exit
npx usqldb -c "SELECT 1"
```

### Backslash Commands

```
General
  \q                  Quit
  \? [commands]       Show help
  \conninfo           Display connection info
  \encoding           Show client encoding
  \! [COMMAND]        Execute shell command

Informational
  \d [NAME]           Describe table/view/index or list all
  \dt[+] [PATTERN]    List tables
  \di[+] [PATTERN]    List indexes
  \dv[+] [PATTERN]    List views
  \ds[+] [PATTERN]    List sequences
  \df[+] [PATTERN]    List functions
  \dn[+]              List schemas
  \du                 List roles
  \l[+]               List databases
  \det                List foreign tables
  \des                List foreign servers
  \dew                List foreign data wrappers

Formatting
  \x                  Toggle expanded display
  \timing             Toggle timing of commands

Input/Output
  \o [FILE]           Send output to file or stdout
  \i FILE             Execute commands from file
  \e [FILE]           Edit query or file with $EDITOR
```

### Browser Usage

```html
<script src="https://cdn.jsdelivr.net/npm/@jaepil/uqa/dist/uqa.umd.js"></script>
<script src="https://cdn.jsdelivr.net/npm/@jaepil/usqldb/dist/usqldb.umd.js"></script>
<script>
  const engine = new usqldb.USQLEngine();
  // ... use engine.sql() as above
</script>
```

## Project Structure

```
src/
  index.ts                          Public API exports
  core/
    engine.ts                       USQLEngine — drop-in replacement for Engine
    compiler.ts                     Row normalization utilities
  pg-compat/
    oid.ts                          OID allocation and PostgreSQL type mapping
    information-schema.ts           23 information_schema view builders
    pg-catalog.ts                   35 pg_catalog table builders
  net/
    pgwire/
      index.ts                     Wire protocol public API
      server.ts                    TCP server with connection management
      connection.ts                Per-client connection handler
      query-executor.ts            Simple Query and Extended Query executor
      server-cli.ts                Standalone server entry point
      auth.ts                      SCRAM-SHA-256 / MD5 / trust authentication
      config.ts                    Server configuration
      message-codec.ts             Frontend/backend message encoding/decoding
      messages.ts                  Message type definitions
      type-codec.ts                PostgreSQL type serialization/deserialization
      read-buffer.ts               Streaming TCP read buffer
      write-buffer.ts              Binary write buffer
      constants.ts                 Protocol constants and format codes
      errors.ts                    PostgreSQL error response builder
  cli/
    index.ts                       CLI public API
    shell.ts                       Interactive SQL shell
    shell-cli.ts                   CLI entry point
    command-handler.ts             Backslash command handlers
    completer.ts                   Context-aware SQL tab-completion
    formatter.ts                   psql-compatible tabular and expanded output
tests/
  pg-compat.test.ts                74 tests covering all catalog views
  catalog-completeness.test.ts     94 tests for catalog query correctness
  cli/
    cli.test.ts                    38 tests for CLI and backslash commands
  net/pgwire/
    server.test.ts                 17 tests for wire protocol server
    auth.test.ts                   12 tests for authentication
    message-codec.test.ts          35 tests for message encoding/decoding
    type-codec.test.ts             53 tests for type serialization
    buffer.test.ts                 20 tests for read/write buffers
    errors.test.ts                 13 tests for error responses
examples/
  01_basic_usage.ts                Basic SQL operations
  02_persistent_storage.ts         File-based persistent database
  03_catalog_introspection.ts      information_schema and pg_catalog queries
  04_pgwire_server.ts              Wire protocol server
  05_pgwire_auth.ts                SCRAM-SHA-256 authentication
  06_pgwire_client.ts              Connecting with node-postgres
  07_pgwire_persistent.ts          Wire protocol with persistent storage
  08_pgwire_shared_engine.ts       Shared engine across connections
  09_advanced_schema.ts            Complex schema with constraints
  10_usqldb_server_cli.sh          Server CLI usage
```

## Development

```bash
# Install dependencies
npm install

# Type check
npm run check

# Run tests
npm test

# Build (type check + Vite bundle + type declarations)
npm run build

# Lint
npm run lint

# Format
npm run format
```

## License

AGPL-3.0-only

## Author

Jaepil Jeong (jaepil@cognica.io) — [Cognica, Inc.](https://github.com/cognica-io)
