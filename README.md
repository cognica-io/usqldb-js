# usqldb-js

PostgreSQL 17-compatible `information_schema` and `pg_catalog` layer for [UQA](https://github.com/cognica-io/uqa-js).

usqldb-js extends the UQA SQL engine with a comprehensive set of PostgreSQL system catalog views so that standard PostgreSQL tools — psql, SQLAlchemy, DBeaver, DataGrip, Django, and others — can introspect the database as if it were a real PostgreSQL 17 instance.

TypeScript port of [usqldb](https://github.com/cognica-io/usqldb) that runs in **web browsers** and Node.js.

## Features

- **23 information_schema views** — schemata, tables, columns, constraints, views, sequences, routines, foreign tables, triggers, and more.
- **35 pg_catalog tables** — pg_class, pg_attribute, pg_type, pg_constraint, pg_index, pg_proc, pg_settings, statistics views, and more, with consistent OID cross-references across all of them.
- **Drop-in engine** — `USQLEngine` is a drop-in replacement for `Engine` from `@jaepil/uqa`. Import it, and every query gets full catalog support.
- **Browser-compatible** — ESM and UMD builds with no Node.js-specific APIs.

## Requirements

- Node.js 20+ (for development and testing)
- `@jaepil/uqa` >= 0.2.0

## Installation

```bash
npm install @jaepil/usqldb
```

## Quick Start

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
tests/
  pg-compat.test.ts                 69 tests covering all catalog views
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
