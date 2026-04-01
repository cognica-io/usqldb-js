//
// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
//
// Copyright (c) 2023-2026 Cognica, Inc.
//

// PostgreSQL 17-compatible information_schema and pg_catalog for UQA.
//
// This package extends UQA's SQL compiler with a comprehensive set of
// PostgreSQL system catalog views, enabling compatibility with standard
// PostgreSQL tools (psql, SQLAlchemy, DBeaver, DataGrip, Django, etc.).

export { USQLEngine } from "./core/engine.js";
export { normalizeRows } from "./core/compiler.js";
export { OIDAllocator } from "./pg-compat/oid.js";
export { InformationSchemaProvider } from "./pg-compat/information-schema.js";
export { PGCatalogProvider } from "./pg-compat/pg-catalog.js";
