# History

## 0.1.0 (2026-04-01)

Initial release. TypeScript port of [usqldb](https://github.com/cognica-io/usqldb).

### Core

- `USQLEngine` — drop-in replacement for `Engine` from `@jaepil/uqa` with full PostgreSQL 17 catalog support.
- `OIDAllocator` — deterministic OID assignment matching PostgreSQL 17 conventions (system 0-16383, user 16384+).
- Row normalization for Arrow-compatible storage (boolean to integer, NaN/Infinity to null).

### PostgreSQL Compatibility

- 23 `information_schema` views: schemata, tables, columns, table_constraints, key_column_usage, referential_constraints, constraint_column_usage, check_constraints, views, sequences, routines, parameters, foreign_tables, foreign_servers, foreign_server_options, foreign_table_options, enabled_roles, applicable_roles, character_sets, collations, domains, element_types, triggers.
- 35 `pg_catalog` tables: pg_namespace, pg_class, pg_attribute, pg_type, pg_constraint, pg_index, pg_attrdef, pg_am, pg_database, pg_roles, pg_user, pg_tables, pg_views, pg_indexes, pg_matviews, pg_sequences, pg_settings, pg_foreign_server, pg_foreign_table, pg_foreign_data_wrapper, pg_description, pg_depend, pg_stat_user_tables, pg_stat_user_indexes, pg_stat_activity, pg_proc, pg_extension, pg_collation, pg_enum, pg_inherits, pg_trigger, pg_statio_user_tables, pg_auth_members, pg_available_extensions, pg_stat_all_tables.
- Consistent OID cross-references across all catalog tables (e.g., pg_class.oid = pg_attribute.attrelid).

### Browser Support

- ESM and UMD builds via Vite.
- No Node.js-specific APIs — runs in web browsers.
