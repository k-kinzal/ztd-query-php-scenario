# ZTD Query Adapter Specifications — Index

> **Single source of truth.** The numbered section files listed below
> (`01-connection.ears.md` through `11-known-issues.ears.md`) are the
> authoritative specifications. All spec updates must target these files.
> The original monolith (`ztd-query-adapter.ears.md`) is preserved for
> historical reference only and must not be updated.

Version: ztd-query-mysqli-adapter v0.1.1, ztd-query-pdo-adapter v0.1.1

## Version Matrix

### Verified Versions (actually tested)

| Component | Verified Versions |
|-----------|-------------------|
| PHP | 8.1, 8.2, 8.3, 8.4, 8.5 |
| MySQL | 8.0 |
| PostgreSQL | 14, 16, 17 |
| SQLite | 3.x (bundled with PHP) |
| ztd-query-pdo-adapter | 0.1.1 |
| ztd-query-mysqli-adapter | 0.1.1 |
| ztd-query-sqlite | 0.1.1 |
| ztd-query-postgres | 0.1.1 |

### PHP Version Test Results (SQLite, 2026-03-09)

| PHP Version | Tests | Pass | Errors | Failures |
|-------------|-------|------|--------|----------|
| 8.1.34 | 1347 | 1173 | 129 | 12 |
| 8.2.30 | 1347 | 1173 | 129 | 12 |
| 8.3.30 | 1347 | 1173 | 129 | 12 |
| 8.4.18 | 1347 | 1180 | 122 | 12 |
| 8.5.1 (local) | 2030 | 1853 | 111 | 11 |

Errors are ZTD CTE rewriter bugs (SQL syntax errors in generated queries), not test or adapter issues.

### Intended Compatibility (aspirational, not regularly tested)

| Component | Range |
|-----------|-------|
| PHP | 8.1 – 8.5 |
| MySQL | 5.6 – 9.1 |
| PostgreSQL | 14 – 18 |
| SQLite | 3 |

### Running Against Different Versions

```bash
# Test against MySQL 5.7
MYSQL_IMAGE=mysql:5.7 vendor/bin/phpunit

# Test against PostgreSQL 14
POSTGRES_IMAGE=postgres:14 vendor/bin/phpunit

# Run a specific test class
vendor/bin/phpunit --filter MysqlBasicCrudTest

# Test against different PHP versions using Docker
docker build --build-arg PHP_VERSION=8.1 -t ztd-test-php81 .
docker run --rm ztd-test-php81 --filter 'Tests\\Pdo\\Sqlite'

# Run the full PHP version matrix
./scripts/run-php-version-matrix.sh
```

## Spec Sections

| File | Section | SPEC-IDs | Description |
|------|---------|----------|-------------|
| [01-connection.ears.md](01-connection.ears.md) | 1. Connection | SPEC-1.1 – SPEC-1.7 | Connection creation, wrapping, auto-detection, schema reflection |
| [02-ztd-mode.ears.md](02-ztd-mode.ears.md) | 2. ZTD Mode | SPEC-2.1 – SPEC-2.4 | Enable/disable, isolation semantics, session state |
| [03-read-operations.ears.md](03-read-operations.ears.md) | 3. Read Operations | SPEC-3.1 – SPEC-3.7 | SELECT, prepared SELECT, complex queries, JSON, composite PKs, NULL handling, fetch methods |
| [04-write-operations.ears.md](04-write-operations.ears.md) | 4. Write Operations | SPEC-4.1 – SPEC-4.12 | INSERT, UPDATE, DELETE, UPSERT, transactions, utility methods |
| [05-ddl-operations.ears.md](05-ddl-operations.ears.md) | 5. DDL Operations | SPEC-5.1 – SPEC-5.3 | CREATE TABLE, ALTER TABLE, DROP TABLE, TRUNCATE |
| [06-unsupported-sql.ears.md](06-unsupported-sql.ears.md) | 6. Unsupported SQL | SPEC-6.1 – SPEC-6.5 | Default behavior, behavior rules, transaction statements, EXPLAIN, CALL |
| [07-unknown-schema.ears.md](07-unknown-schema.ears.md) | 7. Unknown Schema | SPEC-7.1 – SPEC-7.4 | Passthrough, Exception, EmptyResult, Notice modes |
| [08-constraints.ears.md](08-constraints.ears.md) | 8. Constraints | SPEC-8.1 – SPEC-8.3 | Shadow store constraint behavior, error recovery, trigger behavior |
| [09-configuration.ears.md](09-configuration.ears.md) | 9. Configuration | SPEC-9.1 – SPEC-9.2 | ZtdConfig, default configuration |
| [10-platform-notes.ears.md](10-platform-notes.ears.md) | 10. Platform Notes | SPEC-10.2.1 – SPEC-10.2.169 | Platform-specific behavioral notes |
| [11-known-issues.ears.md](11-known-issues.ears.md) | 11. Known Issues | SPEC-11.x | Cross-platform inconsistencies with issue tags |

## Supporting Files

| File | Purpose |
|------|---------|
| [traceability.md](traceability.md) | Spec ID → Test Class → Verified Version matrix |
| [ztd-query-adapter.ears.md](ztd-query-adapter.ears.md) | Original monolithic spec (preserved for reference) |

## SPEC-ID Convention

- Format: `SPEC-{section}.{item}[{suffix}]`
- Examples: `SPEC-1.1`, `SPEC-3.3a`, `SPEC-4.2b`, `SPEC-11.PDO-UPSERT`
- Each ID appears in exactly one section file
- IDs match the original monolith's section numbering
- Status values: `Verified`, `Known Issue`, `Partially Verified`
- Platform scope uses adapter shorthand: `MySQLi`, `MySQL-PDO`, `PostgreSQL-PDO`, `SQLite-PDO`
