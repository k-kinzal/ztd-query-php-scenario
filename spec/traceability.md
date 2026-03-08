# Traceability Matrix

Maps SPEC-IDs to test classes and verified versions.

**Last updated:** 2026-03-08
**Verified environment:** PHP 8.3–8.5, MySQL 8.0, PostgreSQL 16, SQLite 3.x, ztd-query-pdo-adapter v0.1.1, ztd-query-mysqli-adapter v0.1.1

## How to read this matrix

- **SPEC-ID**: Unique identifier from the modular spec files
- **Test Classes**: PHPUnit test classes that verify the spec item (relative to `tests/`)
- **Adapters**: Which adapters are covered (Mi = MySQLi, MP = MySQL-PDO, PG = PostgreSQL-PDO, SL = SQLite-PDO)
- **Status**: V = Verified, P = Partially Verified, K = Known Issue

## 1. Connection

| SPEC-ID | Test Classes | Mi | MP | PG | SL | Status |
|---------|-------------|----|----|----|----|--------|
| SPEC-1.1 | `Mysqli/BasicCrudTest`, `Mysqli/FromMysqliTest` | V | — | — | — | V |
| SPEC-1.2 | `Pdo/MysqlBasicCrudTest`, `Pdo/PostgresBasicCrudTest`, `Pdo/SqliteBasicCrudTest` | — | V | V | V | V |
| SPEC-1.3 | `Mysqli/FromMysqliTest` | V | — | — | — | V |
| SPEC-1.4 | `Pdo/MysqlFromPdoBehaviorTest`, `Pdo/PostgresFromPdoBehaviorTest`, `Pdo/SqliteFromPdoBehaviorTest` | — | V | V | V | V |
| SPEC-1.4a | `Pdo/MysqlConnectFactoryTest`, `Pdo/PostgresConnectFactoryTest`, `Pdo/SqliteConnectFactoryTest` | — | V | V | V | V |
| SPEC-1.5 | `Pdo/AutoDetectionTest`, `Pdo/BasicCrudTest` | — | V | V | V | V |
| SPEC-1.6 | `Mysqli/SchemaReflectionTest`, `Pdo/MysqlSchemaReflectionTest`, `Pdo/PostgresSchemaReflectionTest`, `Pdo/SqliteSchemaReflectionTest`, `Pdo/SchemaReflectionTest` | V | V | V | V | V |
| SPEC-1.7 | `Pdo/AutoDetectionTest` | — | V | V | V | V |

## 2. ZTD Mode

| SPEC-ID | Test Classes | Mi | MP | PG | SL | Status |
|---------|-------------|----|----|----|----|--------|
| SPEC-2.1 | `Scenarios/BasicCrudScenario` (all), `*ZtdLifecycleTest`, `*ZtdToggleErrorHandlingTest` | V | V | V | V | V |
| SPEC-2.2 | `Scenarios/BasicCrudScenario` (all), `*PhysicalShadowOverlayTest`, `*ZtdToggleErrorHandlingTest` | V | V | V | V | V |
| SPEC-2.3 | `Scenarios/BasicCrudScenario` (all), `*ZtdToggleErrorHandlingTest` | V | V | V | V | V |
| SPEC-2.4 | `*SessionIsolationTest`, `*ConcurrentInstancesTest` | V | V | V | V | V |

## 3. Read Operations

| SPEC-ID | Test Classes | Mi | MP | PG | SL | Status |
|---------|-------------|----|----|----|----|--------|
| SPEC-3.1 | `Scenarios/BasicCrudScenario` (all), `*CursorPaginationTest`, `*SoftDeletePatternTest`, `*DecimalPrecisionTest`, `*OffsetPaginationTest`, `*TypeRoundtripTest` | V | V | V | V | V |
| SPEC-3.2 | `Scenarios/PreparedStatementScenario` (all), `*PreparedStatementTest`, `Mysqli/StatementReusePatternTest`, `*CursorPaginationTest`, `*OptimisticLockingTest`, `*PreparedInListTest`, `*OffsetPaginationTest` | V | V | V | V | V |
| SPEC-3.3 | `Scenarios/JoinAndSubqueryScenario` (all), `*ComplexQueryTest`, `*AdvancedQueryPatternsTest`, `*SubqueryPositionsTest`, `*DateTimeFunctionsTest` | V | V | V | V | V |
| SPEC-3.3a | `*DerivedTableAndViewTest` | V | V | V | V | K |
| SPEC-3.3b | `*ViewThroughZtdTest` | V | V | V | V | K |
| SPEC-3.3c | `*RecursiveCteAndRightJoinTest` | V | V | V | V | K |
| SPEC-3.3d | `*ExceptIntersectTest`, `*SetOperationsAndFunctionsTest` | V | V | V | V | P |
| SPEC-3.3e | `*CteDmlTest` | V | V | V | V | K |
| SPEC-3.4 | `*FetchMethodsTest`, `*FetchModesTest`, `*FetchModeAdvancedTest` | V | V | V | V | V |

## 4. Write Operations

| SPEC-ID | Test Classes | Mi | MP | PG | SL | Status |
|---------|-------------|----|----|----|----|--------|
| SPEC-4.1 | `Scenarios/BasicCrudScenario` (all), `Scenarios/WriteOperationScenario` (all), `*BatchInsertTest`, `*DecimalPrecisionTest`, `*TypeRoundtripTest` | V | V | V | V | V |
| SPEC-4.1a | `*InsertSelectUpsertTest`, `*InsertSubqueryPatternsTest` | V | V | V | V | P |
| SPEC-4.2 | `Scenarios/BasicCrudScenario` (all), `Scenarios/WriteOperationScenario` (all), `*OptimisticLockingTest`, `*SoftDeletePatternTest`, `*DecimalPrecisionTest` | V | V | V | V | V |
| SPEC-4.2a | `*UpsertTest`, `*PreparedUpsertTest`, `Mysqli/InsertModifiersTest` | V | V | V | V | P |
| SPEC-4.2b | `*HavingAndReplaceTest`, `*ReplaceMultiRowTest`, `*ConflictResolutionTest`, `Mysqli/InsertModifiersTest` | V | V | — | V | P |
| SPEC-4.2c | `*MultiTableOperationsTest` | V | V | V | — | V |
| SPEC-4.2d | `*MultiTableDeleteTest`, `*MultiTableOperationsTest` | V | V | V | — | P |
| SPEC-4.2e | `*InsertIgnoreTest`, `Mysqli/InsertModifiersTest` | V | V | V | V | V |
| SPEC-4.3 | `Scenarios/BasicCrudScenario` (all), `*DeleteWithoutWhereTest` | V | V | V | V | V |
| SPEC-4.4 | `*ExecReturnValueTest`, `*RowCountTest`, `*OptimisticLockingTest` | V | V | V | V | V |
| SPEC-4.5 | `*WriteResultSetTest` | V | V | V | V | V |
| SPEC-4.6 | `Mysqli/RealQueryTest` | V | — | — | — | V |
| SPEC-4.7 | `Mysqli/StatementIntrospectionTest`, `Mysqli/InsertIdBehaviorTest` | V | — | — | — | V |
| SPEC-4.8 | `Scenarios/TransactionScenario` (all), `*TransactionTest`, `*TransactionWithShadowTest` | V | V | V | V | V |
| SPEC-4.9 | `Mysqli/DelegatedMethodsTest`, `*UtilityMethodsTest` | V | V | V | V | V |
| SPEC-4.10 | `*FetchClassTest` | — | V | V | V | V |
| SPEC-4.11 | `*ErrorModeInteractionTest` | — | V | V | V | V |
| SPEC-4.12 | `Mysqli/StatementIntrospectionTest`, `Mysqli/StatementMethodsTest` | V | — | — | — | V |

## 5. DDL Operations

| SPEC-ID | Test Classes | Mi | MP | PG | SL | Status |
|---------|-------------|----|----|----|----|--------|
| SPEC-5.1 | `*DdlOperationsTest` | V | V | V | V | V |
| SPEC-5.1a | `*AlterTableTest`, `*AlterTableAdvancedTest`, `*AlterTableAfterDataTest`, `*AlterTableErrorTest` | V | V | V | V | P |
| SPEC-5.1b | `*CreateTableVariantsTest` | V | V | V | V | V |
| SPEC-5.1c | `*CtasTest`, `Pdo/SqliteCtasEmptyResultTest` | V | V | V | V | P |
| SPEC-5.2 | `*DdlOperationsTest`, `Pdo/PostgresDropTableCascadeTest` | V | V | V | V | V |
| SPEC-5.3 | `*TruncateReinsertTest`, `Pdo/PostgresTruncateOptionsTest` | V | V | V | — | V |

## 6. Unsupported SQL

| SPEC-ID | Test Classes | Mi | MP | PG | SL | Status |
|---------|-------------|----|----|----|----|--------|
| SPEC-6.1 | `*UnsupportedSqlTest` | V | V | V | V | V |
| SPEC-6.2 | `*BehaviorRuleCombinationsTest`, `*BehaviorRuleConfigTest` | V | V | V | V | V |
| SPEC-6.3 | `*SavepointTest`, `*SavepointBehaviorTest` | V | V | V | V | V |

## 7. Unknown Schema

| SPEC-ID | Test Classes | Mi | MP | PG | SL | Status |
|---------|-------------|----|----|----|----|--------|
| SPEC-7.1 | `*UnknownSchemaTest` | V | V | V | V | P |
| SPEC-7.2 | `*UnknownSchemaTest` | V | V | V | V | P |
| SPEC-7.3 | `Pdo/*UnknownSchemaTest` | — | V | V | V | P |
| SPEC-7.4 | `*UnknownSchemaTest` | V | V | V | V | P |

## 8. Constraints

| SPEC-ID | Test Classes | Mi | MP | PG | SL | Status |
|---------|-------------|----|----|----|----|--------|
| SPEC-8.1 | `*ConstraintBehaviorTest`, `*CheckConstraintBehaviorTest` | V | V | V | V | V |
| SPEC-8.2 | `*ErrorRecoveryTest`, `*ErrorBoundaryTest` | V | V | V | V | V |

## 9. Configuration

| SPEC-ID | Test Classes | Mi | MP | PG | SL | Status |
|---------|-------------|----|----|----|----|--------|
| SPEC-9.1 | `*ConfigurationTest` | V | V | V | V | V |
| SPEC-9.2 | `*ConfigurationTest` | V | V | V | V | V |

## 10. Platform Notes (Selected)

| SPEC-ID | Test Classes | Mi | MP | PG | SL | Status |
|---------|-------------|----|----|----|----|--------|
| SPEC-10.2.17 | `Pdo/MysqlOffsetPaginationTest` | — | V | — | — | V |
| SPEC-10.2.18 | `*DateTimeFunctionsTest` | V | V | V | V | V |

## Legend

- `*FooTest` = shorthand for all platform variants (e.g., `Mysqli/FooTest`, `Pdo/MysqlFooTest`, `Pdo/PostgresFooTest`, `Pdo/SqliteFooTest`)
- `Scenarios/FooScenario` = shared trait used by platform-specific test classes
- Mi = MySQLi adapter, MP = MySQL PDO adapter, PG = PostgreSQL PDO adapter, SL = SQLite PDO adapter
- V = Verified, P = Partially Verified, K = Known Issue
- `—` = Not applicable for this adapter/platform
