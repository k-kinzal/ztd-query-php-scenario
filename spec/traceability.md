# Traceability Matrix

Maps SPEC-IDs to test classes and verified versions.

**Last updated:** 2026-03-09
**Verified environment:** PHP 8.1–8.5, MySQL 8.0, PostgreSQL 14/16/17, SQLite 3.x, ztd-query-pdo-adapter v0.1.1, ztd-query-mysqli-adapter v0.1.1

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
| SPEC-3.1 | `Scenarios/BasicCrudScenario` (all), `*CursorPaginationTest`, `*SoftDeletePatternTest`, `*DecimalPrecisionTest`, `*OffsetPaginationTest`, `*TypeRoundtripTest`, `*EagerLoadingPatternTest` | V | V | V | V | V |
| SPEC-3.2 | `Scenarios/PreparedStatementScenario` (all), `*PreparedStatementTest`, `Mysqli/StatementReusePatternTest`, `*CursorPaginationTest`, `*OptimisticLockingTest`, `*PreparedInListTest`, `*OffsetPaginationTest` | V | V | V | V | V |
| SPEC-3.3 | `Scenarios/JoinAndSubqueryScenario` (all), `*ComplexQueryTest`, `*AdvancedQueryPatternsTest`, `*SubqueryPositionsTest`, `*DateTimeFunctionsTest`, `Pdo/SqliteNestedCaseExpressionTest`, `*MultiAliasJoinTest`, `*SubqueryNestingTest` | V | V | V | V | V |
| SPEC-3.3a | `*DerivedTableAndViewTest`, `*SubqueryNestingTest` | V | V | V | V | K |
| SPEC-3.3b | `*ViewThroughZtdTest` | V | V | V | V | K |
| SPEC-3.3c | `*RecursiveCteAndRightJoinTest` | V | V | V | V | K |
| SPEC-3.3d | `*ExceptIntersectTest`, `*SetOperationsAndFunctionsTest` | V | V | V | V | P |
| SPEC-3.3e | `*CteDmlTest` | V | V | V | V | K |
| SPEC-3.3f | `*FullTextSearchTest` | K | K | K | K | K |
| SPEC-3.3g | `*StoredProcedureTest`, `Pdo/PostgresStoredFunctionTest` | V | V | P | — | V |
| SPEC-3.4 | `*FetchMethodsTest`, `*FetchModesTest`, `*FetchModeAdvancedTest` | V | V | V | V | V |

## 4. Write Operations

| SPEC-ID | Test Classes | Mi | MP | PG | SL | Status |
|---------|-------------|----|----|----|----|--------|
| SPEC-4.1 | `Scenarios/BasicCrudScenario` (all), `Scenarios/WriteOperationScenario` (all), `*BatchInsertTest`, `*DecimalPrecisionTest`, `*TypeRoundtripTest` | V | V | V | V | V |
| SPEC-4.1a | `*InsertSelectUpsertTest`, `*InsertSubqueryPatternsTest`, `Pdo/SqliteInsertSelectExpressionsTest` | V | V | V | V | P |
| SPEC-4.2 | `Scenarios/BasicCrudScenario` (all), `Scenarios/WriteOperationScenario` (all), `*OptimisticLockingTest`, `*SoftDeletePatternTest`, `*DecimalPrecisionTest` | V | V | V | V | V |
| SPEC-4.2a | `*UpsertTest`, `*PreparedUpsertTest`, `Mysqli/InsertModifiersTest` | V | V | V | V | P |
| SPEC-4.2b | `*HavingAndReplaceTest`, `*ReplaceMultiRowTest`, `*ConflictResolutionTest`, `Mysqli/InsertModifiersTest` | V | V | — | V | P |
| SPEC-4.2c | `*MultiTableOperationsTest` | V | V | V | — | V |
| SPEC-4.2d | `*MultiTableDeleteTest`, `*MultiTableOperationsTest` | V | V | V | — | P |
| SPEC-4.2e | `*InsertIgnoreTest`, `Mysqli/InsertModifiersTest` | V | V | V | V | V |
| SPEC-4.3 | `Scenarios/BasicCrudScenario` (all), `*DeleteWithoutWhereTest`, `Pdo/SqliteDeleteWithOrderByLimitTest` | V | V | V | V | V |
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
| SPEC-5.1a | `*AlterTableTest`, `*AlterTableAdvancedTest`, `*AlterTableAfterDataTest`, `*AlterTableErrorTest`, `Pdo/SqliteAlterTableSilentFailureTest` | V | V | V | V | P |
| SPEC-5.1b | `*CreateTableVariantsTest` | V | V | V | V | V |
| SPEC-5.1c | `*CtasTest`, `Pdo/SqliteCtasEmptyResultTest`, `Pdo/SqliteCtasDataLossTest` | V | V | V | V | P |
| SPEC-5.2 | `*DdlOperationsTest`, `Pdo/PostgresDropTableCascadeTest` | V | V | V | V | V |
| SPEC-5.3 | `*TruncateReinsertTest`, `Pdo/PostgresTruncateOptionsTest` | V | V | V | — | V |

## 6. Unsupported SQL

| SPEC-ID | Test Classes | Mi | MP | PG | SL | Status |
|---------|-------------|----|----|----|----|--------|
| SPEC-6.1 | `*UnsupportedSqlTest` | V | V | V | V | V |
| SPEC-6.2 | `*BehaviorRuleCombinationsTest`, `*BehaviorRuleConfigTest` | V | V | V | V | V |
| SPEC-6.3 | `*SavepointTest`, `*SavepointBehaviorTest` | V | V | V | V | V |
| SPEC-6.4 | `*ExplainQueryTest`, `Pdo/SqliteExplainPragmaTest` | V | V | V | V | V |
| SPEC-6.5 | `Mysqli/StoredProcedureTest`, `Pdo/MysqlStoredProcedureTest` | V | V | — | — | V |

## 7. Unknown Schema

| SPEC-ID | Test Classes | Mi | MP | PG | SL | Status |
|---------|-------------|----|----|----|----|--------|
| SPEC-7.1 | `*UnknownSchemaTest`, `*UnknownSchemaWorkflowTest` | V | V | V | V | P |
| SPEC-7.2 | `*UnknownSchemaTest`, `*UnknownSchemaWorkflowTest` | V | V | V | V | P |
| SPEC-7.3 | `Pdo/*UnknownSchemaTest`, `*UnknownSchemaWorkflowTest` | — | V | V | V | P |
| SPEC-7.4 | `*UnknownSchemaTest`, `*UnknownSchemaWorkflowTest` | V | V | V | V | P |

## 8. Constraints

| SPEC-ID | Test Classes | Mi | MP | PG | SL | Status |
|---------|-------------|----|----|----|----|--------|
| SPEC-8.1 | `*ConstraintBehaviorTest`, `*CheckConstraintBehaviorTest` | V | V | V | V | V |
| SPEC-8.2 | `*ErrorRecoveryTest`, `*ErrorBoundaryTest` | V | V | V | V | V |
| SPEC-8.3 | `*TriggerInteractionTest` | V | V | V | V | V |

## 9. Configuration

| SPEC-ID | Test Classes | Mi | MP | PG | SL | Status |
|---------|-------------|----|----|----|----|--------|
| SPEC-9.1 | `*ConfigurationTest` | V | V | V | V | V |
| SPEC-9.2 | `*ConfigurationTest` | V | V | V | V | V |

## 3 (continued). Read Operations

| SPEC-ID | Test Classes | Mi | MP | PG | SL | Status |
|---------|-------------|----|----|----|----|--------|
| SPEC-3.5 | `*JsonFunctionsTest`, `*JsonAndCrossJoinTest`, `Pdo/PostgresJsonbFunctionsTest`, `Pdo/PostgresJsonbOperatorsTest`, `*JsonAggregationEdgeCasesTest`, `Pdo/PostgresJsonbAggregationEdgeCasesTest` | P | P | P | P | P |
| SPEC-3.6 | `*CompositePrimaryKeyTest`, `Mysqli/CompositePkUpsertTest`, `Pdo/PostgresCompositePkUpsertTest`, `*CompositePkEdgeCasesTest` | V | V | V | V | V |
| SPEC-3.7 | `*NullHandlingEdgeCasesTest`, `*NullInAggregatesTest` | V | V | V | V | V |

## 10. Platform Notes (Selected)

| SPEC-ID | Test Classes | Mi | MP | PG | SL | Status |
|---------|-------------|----|----|----|----|--------|
| SPEC-10.2.17 | `Pdo/MysqlOffsetPaginationTest` | — | V | — | — | V |
| SPEC-10.2.18 | `*DateTimeFunctionsTest` | V | V | V | V | V |
| SPEC-10.2.19 | `Pdo/PostgresEnumTypeTest` | — | — | V | — | V |
| SPEC-10.2.20 | `Pdo/MysqlInformationSchemaTest`, `Pdo/PostgresInformationSchemaTest`, `Pdo/SqliteSchemaIntrospectionTest` | — | V | V | V | V |
| SPEC-10.2.21 | `*MultiTenantPatternTest` | V | V | V | V | V |
| SPEC-10.2.22 | `*GeneratedColumnTest`, `*GeneratedColumnEdgeCasesTest` | V | V | V | V | V |
| SPEC-10.2.23 | `*WindowFrameTest`, `*WindowFunctionWithPreparedStmtTest`, `*DateAndAdvancedWindowTest`, `*WindowFunctionEdgeCasesTest` | V | V | V | V | V |
| SPEC-10.2.24 | `*ColumnAliasingTest`, `*ColumnAliasingEdgeCasesTest` | V | V | V | V | V |
| SPEC-10.2.25 | `Pdo/SqliteSqlCommentsTest` | — | — | — | V | V |
| SPEC-10.2.26 | `Pdo/SqliteReservedKeywordIdentifierTest` | — | — | — | V | V |
| SPEC-10.2.27 | `Pdo/PostgresLateralSubqueryTest` | — | — | K | — | K |
| SPEC-10.2.28 | `Pdo/PostgresCteMaterializedTest` | — | — | V | — | V |
| SPEC-10.2.29 | `Pdo/PostgresSequenceTest` | — | — | V | — | V |
| SPEC-10.2.30 | `Pdo/PostgresDollarQuotedStringTest` | — | — | V | — | V |
| SPEC-10.2.31 | `*TagFilteringTest` | V | V | V | V | V |
| SPEC-10.2.32 | `*HierarchicalSelfJoinTest` | V | V | V | V | V |
| SPEC-10.2.33 | `*InventoryWorkflowTest` | V | V | V | V | V |
| SPEC-10.2.34 | `*PivotReportTest`, `*PivotMultiLevelTest` | V | V | V | V | V |
| SPEC-10.2.35 | `*MultiStepTransformationTest` | V | V | V | V | V |
| SPEC-10.2.36 | `*PeriodComparisonTest` | V | V | V | V | V |
| SPEC-10.2.37 | `*EventLogTest` | V | V | V | V | V |
| SPEC-10.2.38 | `*DeduplicationPatternTest`, `*DeduplicationEdgeCasesTest` | V | V | V | V | V |
| SPEC-10.2.39 | `*DataReconciliationTest` | V | V | V | V | V |
| SPEC-10.2.40 | `*LedgerRunningBalanceTest` | V | V | V | V | V |
| SPEC-10.2.41 | `*CascadeCleanupTest` | V | V | V | V | V |
| SPEC-10.2.42 | `*BulkConditionalUpgradeTest` | P | P | P | P | P |
| SPEC-10.2.43 | `*RbacPermissionCheckTest` | V | V | V | V | V |
| SPEC-10.2.44 | `*JobQueueProcessingTest` | V | V | V | V | V |
| SPEC-10.2.45 | `*InvoiceWorkflowTest` | V | V | V | V | V |
| SPEC-10.2.46 | `*ChainedMutationVisibilityTest` | V | V | V | V | V |
| SPEC-10.2.47 | `*SearchFilterPaginationTest` | V | V | V | V | V |
| SPEC-10.2.48 | `*CastAndTypeConversionTest` | V | V | V | V | V |
| SPEC-10.2.49 | `*LargeStringHandlingTest` | V | V | V | V | V |
| SPEC-10.2.50 | `*BitwiseOperationsTest` | V | V | V | P | P |
| SPEC-10.2.51 | `*DynamicFilterBuildingTest` | V | V | V | V | V |
| SPEC-10.2.52 | `*PaginationWithTotalCountTest` | V | V | V | V | V |
| SPEC-10.2.53 | `*DateIntervalTest` | V | V | V | V | V |
| SPEC-10.2.54 | `*MultiColumnSortingTest` | V | V | V | V | V |
| SPEC-10.2.55 | `*UpsertWorkflowTest` | V | V | V | V | V |
| SPEC-10.2.56 | `Pdo/PostgresUpdateFromJoinTest`, `Pdo/SqliteUpdateFromJoinTest` | — | — | V | K | P |
| SPEC-10.2.57 | `Pdo/PostgresPivotReportTest` | — | — | V | — | V |
| SPEC-10.2.58 | `*ReservationBookingTest` | V | V | V | V | V |
| SPEC-10.2.59 | `*LoyaltyPointsTest` | V | V | V | V | V |
| SPEC-10.2.60 | `*ContentVersioningTest` | V | V | V | V | V |
| SPEC-10.2.61 | `*ShoppingCartCheckoutTest` | V | V | V | V | V |
| SPEC-10.2.62 | `*SurveyResultsTest` | V | V | V | V | V |
| SPEC-10.2.63 | `*NotificationInboxTest` | V | V | V | V | V |

## Cross-Cutting Workflow and Integration Tests

The following test classes exercise combinations of multiple specs in realistic usage patterns. They are mapped to their primary spec and serve as integration-level verification.

| Primary SPEC | Test Classes | Mi | MP | PG | SL | Notes |
|-------------|-------------|----|----|----|----|-------|
| SPEC-3.3 | `*AnalyticsWorkflowTest`, `*AdvancedPlatformTest` | V | V | V | V | Complex queries, window functions, CTEs |
| SPEC-3.2 | `*OrmStyleCrudTest` | V | V | V | V | Prepare-once / execute-many pattern |
| SPEC-3.1 | `*ScaleTest`, `*LargeDatasetTest`, `*ExpressionClausesTest`, `*DiagnosticQueriesTest` | V | V | V | V | SELECT at scale, expressions, diagnostics |
| SPEC-4.1 | `*RestApiPatternTest`, `*BatchProcessingWorkflowTest`, `*DataMigrationWorkflowTest`, `*AdvancedOrderAndInsertPatternsTest` | V | V | V | V | Write-heavy workflows |
| SPEC-2.1 | `*RealisticWorkflowTest` | V | V | V | V | End-to-end ZTD enable/disable lifecycle |
| SPEC-10.2.11 | `*SelectForUpdateTest`, `*SelectLockingTest`, `Pdo/PostgresForUpdateSkipLockedTest` | V | V | V | V | Locking clauses (no-op in ZTD) |
| SPEC-10.2.14 | `Pdo/PostgresSpecificFeaturesTest` | — | — | V | — | PostgreSQL-specific functions |
| SPEC-10.2.15 | `Mysqli/SpecificFeaturesTest`, `Pdo/MysqlSpecificFeaturesTest` | V | V | — | — | MySQL-specific functions |
| SPEC-10.2.16 | `Pdo/SqliteSpecificFeaturesTest` | — | — | — | V | SQLite-specific functions |
| SPEC-5.1 | `Mysqli/TemporaryTableTest`, `Pdo/SqliteTemporaryTableTest`, `Pdo/PostgresTemporaryAndUnloggedTableTest` | V | — | V | V | TEMPORARY/UNLOGGED tables |
| SPEC-3.3 | `Pdo/SqliteGlobOperatorTest`, `Pdo/SqliteLikeEscapeTest` | — | — | — | V | SQLite pattern matching |
| SPEC-3.3 | `Pdo/PostgresValuesExpressionTest` | — | — | V | — | PostgreSQL VALUES expression |
| SPEC-3.3 | `*TagFilteringTest` | V | V | V | V | Many-to-many tag filtering, GROUP BY HAVING |
| SPEC-3.3 | `*HierarchicalSelfJoinTest` | V | V | V | V | Self-join tree traversal, org chart pattern |
| SPEC-3.3 | `*PivotReportTest` | V | V | V | V | Cross-tab reporting, conditional aggregation |
| SPEC-4.2 | `*InventoryWorkflowTest` | V | V | V | V | Stock tracking, conditional update, CASE bulk |
| SPEC-4.1 | `*MultiStepTransformationTest` | V | V | V | V | ETL pipeline, interleaved reads/writes |
| SPEC-3.3 | `*PeriodComparisonTest` | V | V | V | V | YoY/QoQ/MoM growth, LAG window, period matrix |
| SPEC-3.1 | `*EventLogTest` | V | V | V | V | Audit trail, time-range, multiple COUNT DISTINCT |
| SPEC-3.3 | `*DeduplicationPatternTest`, `*DeduplicationEdgeCasesTest` | V | V | V | V | ROW_NUMBER dedup, GROUP BY HAVING, DISTINCT ON, composite key dedup |
| SPEC-3.3 | `*DataReconciliationTest` | V | V | V | V | Anti-join, mismatch detection, FULL OUTER JOIN |
| SPEC-10.2.23 | `*LedgerRunningBalanceTest` | V | V | V | V | Running balance via SUM() OVER, PARTITION BY |
| SPEC-4.3 | `*CascadeCleanupTest` | V | V | V | V | Multi-table cascade delete, nested subquery DELETE |
| SPEC-4.2 | `*BulkConditionalUpgradeTest` | P | P | P | P | Bulk UPDATE with aggregate subquery (known issue) |
| SPEC-3.3 | `*RbacPermissionCheckTest` | V | V | V | V | 5-table JOIN, nested EXISTS, junction mutations |
| SPEC-4.2 | `*JobQueueProcessingTest` | V | V | V | V | Priority queue, state machine, UPDATE subquery |
| SPEC-4.1 | `*InvoiceWorkflowTest` | V | V | V | V | Multi-table billing, discount, status transitions |
| SPEC-2.2 | `*ChainedMutationVisibilityTest` | V | V | V | V | Interleaved DML chain, cross-table JOIN visibility |
| SPEC-3.2 | `*SearchFilterPaginationTest` | V | V | V | V | LIKE + range + boolean filters, LIMIT/OFFSET pages |
| SPEC-3.2 | `*DynamicFilterBuildingTest` | V | V | V | V | WHERE 1=1, optional filters, dynamic ORDER BY |
| SPEC-3.1 | `*PaginationWithTotalCountTest` | V | V | V | V | Dual-query pagination + COUNT, keyset pagination |
| SPEC-10.2.18 | `*DateIntervalTest` | V | V | V | V | Date arithmetic, BETWEEN, GROUP BY month, intervals |
| SPEC-3.1 | `*MultiColumnSortingTest` | V | V | V | V | Multi-column ORDER BY, CASE sort, NULL ordering |
| SPEC-4.2a | `*UpsertWorkflowTest` | V | V | V | V | Idempotent writes, read-modify-write, batch upsert |
| SPEC-3.3 | `*ReservationBookingTest` | V | V | V | V | Anti-join availability, 3-table JOIN, conditional aggregation |
| SPEC-3.3 | `*LoyaltyPointsTest` | V | V | V | V | Running balance, tier CASE, window function, earn/redeem |
| SPEC-3.3 | `*ContentVersioningTest` | V | V | V | V | Correlated MAX subquery, version comparison, draft/publish |
| SPEC-4.1 | `*ShoppingCartCheckoutTest` | V | V | V | V | Multi-step checkout, cart aggregation, stock management |
| SPEC-3.3 | `*SurveyResultsTest` | V | V | V | V | Response distribution, percentage calc, conditional aggregation |
| SPEC-4.2 | `*NotificationInboxTest` | V | V | V | V | Batch UPDATE, unread counts, 3-table JOIN, priority filter |

## 11. Known Issues (Selected)

| SPEC-ID | Test Classes | Mi | MP | PG | SL | Status |
|---------|-------------|----|----|----|----|--------|
| SPEC-11.UPDATE-FROM | `Pdo/SqliteUpdateFromJoinTest` | — | — | — | K | K |
| SPEC-11.PG-LATERAL | `Pdo/PostgresLateralSubqueryTest` | — | — | K | — | K |
| SPEC-11.BARE-SUBQUERY-REWRITE | `Pdo/SqliteScalarSubqueryInSelectTest`, `Pdo/SqlitePivotReportTest`, `Pdo/SqliteScalarSubqueryWorkaroundTest` | — | — | — | K | K |
| SPEC-11.UPDATE-AGGREGATE-SUBQUERY | `Pdo/SqliteBulkConditionalUpgradeTest`, `Pdo/SqliteDeduplicationEdgeCasesTest` | — | — | — | K | K |

## Legend

- `*FooTest` = shorthand for all platform variants (e.g., `Mysqli/FooTest`, `Pdo/MysqlFooTest`, `Pdo/PostgresFooTest`, `Pdo/SqliteFooTest`)
- `Scenarios/FooScenario` = shared trait used by platform-specific test classes
- Mi = MySQLi adapter, MP = MySQL PDO adapter, PG = PostgreSQL PDO adapter, SL = SQLite PDO adapter
- V = Verified, P = Partially Verified, K = Known Issue
- `—` = Not applicable for this adapter/platform
