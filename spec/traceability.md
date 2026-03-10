# Traceability Matrix

Maps SPEC-IDs to test classes and verified versions.

**Last updated:** 2026-03-10
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
| SPEC-3.3a | `*DerivedTableAndViewTest`, `*SubqueryNestingTest`, `*SubqueryInExpressionTest`, `*MultiUnionDerivedTest` | V | V | V | V | K |
| SPEC-3.3b | `*ViewThroughZtdTest` | V | V | V | V | K |
| SPEC-3.3c | `*RecursiveCteAndRightJoinTest` | V | V | V | V | K |
| SPEC-3.3d | `*ExceptIntersectTest`, `*SetOperationsAndFunctionsTest` | V | V | V | V | P |
| SPEC-3.3e | `*CteDmlTest`, `*CteDrivenDmlTest`, `*RecursiveCteDmlTest`, `*CteNameCollisionTest` | V | V | V | V | K |
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
| SPEC-10.2.1 | `*TruncateReinsertTest`, `Pdo/PostgresTruncateOptionsTest` | V | V | V | — | V |
| SPEC-10.2.2 | `Mysqli/DelegatedMethodsTest` | V | — | — | — | V |
| SPEC-10.2.3 | `*ConstraintBehaviorTest` | V | V | V | — | V |
| SPEC-10.2.4 | `*UnsupportedSqlTest`, `*BehaviorRuleCombinationsTest` | V | V | V | V | V |
| SPEC-10.2.5 | `*AlterTableTest`, `*AlterTableAdvancedTest` | V | V | V | V | V |
| SPEC-10.2.6 | `Mysqli/TemporaryTableTest`, `Pdo/SqliteTemporaryTableTest`, `Pdo/PostgresTemporaryAndUnloggedTableTest` | V | — | V | V | V |
| SPEC-10.2.7 | `Pdo/PostgresMultiTableOperationsTest` | — | — | V | — | V |
| SPEC-10.2.8 | `Mysqli/ExecuteQueryWriteOpsTest` | V | — | — | — | V |
| SPEC-10.2.9 | `Mysqli/ExecuteQueryWriteOpsTest` | K | — | — | — | K |
| SPEC-10.2.10 | `Mysqli/InsertModifiersTest`, `Pdo/MysqlInsertModifiersTest` | V | V | — | — | V |
| SPEC-10.2.11 | `*SelectForUpdateTest`, `*SelectLockingTest`, `Pdo/PostgresForUpdateSkipLockedTest` | V | V | V | V | V |
| SPEC-10.2.12 | `Pdo/SqliteConflictResolutionTest` | — | — | — | V | V |
| SPEC-10.2.13 | `*MultiColumnSortingTest`, `*NullHandlingEdgeCasesTest` | V | V | V | V | V |
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
| SPEC-10.2.64 | `*ApprovalWorkflowTest` | V | V | V | V | V |
| SPEC-10.2.65 | `*LeaderboardRankingTest` | V | V | V | V | V |
| SPEC-10.2.66 | `*ConfigurationCascadeTest` | V | V | V | V | V |
| SPEC-10.2.67 | `*AuditTrailTest` | V | V | V | V | V |
| SPEC-10.2.68 | `*WaitlistQueueTest` | V | V | V | V | V |
| SPEC-10.2.69 | `*CouponDiscountTest` | V | V | V | V | V |
| SPEC-10.2.70 | `*SubscriptionBillingTest` | V | V | V | V | V |
| SPEC-10.2.71 | `*FinancialLedgerTest` | V | V | V | V | V |
| SPEC-10.2.72 | `*InventoryAllocationTest` | V | V | V | V | V |
| SPEC-10.2.73 | `*OrderFulfillmentTest` | V | V | V | V | V |
| SPEC-10.2.74 | `*SalesReportTest` | V | V | V | V | V |
| SPEC-10.2.75 | `*RetryQueueTest` | V | V | V | V | V |
| SPEC-10.2.76 | `*EmployeeSchedulingTest` | V | V | V | V | V |
| SPEC-10.2.77 | `*GiftCardRedemptionTest` | V | V | V | V | V |
| SPEC-10.2.78 | `*ProductCatalogTest` | V | V | V | V | V |
| SPEC-10.2.79 | `*EmailCampaignTest` | V | V | V | V | V |
| SPEC-10.2.80 | `*TimeTrackingTest` | V | V | V | V | V |
| SPEC-10.2.81 | `*WarrantyClaimTest` | V | V | V | V | V |
| SPEC-10.2.82 | `*ClassEnrollmentTest` | V | V | V | V | V |
| SPEC-10.2.83 | `*PropertyListingTest` | V | V | V | V | V |
| SPEC-10.2.84 | `*DocumentTaggingTest` | V | V | V | V | V |
| SPEC-10.2.85 | `*AuctionBiddingTest` | V | V | V | V | V |
| SPEC-10.2.86 | `*RecipeIngredientTest` | V | V | V | V | V |
| SPEC-10.2.87 | `*ProjectMilestoneTest` | V | V | V | V | V |
| SPEC-10.2.88 | `*CorrelatedUpdateTest` | V | V | K | K | P |
| SPEC-10.2.89 | `*NullCoalescingTest` | V | V | V | V | V |
| SPEC-10.2.90 | `*MultiStepEtlTest` | V | V | P | P | P |
| SPEC-10.2.91 | `*LargeInListTest` | V | V | V | V | V |
| SPEC-10.2.92 | `*UnionQueryTest` | V | V | V | V | V |
| SPEC-10.2.93 | `*StringManipulationTest` | V | V | V | V | V |
| SPEC-10.2.94 | `*BatchInsertPatternTest` | V | V | V | V | V |
| SPEC-10.2.95 | `*ZtdTogglePatternTest` | V | V | V | V | V |
| SPEC-10.2.96 | `*ChainedUserCteTest` | V | V | K | P | P |
| SPEC-10.2.97 | `*RowValueComparisonTest` | V | V | V | V | V |
| SPEC-10.2.98 | `*SqlKeywordInDataTest` | V | V | V | V | V |
| SPEC-10.2.99 | `*TripleSelfJoinTest` | V | V | V | V | V |
| SPEC-10.2.100 | `*EmptyStringVsNullTest` | V | V | V | V | V |
| SPEC-10.2.101 | `*UnicodeDataTest` | V | V | V | V | V |
| SPEC-10.2.102 | `*ConditionalAggregateNoGroupByTest` | V | V | V | V | V |
| SPEC-10.2.103 | `*PolymorphicCommentTest` | V | V | V | V | V |
| SPEC-10.2.104 | `*DataArchivalTest` | V | V | V | P | P |
| SPEC-10.2.105 | `*SocialFeedTest` | V | V | V | V | V |
| SPEC-10.2.106 | `*AccessLogSessionTest` | V | V | V | V | V |
| SPEC-10.2.107 | `*FeatureFlagTest` | V | V | V | V | V |
| SPEC-10.2.108 | `*ReviewRatingTest` | V | V | V | V | V |
| SPEC-10.2.109 | `*ReferralTrackingTest` | V | V | V | V | V |
| SPEC-10.2.110 | `*ContentModerationTest` | V | V | V | V | V |
| SPEC-10.2.111 | `*InventoryHoldTest` | V | V | V | V | V |
| SPEC-10.2.112 | `*DashboardKpiTest` | V | V | V | V | V |
| SPEC-10.2.113 | `*PasswordResetTokenTest` | V | V | V | V | V |
| SPEC-10.2.114 | `*MultiLanguageContentTest` | V | V | V | V | V |
| SPEC-10.2.115 | `*SplitPaymentTest` | V | V | V | V | V |
| SPEC-10.2.116 | `*UserActivityStreakTest` | V | V | V | V | V |
| SPEC-10.2.117 | `*DataRetentionPolicyTest` | V | V | V | V | V |
| SPEC-10.2.118 | `*TaxCalculationTest` | V | V | V | V | V |
| SPEC-10.2.119 | `*SlidingWindowRateLimitTest` | V | V | V | V | V |
| SPEC-10.2.120 | `*EventSourcingProjectionTest` | V | V | V | V | V |
| SPEC-10.2.121 | `*ClosureTableHierarchyTest` | V | V | V | V | V |
| SPEC-10.2.122 | `*TemporalVersionLookupTest` | V | V | V | V | V |
| SPEC-10.2.123 | `*IncrementalSyncDeltaTest` | V | V | V | V | V |
| SPEC-10.2.124 | `*CohortRetentionAnalysisTest` | V | V | V | V | V |
| SPEC-10.2.125 | `*CustomerRfmSegmentationTest` | V | V | V | P | P |
| SPEC-10.2.126 | `*HelpDeskSlaTest` | V | V | V | V | V |
| SPEC-10.2.127 | `*MeetingRoomBookingTest` | V | V | V | V | V |
| SPEC-10.2.128 | `*BudgetRolloverTest` | V | V | V | V | V |
| SPEC-10.2.129 | `*GradebookWeightedAvgTest` | V | V | V | V | V |
| SPEC-10.2.130 | `*FleetServiceTrackingTest` | V | V | V | V | V |
| SPEC-10.2.131 | `*QuotaManagementTest` | V | V | V | V | V |
| SPEC-10.2.132 | `*ShippingTrackerTest` | V | V | V | V | V |
| SPEC-10.2.133 | `*ReturnRefundTest` | V | V | V | V | V |
| SPEC-10.2.134 | `*ChatMessagingTest` | V | V | V | V | V |
| SPEC-10.2.135 | `*AppointmentSchedulingTest` | V | V | V | V | V |
| SPEC-10.2.136 | `*ExpenseReportTest` | V | V | V | V | V |
| SPEC-10.2.137 | `*VotingPollTest` | V | V | V | V | V |
| SPEC-10.2.138 | `*KanbanBoardTest` | V | V | V | V | V |
| SPEC-10.2.139 | `*PrescriptionTrackingTest` | V | V | V | V | V |
| SPEC-10.2.140 | `*PlaylistManagementTest` | V | V | V | V | V |
| SPEC-10.2.141 | `*BadgeAchievementTest` | V | V | V | V | V |
| SPEC-10.2.142 | `*AttendanceTrackerTest` | V | V | V | V | V |
| SPEC-10.2.143 | `*DynamicPricingTest` | V | V | V | V | V |
| SPEC-10.2.144 | `*WarehouseTransferTest` | V | V | V | V | V |
| SPEC-10.2.145 | `*CoursePrerequisiteTest` | V | V | V | V | V |
| SPEC-10.2.146 | `*MealPlanningTest` | V | V | V | V | V |
| SPEC-10.2.147 | `*InsuranceClaimTest` | V | V | V | V | V |
| SPEC-10.2.148 | `*ApiKeyManagementTest` | V | V | V | V | V |

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
| SPEC-3.3 | `*ApprovalWorkflowTest` | V | V | V | V | Quorum check, 3-table JOIN, status transitions with guards |
| SPEC-10.2.23 | `*LeaderboardRankingTest` | V | V | V | V | DENSE_RANK, tied ranking, score history timeline |
| SPEC-3.3 | `*ConfigurationCascadeTest` | V | V | V | V | LEFT JOIN + COALESCE cascade, correlated subquery priority |
| SPEC-3.3 | `*AuditTrailTest` | V | V | V | V | Change logging, correlated MAX for latest state, revert |
| SPEC-10.2.23 | `*WaitlistQueueTest` | V | V | V | V | ROW_NUMBER PARTITION BY, capacity guard, priority queue |
| SPEC-3.3 | `*CouponDiscountTest` | V | V | V | V | Date-range filter, LEFT JOIN usage count, discount calc |
| SPEC-3.3 | `*SubscriptionBillingTest` | V | V | V | V | Correlated subquery balance, CASE paid/unpaid, date-range prepared |
| SPEC-3.3 | `*FinancialLedgerTest` | V | V | V | V | DECIMAL precision, CASE debit/credit sign, double-entry integrity |
| SPEC-4.1 | `*InventoryAllocationTest` | V | V | V | V | Self-referencing arithmetic, reservation/sale conversion, warehouse report |
| SPEC-4.1 | `*OrderFulfillmentTest` | V | V | V | V | Partial/complete fulfillment, conditional COUNT, multi-table JOIN |
| SPEC-3.3 | `*SalesReportTest` | V | V | V | V | CASE pivot by quarter, HAVING threshold, net sales with returns |
| SPEC-4.2 | `*RetryQueueTest` | V | V | V | V | Priority queue, retry state machine, job metrics, LEFT JOIN logs |
| SPEC-3.2 | `*EmployeeSchedulingTest` | V | V | V | V | Date BETWEEN prepared, overlap detection, shift swap |
| SPEC-3.2 | `*GiftCardRedemptionTest` | V | V | V | V | Prepare-once/execute-many, self-referencing UPDATE arithmetic |
| SPEC-3.3 | `*ProductCatalogTest` | V | V | V | V | Self-JOIN hierarchy, faceted COUNT, prepared BETWEEN |
| SPEC-3.3 | `*EmailCampaignTest` | V | V | V | V | Batch INSERT, CASE COUNT percentages, campaign comparison |
| SPEC-3.3 | `*TimeTrackingTest` | V | V | V | V | 3-table SUM, HAVING over-budget, billable hours |
| SPEC-4.2 | `*WarrantyClaimTest` | V | V | V | V | State machine, date arithmetic, cross-table validation |
| SPEC-3.3 | `*ClassEnrollmentTest` | V | V | V | V | COUNT capacity, EXISTS prerequisite, waitlist promotion |
| SPEC-3.2 | `*PropertyListingTest` | V | V | V | V | Multi-filter prepared, BETWEEN, LIMIT/OFFSET pagination |
| SPEC-3.3 | `*DocumentTaggingTest` | V | V | V | V | Many-to-many junction, HAVING COUNT intersection, anti-join |
| SPEC-3.3 | `*AuctionBiddingTest` | V | V | V | V | MAX subquery, bid history, COUNT DISTINCT HAVING |
| SPEC-3.3 | `*RecipeIngredientTest` | V | V | V | V | Arithmetic scaling, SUM aggregation, LEFT JOIN substitutions |
| SPEC-3.3 | `*ProjectMilestoneTest` | V | V | V | V | Completion percentage, overdue detection, HAVING risk |
| SPEC-4.2 | `*CorrelatedUpdateTest` | V | V | K | K | Correlated UPDATE SET (works MySQL, fails PG/SQLite) |
| SPEC-3.7 | `*NullCoalescingTest` | V | V | V | V | COALESCE chains, IS NULL, COUNT(*) vs COUNT(col) |
| SPEC-4.1a | `*MultiStepEtlTest` | V | V | P | P | INSERT SELECT GROUP BY, correlated recalc fails PG/SL |
| SPEC-3.1 | `*LargeInListTest` | V | V | V | V | 20+ item IN list, NOT IN, string IN, multi-param prepared |
| SPEC-3.3d | `*UnionQueryTest` | V | V | V | V | UNION ALL, UNION distinct, with WHERE/ORDER/LIMIT |
| SPEC-3.3 | `*StringManipulationTest` | V | V | V | V | CONCAT, UPPER, LENGTH, TRIM, SUBSTR, REPLACE |
| SPEC-4.1 | `*BatchInsertPatternTest` | V | V | V | V | Empty-start sequential INSERT, mixed DML visibility |
| SPEC-2.1 | `*ZtdTogglePatternTest` | V | V | V | V | Enable/disable toggle, shadow persistence, physical isolation |
| SPEC-3.3 | `*PolymorphicCommentTest` | V | V | V | V | Type discriminator pattern, polymorphic JOIN, CASE entity resolution |
| SPEC-4.1a | `*DataArchivalTest` | V | V | V | P | INSERT SELECT archival, cross-table UNION, SQLite literal NULL |
| SPEC-3.3 | `*SocialFeedTest` | V | V | V | V | 4-table schema, IN subquery feed, mutual friends, reactions |
| SPEC-10.2.23 | `*AccessLogSessionTest` | V | V | V | V | LAG/ROW_NUMBER sessionization, funnel, daily active users |
| SPEC-3.3 | `*FeatureFlagTest` | V | V | V | V | A/B test analysis, CROSS JOIN eligibility, conditional rollout |
| SPEC-3.3 | `*ReviewRatingTest` | V | V | V | V | AVG/COUNT star ratings, LEFT JOIN helpful votes, CASE distribution |
| SPEC-3.3 | `*ReferralTrackingTest` | V | V | V | V | Self-JOIN referral chains, multi-level revenue attribution, HAVING |
| SPEC-3.3 | `*ContentModerationTest` | V | V | V | V | Flag accumulation, HAVING escalation threshold, 3-table LEFT JOIN |
| SPEC-4.2 | `*InventoryHoldTest` | V | V | V | V | Hold/reservation workflow, date-based expiry, stock calculation |
| SPEC-3.3 | `*DashboardKpiTest` | V | V | V | V | Multi-entity aggregation, revenue/segment, monthly trends, top pages |
| SPEC-3.1 | `*PasswordResetTokenTest` | V | V | V | V | Date-filtered token lookup, EXISTS subquery, one-time consumption |
| SPEC-3.3 | `*MultiLanguageContentTest` | V | V | V | V | LEFT JOIN + COALESCE language fallback, translation coverage stats |
| SPEC-4.2 | `*SplitPaymentTest` | V | V | V | V | HAVING SUM integrity, partial refund arithmetic, percentage calc |
| SPEC-10.2.23 | `*UserActivityStreakTest` | V | V | V | V | LAG window for previous-date, gap detection, activity date range |
| SPEC-4.2 | `*DataRetentionPolicyTest` | V | V | V | V | PII anonymization, date-range DELETE, LEFT JOIN inactive detection |
| SPEC-3.3 | `*TaxCalculationTest` | V | V | V | V | Multi-table JOIN tax rules, ROUND arithmetic, AVG by country |
| SPEC-3.3 | `*SlidingWindowRateLimitTest` | V | V | V | V | Rolling time-window quota, BETWEEN datetime, COALESCE override |
| SPEC-10.2.23 | `*EventSourcingProjectionTest` | V | V | V | V | Event log SUM CASE, snapshot+delta, SUM OVER running balance |
| SPEC-3.3 | `*ClosureTableHierarchyTest` | V | V | V | V | 5-level closure table, ancestor/descendant, leaf detection |
| SPEC-3.3 | `*TemporalVersionLookupTest` | V | V | V | V | Effective date ranges, point-in-time lookup, IS NULL current |
| SPEC-4.1 | `*IncrementalSyncDeltaTest` | V | V | V | V | Watermark delta, LEFT JOIN new records, soft-delete detection |
| SPEC-3.3 | `*CohortRetentionAnalysisTest` | V | V | V | V | Cohort sizing, retention by month, churn LEFT JOIN IS NULL |
| SPEC-10.2.23 | `*CustomerRfmSegmentationTest` | V | V | V | P | NTILE window function, quartile scoring, nested AVG subquery (SQLite: derived table empty) |
| SPEC-3.3 | `*HelpDeskSlaTest` | V | V | V | V | MIN correlated subquery, SUM CASE cross-tab, NOT EXISTS unresponded |
| SPEC-3.3 | `*MeetingRoomBookingTest` | V | V | V | V | NOT EXISTS overlap detection, LEFT JOIN COUNT, floor aggregation |
| SPEC-10.2.23 | `*BudgetRolloverTest` | V | V | V | V | SUM(SUM()) OVER cumulative, RANK, budget variance, scalar subquery pct |
| SPEC-3.3 | `*GradebookWeightedAvgTest` | V | V | V | V | SUM(score*weight)/SUM(weight), CROSS JOIN missing, HAVING nested subquery |
| SPEC-10.2.23 | `*FleetServiceTrackingTest` | V | V | V | V | LAG mileage, MAX overdue, SUM cost, active fleet summary |
| SPEC-3.3 | `*QuotaManagementTest` | V | V | V | V | Correlated MAX subquery, percentage ROUND, CASE over-quota, AVG trend |
| SPEC-3.3 | `*ShippingTrackerTest` | V | V | V | V | Double correlated MAX subquery, SUM CASE cross-tab, delivery rate |
| SPEC-3.3 | `*ReturnRefundTest` | V | V | V | V | Double LEFT JOIN, COALESCE, restocking fee arithmetic, prepared SUM |
| SPEC-3.3 | `*ChatMessagingTest` | V | V | V | V | Correlated MAX subquery, NOT EXISTS anti-join, COUNT DISTINCT |
| SPEC-3.3 | `*AppointmentSchedulingTest` | V | V | V | V | Range overlap conflict, GROUP BY COUNT+SUM, multi-table JOIN |
| SPEC-3.3 | `*VotingPollTest` | V | V | V | V | LEFT JOIN percentage, HAVING COUNT, NOT IN subquery anti-join |
| SPEC-3.3 | `*ExpenseReportTest` | V | V | V | V | Self-join manager lookup, 3-table category breakdown, HAVING SUM threshold |
| SPEC-3.3 | `*KanbanBoardTest` | V | V | V | V | EXISTS correlated dependency check, CASE priority labels, SUM CASE completion % |
| SPEC-3.3 | `*PrescriptionTrackingTest` | V | V | V | V | 4-table JOIN, self-referencing UPDATE arithmetic, BETWEEN date filter, COUNT DISTINCT |
| SPEC-3.3 | `*PlaylistManagementTest` | V | V | V | V | 3-table JOIN, UPDATE position arithmetic, GROUP BY genre COUNT, SUM play counts |
| SPEC-3.3 | `*BadgeAchievementTest` | V | V | V | V | CASE WHEN IS NOT NULL, ROUND percentage rarity, UPDATE progress + unlock, WHERE IS NULL |
| SPEC-3.2 | `*AttendanceTrackerTest` | V | V | V | V | SUM CASE status, ROUND attendance rate, prepared BETWEEN, LEFT JOIN anti-pattern, HAVING |
| SPEC-3.3 | `*DynamicPricingTest` | V | V | V | V | Correlated MAX subquery, CASE tier, ROUND competitor diff, derived table GROUP BY |
| SPEC-3.3 | `*WarehouseTransferTest` | V | V | V | V | Self-join source/dest, GROUP BY SUM, HAVING threshold, multi-table INSERT+UPDATE |
| SPEC-3.3 | `*CoursePrerequisiteTest` | V | V | V | V | Double-nested NOT EXISTS, LEFT JOIN IS NULL, COUNT DISTINCT, prepared BETWEEN |
| SPEC-3.3 | `*MealPlanningTest` | V | V | V | V | CROSS JOIN + LEFT JOIN IS NULL (gap detection), SUM through JOIN, dietary HAVING |
| SPEC-3.3 | `*InsuranceClaimTest` | V | V | V | V | CASE status labels, ROUND coverage utilization %, LEFT JOIN COUNT, SUM payouts |
| SPEC-3.3 | `*ApiKeyManagementTest` | V | V | V | V | COUNT/quota ROUND %, SUM CASE error rate, AVG by tier, daily GROUP BY |
| SPEC-10.2.149 | `*ContentModerationQueueTest` | V | V | V | V | CASE IN GROUP BY alias, NOT EXISTS, COUNT GROUP BY reason, prepared JOIN |
| SPEC-10.2.150 | `*ClassroomQuizScoringTest` | V | V | V | V | 4-table JOIN CASE answer match, HAVING dynamic threshold, derived table AVG |
| SPEC-10.2.151 | `*OnboardingChecklistTest` | V | V | V | V | Scalar subquery %, NOT EXISTS outstanding, CASE status labels, INSERT+verify |
| SPEC-10.2.152 | `*LibraryLendingTest` | V | V | V | V | 3-table JOIN, date CASE overdue, date diff late fees, LEFT JOIN availability, member stats |
| SPEC-10.2.153 | `*SkillMatrixTest` | V | V | V | V | HAVING COUNT = scalar subquery, LEFT JOIN COALESCE gap, SUM CASE cross-tab, MIN HAVING |
| SPEC-10.2.154 | `*ParkingGarageTest` | V | V | V | V | COUNT occupancy, ROUND capacity %, COALESCE SUM revenue, double LEFT JOIN IS NULL |
| SPEC-10.2.155 | `*LeaveBalanceTest` | V | V | V | V | 3-table JOIN SUM, LEFT JOIN COALESCE balance, self-join overlap, SUM CASE dept cross-tab, UPDATE+verify |
| SPEC-10.2.156 | `*UsageMeteringTest` | V | V | V | V | SUBSTR month GROUP BY, ROUND CAST utilization %, HAVING quota threshold, LEFT JOIN COALESCE overage |
| SPEC-10.2.157 | `*DocumentWorkflowTest` | V | V | V | V | LEFT JOIN CASE quorum, SUM CASE reviewer workload, correlated MAX subquery, UPDATE status transition |
| SPEC-10.2.158 | `*EquipmentMaintenanceTest` | V | V | V | V | Date arithmetic overdue, LEFT JOIN COUNT+SUM workload, correlated MAX subquery, ROUND AVG cost |
| SPEC-10.2.159 | `*HotelRoomManagementTest` | V | V | V | V | GROUP BY COUNT type, JOIN SUM+ROUND AVG revenue, LEFT JOIN guest history, 3-table rating JOIN |
| SPEC-10.2.160 | `*IncidentManagementTest` | V | V | V | V | CASE priority, COUNT DISTINCT workload, NOT EXISTS unassigned, prepared DISTINCT team |
| SPEC-10.2.161 | `*MembershipTierTest` | V | V | V | V | GROUP BY+SUM spending, SUM+CASE tier eligibility, LEFT JOIN COALESCE benefits, UPDATE+verify, prepared BETWEEN+JOIN |
| SPEC-10.2.162 | `*CustomerNpsTest` | V | V | V | V | CASE NPS categories, ROUND SUM CASE/COUNT NPS%, GROUP BY+SUM CASE channel, LEFT JOIN IS NULL anti-join, prepared BETWEEN |
| SPEC-10.2.163 | `*AssetDepreciationTest` | V | V | V | V | GROUP BY+COUNT+SUM category, correlated MAX subquery latest, ROUND depreciation%, HAVING aggregate threshold, prepared JOIN |
| SPEC-10.2.164 | `Pdo/MysqlSubscriptionRenewalTest`, `Pdo/PostgresSubscriptionRenewalTest`, `Pdo/SqliteSubscriptionRenewalTest` | — | V | V | P | DELETE WHERE IN (subquery+JOIN), INSERT SELECT JOIN, correlated subqueries in SELECT list, prepared HAVING, UPDATE+verify |
| SPEC-10.2.165 | `Pdo/MysqlStudentGradeReportTest`, `Pdo/PostgresStudentGradeReportTest`, `Pdo/SqliteStudentGradeReportTest` | — | V | V | P | CROSS JOIN+LEFT JOIN COALESCE missing=0, CASE WHEN letter grades, weighted average, DELETE EXISTS, prepared HAVING |
| SPEC-10.2.166 | `Pdo/MysqlInventorySnapshotTest`, `Pdo/PostgresInventorySnapshotTest`, `Pdo/SqliteInventorySnapshotTest` | — | V | V | P | UNION ALL derived table (NEW FINDING), INSERT SELECT+UNION ALL, HAVING on UNION ALL, double LEFT JOIN aggregate, prepared UNION ALL |
| SPEC-10.2.167 | `*SalesCommissionTest` | P | P | V | P | ROW_NUMBER OVER, SUM OVER running total, LAG compare, window in derived table (empty on all), prepared window |
| SPEC-10.2.168 | `*ProjectTimesheetTest` | V | V | V | P | ROLLUP subtotals (MySQL/PG), UNION ALL subtotals (SQLite—1 row only), conditional SUM CASE, HAVING threshold, prepared GROUP BY |
| SPEC-10.2.169 | `*WaitlistReservationTest` | P | P | P | P | NOT EXISTS, nested CASE SELECT, scalar subqueries, correlated UPDATE+NOT EXISTS, CASE-in-WHERE+params (wrong count on all) |
| SPEC-10.2.170 | `*FleetVehicleTrackingTest` | V | V | V | V | 3-table JOIN prefix-overlapping names, GROUP BY SUM, COUNT(DISTINCT), self-ref UPDATE, chained self-ref UPDATE, prepared BETWEEN, single-table query with overlapping-name tables |
| SPEC-10.2.171 | `*DonationCampaignTest` | V | V | V | V | INSERT reordered columns, self-ref UPDATE arithmetic, chained self-ref, COUNT(DISTINCT), COALESCE SUM LEFT JOIN zero, ROUND %, DELETE+verify, prepared 3-table JOIN |
| SPEC-10.2.172 | `*TeamRosterTest` | V | V | V | V | GROUP_CONCAT/STRING_AGG in multi-table JOIN, GROUP BY, HAVING COUNT, LEFT JOIN NULL aggregate, GROUP_CONCAT after INSERT/DELETE, prepared GROUP_CONCAT |
| SPEC-10.2.173 | `*DeleteReinsertCycleTest` | V | V | P | P | DELETE+re-INSERT same PK, chained delete-reinsert-update, UPDATE WHERE IN self-ref (PG: duplicate alias), UPDATE WHERE scalar subquery (PG: syntax error), mixed exec/prepare, JOIN after delete-reinsert |
| SPEC-10.2.174 | `*NestedFunctionExprTest` | V | V | P | P | COALESCE(NULLIF()), COALESCE(NULLIF(TRIM())), subquery in BETWEEN, scalar subquery balance, JOIN rate conversion, UPDATE WHERE IN JOIN+GROUP BY (PG: ambiguous, SL: incomplete input), nested CASE+COALESCE, mixed exec/prepare |
| SPEC-10.2.175 | `*PayrollDeductionTest` | V | V | V | P | UPDATE SET multiple cols+arithmetic, SUM CASE cross-tab, HAVING SUM > col*factor, INSERT...SELECT with CASE (SL: 0 rows), derived table GROUP BY+HAVING+multi-table JOIN, prepared BETWEEN |
| SPEC-10.2.176 | `*SupplierPerformanceTest` | V | V | V | V | Derived table multi-table JOIN+GROUP BY (no window), multiple scalar subqueries in SELECT, AVG(CASE), HAVING AVG AND COUNT, UPDATE SET CASE expression, prepared region filter |
| SPEC-10.2.177 | `*CurrencyConversionTest` | V | V | V | V | ROUND multiplication/division, nested CASE classification, UPDATE SET CASE per-currency, SUM CASE ROUND cross-currency, LEFT JOIN derived table net position, prepared arithmetic |

| SPEC-10.2.178 | `*AppointmentSchedulingTest` | V | V | V | V | BETWEEN, EXISTS/NOT EXISTS, COALESCE, UPDATE WHERE BETWEEN, COUNT CASE, prepared BETWEEN |
| SPEC-10.2.179 | `*ProductCatalogSearchTest` | V | V | V | V | LIKE wildcards, multi-filter prepared, LEFT JOIN COUNT, HAVING, ORDER BY alias |
| SPEC-10.2.180 | `*AuditTrailVersioningTest` | V | V | V | V | Sequential UPDATE same row, MAX+1 version, MIN/MAX, GROUP BY HAVING COUNT, LIMIT/OFFSET |
| SPEC-10.2.181 | `*ReferralChainTest` | V | V | V | V | Self-join referral tree, NOT IN with NULLable FK, COALESCE LEFT JOIN, UPDATE counter increment |
| SPEC-10.2.182 | `*SurveyResponseAnalysisTest` | V | V | V | P | INSERT SELECT GROUP BY (SL: 0 rows), multiple DISTINCT aggregates, conditional SUM CASE, prepared HAVING |
| SPEC-10.2.183 | `*WarehouseBinTransferTest` | V | V | V | P | DELETE WHERE LIKE, prepared LIKE wildcard, prepared UPDATE arithmetic self-ref, mixed exec/prepare, INSERT SELECT JOIN (SL: 0 rows) |
| SPEC-10.2.184 | `*UpdateSetFromKeywordTest` | V | V | K | V | UPDATE SET TRIM(FROM), SUBSTRING(FROM), EXTRACT(FROM) — PG parser truncates SET at FROM keyword |

## 11. Known Issues (Selected)

| SPEC-ID | Test Classes | Mi | MP | PG | SL | Status |
|---------|-------------|----|----|----|----|--------|
| SPEC-11.PG-UPDATE-SET-FROM-KEYWORD | `Pdo/PostgresUpdateSetFromKeywordTest` | — | — | K | — | K |
| SPEC-11.UPDATE-FROM | `Pdo/SqliteUpdateFromJoinTest` | — | — | — | K | K |
| SPEC-11.PG-LATERAL | `Pdo/PostgresLateralSubqueryTest` | — | — | K | — | K |
| SPEC-11.BARE-SUBQUERY-REWRITE | `Pdo/SqliteScalarSubqueryInSelectTest`, `Pdo/SqlitePivotReportTest`, `Pdo/SqliteScalarSubqueryWorkaroundTest` | — | — | — | K | K |
| SPEC-11.UPDATE-AGGREGATE-SUBQUERY | `Pdo/SqliteBulkConditionalUpgradeTest`, `Pdo/SqliteDeduplicationEdgeCasesTest`, `Pdo/SqliteDeleteReinsertCycleTest`, `Pdo/SqliteNestedFunctionExprTest` | — | — | — | K | K |
| SPEC-11.DERIVED-TABLE-PREPARED | `*LeaderboardRankingTest` | K | K | — | K | K |
| SPEC-11.CTE-JOIN-BACK | `*ChainedUserCteTest` | — | — | — | K | K |
| SPEC-11.CHECK-COLUMN-NAME | (no dedicated test — avoided by column rename) | K | K | K | K | K |
| SPEC-11.UNION-ALL-DERIVED | `Pdo/SqliteInventorySnapshotTest` | — | — | — | K | K |
| SPEC-11.PG-SELF-REF-UPDATE | `Pdo/PostgresDeleteReinsertCycleTest`, `Pdo/PostgresNestedFunctionExprTest` | — | — | K | — | K |
| SPEC-11.INSERT-SELECT-CASE | `Pdo/SqlitePayrollDeductionTest` | — | — | — | K | K |
| SPEC-11.ALTER-ADD-COL-STALE-SCHEMA | `Pdo/SqliteAlterAddColumnDmlTest`, `Pdo/MysqlAlterAddColumnDmlTest` | — | P | — | K | K |
| SPEC-11.PDO-REPLACE-PREPARED | `Pdo/MysqlReplaceIntoPreparedTest`, `Pdo/SqliteInsertOrReplaceTest` | — | K | — | K | K |
| SPEC-11.PG-GENERATE-SERIES | `Pdo/PostgresGenerateSeriesTest` | — | — | K | — | K |
| SPEC-11.LAST-INSERT-ID | `Pdo/SqliteLastInsertIdShadowTest` | — | — | — | K | K |
| SPEC-11.MULTI-STATEMENT | `Pdo/SqliteMultiStatementExecTest` | — | — | — | K | K |

## New Scenario Tests (2026-03-10)

| Primary SPEC | Test Classes | Mi | MP | PG | SL | Notes |
|-------------|-------------|----|----|----|----|-------|
| SPEC-3.1 | `Pdo/PostgresIlikeAndSimilarToTest` | — | — | V | — | ILIKE, NOT ILIKE, SIMILAR TO, CASE+ILIKE (all pass) |
| SPEC-3.5 | `Pdo/PostgresAggregateFilterTest` | — | — | V | — | Aggregate FILTER clause (all pass) |
| SPEC-3.1 | `Pdo/PostgresGenerateSeriesTest` | — | — | K | — | generate_series+LEFT JOIN (KI: empty), standalone (works) |
| SPEC-4.2 | `Pdo/PostgresUpdateFromTest` | — | — | V | — | UPDATE FROM basic/expression/shadow (pass), multi-table (skip) |
| SPEC-4.3 | `Pdo/PostgresDeleteUsingTest` | — | — | V | — | DELETE USING basic/condition/shadow (all pass) |
| SPEC-4.3 | `Pdo/MysqlDeleteUpdateWithOrderLimitTest`, `Mysqli/MysqliDeleteUpdateWithOrderLimitTest` | V | V | — | — | DELETE/UPDATE ORDER BY LIMIT (all pass) |
| SPEC-4.3 | `Pdo/MysqlMultiTableDeleteJoinTest` | — | V | — | — | Single-table+JOIN (pass), multi-table (KI #26), LEFT JOIN (pass) |
| SPEC-5.2 | `Pdo/SqliteAlterAddColumnDmlTest` | — | — | — | K | ADD COLUMN→DML fails on SELECT (KI #54) |
| SPEC-5.2 | `Pdo/MysqlAlterAddColumnDmlTest` | — | V | — | — | ADD COLUMN→DML works except DEFAULT on existing rows |
| SPEC-4.4 | `Pdo/MysqlReplaceIntoPreparedTest` | — | K | — | — | REPLACE prepared creates duplicate PK (KI #55) |
| SPEC-4.4 | `Pdo/SqliteInsertOrReplaceTest` | — | — | — | K | INSERT OR REPLACE prepared: duplicate PK (KI #55) |
| SPEC-4.1 | `Pdo/SqliteInsertSubqueryInValuesTest` | — | — | — | V | INSERT with scalar subquery in VALUES (all pass) |
| SPEC-3.3 | `Pdo/SqliteNaturalJoinTest` | — | — | — | V | NATURAL JOIN, NATURAL LEFT JOIN, shadow data, aggregate (all pass) |
| SPEC-4.1a | `Pdo/SqliteSelfReferencingInsertSelectTest` | — | — | — | P | Self-ref INSERT...SELECT: row count OK, computed columns NULL (KI #20) |
| SPEC-3.2 | `Pdo/SqliteNamedParametersTest` | — | — | — | P | Named :params work for SELECT/INSERT/UPDATE/DELETE/JOIN; HAVING fails (KI #22) |
| SPEC-3.3 | `Pdo/SqliteExistsExpressionTest` | — | — | — | V | EXISTS scalar, CASE EXISTS, NOT EXISTS, shadow-aware (all pass) |
| SPEC-3.3 | `Pdo/SqliteGroupByExpressionTest` | — | — | — | V | GROUP BY CASE/SUBSTR/arithmetic/COALESCE, HAVING (all pass) |
| SPEC-3.3 | `Pdo/SqliteAliasCollisionTest` | — | — | — | V | Subquery alias=table name, self-join, correlated same-table (all pass) |
| SPEC-3.3 | `Pdo/SqliteMultipleScalarSubqueryTest` | — | — | — | V | 2-3 scalar subqueries in SELECT, mixed correlated/non-correlated (all pass) |
| SPEC-4.7 | `Pdo/SqliteLastInsertIdShadowTest` | — | — | — | K | lastInsertId() always returns '0' in shadow mode (NEW Issue #77) |
| SPEC-6.1 | `Pdo/SqliteMultiStatementExecTest` | — | — | — | K | Multi-statement throws ZTD Write Protection (NEW Issue #78) |
| SPEC-3.2 | `Pdo/SqliteInterleavedPreparedStatementsTest` | — | — | — | P | Interleaved prepare works for reads; prepared INSERT/UPDATE invisible (KI #23) |
| SPEC-4.1 | `Pdo/SqliteInsertDefaultValuesTest` | — | — | — | K | INSERT DEFAULT VALUES fails; partial-col DEFAULT → NULL (KI #21/#31) |
| SPEC-3.3 | `*WindowFunctionQueryTest` | V | V | V | V | Window functions (ROW_NUMBER, RANK, DENSE_RANK, SUM OVER, LAG, LEAD, NTILE) work on all platforms |
| SPEC-4.1 | `Pdo/SqliteInsertFromUnionTest` | — | — | — | V | INSERT...SELECT UNION/UNION ALL works on SQLite |
| SPEC-4.1 | `Pdo/MysqlInsertFromUnionTest` | — | K | — | — | INSERT...SELECT UNION rejected as multi-statement on MySQL (Issue #103) |
| SPEC-4.1 | `Mysqli/InsertFromUnionTest` | K | — | — | — | INSERT...SELECT UNION rejected as multi-statement on MySQL (Issue #103) |
| SPEC-4.1 | `Pdo/PostgresInsertFromUnionTest` | — | — | V | — | INSERT...SELECT UNION/INTERSECT/EXCEPT works on PostgreSQL |
| SPEC-3.3 | `Pdo/SqliteSetOperationsQueryTest` | — | — | — | P | UNION/UNION ALL pass; multi-column INTERSECT/EXCEPT empty (KI #50) |
| SPEC-3.3 | `Pdo/SqlitePreparedBetweenAndCaseHavingTest` | — | — | — | P | BETWEEN works; CASE in HAVING with params empty (KI #22) |
| SPEC-3.3 | `Pdo/SqliteGroupConcatAndMultiDmlLifecycleTest` | — | — | — | V | GROUP_CONCAT, multi-DML lifecycle, sequential mutations (all pass) |
| SPEC-10.2.225 | `Pdo/MysqlColumnOrderAndMultiRowInsertTest`, `Pdo/PostgresColumnOrderAndMultiRowInsertTest`, `Mysqli/ColumnOrderAndMultiRowInsertTest` | V | V | V | — | Column order INSERT, multi-row VALUES (all pass; extends SQLite test) |
| SPEC-10.2.226 | `Pdo/MysqlHavingPreparedAndCompoundWhereTest`, `Pdo/PostgresHavingPreparedAndCompoundWhereTest`, `Pdo/SqliteHavingPreparedAndCompoundWhereTest` | — | V | V | P | HAVING+params (MySQL/PG pass, SQLite KI #22); compound WHERE (all pass) |
| SPEC-10.2.227 | `Pdo/MysqlCaseWhereAndUnionSelectTest`, `Pdo/PostgresCaseWhereAndUnionSelectTest`, `Pdo/SqliteCaseWhereAndUnionSelectTest` | — | V | V | V | CASE WHERE SELECT, UNION SELECT (all pass); nested CASE PG type issue |
| SPEC-10.2.228 | `Pdo/MysqlGroupByExpressionAndInsertFunctionTest`, `Pdo/SqliteGroupByExpressionAndInsertFunctionTest` | — | V | — | V | GROUP BY CASE/function, INSERT with functions (all pass) |
| SPEC-10.2.229 | `Pdo/MysqlPreparedCaseSetAndSubqueryInsertTest`, `Pdo/PostgresPreparedCaseSetAndSubqueryInsertTest` | — | V | V | — | Prepared CASE SET (?-params), INSERT subquery VALUES, self-ref DELETE (all pass) |
| SPEC-10.2.230 | `Pdo/SqliteTableNamePrefixConfusionTest` | — | — | — | V | Table name prefix isolation (orders/order_items/order_archive all pass) |
| SPEC-10.2.231 | `Pdo/PostgresBooleanColumnShadowTest` | — | — | P | — | BOOLEAN TRUE works; FALSE fails CAST('' AS BOOLEAN) (confirms Issue #6) |

| SPEC-10.2.247 | `Pdo/SqlitePreparedLimitOffsetParamsTest`, `Pdo/MysqlPreparedLimitOffsetParamsTest`, `Pdo/PostgresPreparedLimitOffsetParamsTest` | — | P | P | V | LIMIT ? OFFSET ? pagination: SQLite pass, MySQL needs PARAM_INT, PG $N ignored (extends #106) |
| SPEC-10.2.248 | `Pdo/SqliteInsertWhereNotExistsTest`, `Pdo/MysqlInsertWhereNotExistsTest`, `Pdo/PostgresInsertWhereNotExistsTest` | — | V | P | P | Anti-join INSERT NOT EXISTS: MySQL pass, SQLite/PG self-ref fails (extends #20/#106) |
| SPEC-10.2.249 | `Pdo/SqliteUpdateMultiSubquerySetTest`, `Pdo/MysqlUpdateMultiSubquerySetTest`, `Pdo/PostgresUpdateMultiSubquerySetTest` | — | V | K | K | Multi-subquery UPDATE SET: MySQL pass, SQLite syntax error (#51), PG grouping error (#61) |
| SPEC-10.2.250 | `Pdo/SqliteDeleteWithAggregatedInSubqueryTest`, `Pdo/MysqlDeleteWithAggregatedInSubqueryTest`, `Pdo/PostgresDeleteWithAggregatedInSubqueryTest` | — | V | P | K | DELETE IN (GROUP BY HAVING): MySQL pass, SQLite incomplete input, PG $1 fails (#106) |
| SPEC-10.2.251 | `Pdo/SqliteSequentialDmlSubqueryVisibilityTest`, `Pdo/MysqlSequentialDmlSubqueryVisibilityTest`, `Pdo/PostgresSequentialDmlSubqueryVisibilityTest` | — | P | P | P | Sequential DML chain: basic pass, cross-table JOIN fails (#20/#49/#51/#61) |
| SPEC-10.2.252 | `Pdo/SqliteInsertSelectPartialColumnListTest`, `Pdo/MysqlInsertSelectPartialColumnListTest`, `Pdo/PostgresInsertSelectPartialColumnListTest` | — | V | K | K | Partial column INSERT SELECT: MySQL pass, SQLite/PG NULL columns (extends #20) |
| SPEC-10.2.253 | `Pdo/SqliteThreeTableJoinDmlTest`, `Pdo/MysqlThreeTableJoinDmlTest`, `Pdo/PostgresThreeTableJoinDmlTest`, `Mysqli/ThreeTableJoinDmlTest` | V | V | P | P | 3-table JOIN: MySQL all pass; PG exec pass/$1 fails; SL exec pass/HAVING fails |
| SPEC-10.2.254 | `Pdo/SqliteDeleteChainedExistsTest`, `Pdo/MysqlDeleteChainedExistsTest`, `Pdo/PostgresDeleteChainedExistsTest` | — | V | P | V | Chained EXISTS DELETE: MySQL/SQLite pass; PG exec pass/$1 fails (#106) |
| SPEC-10.2.255 | `Pdo/SqliteCoalesceInDmlTest` | — | — | — | V | COALESCE in DML: exec pass, prepared DELETE affected by PDO type affinity |
| SPEC-10.2.256 | `Pdo/SqliteCaseInInsertValuesTest` | — | — | — | V | CASE in INSERT VALUES: exec pass, prepared affected by SQLite type affinity |
| SPEC-10.2.257 | `Pdo/SqliteStringFunctionDmlTest`, `Pdo/MysqlStringFunctionDmlTest`, `Pdo/PostgresStringFunctionDmlTest` | — | V | K | V | String functions: MySQL/SQLite pass; PG $N params store NULL (#108) |
| SPEC-10.2.258 | `Pdo/SqliteNullParamDmlTest` | — | — | — | V | NULL param binding: INSERT/UPDATE/DELETE all work correctly |
| SPEC-10.2.259 | `Pdo/SqliteSelfReferencingDeleteInTest` | — | — | — | V | Self-ref DELETE/UPDATE (no GROUP BY): all pass |
| SPEC-10.2.260 | `Pdo/SqliteDeleteWithMultipleSubqueryConditionsTest` | — | — | — | V | Multi-subquery DELETE (IN+NOT IN+scalar+EXISTS): all pass |
| SPEC-10.2.261 | `Pdo/SqliteUpdateWithSubqueryInMultipleSetsTest` | — | — | — | V | Non-correlated multi-aggregate SET: all pass (contrasts correlated variants) |
| SPEC-10.2.262 | `Pdo/MysqlMultiCorrelatedSetUpdateTest`, `Pdo/PostgresMultiCorrelatedSetUpdateTest`, `Mysqli/MultiCorrelatedSetUpdateTest` | V | V | K | K | Multi-correlated SET: MySQL all pass; PG GROUP BY error (all variants incl. non-correlated); SL syntax error |
| SPEC-11.PREPARED-BETWEEN-DML | `Pdo/SqliteBetweenParamDmlTest`, `Pdo/MysqlBetweenParamDmlTest`, `Pdo/PostgresBetweenParamDmlTest`, `Mysqli/BetweenParamDmlTest` | K | K | K | V | Prepared BETWEEN DML: SQLite all pass; MySQL/PG/MySQLi no-op (#118) |
| SPEC-11.CAST-IN-DML | `Pdo/SqliteCastInDmlTest`, `Pdo/MysqlCastInDmlTest`, `Pdo/PostgresCastInDmlTest`, `Mysqli/CastInDmlTest` | K | P | K | K | CAST in DML: INSERT SELECT 0-values (SL/PG); DELETE WHERE ignored (MY/PG/Mi) (#119) |
| SPEC-10.2.263 | `Pdo/SqliteSelfJoinDmlTest`, `Pdo/MysqlSelfJoinDmlTest`, `Pdo/PostgresSelfJoinDmlTest`, `Mysqli/SelfJoinDmlTest` | V | V | P | P | Self-join DML: DELETE pass all; UPDATE correlated SL/PG syntax error (extends #51) |
| SPEC-10.2.264 | `Pdo/SqliteParamInListDmlTest`, `Pdo/MysqlParamInListDmlTest`, `Pdo/PostgresParamInListDmlTest`, `Mysqli/ParamInListDmlTest` | V | V | V | V | Parameterized IN list DML: all pass on all platforms |
| SPEC-10.2.265 | `Pdo/SqliteLikeParamDmlTest` | — | — | — | V | LIKE with prepared wildcard params: all pass on SQLite |
| SPEC-10.2.266 | `Pdo/SqliteInsertSelectUnionAdvancedTest`, `Pdo/MysqlInsertSelectUnionAdvancedTest`, `Pdo/PostgresInsertSelectUnionAdvancedTest`, `Mysqli/InsertSelectUnionAdvancedTest` | K | K | V | P | Advanced INSERT UNION: PG all pass; MySQL multi-stmt error; SL triple fails (extends #103) |

| SPEC-11.SAVEPOINT-BLOCKED | `Pdo/SqliteSavepointTest`, `Pdo/MysqlSavepointTest`, `Pdo/PostgresSavepointTest`, `Mysqli/SavepointTest` | K | K | K | K | SAVEPOINT/RELEASE/ROLLBACK TO blocked (MY/Mi/SL) or shadow ignores (PG) (#120) |
| SPEC-11.SQLITE-RETURNING | `Pdo/SqliteReturningClauseTest` | — | — | — | K | INSERT returns empty; UPDATE/DELETE syntax error; extends #32/#53 (#121) |
| SPEC-11.TEMP-TABLE-DML | `Pdo/SqliteTempTableDmlTest` | — | — | — | K | CREATE TEMP works; subsequent DML blocked by Write Protection (#122) |
| SPEC-11.VIEW-EMPTY | `Pdo/SqliteViewDmlTest`, `Pdo/MysqlViewDmlTest`, `Pdo/PostgresViewDmlTest` | — | K | K | K | All view SELECT types return 0 rows after shadow DML on all platforms (#123) |
| SPEC-11.GENERATED-COL-NULL | `Pdo/SqliteGeneratedColumnTest`, `Pdo/MysqlGeneratedColumnTest`, `Pdo/PostgresGeneratedColumnTest`, `Mysqli/GeneratedColumnTest` | K | K | K | K | Generated columns NULL in shadow; WHERE/DELETE on generated col ineffective (#124) |
| SPEC-11.COALESCE-MULTI-PARAM | `Pdo/SqliteCoalesceInDmlTest` | — | — | — | K | Prepared DELETE COALESCE(col,?) < ? deletes all rows (#125) |
| SPEC-10.2.267 | `Pdo/SqliteDateTimeDmlTest`, `Pdo/MysqlDateTimeDmlTest`, `Pdo/PostgresDateTimeDmlTest` | — | V | P | V | Date/time DML: SET/WHERE work; PG TO_CHAR type error in shadow; INSERT SELECT func → NULL (#83) |
| SPEC-10.2.268 | `Pdo/SqliteNamedParamDmlTest`, `Pdo/MysqlNamedParamDmlTest`, `Pdo/PostgresNamedParamDmlTest` | — | V | V | V | PDO named params (:name style) all pass on all platforms |
| SPEC-10.2.269 | `Pdo/SqliteSubqueryInValuesDmlTest` | — | — | — | V | Subquery in INSERT VALUES: scalar MAX, COUNT, multi-subquery all pass |
| SPEC-10.2.270 | `Pdo/SqliteForeignKeyCascadeTest` | — | — | — | V | FK CASCADE: basic pass; ON DELETE CASCADE not applied in shadow (by-design SPEC-8.1) |
| SPEC-11.FK-CASCADE-SHADOW | `Mysqli/FkCascadeShadowTest`, `Pdo/MysqlFkCascadeShadowTest`, `Pdo/PostgresFkCascadeShadowTest`, `Pdo/SqliteFkCascadeShadowTest` | K | K | K | K | FK CASCADE DELETE/UPDATE not reflected in shadow store (#126) |
| SPEC-11.FK-INSERT-PARSE | `Pdo/MysqlFkInsertParseTest` | — | K | — | — | INSERT without column list on FK table miscount (#127) |
| SPEC-11.PG-IN-CLAUSE-DOLLAR-DML | `Pdo/PostgresBatchInClauseDmlTest`, `Pdo/PostgresSubstrReplaceDmlTest` | — | — | K | — | PG: prepared DML with IN ($N) is no-op; string funcs with $N fail (#128) |
| SPEC-11.SCALAR-FUNC-PREPARED-DELETE | `Pdo/SqliteGreatestLeastDmlTest` | — | — | — | K | DELETE WHERE MIN(a,b) < ? deletes all rows (#129) |
| SPEC-11.UPDATE-DELETE-ORDER-BY-LIMIT | `Mysqli/UpdateOrderByLimitTest`, `Pdo/MysqlUpdateOrderByLimitTest` | K | K | — | — | UPDATE/DELETE ORDER BY LIMIT is no-op (#130) |
| SPEC-11.FILTER-CLAUSE-DML | `Pdo/PostgresAggregateFilterDmlTest`, `Pdo/SqliteAggregateFilterDmlTest` | — | — | K | K | FILTER clause: INSERT loses alias; UPDATE subquery syntax error (#131) |
| SPEC-11.DISTINCT-ON-DML | `Pdo/PostgresDistinctOnDmlTest` | — | — | K | — | DELETE/UPDATE with DISTINCT ON subquery syntax error (#132) |
| SPEC-11.UPSERT-IF-VALUES-ZERO | `Mysqli/ConditionalUpsertTest`, `Pdo/MysqlConditionalUpsertTest` | K | K | — | — | ON DUPLICATE KEY UPDATE IF()/VALUES() evaluates to 0 (#133) |
| SPEC-11.UPDATE-IN-SET-OPERATION | `Pdo/PostgresIntersectExceptDmlTest` | — | — | K | — | UPDATE WHERE IN (EXCEPT) syntax error (#134) |
| SPEC-10.2.271 | `Mysqli/SelectForUpdateTest`, `Pdo/MysqlSelectForUpdateTest`, `Pdo/PostgresSelectForUpdateTest`, `Pdo/SqliteSelectForUpdateTest` | V | V | V | V | SELECT FOR UPDATE/SHARE: locking clauses preserved by CTE rewriter on all platforms |
| SPEC-10.2.272 | `Mysqli/IntersectExceptDmlTest`, `Pdo/MysqlIntersectExceptDmlTest`, `Pdo/PostgresIntersectExceptDmlTest`, `Pdo/SqliteIntersectExceptDmlTest` | K | K | P | V | INTERSECT/EXCEPT DML: SL all pass; PG UPDATE fails; MySQL INSERT multi-stmt (extends #14) |
| SPEC-10.2.273 | `Pdo/PostgresAggregateFilterDmlTest`, `Pdo/SqliteAggregateFilterDmlTest` | — | — | K | K | Aggregate FILTER: SELECT works; INSERT/UPDATE fail (#131) |
| SPEC-10.2.274 | `Mysqli/ConditionalUpsertTest`, `Pdo/MysqlConditionalUpsertTest`, `Pdo/PostgresConditionalUpsertTest`, `Pdo/SqliteConditionalUpsertTest` | K | K | K | K | Conditional upsert WHERE ignored; IF(VALUES()) → 0 (#133, extends #30) |
| SPEC-10.2.275 | `Mysqli/UpdateOrderByLimitTest`, `Pdo/MysqlUpdateOrderByLimitTest` | K | K | — | — | UPDATE/DELETE ORDER BY LIMIT no-op on MySQL (#130) |
| SPEC-10.2.276 | `Pdo/PostgresMultiCteDmlTest`, `Pdo/SqliteMultiCteDmlTest` | — | — | K | K | Writable CTEs: PG "relation not found"; SL correctly rejects (extends #28) |
| SPEC-10.2.270-new | `Pdo/PostgresDistinctOnDmlTest` | — | — | P | — | DISTINCT ON: SELECT/INSERT pass; DELETE/UPDATE subquery fails (#132) |
| SPEC-10.2.298 | `Pdo/MysqlNullifInDmlTest`, `Pdo/PostgresNullifInDmlTest`, `Pdo/SqliteNullifInDmlTest`, `Mysqli/NullifInDmlTest` | V | V | P | P | NULLIF in DML: MySQL/MySQLi all pass; SL/PG prepared param + INSERT...SELECT broken (extends #80, #83) |
| SPEC-10.2.299 | `Pdo/MysqlBatchCaseUpdateByIdTest`, `Pdo/PostgresBatchCaseUpdateByIdTest`, `Pdo/SqliteBatchCaseUpdateByIdTest`, `Mysqli/BatchCaseUpdateByIdTest` | V | V | V | V | Batch CASE UPDATE by id (ORM pattern): all pass on all platforms |
| SPEC-10.2.300 | `Pdo/MysqlPreparedRePrepareTest`, `Pdo/PostgresPreparedRePrepareTest`, `Pdo/SqlitePreparedRePrepareTest`, `Mysqli/PreparedRePrepareTest` | V | P | P | P | Prepared re-prepare: MySQLi all pass; PDO UPDATE→paramless SELECT stale (#146) |
| SPEC-10.2.301 | `Mysqli/SubqueryInValuesTest`, `Pdo/MysqlSubqueryInValuesTest`, `Pdo/PostgresSubqueryInValuesTest`, `Pdo/SqliteSubqueryInValuesTest` | V | V | V | V | Scalar subquery in INSERT VALUES: all pass on all platforms |
| SPEC-10.2.302 | `Mysqli/BooleanWhereInDmlTest`, `Pdo/MysqlBooleanWhereInDmlTest`, `Pdo/PostgresBooleanWhereInDmlTest`, `Pdo/SqliteBooleanWhereInDmlTest` | V | V | K | V | Implicit boolean WHERE DML: MySQL/SQLite pass; PG BOOLEAN CAST fails (extends #6) |
| SPEC-10.2.303 | `Mysqli/CorrelatedAggregateUpdateTest`, `Pdo/MysqlCorrelatedAggregateUpdateTest`, `Pdo/PostgresCorrelatedAggregateUpdateTest`, `Pdo/SqliteCorrelatedAggregateUpdateTest` | V | V | K | K | Correlated aggregate UPDATE SET: MySQL passes; PG GROUP BY error; SL syntax error (#147) |
| SPEC-10.2.304 | `Mysqli/IntervalArithmeticDmlTest`, `Pdo/MysqlIntervalArithmeticDmlTest`, `Pdo/PostgresIntervalArithmeticDmlTest`, `Pdo/SqliteIntervalArithmeticDmlTest` | P | P | V | V | Interval in UPDATE SET: PG/SL pass; MySQL INTERVAL syntax error, DATE_ADD works (#148) |
| SPEC-10.2.336 | `Pdo/SqliteInsertColumnOrderTest` | — | — | — | V | INSERT with non-DDL column order: correctly maps values by name |
| SPEC-10.2.337 | `Pdo/SqliteSubqueryInValuesTest` | — | — | — | V | Subquery in INSERT VALUES: scalar, self-ref, multiple, prepared all work |
| SPEC-10.2.338 | `Pdo/SqliteInsertSelectWithLimitTest` | — | — | — | V | INSERT...SELECT LIMIT/OFFSET: correctly restricts rows |
| SPEC-10.2.339 | `Pdo/SqliteExpressionInValuesTest` | — | — | — | V | Computed expressions in INSERT VALUES: concat, math, func, CASE, COALESCE all work |
| SPEC-10.2.340 | `Pdo/SqliteNaturalJoinTest` | — | — | — | V | NATURAL JOIN / NATURAL LEFT JOIN: works correctly including after DML |
| SPEC-10.2.341 | `Pdo/SqliteUpdateMultiColumnArithmeticTest` | — | — | — | V | UPDATE SET multi-column arithmetic: add, formula, swap, chained, prepared all work |
| SPEC-10.2.342 | `Pdo/SqlitePreparedBeforeInsertVisibilityTest`, `Pdo/SqliteConcurrentPreparedStmtTest` | — | — | — | K | Prepared SELECT CTE baked at prepare() time: all patterns fail (extends #87) |
| SPEC-10.2.343 | `Pdo/SqliteDeleteWithLimitTest` | — | — | — | V | DELETE ORDER BY LIMIT: works correctly on SQLite (MySQL #130 no-op) |
| SPEC-10.2.344 | `Pdo/SqliteHavingWithoutGroupByTest` | — | — | — | P | HAVING without GROUP BY: non-prepared works, prepared fails (extends #22) |
| SPEC-10.2.361 | `Pdo/PostgresMergeStatementTest` | — | — | K | — | MERGE INTO blocked by Write Protection on PG 15+ (#162) |
| SPEC-10.2.362 | `Pdo/PostgresCopyStatementTest` | — | — | K | — | COPY bypasses shadow store / blocked by Write Protection (#163) |
| SPEC-10.2.363 | `Pdo/MysqlLoadDataTest`, `Mysqli/LoadDataTest` | K | K | — | — | LOAD DATA blocked by Write Protection (#164) |
| SPEC-10.2.364 | `Pdo/SqliteUpdateFromValuesTest` | — | — | — | K | SQLite UPDATE FROM syntax error through CTE rewriter (confirms #72) |
| SPEC-10.2.365 | `Pdo/PostgresUpdateFromValuesTest` | — | — | P | — | UPDATE FROM VALUES works; prepared $N variant no-op (extends #106) |
| SPEC-10.2.366 | `Pdo/PostgresSelectIntoTest` | — | — | V | — | SELECT INTO / CTAS correctly reflects shadow DML |
| SPEC-10.2.367 | `Pdo/MysqlUpdateJoinValuesTest`, `Mysqli/UpdateJoinValuesTest` | K | K | — | — | UPDATE JOIN with inline subquery: identifier too long (confirms #104/#115) |
| SPEC-10.2.368 | `Pdo/MysqlUpdateJoinValuesTest`, `Mysqli/UpdateJoinValuesTest` | V | V | — | — | DELETE JOIN with inline subquery works correctly |
| SPEC-10.2.369 | `*MultiColumnInDmlTest` | K | K | K | K | Multi-column IN tuple in DML silently ignored (#165) |
| SPEC-10.2.370 | `*InsertSelectWindowTest` | V | V | K | K | INSERT...SELECT with window: MySQL works, PG/SQLite 0 rows (#166) |
| SPEC-10.2.371 | `Pdo/PostgresOrderedSetAggregateTest` | — | — | V | — | Ordered-set aggregates (WITHIN GROUP) work through shadow |
| SPEC-10.2.372 | `Pdo/PostgresOrderedSetAggregateTest` | — | — | K | — | Prepared $N in ordered-set aggregate returns 0 (#167) |
| SPEC-10.2.373 | `Pdo/PostgresOrderedSetAggregateTest` | — | — | K | — | UPDATE SET subquery same table: duplicate alias (#168) |
| SPEC-10.2.374 | `Pdo/PostgresWindowExcludeClauseTest`, `Pdo/SqliteWindowExcludeClauseTest` | — | — | V | V | Window EXCLUDE clause works through shadow store |
| SPEC-10.2.375 | `*ChainedCteShadowTest` | V | V | K | V | Chained CTEs: MySQL/SQLite work, PG empty (extends #4) |
| SPEC-10.2.376 | `Pdo/PostgresWindowIgnoreNullsTest`, `Pdo/SqliteWindowIgnoreNullsTest` | — | — | — | — | IGNORE NULLS: platform limitation (PG 17+ / SQLite N/A) |
| SPEC-10.2.377 | `Pdo/PostgresPartialIndexUpsertTest` | — | — | K | — | ON CONFLICT partial index: inserts duplicate instead of upsert (#169) |
| SPEC-10.2.378 | `Pdo/PostgresDomainTypeDmlTest` | — | — | P | — | Domain types: INSERT/agg work, UPDATE/DELETE fail (#170) |
| SPEC-10.2.379 | `Pdo/PostgresLateralDmlTest` | — | — | K | — | LATERAL in DELETE USING / UPDATE FROM silently ignored (extends PG-LATERAL) |
| SPEC-10.2.380 | `Pdo/PostgresInsertGenerateSeriesTest` | — | — | K | — | INSERT from generate_series: type casting broken (extends PG-GENERATE-SERIES) |
| SPEC-10.2.381 | `*ValuesExpressionDmlTest` | K | K | K | K | VALUES in DML subquery: type error (PG) or silent no-op (MySQL/SQLite) |
| SPEC-10.2.382 | `*CteInSubqueryDmlTest` | P | P | P | P | Nested CTE in DML subquery: syntax errors or silent no-op |
| SPEC-10.2.383 | `*InsertSelectGroupByHavingTest` | P | P | P | P | INSERT...SELECT GROUP BY HAVING: basic works, prior DML not reflected |
| SPEC-10.2.384 | `*ReturningChainDmlTest` | — | — | K | K | RETURNING chain DML: confirms #32/#53/#121 |

## Legend

- `*FooTest` = shorthand for all platform variants (e.g., `Mysqli/FooTest`, `Pdo/MysqlFooTest`, `Pdo/PostgresFooTest`, `Pdo/SqliteFooTest`)
- `Scenarios/FooScenario` = shared trait used by platform-specific test classes
- Mi = MySQLi adapter, MP = MySQL PDO adapter, PG = PostgreSQL PDO adapter, SL = SQLite PDO adapter
- V = Verified, P = Partially Verified, K = Known Issue
- `—` = Not applicable for this adapter/platform
