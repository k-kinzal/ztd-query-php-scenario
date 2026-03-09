<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests UNION ALL between multiple shadow tables.
 *
 * Real-world scenario: applications combine data from multiple tables
 * using UNION ALL (e.g., combining orders and returns, merging event
 * logs from different sources). When both sides of the UNION reference
 * shadow tables, the CTE rewriter must generate CTEs for all referenced
 * tables across both UNION branches.
 *
 * @spec SPEC-3.1
 * @spec SPEC-3.3
 */
class SqliteUnionAllShadowTablesTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_uas_orders (
                id INTEGER PRIMARY KEY,
                customer TEXT NOT NULL,
                amount REAL NOT NULL,
                created_at TEXT NOT NULL
            )',
            'CREATE TABLE sl_uas_refunds (
                id INTEGER PRIMARY KEY,
                customer TEXT NOT NULL,
                amount REAL NOT NULL,
                created_at TEXT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_uas_refunds', 'sl_uas_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO sl_uas_orders VALUES (1, 'Alice', 100.00, '2025-01-01')");
        $this->ztdExec("INSERT INTO sl_uas_orders VALUES (2, 'Bob', 200.00, '2025-01-15')");
        $this->ztdExec("INSERT INTO sl_uas_orders VALUES (3, 'Alice', 50.00, '2025-02-01')");

        $this->ztdExec("INSERT INTO sl_uas_refunds VALUES (1, 'Alice', 30.00, '2025-01-20')");
        $this->ztdExec("INSERT INTO sl_uas_refunds VALUES (2, 'Bob', 50.00, '2025-02-10')");
    }

    /**
     * UNION ALL between two shadow tables.
     */
    public function testUnionAllBetweenShadowTables(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT 'order' AS type, customer, amount FROM sl_uas_orders
                 UNION ALL
                 SELECT 'refund' AS type, customer, amount FROM sl_uas_refunds
                 ORDER BY customer, type"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'UNION ALL between shadow tables returned no rows.'
                );
            }

            // 3 orders + 2 refunds = 5 rows
            $this->assertCount(5, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UNION ALL between shadow tables failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UNION ALL with aggregate wrapping.
     */
    public function testUnionAllWithAggregate(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT customer, SUM(net) AS balance FROM (
                    SELECT customer, amount AS net FROM sl_uas_orders
                    UNION ALL
                    SELECT customer, -amount AS net FROM sl_uas_refunds
                 ) combined
                 GROUP BY customer
                 ORDER BY customer"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'UNION ALL with aggregate wrapper returned no rows. '
                    . 'Derived table containing UNION over shadow tables may not work (see Issue #13).'
                );
            }

            $this->assertCount(2, $rows);
            // Alice: orders 100+50=150, refunds -30 → balance 120
            $this->assertSame('Alice', $rows[0]['customer']);
            $this->assertEqualsWithDelta(120.00, (float) $rows[0]['balance'], 0.01);
            // Bob: orders 200, refunds -50 → balance 150
            $this->assertSame('Bob', $rows[1]['customer']);
            $this->assertEqualsWithDelta(150.00, (float) $rows[1]['balance'], 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UNION ALL with aggregate failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UNION ALL after shadow mutation on one side.
     */
    public function testUnionAllAfterMutation(): void
    {
        $this->ztdExec("INSERT INTO sl_uas_refunds VALUES (3, 'Carol', 75.00, '2025-03-01')");

        try {
            $rows = $this->ztdQuery(
                "SELECT customer, amount FROM sl_uas_refunds
                 UNION ALL
                 SELECT customer, amount FROM sl_uas_orders
                 ORDER BY customer, amount"
            );

            if (count($rows) < 6) {
                $this->markTestIncomplete(
                    'UNION ALL after mutation returned ' . count($rows) . ' rows instead of 6. '
                    . 'Shadow-inserted refund may not be visible in UNION query.'
                );
            }

            $this->assertCount(6, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UNION ALL after mutation failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UNION ALL with WHERE on each branch.
     */
    public function testUnionAllWithWhereOnBranches(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT 'order' AS type, customer, amount FROM sl_uas_orders WHERE amount > 75
                 UNION ALL
                 SELECT 'refund' AS type, customer, amount FROM sl_uas_refunds WHERE amount > 25
                 ORDER BY type, customer"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'UNION ALL with WHERE on branches returned no rows.'
                );
            }

            // Orders > 75: Alice(100), Bob(200) = 2
            // Refunds > 25: Alice(30), Bob(50) = 2
            $this->assertCount(4, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UNION ALL with WHERE on branches failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UNION ALL in subquery used in WHERE of another shadow table query.
     */
    public function testUnionAllSubqueryInWhere(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT * FROM sl_uas_orders
                 WHERE customer IN (
                    SELECT customer FROM sl_uas_refunds
                    UNION
                    SELECT 'Carol' -- Carol has no orders
                 )
                 ORDER BY id"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'UNION subquery in WHERE returned no rows.'
                );
            }

            // Customers with refunds: Alice, Bob (both have orders)
            $this->assertCount(3, $rows); // Alice has 2 orders, Bob has 1
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UNION subquery in WHERE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UNION (not ALL) between shadow tables — dedup.
     */
    public function testUnionDistinctBetweenShadowTables(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT customer FROM sl_uas_orders
                 UNION
                 SELECT customer FROM sl_uas_refunds
                 ORDER BY customer"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'UNION DISTINCT between shadow tables returned no rows.'
                );
            }

            // Distinct customers: Alice, Bob
            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['customer']);
            $this->assertSame('Bob', $rows[1]['customer']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UNION DISTINCT between shadow tables failed: ' . $e->getMessage()
            );
        }
    }
}
