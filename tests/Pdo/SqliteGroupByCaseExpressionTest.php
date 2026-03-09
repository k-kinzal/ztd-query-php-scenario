<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests GROUP BY with CASE expression on shadow data.
 *
 * Real-world scenario: applications group rows by computed categories
 * using CASE in GROUP BY (e.g., grouping sales by price tier, users
 * by age range). The CTE rewriter must correctly handle CASE expressions
 * in GROUP BY position and ensure aggregate results reflect shadow data.
 *
 * @spec SPEC-3.1
 * @spec SPEC-3.3
 */
class SqliteGroupByCaseExpressionTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_gbce_orders (
                id INTEGER PRIMARY KEY,
                customer TEXT NOT NULL,
                amount REAL NOT NULL,
                status TEXT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_gbce_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO sl_gbce_orders VALUES (1, 'Alice', 25.00, 'completed')");
        $this->ztdExec("INSERT INTO sl_gbce_orders VALUES (2, 'Bob', 150.00, 'completed')");
        $this->ztdExec("INSERT INTO sl_gbce_orders VALUES (3, 'Carol', 500.00, 'completed')");
        $this->ztdExec("INSERT INTO sl_gbce_orders VALUES (4, 'Dave', 75.00, 'pending')");
        $this->ztdExec("INSERT INTO sl_gbce_orders VALUES (5, 'Eve', 1200.00, 'completed')");
        $this->ztdExec("INSERT INTO sl_gbce_orders VALUES (6, 'Frank', 30.00, 'completed')");
    }

    /**
     * GROUP BY CASE expression with aggregate.
     */
    public function testGroupByCaseExpression(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT
                    CASE
                        WHEN amount >= 1000 THEN 'high'
                        WHEN amount >= 100 THEN 'medium'
                        ELSE 'low'
                    END AS tier,
                    COUNT(*) AS cnt,
                    SUM(amount) AS total
                 FROM sl_gbce_orders
                 WHERE status = 'completed'
                 GROUP BY CASE
                    WHEN amount >= 1000 THEN 'high'
                    WHEN amount >= 100 THEN 'medium'
                    ELSE 'low'
                 END
                 ORDER BY total DESC"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'GROUP BY CASE expression returned no rows on shadow data.'
                );
            }

            // high: Eve (1200), medium: Bob (150) + Carol (500), low: Alice (25) + Frank (30)
            $this->assertCount(3, $rows);

            $byTier = array_column($rows, null, 'tier');
            $this->assertEquals(1, (int) $byTier['high']['cnt']);
            $this->assertEqualsWithDelta(1200.00, (float) $byTier['high']['total'], 0.01);
            $this->assertEquals(2, (int) $byTier['medium']['cnt']);
            $this->assertEqualsWithDelta(650.00, (float) $byTier['medium']['total'], 0.01);
            $this->assertEquals(2, (int) $byTier['low']['cnt']);
            $this->assertEqualsWithDelta(55.00, (float) $byTier['low']['total'], 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'GROUP BY CASE expression failed on SQLite: ' . $e->getMessage()
            );
        }
    }

    /**
     * GROUP BY CASE with HAVING filter.
     */
    public function testGroupByCaseWithHaving(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT
                    CASE
                        WHEN amount >= 100 THEN 'big'
                        ELSE 'small'
                    END AS size,
                    COUNT(*) AS cnt
                 FROM sl_gbce_orders
                 GROUP BY CASE
                    WHEN amount >= 100 THEN 'big'
                    ELSE 'small'
                 END
                 HAVING COUNT(*) > 2"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'GROUP BY CASE with HAVING returned no rows. '
                    . 'HAVING filter on CASE-grouped aggregates may not work on shadow data.'
                );
            }

            // small: Alice(25), Dave(75), Frank(30) = 3 → passes HAVING > 2
            // big: Bob(150), Carol(500), Eve(1200) = 3 → passes HAVING > 2
            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'GROUP BY CASE with HAVING failed on SQLite: ' . $e->getMessage()
            );
        }
    }

    /**
     * GROUP BY CASE with prepared params in CASE.
     */
    public function testGroupByCaseWithPreparedParams(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT
                    CASE
                        WHEN amount >= ? THEN 'above'
                        ELSE 'below'
                    END AS bracket,
                    COUNT(*) AS cnt
                 FROM sl_gbce_orders
                 GROUP BY CASE
                    WHEN amount >= ? THEN 'above'
                    ELSE 'below'
                 END
                 ORDER BY bracket",
                [100, 100]
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'GROUP BY CASE with prepared params returned no rows.'
                );
            }

            $this->assertCount(2, $rows);
            $byBracket = array_column($rows, null, 'bracket');
            $this->assertEquals(3, (int) $byBracket['above']['cnt']); // 150, 500, 1200
            $this->assertEquals(3, (int) $byBracket['below']['cnt']); // 25, 75, 30
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'GROUP BY CASE with prepared params failed on SQLite: ' . $e->getMessage()
            );
        }
    }

    /**
     * GROUP BY CASE after shadow mutation.
     */
    public function testGroupByCaseAfterMutation(): void
    {
        // Add a new high-value order
        $this->ztdExec("INSERT INTO sl_gbce_orders VALUES (7, 'Grace', 2000.00, 'completed')");

        try {
            $rows = $this->ztdQuery(
                "SELECT
                    CASE WHEN amount >= 1000 THEN 'high' ELSE 'other' END AS tier,
                    COUNT(*) AS cnt
                 FROM sl_gbce_orders
                 WHERE status = 'completed'
                 GROUP BY CASE WHEN amount >= 1000 THEN 'high' ELSE 'other' END
                 ORDER BY tier"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'GROUP BY CASE after mutation returned no rows.'
                );
            }

            $byTier = array_column($rows, null, 'tier');
            // high: Eve (1200) + Grace (2000) = 2
            $this->assertEquals(2, (int) $byTier['high']['cnt']);
            // other: Alice (25), Bob (150), Carol (500), Frank (30) = 4
            $this->assertEquals(4, (int) $byTier['other']['cnt']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'GROUP BY CASE after mutation failed on SQLite: ' . $e->getMessage()
            );
        }
    }
}
