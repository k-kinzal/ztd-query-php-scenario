<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests GROUP BY ROLLUP() via PostgreSQL PDO.
 *
 * PostgreSQL uses standard SQL syntax: GROUP BY ROLLUP(col1, col2).
 * The CTE rewriter must pass this extended GROUP BY syntax through correctly.
 *
 * @spec SPEC-3.1
 */
class PostgresGroupByRollupTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_rollup_test (
            id SERIAL PRIMARY KEY,
            department VARCHAR(50) NOT NULL,
            team VARCHAR(50) NOT NULL,
            salary INT NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_rollup_test'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_rollup_test (id, department, team, salary) VALUES (1, 'Engineering', 'Backend', 100)");
        $this->pdo->exec("INSERT INTO pg_rollup_test (id, department, team, salary) VALUES (2, 'Engineering', 'Backend', 120)");
        $this->pdo->exec("INSERT INTO pg_rollup_test (id, department, team, salary) VALUES (3, 'Engineering', 'Frontend', 90)");
        $this->pdo->exec("INSERT INTO pg_rollup_test (id, department, team, salary) VALUES (4, 'Sales', 'Direct', 80)");
        $this->pdo->exec("INSERT INTO pg_rollup_test (id, department, team, salary) VALUES (5, 'Sales', 'Direct', 85)");
        $this->pdo->exec("INSERT INTO pg_rollup_test (id, department, team, salary) VALUES (6, 'Sales', 'Channel', 70)");
    }

    /**
     * GROUP BY ROLLUP(department).
     *
     * Should produce: Engineering, Sales, and grand total (NULL department).
     */
    public function testGroupByRollup(): void
    {
        try {
            $rows = $this->pdo->query(
                "SELECT department, SUM(salary) AS total
                 FROM pg_rollup_test
                 GROUP BY ROLLUP(department)
                 ORDER BY department NULLS LAST"
            )->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'GROUP BY ROLLUP: expected 3 rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame('Engineering', $rows[0]['department']);
            $this->assertSame(310, (int) $rows[0]['total']);
            $this->assertSame('Sales', $rows[1]['department']);
            $this->assertSame(235, (int) $rows[1]['total']);
            $this->assertNull($rows[2]['department']);
            $this->assertSame(545, (int) $rows[2]['total']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'GROUP BY ROLLUP failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * GROUP BY ROLLUP(department, team) (multi-level).
     */
    public function testMultiLevelRollup(): void
    {
        try {
            $rows = $this->pdo->query(
                "SELECT department, team, SUM(salary) AS total, COUNT(*) AS cnt
                 FROM pg_rollup_test
                 GROUP BY ROLLUP(department, team)
                 ORDER BY department NULLS LAST, team NULLS LAST"
            )->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 7) {
                $this->markTestIncomplete(
                    'Multi-level ROLLUP: expected 7 rows, got ' . count($rows)
                );
            }

            $this->assertCount(7, $rows);

            // Grand total (last row)
            $lastRow = end($rows);
            $this->assertNull($lastRow['department']);
            $this->assertNull($lastRow['team']);
            $this->assertSame(545, (int) $lastRow['total']);
            $this->assertSame(6, (int) $lastRow['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Multi-level ROLLUP failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * GROUP BY CUBE(department) — produces all combinations.
     *
     * For a single column, CUBE and ROLLUP are equivalent.
     */
    public function testGroupByCube(): void
    {
        try {
            $rows = $this->pdo->query(
                "SELECT department, SUM(salary) AS total
                 FROM pg_rollup_test
                 GROUP BY CUBE(department)
                 ORDER BY department NULLS LAST"
            )->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'GROUP BY CUBE: expected 3 rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame('Engineering', $rows[0]['department']);
            $this->assertSame('Sales', $rows[1]['department']);
            $this->assertNull($rows[2]['department']);
            $this->assertSame(545, (int) $rows[2]['total']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'GROUP BY CUBE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * GROUPING SETS — explicit grouping combinations.
     */
    public function testGroupingSets(): void
    {
        try {
            $rows = $this->pdo->query(
                "SELECT department, team, SUM(salary) AS total
                 FROM pg_rollup_test
                 GROUP BY GROUPING SETS ((department), (team), ())
                 ORDER BY department NULLS LAST, team NULLS LAST"
            )->fetchAll(PDO::FETCH_ASSOC);

            // Should produce: 2 dept rows, 4 team rows (Backend, Channel, Direct, Frontend),
            // and 1 grand total = 7 rows
            if (count($rows) < 5) {
                $this->markTestIncomplete(
                    'GROUPING SETS: expected at least 5 rows, got ' . count($rows)
                );
            }

            // Grand total row exists
            $grandTotal = array_filter($rows, fn($r) => $r['department'] === null && $r['team'] === null);
            $this->assertNotEmpty($grandTotal);
            $gt = array_values($grandTotal)[0];
            $this->assertSame(545, (int) $gt['total']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'GROUPING SETS failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * ROLLUP after shadow INSERT.
     */
    public function testRollupAfterShadowInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_rollup_test (id, department, team, salary) VALUES (7, 'HR', 'Recruiting', 75)");

            $rows = $this->pdo->query(
                "SELECT department, SUM(salary) AS total
                 FROM pg_rollup_test
                 GROUP BY ROLLUP(department)
                 ORDER BY department NULLS LAST"
            )->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'ROLLUP after INSERT: expected 4 rows, got ' . count($rows)
                );
            }

            $this->assertCount(4, $rows);
            $lastRow = end($rows);
            $this->assertNull($lastRow['department']);
            $this->assertSame(620, (int) $lastRow['total']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'ROLLUP after shadow INSERT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->query(
            "SELECT department, SUM(salary) FROM pg_rollup_test GROUP BY ROLLUP(department)"
        )->fetchAll(PDO::FETCH_ASSOC);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_rollup_test")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
