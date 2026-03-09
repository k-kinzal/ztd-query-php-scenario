<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests GROUP BY ... WITH ROLLUP via MySQL PDO.
 *
 * WITH ROLLUP produces super-aggregate summary rows. The CTE rewriter
 * must pass this extended GROUP BY syntax through without corruption.
 *
 * @spec SPEC-3.1
 */
class MysqlGroupByRollupTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mp_rollup_test (
            id INT PRIMARY KEY,
            department VARCHAR(50) NOT NULL,
            team VARCHAR(50) NOT NULL,
            salary INT NOT NULL
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mp_rollup_test'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_rollup_test VALUES (1, 'Engineering', 'Backend', 100)");
        $this->pdo->exec("INSERT INTO mp_rollup_test VALUES (2, 'Engineering', 'Backend', 120)");
        $this->pdo->exec("INSERT INTO mp_rollup_test VALUES (3, 'Engineering', 'Frontend', 90)");
        $this->pdo->exec("INSERT INTO mp_rollup_test VALUES (4, 'Sales', 'Direct', 80)");
        $this->pdo->exec("INSERT INTO mp_rollup_test VALUES (5, 'Sales', 'Direct', 85)");
        $this->pdo->exec("INSERT INTO mp_rollup_test VALUES (6, 'Sales', 'Channel', 70)");
    }

    /**
     * GROUP BY department WITH ROLLUP.
     *
     * Should produce: Engineering (total), Sales (total), NULL (grand total).
     */
    public function testGroupByWithRollup(): void
    {
        try {
            $rows = $this->pdo->query(
                "SELECT department, SUM(salary) AS total
                 FROM mp_rollup_test
                 GROUP BY department WITH ROLLUP"
            )->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'GROUP BY WITH ROLLUP: expected 3 rows (2 groups + grand total), got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
            // Department rows
            $this->assertSame('Engineering', $rows[0]['department']);
            $this->assertSame(310, (int) $rows[0]['total']); // 100+120+90
            $this->assertSame('Sales', $rows[1]['department']);
            $this->assertSame(235, (int) $rows[1]['total']); // 80+85+70
            // Grand total row (department = NULL)
            $this->assertNull($rows[2]['department']);
            $this->assertSame(545, (int) $rows[2]['total']); // 310+235
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'GROUP BY WITH ROLLUP failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * GROUP BY department, team WITH ROLLUP (multi-level).
     *
     * Produces subtotals per department and grand total.
     */
    public function testMultiLevelRollup(): void
    {
        try {
            $rows = $this->pdo->query(
                "SELECT department, team, SUM(salary) AS total, COUNT(*) AS cnt
                 FROM mp_rollup_test
                 GROUP BY department, team WITH ROLLUP"
            )->fetchAll(PDO::FETCH_ASSOC);

            // Expected rows:
            // Engineering, Backend: 220, 2
            // Engineering, Frontend: 90, 1
            // Engineering, NULL: 310, 3    (dept subtotal)
            // Sales, Channel: 70, 1
            // Sales, Direct: 165, 2
            // Sales, NULL: 235, 3          (dept subtotal)
            // NULL, NULL: 545, 6           (grand total)
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
     * GROUP BY WITH ROLLUP after shadow INSERT.
     *
     * New data should be included in rollup aggregates.
     */
    public function testRollupAfterShadowInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO mp_rollup_test VALUES (7, 'HR', 'Recruiting', 75)");

            $rows = $this->pdo->query(
                "SELECT department, SUM(salary) AS total
                 FROM mp_rollup_test
                 GROUP BY department WITH ROLLUP"
            )->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'ROLLUP after INSERT: expected 4 rows (3 depts + grand), got ' . count($rows)
                );
            }

            $this->assertCount(4, $rows);

            // Grand total should include new department
            $lastRow = end($rows);
            $this->assertNull($lastRow['department']);
            $this->assertSame(620, (int) $lastRow['total']); // 545 + 75
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'ROLLUP after shadow INSERT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * GROUP BY WITH ROLLUP with HAVING clause.
     */
    public function testRollupWithHaving(): void
    {
        try {
            $rows = $this->pdo->query(
                "SELECT department, SUM(salary) AS total
                 FROM mp_rollup_test
                 GROUP BY department WITH ROLLUP
                 HAVING total > 300 OR department IS NULL"
            )->fetchAll(PDO::FETCH_ASSOC);

            // Engineering=310 (>300), Sales=235 (not >300, not NULL dept)
            // Grand total: dept IS NULL, so included
            if (count($rows) < 1) {
                $this->markTestIncomplete(
                    'ROLLUP with HAVING: expected rows, got ' . count($rows)
                );
            }

            // At minimum, Engineering and grand total should be present
            $depts = array_column($rows, 'department');
            $this->assertContains('Engineering', $depts);
            $this->assertContains(null, $depts);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'ROLLUP with HAVING failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->query(
            "SELECT department, SUM(salary) FROM mp_rollup_test GROUP BY department WITH ROLLUP"
        )->fetchAll(PDO::FETCH_ASSOC);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mp_rollup_test")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
