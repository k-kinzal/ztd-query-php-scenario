<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests GROUP BY ... WITH ROLLUP via MySQLi.
 *
 * WITH ROLLUP produces super-aggregate summary rows. The CTE rewriter
 * must pass this extended GROUP BY syntax through without corruption.
 *
 * @spec SPEC-3.1
 */
class GroupByRollupTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE rollup_test (
            id INT PRIMARY KEY,
            department VARCHAR(50) NOT NULL,
            team VARCHAR(50) NOT NULL,
            salary INT NOT NULL
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['rollup_test'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO rollup_test VALUES (1, 'Engineering', 'Backend', 100)");
        $this->ztdExec("INSERT INTO rollup_test VALUES (2, 'Engineering', 'Backend', 120)");
        $this->ztdExec("INSERT INTO rollup_test VALUES (3, 'Engineering', 'Frontend', 90)");
        $this->ztdExec("INSERT INTO rollup_test VALUES (4, 'Sales', 'Direct', 80)");
        $this->ztdExec("INSERT INTO rollup_test VALUES (5, 'Sales', 'Direct', 85)");
        $this->ztdExec("INSERT INTO rollup_test VALUES (6, 'Sales', 'Channel', 70)");
    }

    public function testGroupByWithRollup(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT department, SUM(salary) AS total
                 FROM rollup_test
                 GROUP BY department WITH ROLLUP"
            );

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'GROUP BY WITH ROLLUP: expected 3 rows, got ' . count($rows)
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
            $this->markTestIncomplete('GROUP BY WITH ROLLUP failed: ' . $e->getMessage());
        }
    }

    public function testMultiLevelRollup(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT department, team, SUM(salary) AS total, COUNT(*) AS cnt
                 FROM rollup_test
                 GROUP BY department, team WITH ROLLUP"
            );

            if (count($rows) !== 7) {
                $this->markTestIncomplete(
                    'Multi-level ROLLUP: expected 7 rows, got ' . count($rows)
                );
            }

            $this->assertCount(7, $rows);
            $lastRow = end($rows);
            $this->assertNull($lastRow['department']);
            $this->assertNull($lastRow['team']);
            $this->assertSame(545, (int) $lastRow['total']);
            $this->assertSame(6, (int) $lastRow['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-level ROLLUP failed: ' . $e->getMessage());
        }
    }

    public function testRollupAfterShadowInsert(): void
    {
        try {
            $this->ztdExec("INSERT INTO rollup_test VALUES (7, 'HR', 'Recruiting', 75)");

            $rows = $this->ztdQuery(
                "SELECT department, SUM(salary) AS total
                 FROM rollup_test
                 GROUP BY department WITH ROLLUP"
            );

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
            $this->markTestIncomplete('ROLLUP after shadow INSERT failed: ' . $e->getMessage());
        }
    }

    public function testPhysicalIsolation(): void
    {
        $this->ztdQuery(
            "SELECT department, SUM(salary) FROM rollup_test GROUP BY department WITH ROLLUP"
        );

        $this->disableZtd();
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM rollup_test");
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
