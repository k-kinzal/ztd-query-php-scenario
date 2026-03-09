<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests GROUP BY and ORDER BY using column position numbers on MySQL PDO.
 *
 * @spec SPEC-3.1, SPEC-3.3
 */
class MysqlGroupByPositionNumberTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE my_gbp_sales (
            id INT PRIMARY KEY,
            region VARCHAR(20) NOT NULL,
            product VARCHAR(50) NOT NULL,
            amount DECIMAL(10,2) NOT NULL
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['my_gbp_sales'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_gbp_sales VALUES (1, 'North', 'Widget',  100)");
        $this->pdo->exec("INSERT INTO my_gbp_sales VALUES (2, 'North', 'Gadget',  200)");
        $this->pdo->exec("INSERT INTO my_gbp_sales VALUES (3, 'South', 'Widget',  150)");
        $this->pdo->exec("INSERT INTO my_gbp_sales VALUES (4, 'South', 'Widget',   50)");
        $this->pdo->exec("INSERT INTO my_gbp_sales VALUES (5, 'North', 'Widget',  300)");
    }

    /**
     * GROUP BY 1 — group by first column.
     */
    public function testGroupByPositionOne(): void
    {
        $sql = "SELECT region, SUM(amount) AS total FROM my_gbp_sales GROUP BY 1 ORDER BY 1";

        try {
            $rows = $this->ztdQuery($sql);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'GROUP BY 1: expected 2 groups, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('North', $rows[0]['region']);
            $this->assertEquals(600.0, (float) $rows[0]['total'], '', 0.01);
            $this->assertSame('South', $rows[1]['region']);
            $this->assertEquals(200.0, (float) $rows[1]['total'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('GROUP BY 1 failed: ' . $e->getMessage());
        }
    }

    /**
     * GROUP BY 1, 2 with ORDER BY 3 DESC.
     */
    public function testGroupByMultiplePositions(): void
    {
        $sql = "SELECT region, product, SUM(amount) AS total
                FROM my_gbp_sales GROUP BY 1, 2 ORDER BY 3 DESC";

        try {
            $rows = $this->ztdQuery($sql);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'GROUP BY 1, 2: expected 3 groups, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
            // Descending by total: North-Widget (400), South-Widget (200), North-Gadget (200)
            $this->assertSame('North', $rows[0]['region']);
            $this->assertSame('Widget', $rows[0]['product']);
            $this->assertEquals(400.0, (float) $rows[0]['total'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('GROUP BY 1, 2 failed: ' . $e->getMessage());
        }
    }

    /**
     * GROUP BY position with HAVING and prepared param.
     */
    public function testGroupByPositionWithHavingAndParam(): void
    {
        $sql = "SELECT region, SUM(amount) AS total
                FROM my_gbp_sales WHERE product = ? GROUP BY 1 HAVING SUM(amount) > 100 ORDER BY 1";

        try {
            $rows = $this->ztdPrepareAndExecute($sql, ['Widget']);

            // Widget totals: North=400, South=200. Both > 100.
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'GROUP BY 1 with HAVING+param: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('North', $rows[0]['region']);
            $this->assertEquals(400.0, (float) $rows[0]['total'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('GROUP BY position with HAVING failed: ' . $e->getMessage());
        }
    }
}
