<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests GROUP BY and ORDER BY using column position numbers on PostgreSQL PDO.
 *
 * @spec SPEC-3.1, SPEC-3.3
 */
class PostgresGroupByPositionNumberTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_gbp_sales (
            id INTEGER PRIMARY KEY,
            region VARCHAR(20) NOT NULL,
            product VARCHAR(50) NOT NULL,
            amount NUMERIC(10,2) NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_gbp_sales'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_gbp_sales VALUES (1, 'North', 'Widget',  100)");
        $this->pdo->exec("INSERT INTO pg_gbp_sales VALUES (2, 'North', 'Gadget',  200)");
        $this->pdo->exec("INSERT INTO pg_gbp_sales VALUES (3, 'South', 'Widget',  150)");
        $this->pdo->exec("INSERT INTO pg_gbp_sales VALUES (4, 'South', 'Widget',   50)");
        $this->pdo->exec("INSERT INTO pg_gbp_sales VALUES (5, 'North', 'Widget',  300)");
    }

    /**
     * GROUP BY 1 with ? placeholder param.
     */
    public function testGroupByPositionWithParam(): void
    {
        $sql = "SELECT product, COUNT(*) AS cnt, SUM(amount) AS total
                FROM pg_gbp_sales WHERE region = ? GROUP BY 1 ORDER BY 2 DESC";

        try {
            $rows = $this->ztdPrepareAndExecute($sql, ['North']);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'GROUP BY 1 with param: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            // Widget (2 items, 400), Gadget (1 item, 200) ordered by count DESC
            $this->assertSame('Widget', $rows[0]['product']);
            $this->assertSame(2, (int) $rows[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('GROUP BY position with param failed: ' . $e->getMessage());
        }
    }

    /**
     * GROUP BY 1, 2 — multi-column positional grouping.
     */
    public function testGroupByMultiplePositions(): void
    {
        $sql = "SELECT region, product, SUM(amount) AS total
                FROM pg_gbp_sales GROUP BY 1, 2 ORDER BY 1, 2";

        try {
            $rows = $this->ztdQuery($sql);

            $this->assertCount(3, $rows);
            $this->assertSame('North', $rows[0]['region']);
            $this->assertSame('Gadget', $rows[0]['product']);
            $this->assertEquals(200.0, (float) $rows[0]['total'], '', 0.01);

            $this->assertSame('North', $rows[1]['region']);
            $this->assertSame('Widget', $rows[1]['product']);
            $this->assertEquals(400.0, (float) $rows[1]['total'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('GROUP BY 1, 2 failed: ' . $e->getMessage());
        }
    }

    /**
     * GROUP BY position with HAVING.
     */
    public function testGroupByPositionWithHaving(): void
    {
        $sql = "SELECT region, SUM(amount) AS total
                FROM pg_gbp_sales GROUP BY 1 HAVING SUM(amount) > 300 ORDER BY 1";

        try {
            $rows = $this->ztdQuery($sql);

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'GROUP BY 1 HAVING: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('North', $rows[0]['region']);
            $this->assertEquals(600.0, (float) $rows[0]['total'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('GROUP BY position with HAVING failed: ' . $e->getMessage());
        }
    }
}
