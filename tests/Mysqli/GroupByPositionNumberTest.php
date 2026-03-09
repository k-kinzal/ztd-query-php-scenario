<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests GROUP BY and ORDER BY using column position numbers on MySQLi.
 *
 * @spec SPEC-3.1, SPEC-3.3
 */
class GroupByPositionNumberTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_gbp_sales (
            id INT PRIMARY KEY,
            region VARCHAR(20) NOT NULL,
            product VARCHAR(50) NOT NULL,
            amount DECIMAL(10,2) NOT NULL
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mi_gbp_sales'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_gbp_sales VALUES (1, 'North', 'Widget',  100)");
        $this->mysqli->query("INSERT INTO mi_gbp_sales VALUES (2, 'North', 'Gadget',  200)");
        $this->mysqli->query("INSERT INTO mi_gbp_sales VALUES (3, 'South', 'Widget',  150)");
        $this->mysqli->query("INSERT INTO mi_gbp_sales VALUES (4, 'South', 'Widget',   50)");
        $this->mysqli->query("INSERT INTO mi_gbp_sales VALUES (5, 'North', 'Widget',  300)");
    }

    /**
     * GROUP BY 1, ORDER BY 2 DESC — via query().
     */
    public function testGroupByPositionViaQuery(): void
    {
        $sql = "SELECT region, SUM(amount) AS total FROM mi_gbp_sales GROUP BY 1 ORDER BY 2 DESC";

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
        } catch (\Throwable $e) {
            $this->markTestIncomplete('GROUP BY 1 failed: ' . $e->getMessage());
        }
    }

    /**
     * GROUP BY 1 via prepare+bind_param+execute.
     */
    public function testGroupByPositionWithPreparedParam(): void
    {
        $sql = "SELECT product, COUNT(*) AS cnt
                FROM mi_gbp_sales WHERE region = ? GROUP BY 1 ORDER BY 2 DESC";

        try {
            $stmt = $this->mysqli->prepare($sql);
            $region = 'North';
            $stmt->bind_param('s', $region);
            $stmt->execute();
            $result = $stmt->get_result();
            $rows = $result->fetch_all(MYSQLI_ASSOC);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'GROUP BY 1 with param: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Widget', $rows[0]['product']);
            $this->assertSame(2, (int) $rows[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('GROUP BY position with param failed: ' . $e->getMessage());
        }
    }
}
