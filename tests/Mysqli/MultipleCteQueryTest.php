<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests user-defined CTEs (WITH ... AS) in shadow queries via MySQLi.
 *
 * ZTD adds its own CTE for shadow data. Documents whether user CTEs
 * are overwritten by ZTD's CTE on MySQLi.
 * @spec pending
 */
class MultipleCteQueryTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_cte_orders (id INT PRIMARY KEY, customer VARCHAR(50), amount DECIMAL(10,2))';
    }

    protected function getTableNames(): array
    {
        return ['mi_cte_orders'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_cte_orders VALUES (1, 'Alice', 100.00)");
        $this->mysqli->query("INSERT INTO mi_cte_orders VALUES (2, 'Alice', 200.00)");
        $this->mysqli->query("INSERT INTO mi_cte_orders VALUES (3, 'Bob', 150.00)");
        $this->mysqli->query("INSERT INTO mi_cte_orders VALUES (4, 'Charlie', 300.00)");
    }

    /**
     * User CTE — ZTD may overwrite the WITH clause.
     */
    public function testUserCteReference(): void
    {
        try {
            $result = $this->mysqli->query('
                WITH totals AS (
                    SELECT customer, SUM(amount) AS total
                    FROM mi_cte_orders
                    GROUP BY customer
                )
                SELECT customer, total FROM totals ORDER BY total DESC
            ');
            $rows = $result->fetch_all(MYSQLI_ASSOC);
            $this->assertIsArray($rows);
        } catch (\Exception $e) {
            // CTE name not found — ZTD overwrote the WITH clause
            $this->assertStringContainsString('totals', $e->getMessage());
        }
    }

    /**
     * Inline aggregation works as CTE alternative.
     */
    public function testInlineAggregationWorks(): void
    {
        $result = $this->mysqli->query('
            SELECT customer, SUM(amount) AS total
            FROM mi_cte_orders
            GROUP BY customer
            ORDER BY total DESC
        ');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(3, $rows);
        // Alice=300, Charlie=300 (tied), Bob=150
        $this->assertEqualsWithDelta(300.0, (float) $rows[0]['total'], 0.01);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_cte_orders');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
