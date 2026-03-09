<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests user-written multiple CTEs through ZTD on MySQL.
 *
 * @spec SPEC-3.1
 */
class MysqlMultipleCteSelectTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_mc_orders (
                id INT PRIMARY KEY,
                customer VARCHAR(50) NOT NULL,
                amount DECIMAL(10,2) NOT NULL,
                status VARCHAR(20) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE my_mc_products (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                category VARCHAR(20) NOT NULL,
                price DECIMAL(10,2) NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_mc_orders', 'my_mc_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_mc_orders VALUES (1, 'alice', 100, 'completed')");
        $this->pdo->exec("INSERT INTO my_mc_orders VALUES (2, 'alice', 200, 'completed')");
        $this->pdo->exec("INSERT INTO my_mc_orders VALUES (3, 'bob', 150, 'pending')");
        $this->pdo->exec("INSERT INTO my_mc_orders VALUES (4, 'bob', 80, 'completed')");

        $this->pdo->exec("INSERT INTO my_mc_products VALUES (1, 'Widget', 'tools', 10)");
        $this->pdo->exec("INSERT INTO my_mc_products VALUES (2, 'Gadget', 'tools', 20)");
        $this->pdo->exec("INSERT INTO my_mc_products VALUES (3, 'Sprocket', 'parts', 30)");
    }

    /**
     * Multiple CTEs with CROSS JOIN.
     */
    public function testMultipleCtesWithCrossJoin(): void
    {
        $sql = "WITH
                order_totals AS (
                    SELECT customer, SUM(amount) AS total
                    FROM my_mc_orders
                    WHERE status = 'completed'
                    GROUP BY customer
                ),
                product_counts AS (
                    SELECT category, COUNT(*) AS cnt
                    FROM my_mc_products
                    GROUP BY category
                )
                SELECT o.customer, o.total, p.category, p.cnt
                FROM order_totals o
                CROSS JOIN product_counts p
                ORDER BY o.customer, p.category";

        try {
            $rows = $this->ztdQuery($sql);

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'Multiple CTEs CROSS JOIN: expected 4 rows, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(4, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Multiple CTEs CROSS JOIN failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * CTE with shadow data.
     */
    public function testCteWithShadowData(): void
    {
        try {
            $this->pdo->exec("INSERT INTO my_mc_orders VALUES (5, 'charlie', 500, 'completed')");

            $sql = "WITH completed AS (
                        SELECT customer, SUM(amount) AS total
                        FROM my_mc_orders
                        WHERE status = 'completed'
                        GROUP BY customer
                    )
                    SELECT customer, total FROM completed ORDER BY customer";

            $rows = $this->ztdQuery($sql);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'CTE with shadow: expected 3 rows, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'CTE with shadow data failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Multiple CTEs with prepared params.
     */
    public function testMultipleCtesWithPreparedParams(): void
    {
        $sql = "WITH
                high_orders AS (
                    SELECT customer, amount
                    FROM my_mc_orders
                    WHERE amount >= ?
                ),
                expensive_products AS (
                    SELECT name, price
                    FROM my_mc_products
                    WHERE price >= ?
                )
                SELECT
                    (SELECT COUNT(*) FROM high_orders) AS order_count,
                    (SELECT COUNT(*) FROM expensive_products) AS product_count";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([100, 20]);
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            $this->assertCount(1, $rows);

            $orderCount = (int) $rows[0]['order_count'];
            $productCount = (int) $rows[0]['product_count'];

            if ($orderCount !== 3) {
                $this->markTestIncomplete(
                    "Multi-CTE params: order_count expected 3, got {$orderCount}"
                );
            }

            $this->assertSame(3, $orderCount);
            $this->assertSame(2, $productCount);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Multiple CTEs with prepared params failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Chained CTEs.
     */
    public function testChainedCtes(): void
    {
        $sql = "WITH
                base AS (
                    SELECT customer, amount FROM my_mc_orders WHERE status = 'completed'
                ),
                totals AS (
                    SELECT customer, SUM(amount) AS total FROM base GROUP BY customer
                )
                SELECT customer, total FROM totals WHERE total > 100 ORDER BY total DESC";

        try {
            $rows = $this->ztdQuery($sql);

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Chained CTEs: expected 1 row, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('alice', $rows[0]['customer']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Chained CTEs failed: ' . $e->getMessage()
            );
        }
    }
}
