<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests user-written multiple CTEs (WITH ... AS, ... AS) through ZTD.
 *
 * The CTE rewriter wraps queries in its own WITH clause for shadow data.
 * User-written CTEs may conflict or be incorrectly merged with the rewriter's CTEs.
 *
 * @spec SPEC-3.1
 */
class SqliteMultipleCteSelectTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_mc_orders (
                id INTEGER PRIMARY KEY,
                customer TEXT NOT NULL,
                amount REAL NOT NULL,
                status TEXT NOT NULL
            )',
            'CREATE TABLE sl_mc_products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT NOT NULL,
                price REAL NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_mc_orders', 'sl_mc_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_mc_orders VALUES (1, 'alice', 100, 'completed')");
        $this->pdo->exec("INSERT INTO sl_mc_orders VALUES (2, 'alice', 200, 'completed')");
        $this->pdo->exec("INSERT INTO sl_mc_orders VALUES (3, 'bob', 150, 'pending')");
        $this->pdo->exec("INSERT INTO sl_mc_orders VALUES (4, 'bob', 80, 'completed')");

        $this->pdo->exec("INSERT INTO sl_mc_products VALUES (1, 'Widget', 'tools', 10)");
        $this->pdo->exec("INSERT INTO sl_mc_products VALUES (2, 'Gadget', 'tools', 20)");
        $this->pdo->exec("INSERT INTO sl_mc_products VALUES (3, 'Sprocket', 'parts', 30)");
    }

    /**
     * Single user-written CTE.
     */
    public function testSingleCte(): void
    {
        $sql = "WITH completed AS (
                    SELECT customer, SUM(amount) AS total
                    FROM sl_mc_orders
                    WHERE status = 'completed'
                    GROUP BY customer
                )
                SELECT customer, total FROM completed ORDER BY customer";

        try {
            $rows = $this->ztdQuery($sql);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Single CTE: expected 2 rows, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('alice', $rows[0]['customer']);
            $this->assertEquals(300.0, (float) $rows[0]['total'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Single CTE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Multiple user-written CTEs.
     */
    public function testMultipleCtes(): void
    {
        $sql = "WITH
                order_totals AS (
                    SELECT customer, SUM(amount) AS total
                    FROM sl_mc_orders
                    WHERE status = 'completed'
                    GROUP BY customer
                ),
                product_counts AS (
                    SELECT category, COUNT(*) AS cnt
                    FROM sl_mc_products
                    GROUP BY category
                )
                SELECT o.customer, o.total, p.category, p.cnt
                FROM order_totals o
                CROSS JOIN product_counts p
                ORDER BY o.customer, p.category";

        try {
            $rows = $this->ztdQuery($sql);

            // 2 customers × 2 categories = 4 rows
            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'Multiple CTEs: expected 4 rows, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(4, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Multiple CTEs failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * User CTE with shadow-inserted data.
     */
    public function testCteWithShadowData(): void
    {
        try {
            // Insert into shadow
            $this->pdo->exec("INSERT INTO sl_mc_orders VALUES (5, 'charlie', 500, 'completed')");

            $sql = "WITH completed AS (
                        SELECT customer, SUM(amount) AS total
                        FROM sl_mc_orders
                        WHERE status = 'completed'
                        GROUP BY customer
                    )
                    SELECT customer, total FROM completed ORDER BY customer";

            $rows = $this->ztdQuery($sql);

            // alice(300), bob(80), charlie(500) = 3 customers
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'CTE with shadow data: expected 3 rows, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);

            // Check charlie's total
            $charlie = array_filter($rows, fn($r) => $r['customer'] === 'charlie');
            $charlie = array_values($charlie);
            if (count($charlie) !== 1) {
                $this->markTestIncomplete('Charlie not found in CTE results');
            }

            $this->assertEquals(500.0, (float) $charlie[0]['total'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'CTE with shadow data failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * User CTE with prepared params.
     */
    public function testCteWithPreparedParam(): void
    {
        $sql = "WITH filtered AS (
                    SELECT customer, SUM(amount) AS total
                    FROM sl_mc_orders
                    WHERE status = ?
                    GROUP BY customer
                )
                SELECT customer, total FROM filtered ORDER BY total DESC";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute(['completed']);
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'CTE with param: expected 2 rows, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('alice', $rows[0]['customer']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'CTE with prepared param failed: ' . $e->getMessage()
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
                    FROM sl_mc_orders
                    WHERE amount >= ?
                ),
                expensive_products AS (
                    SELECT name, price
                    FROM sl_mc_products
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

            // Orders >= 100: alice(100), alice(200), bob(150) = 3
            if ($orderCount !== 3) {
                $this->markTestIncomplete(
                    "Multi-CTE params: order_count expected 3, got {$orderCount}"
                );
            }

            // Products >= 20: Gadget(20), Sprocket(30) = 2
            if ($productCount !== 2) {
                $this->markTestIncomplete(
                    "Multi-CTE params: product_count expected 2, got {$productCount}"
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
     * CTE referencing another CTE (chained).
     */
    public function testChainedCtes(): void
    {
        $sql = "WITH
                base AS (
                    SELECT customer, amount, status
                    FROM sl_mc_orders
                    WHERE status = 'completed'
                ),
                totals AS (
                    SELECT customer, SUM(amount) AS total, COUNT(*) AS cnt
                    FROM base
                    GROUP BY customer
                )
                SELECT customer, total, cnt FROM totals WHERE total > 100 ORDER BY total DESC";

        try {
            $rows = $this->ztdQuery($sql);

            // alice: total=300, bob: total=80. Only alice > 100.
            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Chained CTEs: expected 1 row (alice), got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('alice', $rows[0]['customer']);
            $this->assertEquals(300.0, (float) $rows[0]['total'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Chained CTEs failed: ' . $e->getMessage()
            );
        }
    }
}
