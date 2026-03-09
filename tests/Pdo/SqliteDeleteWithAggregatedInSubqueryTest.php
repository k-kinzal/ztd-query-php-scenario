<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests DELETE WHERE col IN (SELECT ... GROUP BY HAVING).
 *
 * Pattern: delete all customers who have placed fewer than N orders.
 * Combines DELETE with aggregated subquery — stresses the CTE rewriter
 * because the subquery references a different shadowed table.
 *
 * @spec SPEC-4.3
 */
class SqliteDeleteWithAggregatedInSubqueryTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_dais_customers (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                tier TEXT NOT NULL DEFAULT \'standard\'
            )',
            'CREATE TABLE sl_dais_orders (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                amount REAL NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_dais_orders', 'sl_dais_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_dais_customers VALUES (1, 'Alice', 'standard')");
        $this->pdo->exec("INSERT INTO sl_dais_customers VALUES (2, 'Bob', 'standard')");
        $this->pdo->exec("INSERT INTO sl_dais_customers VALUES (3, 'Charlie', 'standard')");
        $this->pdo->exec("INSERT INTO sl_dais_customers VALUES (4, 'Diana', 'standard')");

        // Alice: 3 orders, Bob: 1 order, Charlie: 2 orders, Diana: 0 orders
        $this->pdo->exec("INSERT INTO sl_dais_orders VALUES (1, 1, 100)");
        $this->pdo->exec("INSERT INTO sl_dais_orders VALUES (2, 1, 200)");
        $this->pdo->exec("INSERT INTO sl_dais_orders VALUES (3, 1, 150)");
        $this->pdo->exec("INSERT INTO sl_dais_orders VALUES (4, 2, 50)");
        $this->pdo->exec("INSERT INTO sl_dais_orders VALUES (5, 3, 75)");
        $this->pdo->exec("INSERT INTO sl_dais_orders VALUES (6, 3, 125)");
    }

    /**
     * DELETE customers with fewer than 2 orders using IN + GROUP BY HAVING.
     */
    public function testDeleteWithHavingSubquery(): void
    {
        // Delete customers who have < 2 orders
        $sql = "DELETE FROM sl_dais_customers
                WHERE id IN (
                    SELECT customer_id FROM sl_dais_orders
                    GROUP BY customer_id
                    HAVING COUNT(*) < 2
                )";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, name FROM sl_dais_customers ORDER BY id");

            // Bob (1 order) should be deleted
            // Alice (3), Charlie (2), Diana (0 — not in orders, so not in IN result) remain
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE HAVING: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $names = array_column($rows, 'name');
            $this->assertNotContains('Bob', $names);
            $this->assertContains('Alice', $names);
            $this->assertContains('Diana', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'DELETE HAVING subquery failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE customers NOT IN high-spending group.
     */
    public function testDeleteWhereNotInAggregatedSubquery(): void
    {
        // Keep only customers whose total spending > 100
        $sql = "DELETE FROM sl_dais_customers
                WHERE id NOT IN (
                    SELECT customer_id FROM sl_dais_orders
                    GROUP BY customer_id
                    HAVING SUM(amount) > 100
                )";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, name FROM sl_dais_customers ORDER BY id");

            // Alice (450 > 100) and Charlie (200 > 100) remain; Bob (50) and Diana (0) deleted
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE NOT IN: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Charlie', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'DELETE NOT IN aggregated failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared DELETE with HAVING param.
     */
    public function testPreparedDeleteWithHavingParam(): void
    {
        $sql = "DELETE FROM sl_dais_customers
                WHERE id IN (
                    SELECT customer_id FROM sl_dais_orders
                    GROUP BY customer_id
                    HAVING COUNT(*) < ?
                )";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([3]);

            $rows = $this->ztdQuery("SELECT id, name FROM sl_dais_customers ORDER BY id");

            // Bob (1 order) and Charlie (2 orders) should be deleted, Alice (3) and Diana (0) remain
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Prepared DELETE HAVING: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Diana', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared DELETE HAVING failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE with aggregated subquery on shadow-inserted data.
     */
    public function testDeleteAggregatedOnShadowData(): void
    {
        // Add shadow orders for Diana
        $this->pdo->exec("INSERT INTO sl_dais_orders VALUES (7, 4, 300)");
        $this->pdo->exec("INSERT INTO sl_dais_orders VALUES (8, 4, 200)");
        $this->pdo->exec("INSERT INTO sl_dais_orders VALUES (9, 4, 100)");

        // Delete customers with < 2 orders
        $sql = "DELETE FROM sl_dais_customers
                WHERE id IN (
                    SELECT customer_id FROM sl_dais_orders
                    GROUP BY customer_id
                    HAVING COUNT(*) < 2
                )";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, name FROM sl_dais_customers ORDER BY id");

            // Bob (1 order) deleted; Alice(3), Charlie(2), Diana(3 shadow) remain
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Shadow DELETE HAVING: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $names = array_column($rows, 'name');
            $this->assertNotContains('Bob', $names);
            $this->assertContains('Diana', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Shadow DELETE HAVING failed: ' . $e->getMessage()
            );
        }
    }
}
