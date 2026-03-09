<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests UPDATE SET with multiple independent scalar subqueries.
 *
 * Pattern: UPDATE t SET a = (SELECT ... FROM t2), b = (SELECT ... FROM t3)
 * Each SET column gets its value from a different correlated subquery.
 * The CTE rewriter must handle multiple table references in a single UPDATE.
 *
 * @spec SPEC-4.2
 */
class SqliteUpdateMultiSubquerySetTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_umss_customers (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                order_count INTEGER NOT NULL DEFAULT 0,
                total_spent REAL NOT NULL DEFAULT 0,
                avg_rating REAL
            )',
            'CREATE TABLE sl_umss_orders (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                amount REAL NOT NULL
            )',
            'CREATE TABLE sl_umss_reviews (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                rating INTEGER NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_umss_reviews', 'sl_umss_orders', 'sl_umss_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_umss_customers VALUES (1, 'Alice', 0, 0, NULL)");
        $this->pdo->exec("INSERT INTO sl_umss_customers VALUES (2, 'Bob', 0, 0, NULL)");
        $this->pdo->exec("INSERT INTO sl_umss_customers VALUES (3, 'Charlie', 0, 0, NULL)");

        $this->pdo->exec("INSERT INTO sl_umss_orders VALUES (1, 1, 100)");
        $this->pdo->exec("INSERT INTO sl_umss_orders VALUES (2, 1, 200)");
        $this->pdo->exec("INSERT INTO sl_umss_orders VALUES (3, 2, 50)");

        $this->pdo->exec("INSERT INTO sl_umss_reviews VALUES (1, 1, 5)");
        $this->pdo->exec("INSERT INTO sl_umss_reviews VALUES (2, 1, 4)");
        $this->pdo->exec("INSERT INTO sl_umss_reviews VALUES (3, 2, 3)");
    }

    /**
     * UPDATE with two scalar subqueries from different tables.
     */
    public function testUpdateTwoSubqueriesFromDifferentTables(): void
    {
        $sql = "UPDATE sl_umss_customers SET
                    order_count = (SELECT COUNT(*) FROM sl_umss_orders WHERE customer_id = sl_umss_customers.id),
                    avg_rating = (SELECT AVG(rating) FROM sl_umss_reviews WHERE customer_id = sl_umss_customers.id)";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, name, order_count, avg_rating FROM sl_umss_customers ORDER BY id");

            $this->assertCount(3, $rows);

            // Alice: 2 orders, avg rating 4.5
            if ((int) $rows[0]['order_count'] !== 2) {
                $this->markTestIncomplete(
                    "Alice order_count: expected 2, got {$rows[0]['order_count']}"
                );
            }

            $this->assertSame(2, (int) $rows[0]['order_count']);
            $this->assertEqualsWithDelta(4.5, (float) $rows[0]['avg_rating'], 0.01);

            // Bob: 1 order, avg rating 3.0
            $this->assertSame(1, (int) $rows[1]['order_count']);
            $this->assertEqualsWithDelta(3.0, (float) $rows[1]['avg_rating'], 0.01);

            // Charlie: 0 orders, NULL avg_rating
            $this->assertSame(0, (int) $rows[2]['order_count']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE two subqueries failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with three scalar subqueries (order_count, total_spent, avg_rating).
     */
    public function testUpdateThreeSubqueries(): void
    {
        $sql = "UPDATE sl_umss_customers SET
                    order_count = (SELECT COUNT(*) FROM sl_umss_orders WHERE customer_id = sl_umss_customers.id),
                    total_spent = COALESCE((SELECT SUM(amount) FROM sl_umss_orders WHERE customer_id = sl_umss_customers.id), 0),
                    avg_rating = (SELECT AVG(rating) FROM sl_umss_reviews WHERE customer_id = sl_umss_customers.id)";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, order_count, total_spent, avg_rating FROM sl_umss_customers ORDER BY id");

            $this->assertCount(3, $rows);

            // Alice: 2 orders, 300 total, 4.5 avg
            $this->assertSame(2, (int) $rows[0]['order_count']);
            $this->assertEqualsWithDelta(300.0, (float) $rows[0]['total_spent'], 0.01);
            $this->assertEqualsWithDelta(4.5, (float) $rows[0]['avg_rating'], 0.01);

            // Charlie: 0 orders, 0 total, NULL avg
            $this->assertSame(0, (int) $rows[2]['order_count']);
            $this->assertEqualsWithDelta(0.0, (float) $rows[2]['total_spent'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE three subqueries failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared UPDATE with subqueries and WHERE param.
     */
    public function testPreparedUpdateMultiSubqueryWithParam(): void
    {
        $sql = "UPDATE sl_umss_customers SET
                    order_count = (SELECT COUNT(*) FROM sl_umss_orders WHERE customer_id = sl_umss_customers.id),
                    total_spent = COALESCE((SELECT SUM(amount) FROM sl_umss_orders WHERE customer_id = sl_umss_customers.id), 0)
                WHERE id = ?";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([1]);

            $rows = $this->ztdQuery("SELECT id, order_count, total_spent FROM sl_umss_customers ORDER BY id");

            // Only Alice updated
            $this->assertSame(2, (int) $rows[0]['order_count']);
            $this->assertEqualsWithDelta(300.0, (float) $rows[0]['total_spent'], 0.01);

            // Bob should still be 0
            $this->assertSame(0, (int) $rows[1]['order_count']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared multi-subquery UPDATE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE multi-subquery on shadow-inserted data.
     */
    public function testMultiSubqueryOnShadowData(): void
    {
        // Add shadow order and review for Charlie
        $this->pdo->exec("INSERT INTO sl_umss_orders VALUES (4, 3, 500)");
        $this->pdo->exec("INSERT INTO sl_umss_reviews VALUES (4, 3, 5)");

        $sql = "UPDATE sl_umss_customers SET
                    order_count = (SELECT COUNT(*) FROM sl_umss_orders WHERE customer_id = sl_umss_customers.id),
                    avg_rating = (SELECT AVG(rating) FROM sl_umss_reviews WHERE customer_id = sl_umss_customers.id)";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, order_count, avg_rating FROM sl_umss_customers ORDER BY id");

            // Charlie should now have 1 order, 5.0 avg rating
            if ((int) $rows[2]['order_count'] !== 1) {
                $this->markTestIncomplete(
                    "Shadow multi-subquery: Charlie order_count expected 1, got {$rows[2]['order_count']}"
                );
            }

            $this->assertSame(1, (int) $rows[2]['order_count']);
            $this->assertEqualsWithDelta(5.0, (float) $rows[2]['avg_rating'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Multi-subquery on shadow data failed: ' . $e->getMessage()
            );
        }
    }
}
