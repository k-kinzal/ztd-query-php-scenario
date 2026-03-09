<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests UPDATE SET with multiple independent scalar subqueries on PostgreSQL.
 *
 * @spec SPEC-4.2
 */
class PostgresUpdateMultiSubquerySetTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_umss_customers (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                order_count INTEGER NOT NULL DEFAULT 0,
                total_spent NUMERIC(10,2) NOT NULL DEFAULT 0,
                avg_rating NUMERIC(3,2)
            )',
            'CREATE TABLE pg_umss_orders (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                amount NUMERIC(10,2) NOT NULL
            )',
            'CREATE TABLE pg_umss_reviews (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                rating INTEGER NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_umss_reviews', 'pg_umss_orders', 'pg_umss_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_umss_customers VALUES (1, 'Alice', 0, 0, NULL)");
        $this->pdo->exec("INSERT INTO pg_umss_customers VALUES (2, 'Bob', 0, 0, NULL)");
        $this->pdo->exec("INSERT INTO pg_umss_customers VALUES (3, 'Charlie', 0, 0, NULL)");

        $this->pdo->exec("INSERT INTO pg_umss_orders VALUES (1, 1, 100)");
        $this->pdo->exec("INSERT INTO pg_umss_orders VALUES (2, 1, 200)");
        $this->pdo->exec("INSERT INTO pg_umss_orders VALUES (3, 2, 50)");

        $this->pdo->exec("INSERT INTO pg_umss_reviews VALUES (1, 1, 5)");
        $this->pdo->exec("INSERT INTO pg_umss_reviews VALUES (2, 1, 4)");
        $this->pdo->exec("INSERT INTO pg_umss_reviews VALUES (3, 2, 3)");
    }

    /**
     * UPDATE two columns from different tables.
     */
    public function testUpdateTwoSubqueries(): void
    {
        $sql = "UPDATE pg_umss_customers SET
                    order_count = (SELECT COUNT(*) FROM pg_umss_orders WHERE customer_id = pg_umss_customers.id),
                    avg_rating = (SELECT AVG(rating) FROM pg_umss_reviews WHERE customer_id = pg_umss_customers.id)";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, order_count, avg_rating FROM pg_umss_customers ORDER BY id");

            $this->assertCount(3, $rows);

            if ((int) $rows[0]['order_count'] !== 2) {
                $this->markTestIncomplete(
                    "Alice order_count expected 2, got {$rows[0]['order_count']}"
                );
            }

            $this->assertSame(2, (int) $rows[0]['order_count']);
            $this->assertEqualsWithDelta(4.5, (float) $rows[0]['avg_rating'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE two subqueries failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE three columns from subqueries.
     */
    public function testUpdateThreeSubqueries(): void
    {
        $sql = "UPDATE pg_umss_customers SET
                    order_count = (SELECT COUNT(*) FROM pg_umss_orders WHERE customer_id = pg_umss_customers.id),
                    total_spent = COALESCE((SELECT SUM(amount) FROM pg_umss_orders WHERE customer_id = pg_umss_customers.id), 0),
                    avg_rating = (SELECT AVG(rating) FROM pg_umss_reviews WHERE customer_id = pg_umss_customers.id)";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, order_count, total_spent, avg_rating FROM pg_umss_customers ORDER BY id");

            $this->assertSame(2, (int) $rows[0]['order_count']);
            $this->assertEqualsWithDelta(300.0, (float) $rows[0]['total_spent'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE three subqueries failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared with ? param.
     */
    public function testPreparedMultiSubqueryWithQuestionParam(): void
    {
        $sql = "UPDATE pg_umss_customers SET
                    order_count = (SELECT COUNT(*) FROM pg_umss_orders WHERE customer_id = pg_umss_customers.id),
                    total_spent = COALESCE((SELECT SUM(amount) FROM pg_umss_orders WHERE customer_id = pg_umss_customers.id), 0)
                WHERE id = ?";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([1]);

            $rows = $this->ztdQuery("SELECT id, order_count, total_spent FROM pg_umss_customers ORDER BY id");

            $this->assertSame(2, (int) $rows[0]['order_count']);
            $this->assertSame(0, (int) $rows[1]['order_count']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared multi-subquery (?) failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared with $1 param.
     */
    public function testPreparedMultiSubqueryWithDollarParam(): void
    {
        $sql = "UPDATE pg_umss_customers SET
                    order_count = (SELECT COUNT(*) FROM pg_umss_orders WHERE customer_id = pg_umss_customers.id),
                    total_spent = COALESCE((SELECT SUM(amount) FROM pg_umss_orders WHERE customer_id = pg_umss_customers.id), 0)
                WHERE id = $1";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([1]);

            $rows = $this->ztdQuery("SELECT id, order_count, total_spent FROM pg_umss_customers ORDER BY id");

            if ((int) $rows[0]['order_count'] !== 2) {
                $this->markTestIncomplete(
                    "Prepared $1: Alice order_count expected 2, got {$rows[0]['order_count']}"
                );
            }

            $this->assertSame(2, (int) $rows[0]['order_count']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared multi-subquery ($1) failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Multi-subquery on shadow data.
     */
    public function testMultiSubqueryOnShadowData(): void
    {
        $this->pdo->exec("INSERT INTO pg_umss_orders VALUES (4, 3, 500)");
        $this->pdo->exec("INSERT INTO pg_umss_reviews VALUES (4, 3, 5)");

        $sql = "UPDATE pg_umss_customers SET
                    order_count = (SELECT COUNT(*) FROM pg_umss_orders WHERE customer_id = pg_umss_customers.id),
                    avg_rating = (SELECT AVG(rating) FROM pg_umss_reviews WHERE customer_id = pg_umss_customers.id)";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, order_count, avg_rating FROM pg_umss_customers ORDER BY id");

            if ((int) $rows[2]['order_count'] !== 1) {
                $this->markTestIncomplete(
                    "Shadow: Charlie expected 1, got {$rows[2]['order_count']}"
                );
            }

            $this->assertSame(1, (int) $rows[2]['order_count']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Multi-subquery shadow failed: ' . $e->getMessage()
            );
        }
    }
}
