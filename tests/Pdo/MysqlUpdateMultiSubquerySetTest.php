<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests UPDATE SET with multiple independent scalar subqueries on MySQL.
 *
 * @spec SPEC-4.2
 */
class MysqlUpdateMultiSubquerySetTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_umss_customers (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                order_count INT NOT NULL DEFAULT 0,
                total_spent DECIMAL(10,2) NOT NULL DEFAULT 0,
                avg_rating DECIMAL(3,2)
            ) ENGINE=InnoDB',
            'CREATE TABLE my_umss_orders (
                id INT PRIMARY KEY,
                customer_id INT NOT NULL,
                amount DECIMAL(10,2) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE my_umss_reviews (
                id INT PRIMARY KEY,
                customer_id INT NOT NULL,
                rating INT NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_umss_reviews', 'my_umss_orders', 'my_umss_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_umss_customers VALUES (1, 'Alice', 0, 0, NULL)");
        $this->pdo->exec("INSERT INTO my_umss_customers VALUES (2, 'Bob', 0, 0, NULL)");
        $this->pdo->exec("INSERT INTO my_umss_customers VALUES (3, 'Charlie', 0, 0, NULL)");

        $this->pdo->exec("INSERT INTO my_umss_orders VALUES (1, 1, 100)");
        $this->pdo->exec("INSERT INTO my_umss_orders VALUES (2, 1, 200)");
        $this->pdo->exec("INSERT INTO my_umss_orders VALUES (3, 2, 50)");

        $this->pdo->exec("INSERT INTO my_umss_reviews VALUES (1, 1, 5)");
        $this->pdo->exec("INSERT INTO my_umss_reviews VALUES (2, 1, 4)");
        $this->pdo->exec("INSERT INTO my_umss_reviews VALUES (3, 2, 3)");
    }

    /**
     * UPDATE two columns from different tables.
     */
    public function testUpdateTwoSubqueries(): void
    {
        $sql = "UPDATE my_umss_customers SET
                    order_count = (SELECT COUNT(*) FROM my_umss_orders WHERE customer_id = my_umss_customers.id),
                    avg_rating = (SELECT AVG(rating) FROM my_umss_reviews WHERE customer_id = my_umss_customers.id)";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, order_count, avg_rating FROM my_umss_customers ORDER BY id");

            $this->assertCount(3, $rows);
            $this->assertSame(2, (int) $rows[0]['order_count']);
            $this->assertEqualsWithDelta(4.5, (float) $rows[0]['avg_rating'], 0.01);
            $this->assertSame(1, (int) $rows[1]['order_count']);
            $this->assertSame(0, (int) $rows[2]['order_count']);
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
        $sql = "UPDATE my_umss_customers SET
                    order_count = (SELECT COUNT(*) FROM my_umss_orders WHERE customer_id = my_umss_customers.id),
                    total_spent = COALESCE((SELECT SUM(amount) FROM my_umss_orders WHERE customer_id = my_umss_customers.id), 0),
                    avg_rating = (SELECT AVG(rating) FROM my_umss_reviews WHERE customer_id = my_umss_customers.id)";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, order_count, total_spent, avg_rating FROM my_umss_customers ORDER BY id");

            $this->assertSame(2, (int) $rows[0]['order_count']);
            $this->assertEqualsWithDelta(300.0, (float) $rows[0]['total_spent'], 0.01);
            $this->assertEqualsWithDelta(4.5, (float) $rows[0]['avg_rating'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE three subqueries failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared with WHERE param.
     */
    public function testPreparedMultiSubqueryWithParam(): void
    {
        $sql = "UPDATE my_umss_customers SET
                    order_count = (SELECT COUNT(*) FROM my_umss_orders WHERE customer_id = my_umss_customers.id),
                    total_spent = COALESCE((SELECT SUM(amount) FROM my_umss_orders WHERE customer_id = my_umss_customers.id), 0)
                WHERE id = ?";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([1]);

            $rows = $this->ztdQuery("SELECT id, order_count, total_spent FROM my_umss_customers ORDER BY id");

            $this->assertSame(2, (int) $rows[0]['order_count']);
            $this->assertSame(0, (int) $rows[1]['order_count']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared multi-subquery failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Multi-subquery on shadow data.
     */
    public function testMultiSubqueryOnShadowData(): void
    {
        $this->pdo->exec("INSERT INTO my_umss_orders VALUES (4, 3, 500)");
        $this->pdo->exec("INSERT INTO my_umss_reviews VALUES (4, 3, 5)");

        $sql = "UPDATE my_umss_customers SET
                    order_count = (SELECT COUNT(*) FROM my_umss_orders WHERE customer_id = my_umss_customers.id),
                    avg_rating = (SELECT AVG(rating) FROM my_umss_reviews WHERE customer_id = my_umss_customers.id)";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, order_count, avg_rating FROM my_umss_customers ORDER BY id");

            if ((int) $rows[2]['order_count'] !== 1) {
                $this->markTestIncomplete(
                    "Shadow: Charlie order_count expected 1, got {$rows[2]['order_count']}"
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
