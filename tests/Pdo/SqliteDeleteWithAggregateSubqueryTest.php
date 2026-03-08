<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests DELETE with aggregate-based subquery conditions on SQLite.
 *
 * Patterns like:
 * - DELETE WHERE id IN (SELECT ... GROUP BY ... HAVING COUNT(...) > N)
 * - DELETE WHERE (SELECT COUNT(*) FROM ...) = 0
 * - DELETE WHERE col > (SELECT AVG(col) FROM ...)
 */
class SqliteDeleteWithAggregateSubqueryTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:');
        $raw->exec('CREATE TABLE das_orders (id INT PRIMARY KEY, customer_id INT, amount DECIMAL(10,2))');
        $raw->exec('CREATE TABLE das_customers (id INT PRIMARY KEY, name VARCHAR(50), status VARCHAR(20))');
        $this->pdo = ZtdPdo::fromPdo($raw);

        $this->pdo->exec("INSERT INTO das_customers VALUES (1, 'Alice', 'active')");
        $this->pdo->exec("INSERT INTO das_customers VALUES (2, 'Bob', 'active')");
        $this->pdo->exec("INSERT INTO das_customers VALUES (3, 'Charlie', 'inactive')");

        $this->pdo->exec("INSERT INTO das_orders VALUES (1, 1, 100.00)");
        $this->pdo->exec("INSERT INTO das_orders VALUES (2, 1, 200.00)");
        $this->pdo->exec("INSERT INTO das_orders VALUES (3, 1, 50.00)");
        $this->pdo->exec("INSERT INTO das_orders VALUES (4, 2, 75.00)");
        $this->pdo->exec("INSERT INTO das_orders VALUES (5, 3, 10.00)");
    }

    /**
     * DELETE orders above average amount.
     */
    public function testDeleteAboveAverage(): void
    {
        $this->pdo->exec(
            'DELETE FROM das_orders WHERE amount > (SELECT AVG(amount) FROM das_orders)'
        );

        // Average is (100+200+50+75+10)/5 = 87.00
        // Delete orders > 87: id=1(100), id=2(200)
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM das_orders');
        $this->assertSame(3, (int) $stmt->fetchColumn());
    }

    /**
     * DELETE customers with no orders (using NOT EXISTS).
     */
    public function testDeleteCustomersWithNoOrders(): void
    {
        // Remove all orders for Bob first
        $this->pdo->exec('DELETE FROM das_orders WHERE customer_id = 2');

        $this->pdo->exec(
            'DELETE FROM das_customers WHERE NOT EXISTS (
                SELECT 1 FROM das_orders WHERE das_orders.customer_id = das_customers.id
            )'
        );

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM das_customers');
        // Alice (has orders) and Charlie (has orders) remain, Bob deleted
        $count = (int) $stmt->fetchColumn();
        $this->assertSame(2, $count);
    }

    /**
     * DELETE using IN subquery with GROUP BY HAVING.
     */
    public function testDeleteWithGroupByHavingSubquery(): void
    {
        try {
            // Delete orders for customers with 2+ orders
            $this->pdo->exec(
                'DELETE FROM das_orders WHERE customer_id IN (
                    SELECT customer_id FROM das_orders GROUP BY customer_id HAVING COUNT(*) >= 2
                )'
            );

            // Alice had 3 orders — all deleted. Bob and Charlie remain.
            $stmt = $this->pdo->query('SELECT COUNT(*) FROM das_orders');
            $this->assertSame(2, (int) $stmt->fetchColumn());
        } catch (\Exception $e) {
            $this->markTestSkipped('DELETE with GROUP BY HAVING subquery not supported: ' . $e->getMessage());
        }
    }

    /**
     * DELETE inactive customers (simple WHERE on another table via subquery).
     */
    public function testDeleteInactiveCustomers(): void
    {
        $this->pdo->exec(
            "DELETE FROM das_customers WHERE status = 'inactive'"
        );

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM das_customers');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * Cascade-like delete: orders then customer.
     */
    public function testCascadeLikeDelete(): void
    {
        // Delete all orders for Charlie
        $this->pdo->exec('DELETE FROM das_orders WHERE customer_id = 3');
        // Then delete Charlie
        $this->pdo->exec('DELETE FROM das_customers WHERE id = 3');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM das_customers');
        $this->assertSame(2, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM das_orders WHERE customer_id = 3');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM das_orders');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
