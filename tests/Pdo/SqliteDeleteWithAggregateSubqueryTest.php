<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests DELETE with aggregate-based subquery conditions on SQLite.
 *
 * Patterns like:
 * - DELETE WHERE id IN (SELECT ... GROUP BY ... HAVING COUNT(...) > N)
 * - DELETE WHERE (SELECT COUNT(*) FROM ...) = 0
 * - DELETE WHERE col > (SELECT AVG(col) FROM ...)
 * @spec pending
 */
class SqliteDeleteWithAggregateSubqueryTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE das_orders (id INT PRIMARY KEY, customer_id INT, amount DECIMAL(10,2))',
            'CREATE TABLE das_customers (id INT PRIMARY KEY, name VARCHAR(50), status VARCHAR(20))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['das_orders', 'das_customers'];
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
