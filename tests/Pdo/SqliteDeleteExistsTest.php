<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests DELETE with EXISTS/NOT EXISTS subqueries through ZTD shadow store.
 *
 * These are common patterns for conditional deletion that require the CTE
 * rewriter to correctly handle subqueries inside DELETE WHERE clauses.
 * @spec SPEC-4.7
 */
class SqliteDeleteExistsTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE de_orders (id INT PRIMARY KEY, customer_id INT, status VARCHAR(20), total INT)',
            'CREATE TABLE de_customers (id INT PRIMARY KEY, name VARCHAR(50), active INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['de_orders', 'de_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO de_customers VALUES (1, 'Alice', 1)");
        $this->pdo->exec("INSERT INTO de_customers VALUES (2, 'Bob', 0)");
        $this->pdo->exec("INSERT INTO de_customers VALUES (3, 'Charlie', 1)");

        $this->pdo->exec("INSERT INTO de_orders VALUES (1, 1, 'shipped', 100)");
        $this->pdo->exec("INSERT INTO de_orders VALUES (2, 1, 'pending', 200)");
        $this->pdo->exec("INSERT INTO de_orders VALUES (3, 2, 'pending', 50)");
        $this->pdo->exec("INSERT INTO de_orders VALUES (4, 3, 'shipped', 300)");
    }

    /**
     * DELETE with EXISTS: remove orders for inactive customers.
     */
    public function testDeleteWhereExists(): void
    {
        $this->pdo->exec(
            'DELETE FROM de_orders
             WHERE EXISTS (
                 SELECT 1 FROM de_customers c
                 WHERE c.id = de_orders.customer_id AND c.active = 0
             )'
        );

        $rows = $this->ztdQuery('SELECT * FROM de_orders ORDER BY id');
        // Bob (inactive) had order #3, should be deleted
        $this->assertCount(3, $rows);
        $ids = array_map('intval', array_column($rows, 'id'));
        $this->assertContains(1, $ids);
        $this->assertContains(2, $ids);
        $this->assertContains(4, $ids);
    }

    /**
     * DELETE with NOT EXISTS: remove customers with no orders.
     */
    public function testDeleteWhereNotExists(): void
    {
        // Remove customer 3's order first
        $this->pdo->exec('DELETE FROM de_orders WHERE customer_id = 3');

        // Now delete customers with no orders
        $this->pdo->exec(
            'DELETE FROM de_customers
             WHERE NOT EXISTS (
                 SELECT 1 FROM de_orders o WHERE o.customer_id = de_customers.id
             )'
        );

        $rows = $this->ztdQuery('SELECT * FROM de_customers ORDER BY id');
        // Charlie has no orders, should be deleted. Alice and Bob have orders.
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
    }

    /**
     * DELETE with IN subquery: delete orders matching a condition from another table.
     */
    public function testDeleteWhereInSubquery(): void
    {
        $this->pdo->exec(
            'DELETE FROM de_orders
             WHERE customer_id IN (SELECT id FROM de_customers WHERE active = 0)'
        );

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM de_orders');
        $this->assertSame('3', (string) $rows[0]['cnt']);
    }

    /**
     * DELETE with NOT IN subquery.
     */
    public function testDeleteWhereNotInSubquery(): void
    {
        $this->pdo->exec(
            'DELETE FROM de_orders
             WHERE customer_id NOT IN (SELECT id FROM de_customers WHERE active = 1)'
        );

        $rows = $this->ztdQuery('SELECT * FROM de_orders ORDER BY id');
        // Should keep orders for active customers (Alice=1, Charlie=3)
        $this->assertCount(3, $rows);
    }

    /**
     * DELETE with EXISTS after prior mutation.
     */
    public function testDeleteExistsAfterMutation(): void
    {
        // Deactivate Alice
        $this->pdo->exec('UPDATE de_customers SET active = 0 WHERE id = 1');

        // Delete orders for inactive customers
        $this->pdo->exec(
            'DELETE FROM de_orders
             WHERE EXISTS (
                 SELECT 1 FROM de_customers c
                 WHERE c.id = de_orders.customer_id AND c.active = 0
             )'
        );

        $rows = $this->ztdQuery('SELECT * FROM de_orders ORDER BY id');
        // Both Alice (now inactive) and Bob (inactive) orders should be gone
        // Only Charlie's order (#4) remains
        $this->assertCount(1, $rows);
        $this->assertSame('4', (string) $rows[0]['id']);
    }

    /**
     * Chained DELETE: delete orders first, then orphaned customers.
     */
    public function testChainedDeleteWithExistsCheck(): void
    {
        // Delete all pending orders
        $this->pdo->exec("DELETE FROM de_orders WHERE status = 'pending'");

        // Verify
        $orders = $this->ztdQuery('SELECT * FROM de_orders ORDER BY id');
        $this->assertCount(2, $orders); // Only shipped orders remain

        // Delete customers who have no remaining orders
        $this->pdo->exec(
            'DELETE FROM de_customers
             WHERE NOT EXISTS (SELECT 1 FROM de_orders o WHERE o.customer_id = de_customers.id)'
        );

        $customers = $this->ztdQuery('SELECT * FROM de_customers ORDER BY id');
        // Bob had only a pending order, now has none → deleted
        $this->assertCount(2, $customers);
        $this->assertSame('Alice', $customers[0]['name']);
        $this->assertSame('Charlie', $customers[1]['name']);
    }

    /**
     * Physical table isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM de_orders');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
