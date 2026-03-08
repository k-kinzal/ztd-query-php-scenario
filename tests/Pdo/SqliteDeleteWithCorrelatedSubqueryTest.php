<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests DELETE with correlated subqueries in WHERE clause on SQLite.
 *
 * DELETE ... WHERE EXISTS (SELECT ... WHERE outer.col = inner.col)
 * is a common pattern for removing orphaned or conditionally matched rows.
 * @spec pending
 */
class SqliteDeleteWithCorrelatedSubqueryTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_del_customers (id INTEGER PRIMARY KEY, name TEXT, active INTEGER)',
            'CREATE TABLE sl_del_orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_del_customers', 'sl_del_orders'];
    }


    /**
     * DELETE with EXISTS correlated subquery.
     */
    public function testDeleteWithExistsSubquery(): void
    {
        // Delete customers who have orders
        $this->pdo->exec('
            DELETE FROM sl_del_customers
            WHERE EXISTS (SELECT 1 FROM sl_del_orders o WHERE o.customer_id = sl_del_customers.id)
        ');

        $stmt = $this->pdo->query('SELECT name FROM sl_del_customers ORDER BY name');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        // Alice and Charlie have orders — should be deleted
        // Bob and Diana have no orders — should remain
        $this->assertCount(2, $rows);
        $this->assertSame('Bob', $rows[0]);
        $this->assertSame('Diana', $rows[1]);
    }

    /**
     * DELETE with NOT EXISTS correlated subquery (delete orphans).
     */
    public function testDeleteWithNotExistsSubquery(): void
    {
        // Delete customers who have NO orders (orphans)
        $this->pdo->exec('
            DELETE FROM sl_del_customers
            WHERE NOT EXISTS (SELECT 1 FROM sl_del_orders o WHERE o.customer_id = sl_del_customers.id)
        ');

        $stmt = $this->pdo->query('SELECT name FROM sl_del_customers ORDER BY name');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        // Bob and Diana have no orders — should be deleted
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]);
        $this->assertSame('Charlie', $rows[1]);
    }

    /**
     * DELETE with IN subquery.
     */
    public function testDeleteWithInSubquery(): void
    {
        $this->pdo->exec('
            DELETE FROM sl_del_orders
            WHERE customer_id IN (SELECT id FROM sl_del_customers WHERE active = 0)
        ');

        // No orders belong to inactive customers (Bob=2, Diana=4), so count unchanged
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_del_orders');
        $this->assertSame(3, (int) $stmt->fetchColumn());
    }

    /**
     * DELETE with scalar subquery comparison.
     */
    public function testDeleteWithScalarSubquery(): void
    {
        $this->pdo->exec('
            DELETE FROM sl_del_orders
            WHERE amount > (SELECT AVG(amount) FROM sl_del_orders)
        ');

        // AVG = (100+200+150)/3 = 150. Only amount=200 > 150.
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_del_orders');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * DELETE followed by correlated SELECT to verify consistency.
     */
    public function testDeleteThenCorrelatedSelect(): void
    {
        $this->pdo->exec('DELETE FROM sl_del_orders WHERE customer_id = 1');

        $stmt = $this->pdo->query('
            SELECT c.name,
                   (SELECT COUNT(*) FROM sl_del_orders o WHERE o.customer_id = c.id) AS order_count
            FROM sl_del_customers c
            ORDER BY c.id
        ');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['order_count']); // Alice — deleted
        $this->assertSame(0, (int) $rows[1]['order_count']); // Bob
        $this->assertSame(1, (int) $rows[2]['order_count']); // Charlie
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec('
            DELETE FROM sl_del_customers
            WHERE EXISTS (SELECT 1 FROM sl_del_orders o WHERE o.customer_id = sl_del_customers.id)
        ');

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_del_customers');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
