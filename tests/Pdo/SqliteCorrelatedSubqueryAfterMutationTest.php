<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests correlated subqueries in SELECT after mutations in shadow store.
 *
 * Correlated subqueries reference the outer query's row and must
 * correctly reflect shadow mutations (INSERT/UPDATE/DELETE).
 * @spec SPEC-3.3
 */
class SqliteCorrelatedSubqueryAfterMutationTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_corr_orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL)',
            'CREATE TABLE sl_corr_customers (id INTEGER PRIMARY KEY, name TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_corr_orders', 'sl_corr_customers'];
    }



    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_corr_customers VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO sl_corr_customers VALUES (2, 'Bob')");
        $this->pdo->exec("INSERT INTO sl_corr_customers VALUES (3, 'Charlie')");
        $this->pdo->exec("INSERT INTO sl_corr_orders VALUES (1, 1, 100.00)");
        $this->pdo->exec("INSERT INTO sl_corr_orders VALUES (2, 1, 200.00)");
        $this->pdo->exec("INSERT INTO sl_corr_orders VALUES (3, 2, 150.00)");
    }
    /**
     * Scalar correlated subquery in SELECT list.
     */
    public function testScalarCorrelatedSubqueryInSelect(): void
    {
        $stmt = $this->pdo->query('
            SELECT c.name,
                   (SELECT SUM(o.amount) FROM sl_corr_orders o WHERE o.customer_id = c.id) AS total
            FROM sl_corr_customers c
            ORDER BY c.id
        ');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEqualsWithDelta(300.0, (float) $rows[0]['total'], 0.01);
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertEqualsWithDelta(150.0, (float) $rows[1]['total'], 0.01);
        // Charlie has no orders
        $this->assertSame('Charlie', $rows[2]['name']);
        $this->assertNull($rows[2]['total']);
    }

    /**
     * Correlated subquery reflects INSERT mutation.
     */
    public function testCorrelatedSubqueryReflectsInsert(): void
    {
        $this->pdo->exec("INSERT INTO sl_corr_orders VALUES (4, 3, 500.00)");

        $stmt = $this->pdo->query('
            SELECT c.name,
                   (SELECT SUM(o.amount) FROM sl_corr_orders o WHERE o.customer_id = c.id) AS total
            FROM sl_corr_customers c
            WHERE c.id = 3
        ');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Charlie', $row['name']);
        $this->assertEqualsWithDelta(500.0, (float) $row['total'], 0.01);
    }

    /**
     * Correlated subquery reflects UPDATE mutation.
     */
    public function testCorrelatedSubqueryReflectsUpdate(): void
    {
        $this->pdo->exec('UPDATE sl_corr_orders SET amount = 999.00 WHERE id = 1');

        $stmt = $this->pdo->query('
            SELECT c.name,
                   (SELECT SUM(o.amount) FROM sl_corr_orders o WHERE o.customer_id = c.id) AS total
            FROM sl_corr_customers c
            WHERE c.id = 1
        ');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(1199.0, (float) $row['total'], 0.01); // 999 + 200
    }

    /**
     * Correlated subquery reflects DELETE mutation.
     */
    public function testCorrelatedSubqueryReflectsDelete(): void
    {
        $this->pdo->exec('DELETE FROM sl_corr_orders WHERE id = 2');

        $stmt = $this->pdo->query('
            SELECT c.name,
                   (SELECT SUM(o.amount) FROM sl_corr_orders o WHERE o.customer_id = c.id) AS total
            FROM sl_corr_customers c
            WHERE c.id = 1
        ');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(100.0, (float) $row['total'], 0.01); // only order 1 left
    }

    /**
     * EXISTS correlated subquery after mutation.
     */
    public function testExistsCorrelatedSubqueryAfterMutation(): void
    {
        $this->pdo->exec("INSERT INTO sl_corr_orders VALUES (4, 3, 50.00)");

        $stmt = $this->pdo->query('
            SELECT c.name
            FROM sl_corr_customers c
            WHERE EXISTS (SELECT 1 FROM sl_corr_orders o WHERE o.customer_id = c.id)
            ORDER BY c.name
        ');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(3, $rows); // All 3 now have orders
        $this->assertSame('Alice', $rows[0]);
        $this->assertSame('Bob', $rows[1]);
        $this->assertSame('Charlie', $rows[2]);
    }

    /**
     * NOT EXISTS after DELETE removes all orders for a customer.
     */
    public function testNotExistsAfterDeleteAllOrders(): void
    {
        $this->pdo->exec('DELETE FROM sl_corr_orders WHERE customer_id = 2');

        $stmt = $this->pdo->query('
            SELECT c.name
            FROM sl_corr_customers c
            WHERE NOT EXISTS (SELECT 1 FROM sl_corr_orders o WHERE o.customer_id = c.id)
            ORDER BY c.name
        ');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertContains('Bob', $rows);
        $this->assertContains('Charlie', $rows);
        $this->assertNotContains('Alice', $rows);
    }

    /**
     * Correlated subquery with COUNT in SELECT.
     */
    public function testCorrelatedSubqueryWithCount(): void
    {
        $stmt = $this->pdo->query('
            SELECT c.name,
                   (SELECT COUNT(*) FROM sl_corr_orders o WHERE o.customer_id = c.id) AS order_count
            FROM sl_corr_customers c
            ORDER BY c.id
        ');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(2, (int) $rows[0]['order_count']); // Alice
        $this->assertSame(1, (int) $rows[1]['order_count']); // Bob
        $this->assertSame(0, (int) $rows[2]['order_count']); // Charlie
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_corr_orders');
        $this->assertSame(0, (int) $stmt->fetchColumn());
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_corr_customers');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
