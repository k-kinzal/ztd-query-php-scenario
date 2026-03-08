<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests JOIN + aggregate queries after shadow mutations on SQLite.
 *
 * Validates that multi-table JOINs with GROUP BY and aggregate functions
 * correctly reflect INSERT, UPDATE, and DELETE mutations in shadow state.
 * @spec pending
 */
class SqliteJoinAggregateAfterMutationTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_jag_customers (id INTEGER PRIMARY KEY, name TEXT)',
            'CREATE TABLE sl_jag_orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_jag_customers', 'sl_jag_orders'];
    }


    /**
     * LEFT JOIN with COUNT after INSERT.
     */
    public function testLeftJoinCountAfterInsert(): void
    {
        $this->pdo->exec('INSERT INTO sl_jag_orders VALUES (4, 3, 75.00)');

        $stmt = $this->pdo->query('
            SELECT c.name, COUNT(o.id) AS order_count
            FROM sl_jag_customers c
            LEFT JOIN sl_jag_orders o ON c.id = o.customer_id
            GROUP BY c.id, c.name
            ORDER BY c.id
        ');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(2, (int) $rows[0]['order_count']); // Alice: 2
        $this->assertSame(1, (int) $rows[1]['order_count']); // Bob: 1
        $this->assertSame(1, (int) $rows[2]['order_count']); // Charlie: 1 (new)
    }

    /**
     * SUM aggregate after UPDATE.
     */
    public function testSumAfterUpdate(): void
    {
        $this->pdo->exec('UPDATE sl_jag_orders SET amount = 500.00 WHERE id = 1');

        $stmt = $this->pdo->query('
            SELECT c.name, SUM(o.amount) AS total
            FROM sl_jag_customers c
            JOIN sl_jag_orders o ON c.id = o.customer_id
            GROUP BY c.id, c.name
            ORDER BY c.id
        ');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(700.00, (float) $rows[0]['total'], 0.01); // 500 + 200
    }

    /**
     * LEFT JOIN after DELETE shows zero count.
     */
    public function testLeftJoinAfterDeleteAllOrders(): void
    {
        $this->pdo->exec('DELETE FROM sl_jag_orders WHERE customer_id = 1');

        $stmt = $this->pdo->query('
            SELECT c.name, COUNT(o.id) AS order_count
            FROM sl_jag_customers c
            LEFT JOIN sl_jag_orders o ON c.id = o.customer_id
            GROUP BY c.id, c.name
            ORDER BY c.id
        ');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['order_count']); // Alice: 0 (deleted)
        $this->assertSame(1, (int) $rows[1]['order_count']); // Bob: 1
    }

    /**
     * HAVING filter after mutation.
     */
    public function testHavingAfterInsert(): void
    {
        $this->pdo->exec('INSERT INTO sl_jag_orders VALUES (4, 2, 150.00)');

        $stmt = $this->pdo->query('
            SELECT c.name, SUM(o.amount) AS total
            FROM sl_jag_customers c
            JOIN sl_jag_orders o ON c.id = o.customer_id
            GROUP BY c.id, c.name
            HAVING SUM(o.amount) >= 200
            ORDER BY total DESC
        ');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows); // Alice: 300, Bob: 200
        $this->assertEqualsWithDelta(300.00, (float) $rows[0]['total'], 0.01);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_jag_customers');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
