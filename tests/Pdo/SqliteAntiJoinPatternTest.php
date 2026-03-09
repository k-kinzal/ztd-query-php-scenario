<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests anti-join patterns through CTE shadow store.
 *
 * Anti-joins find rows in one table that have no matching rows in another.
 * Three common patterns: LEFT JOIN WHERE IS NULL, NOT EXISTS, NOT IN.
 * All three should produce identical results through ZTD.
 *
 * Also tests semi-join pattern (EXISTS) for completeness.
 *
 * @spec SPEC-3.3
 */
class SqliteAntiJoinPatternTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_aj_customers (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL
            )',
            'CREATE TABLE sl_aj_orders (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                total REAL NOT NULL,
                order_date TEXT NOT NULL
            )',
            'CREATE TABLE sl_aj_returns (
                id INTEGER PRIMARY KEY,
                order_id INTEGER NOT NULL,
                reason TEXT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_aj_returns', 'sl_aj_orders', 'sl_aj_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_aj_customers VALUES (1, 'Alice', 'alice@example.com')");
        $this->pdo->exec("INSERT INTO sl_aj_customers VALUES (2, 'Bob', 'bob@example.com')");
        $this->pdo->exec("INSERT INTO sl_aj_customers VALUES (3, 'Carol', 'carol@example.com')");
        $this->pdo->exec("INSERT INTO sl_aj_customers VALUES (4, 'Dave', 'dave@example.com')");
        $this->pdo->exec("INSERT INTO sl_aj_customers VALUES (5, 'Eve', 'eve@example.com')");

        // Alice and Bob have orders; Carol, Dave, Eve do not
        $this->pdo->exec("INSERT INTO sl_aj_orders VALUES (1, 1, 150.00, '2025-01-10')");
        $this->pdo->exec("INSERT INTO sl_aj_orders VALUES (2, 1, 200.00, '2025-01-15')");
        $this->pdo->exec("INSERT INTO sl_aj_orders VALUES (3, 2, 75.00, '2025-01-12')");

        // Order 1 has a return; orders 2 and 3 do not
        $this->pdo->exec("INSERT INTO sl_aj_returns VALUES (1, 1, 'Defective')");
    }

    /**
     * Anti-join via LEFT JOIN ... WHERE IS NULL.
     * Find customers who have never ordered.
     */
    public function testAntiJoinLeftJoinWhereNull(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.name
             FROM sl_aj_customers c
             LEFT JOIN sl_aj_orders o ON o.customer_id = c.id
             WHERE o.id IS NULL
             ORDER BY c.name"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Carol', $rows[0]['name']);
        $this->assertSame('Dave', $rows[1]['name']);
        $this->assertSame('Eve', $rows[2]['name']);
    }

    /**
     * Anti-join via NOT EXISTS.
     */
    public function testAntiJoinNotExists(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.name
             FROM sl_aj_customers c
             WHERE NOT EXISTS (
                 SELECT 1 FROM sl_aj_orders o WHERE o.customer_id = c.id
             )
             ORDER BY c.name"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Carol', $rows[0]['name']);
        $this->assertSame('Dave', $rows[1]['name']);
        $this->assertSame('Eve', $rows[2]['name']);
    }

    /**
     * Anti-join via NOT IN.
     */
    public function testAntiJoinNotIn(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.name
             FROM sl_aj_customers c
             WHERE c.id NOT IN (SELECT customer_id FROM sl_aj_orders)
             ORDER BY c.name"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Carol', $rows[0]['name']);
        $this->assertSame('Dave', $rows[1]['name']);
        $this->assertSame('Eve', $rows[2]['name']);
    }

    /**
     * Semi-join via EXISTS — find customers who HAVE ordered.
     */
    public function testSemiJoinExists(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.name
             FROM sl_aj_customers c
             WHERE EXISTS (
                 SELECT 1 FROM sl_aj_orders o WHERE o.customer_id = c.id
             )
             ORDER BY c.name"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
    }

    /**
     * Chained anti-join: orders without returns.
     */
    public function testChainedAntiJoin(): void
    {
        $rows = $this->ztdQuery(
            "SELECT o.id AS order_id, c.name
             FROM sl_aj_orders o
             JOIN sl_aj_customers c ON c.id = o.customer_id
             LEFT JOIN sl_aj_returns r ON r.order_id = o.id
             WHERE r.id IS NULL
             ORDER BY o.id"
        );

        $this->assertCount(2, $rows);
        $this->assertEquals(2, (int) $rows[0]['order_id']);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEquals(3, (int) $rows[1]['order_id']);
        $this->assertSame('Bob', $rows[1]['name']);
    }

    /**
     * Double anti-join: customers with no orders AND no returns on any order.
     * This uses two levels of anti-join.
     */
    public function testDoubleNotExists(): void
    {
        // Customers who have orders but none of their orders have returns
        $rows = $this->ztdQuery(
            "SELECT c.name
             FROM sl_aj_customers c
             WHERE EXISTS (
                 SELECT 1 FROM sl_aj_orders o WHERE o.customer_id = c.id
             )
             AND NOT EXISTS (
                 SELECT 1 FROM sl_aj_orders o
                 JOIN sl_aj_returns r ON r.order_id = o.id
                 WHERE o.customer_id = c.id
             )
             ORDER BY c.name"
        );

        // Bob has orders (order 3) but no returns on any of them
        // Alice has orders but order 1 has a return → she has returns
        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
    }

    /**
     * Anti-join after INSERT — new customer should appear in "no orders" list.
     */
    public function testAntiJoinAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO sl_aj_customers VALUES (6, 'Frank', 'frank@example.com')");

        $rows = $this->ztdQuery(
            "SELECT c.name
             FROM sl_aj_customers c
             LEFT JOIN sl_aj_orders o ON o.customer_id = c.id
             WHERE o.id IS NULL
             ORDER BY c.name"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('Carol', $rows[0]['name']);
        $this->assertSame('Dave', $rows[1]['name']);
        $this->assertSame('Eve', $rows[2]['name']);
        $this->assertSame('Frank', $rows[3]['name']);
    }

    /**
     * Anti-join shrinks after adding an order for a no-order customer.
     */
    public function testAntiJoinShrinksAfterOrder(): void
    {
        $this->pdo->exec("INSERT INTO sl_aj_orders VALUES (4, 3, 50.00, '2025-02-01')");

        $rows = $this->ztdQuery(
            "SELECT c.name
             FROM sl_aj_customers c
             LEFT JOIN sl_aj_orders o ON o.customer_id = c.id
             WHERE o.id IS NULL
             ORDER BY c.name"
        );

        // Carol now has an order, so only Dave and Eve remain
        $this->assertCount(2, $rows);
        $this->assertSame('Dave', $rows[0]['name']);
        $this->assertSame('Eve', $rows[1]['name']);
    }

    /**
     * Anti-join with DELETE — removing an order makes customer reappear.
     */
    public function testAntiJoinAfterDelete(): void
    {
        // Delete all of Bob's orders
        $this->pdo->exec("DELETE FROM sl_aj_orders WHERE customer_id = 2");

        $rows = $this->ztdQuery(
            "SELECT c.name
             FROM sl_aj_customers c
             LEFT JOIN sl_aj_orders o ON o.customer_id = c.id
             WHERE o.id IS NULL
             ORDER BY c.name"
        );

        // Bob now has no orders → 4 customers without orders
        $this->assertCount(4, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
        $this->assertSame('Carol', $rows[1]['name']);
    }

    /**
     * Prepared anti-join with EXISTS.
     */
    public function testPreparedNotExists(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT c.name
             FROM sl_aj_customers c
             WHERE NOT EXISTS (
                 SELECT 1 FROM sl_aj_orders o
                 WHERE o.customer_id = c.id AND o.total > ?
             )
             ORDER BY c.name",
            [100.00]
        );

        // Orders > 100: order 1 (150, Alice), order 2 (200, Alice)
        // Bob's order is 75 (not > 100) → Bob appears (no qualifying orders)
        // Carol, Dave, Eve have no orders at all
        $this->assertCount(4, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
        $this->assertSame('Carol', $rows[1]['name']);
        $this->assertSame('Dave', $rows[2]['name']);
        $this->assertSame('Eve', $rows[3]['name']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_aj_customers")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
