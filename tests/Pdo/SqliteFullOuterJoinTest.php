<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests FULL OUTER JOIN through SQLite CTE shadow store.
 *
 * SQLite added FULL OUTER JOIN support in 3.39.0 (2022-07-21).
 * FULL JOIN produces NULLs on both sides for unmatched rows,
 * which may challenge CTE type casting and column resolution.
 */
class SqliteFullOuterJoinTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_foj_customers (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                region TEXT NOT NULL
            )',
            'CREATE TABLE sl_foj_orders (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                amount REAL NOT NULL,
                order_date TEXT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_foj_orders', 'sl_foj_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Check SQLite version supports FULL OUTER JOIN
        $version = $this->pdo->query('SELECT sqlite_version()')->fetchColumn();
        if (version_compare($version, '3.39.0', '<')) {
            $this->markTestSkipped("FULL OUTER JOIN requires SQLite 3.39.0+, got {$version}");
        }

        $this->pdo->exec("INSERT INTO sl_foj_customers VALUES (1, 'Alice', 'North')");
        $this->pdo->exec("INSERT INTO sl_foj_customers VALUES (2, 'Bob', 'South')");
        $this->pdo->exec("INSERT INTO sl_foj_customers VALUES (3, 'Carol', 'East')");

        // Order with customer_id=1 (Alice), customer_id=2 (Bob), and customer_id=99 (no customer)
        $this->pdo->exec("INSERT INTO sl_foj_orders VALUES (1, 1, 100.00, '2025-01-01')");
        $this->pdo->exec("INSERT INTO sl_foj_orders VALUES (2, 2, 200.00, '2025-01-02')");
        $this->pdo->exec("INSERT INTO sl_foj_orders VALUES (3, 99, 50.00, '2025-01-03')");
    }

    /**
     * FULL OUTER JOIN — should include all customers and all orders,
     * with NULLs for unmatched sides.
     */
    public function testFullOuterJoin(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.name, o.amount
             FROM sl_foj_customers c
             FULL OUTER JOIN sl_foj_orders o ON c.id = o.customer_id
             ORDER BY COALESCE(c.id, o.customer_id + 100)"
        );

        // Expect: Alice+100, Bob+200, Carol+NULL, NULL+50 (orphan order)
        $this->assertCount(4, $rows, 'FULL JOIN should produce 4 rows');

        $names = array_column($rows, 'name');
        $this->assertContains('Alice', $names);
        $this->assertContains('Bob', $names);
        $this->assertContains('Carol', $names);
        $this->assertContains(null, $names, 'Orphan order should have NULL customer name');
    }

    /**
     * FULL OUTER JOIN with aggregate.
     */
    public function testFullOuterJoinWithAggregate(): void
    {
        $rows = $this->ztdQuery(
            "SELECT
                COALESCE(c.name, 'Unknown') AS customer_name,
                COUNT(o.id) AS order_count,
                COALESCE(SUM(o.amount), 0) AS total_amount
             FROM sl_foj_customers c
             FULL OUTER JOIN sl_foj_orders o ON c.id = o.customer_id
             GROUP BY COALESCE(c.name, 'Unknown')
             ORDER BY customer_name"
        );

        $this->assertGreaterThanOrEqual(3, count($rows));

        $alice = array_values(array_filter($rows, fn($r) => $r['customer_name'] === 'Alice'));
        $this->assertCount(1, $alice);
        $this->assertEquals(1, $alice[0]['order_count']);
        $this->assertEquals(100.0, (float) $alice[0]['total_amount']);

        $carol = array_values(array_filter($rows, fn($r) => $r['customer_name'] === 'Carol'));
        $this->assertCount(1, $carol);
        $this->assertEquals(0, $carol[0]['order_count']);
    }

    /**
     * FULL OUTER JOIN with WHERE clause.
     */
    public function testFullOuterJoinWithWhere(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.name, o.amount
             FROM sl_foj_customers c
             FULL OUTER JOIN sl_foj_orders o ON c.id = o.customer_id
             WHERE o.amount > 75 OR o.amount IS NULL
             ORDER BY c.name"
        );

        // Alice (100), Bob (200), Carol (NULL amount — no order)
        $this->assertCount(3, $rows);
    }

    /**
     * FULL JOIN with prepared statement.
     */
    public function testFullOuterJoinPrepared(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT c.name, o.amount
             FROM sl_foj_customers c
             FULL OUTER JOIN sl_foj_orders o ON c.id = o.customer_id
             WHERE o.amount > ? OR o.amount IS NULL
             ORDER BY c.name",
            [150]
        );

        // Bob (200), Carol (NULL)
        $this->assertCount(2, $rows);
    }

    /**
     * Physical isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $count = (int) $this->pdo->query('SELECT COUNT(*) FROM sl_foj_customers')->fetchColumn();
        $this->assertSame(0, $count);
    }
}
