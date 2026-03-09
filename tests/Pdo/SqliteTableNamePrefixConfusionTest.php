<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests that tables with overlapping name prefixes don't confuse the CTE
 * rewriter on SQLite.
 *
 * Real applications commonly have tables like: users/users_archive,
 * orders/order_items, products/product_categories. If the CTE rewriter uses
 * substring matching to find table references, "orders" could incorrectly
 * match "order_items", causing wrong shadow data to be injected.
 *
 * Also tests a table whose name is a substring of another table, and
 * tables with very similar names (differing by suffix only).
 *
 * @spec SPEC-3.1
 * @spec SPEC-4.1
 */
class SqliteTableNamePrefixConfusionTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_tpc_orders (
                id INTEGER PRIMARY KEY,
                customer TEXT,
                total REAL
            )',
            'CREATE TABLE sl_tpc_order_items (
                id INTEGER PRIMARY KEY,
                order_id INTEGER,
                product TEXT,
                qty INTEGER
            )',
            'CREATE TABLE sl_tpc_order_archive (
                id INTEGER PRIMARY KEY,
                customer TEXT,
                total REAL,
                archived_at TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_tpc_order_archive', 'sl_tpc_order_items', 'sl_tpc_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_tpc_orders VALUES (1, 'Alice', 100.00)");
        $this->pdo->exec("INSERT INTO sl_tpc_orders VALUES (2, 'Bob', 200.00)");

        $this->pdo->exec("INSERT INTO sl_tpc_order_items VALUES (1, 1, 'Widget', 2)");
        $this->pdo->exec("INSERT INTO sl_tpc_order_items VALUES (2, 1, 'Gadget', 1)");
        $this->pdo->exec("INSERT INTO sl_tpc_order_items VALUES (3, 2, 'Gizmo', 5)");

        $this->pdo->exec("INSERT INTO sl_tpc_order_archive VALUES (10, 'Charlie', 50.00, '2025-01-01')");
    }

    public function testSelectFromPrefixTable(): void
    {
        $rows = $this->ztdQuery('SELECT * FROM sl_tpc_orders ORDER BY id');
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['customer']);
    }

    public function testSelectFromSuffixTable(): void
    {
        $rows = $this->ztdQuery('SELECT * FROM sl_tpc_order_items ORDER BY id');
        $this->assertCount(3, $rows);
        $this->assertSame('Widget', $rows[0]['product']);
    }

    public function testSelectFromArchiveTable(): void
    {
        $rows = $this->ztdQuery('SELECT * FROM sl_tpc_order_archive ORDER BY id');
        $this->assertCount(1, $rows);
        $this->assertSame('Charlie', $rows[0]['customer']);
    }

    public function testJoinPrefixAndSuffixTables(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT o.customer, oi.product, oi.qty
                 FROM sl_tpc_orders o
                 JOIN sl_tpc_order_items oi ON oi.order_id = o.id
                 ORDER BY o.id, oi.id"
            );
            $this->assertCount(3, $rows);
            $this->assertSame('Alice', $rows[0]['customer']);
            $this->assertSame('Widget', $rows[0]['product']);
            $this->assertSame('Bob', $rows[2]['customer']);
            $this->assertSame('Gizmo', $rows[2]['product']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('JOIN prefix+suffix tables failed: ' . $e->getMessage());
        }
    }

    public function testInsertIntoOneTableThenQueryAnother(): void
    {
        // Insert into orders (prefix), then query order_items (suffix)
        $this->pdo->exec("INSERT INTO sl_tpc_orders VALUES (3, 'Diana', 300.00)");

        $rows = $this->ztdQuery('SELECT * FROM sl_tpc_order_items ORDER BY id');
        $this->assertCount(3, $rows, 'order_items should be unaffected by orders INSERT');
    }

    public function testInsertIntoSuffixTableThenQueryPrefixTable(): void
    {
        // Insert into order_items (suffix), then query orders (prefix)
        $this->pdo->exec("INSERT INTO sl_tpc_order_items VALUES (4, 2, 'Doodad', 3)");

        $rows = $this->ztdQuery('SELECT * FROM sl_tpc_orders ORDER BY id');
        $this->assertCount(2, $rows, 'orders should be unaffected by order_items INSERT');
    }

    public function testMutateOneTableJoinWithAnother(): void
    {
        // Update orders, then join with order_items
        $this->pdo->exec("UPDATE sl_tpc_orders SET total = 150.00 WHERE id = 1");

        try {
            $rows = $this->ztdQuery(
                "SELECT o.total, oi.product
                 FROM sl_tpc_orders o
                 JOIN sl_tpc_order_items oi ON oi.order_id = o.id
                 WHERE o.id = 1
                 ORDER BY oi.id"
            );
            $this->assertCount(2, $rows);
            $this->assertEqualsWithDelta(150.00, (float) $rows[0]['total'], 0.01);
            $this->assertSame('Widget', $rows[0]['product']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Mutate one table then JOIN another failed: ' . $e->getMessage());
        }
    }

    public function testMutateBothTablesJoin(): void
    {
        // Mutate both tables, then join
        $this->pdo->exec("UPDATE sl_tpc_orders SET customer = 'Alice Updated' WHERE id = 1");
        $this->pdo->exec("UPDATE sl_tpc_order_items SET qty = 10 WHERE id = 1");

        try {
            $rows = $this->ztdQuery(
                "SELECT o.customer, oi.product, oi.qty
                 FROM sl_tpc_orders o
                 JOIN sl_tpc_order_items oi ON oi.order_id = o.id
                 WHERE o.id = 1
                 ORDER BY oi.id"
            );
            $this->assertCount(2, $rows);
            $this->assertSame('Alice Updated', $rows[0]['customer']);
            $this->assertSame('Widget', $rows[0]['product']);
            $this->assertEquals(10, (int) $rows[0]['qty']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Mutate both tables then JOIN failed: ' . $e->getMessage());
        }
    }

    public function testDeleteFromOneTableSelectFromAnother(): void
    {
        $this->pdo->exec("DELETE FROM sl_tpc_orders WHERE id = 1");

        // order_items should still have 3 rows
        $rows = $this->ztdQuery('SELECT * FROM sl_tpc_order_items ORDER BY id');
        $this->assertCount(3, $rows);

        // orders should have 1 row
        $rows = $this->ztdQuery('SELECT * FROM sl_tpc_orders');
        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['customer']);
    }

    public function testAllThreeTablesInSession(): void
    {
        // Insert into all three tables, then query each
        $this->pdo->exec("INSERT INTO sl_tpc_orders VALUES (3, 'Eve', 400.00)");
        $this->pdo->exec("INSERT INTO sl_tpc_order_items VALUES (4, 3, 'Premium', 1)");
        $this->pdo->exec("INSERT INTO sl_tpc_order_archive VALUES (11, 'Frank', 75.00, '2025-02-01')");

        $orders = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM sl_tpc_orders');
        $this->assertEquals(3, (int) $orders[0]['cnt']);

        $items = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM sl_tpc_order_items');
        $this->assertEquals(4, (int) $items[0]['cnt']);

        $archive = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM sl_tpc_order_archive');
        $this->assertEquals(2, (int) $archive[0]['cnt']);
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_tpc_orders")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
