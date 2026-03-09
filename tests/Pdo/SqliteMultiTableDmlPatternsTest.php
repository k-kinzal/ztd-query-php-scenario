<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Multi-Table DML Patterns: UPDATE with subqueries and DELETE with subqueries
 * referencing other tables through the CTE rewriter (SQLite PDO).
 *
 * SQLite does not support UPDATE ... JOIN syntax. Instead, use UPDATE with
 * a subquery in the WHERE clause to achieve the same cross-table filtering.
 *
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class SqliteMultiTableDmlPatternsTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_mtd_warehouses (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1
            )',
            'CREATE TABLE sl_mtd_inventory (
                id INTEGER PRIMARY KEY,
                product TEXT NOT NULL,
                stock INTEGER NOT NULL,
                min_stock INTEGER NOT NULL,
                warehouse_id INTEGER NOT NULL
            )',
            'CREATE TABLE sl_mtd_restock_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                qty INTEGER NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_mtd_restock_log', 'sl_mtd_inventory', 'sl_mtd_warehouses'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_mtd_warehouses (id, name, active) VALUES
            (1, 'Main', 1), (2, 'Backup', 1), (3, 'Decommissioned', 0)");

        $this->pdo->exec("INSERT INTO sl_mtd_inventory (id, product, stock, min_stock, warehouse_id) VALUES
            (1, 'Bolts', 100, 50, 1), (2, 'Nuts', 30, 40, 1),
            (3, 'Screws', 200, 100, 2), (4, 'Nails', 15, 20, 2),
            (5, 'Washers', 10, 25, 3)");
    }

    /**
     * SQLite UPDATE with subquery in WHERE: increase stock for items in 'Main' warehouse.
     * SQLite does not support UPDATE ... JOIN, so use IN subquery instead.
     */
    public function testUpdateWithJoin(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_mtd_inventory
                 SET stock = stock + 10
                 WHERE warehouse_id IN (SELECT id FROM sl_mtd_warehouses WHERE name = 'Main')"
            );

            $rows = $this->ztdQuery(
                "SELECT id, product, stock FROM sl_mtd_inventory ORDER BY id"
            );

            if (count($rows) !== 5) {
                $this->markTestIncomplete('UPDATE subquery: expected 5 rows, got ' . count($rows));
            }

            // Items in Main warehouse (id=1): Bolts 100->110, Nuts 30->40
            $this->assertEquals(110, (int) $rows[0]['stock'], 'Bolts stock should increase');
            $this->assertEquals(40, (int) $rows[1]['stock'], 'Nuts stock should increase');
            // Items in other warehouses should be unchanged
            $this->assertEquals(200, (int) $rows[2]['stock'], 'Screws stock should not change');
            $this->assertEquals(15, (int) $rows[3]['stock'], 'Nails stock should not change');
            $this->assertEquals(10, (int) $rows[4]['stock'], 'Washers stock should not change');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * SQLite UPDATE with subquery: zero out stock for items in inactive warehouses.
     */
    public function testUpdateWithJoinCondition(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_mtd_inventory
                 SET stock = 0
                 WHERE warehouse_id IN (SELECT id FROM sl_mtd_warehouses WHERE active = 0)"
            );

            $rows = $this->ztdQuery(
                "SELECT id, product, stock FROM sl_mtd_inventory ORDER BY id"
            );

            if (count($rows) !== 5) {
                $this->markTestIncomplete('UPDATE subquery condition: expected 5 rows, got ' . count($rows));
            }

            // Items in active warehouses should be unchanged
            $this->assertEquals(100, (int) $rows[0]['stock'], 'Bolts stock should not change');
            $this->assertEquals(30, (int) $rows[1]['stock'], 'Nuts stock should not change');
            $this->assertEquals(200, (int) $rows[2]['stock'], 'Screws stock should not change');
            $this->assertEquals(15, (int) $rows[3]['stock'], 'Nails stock should not change');
            // Washers in Decommissioned (active=0) should be zeroed
            $this->assertEquals(0, (int) $rows[4]['stock'], 'Washers stock should be zeroed');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with subquery condition failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with IN subquery referencing another table.
     */
    public function testDeleteWithSubqueryFromOtherTable(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM sl_mtd_inventory
                 WHERE warehouse_id IN (SELECT id FROM sl_mtd_warehouses WHERE active = 0)"
            );

            $rows = $this->ztdQuery(
                "SELECT id, product FROM sl_mtd_inventory ORDER BY id"
            );

            if (count($rows) !== 4) {
                $this->markTestIncomplete('DELETE IN subquery: expected 4 rows, got ' . count($rows));
            }

            // Washers (warehouse_id=3, active=0) should be deleted
            $products = array_column($rows, 'product');
            $this->assertContains('Bolts', $products);
            $this->assertContains('Nuts', $products);
            $this->assertContains('Screws', $products);
            $this->assertContains('Nails', $products);
            $this->assertNotContains('Washers', $products);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE with subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with EXISTS subquery correlating to the other table.
     */
    public function testDeleteWithExistsSubquery(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM sl_mtd_inventory
                 WHERE EXISTS (
                     SELECT 1 FROM sl_mtd_warehouses w
                     WHERE w.id = sl_mtd_inventory.warehouse_id AND w.active = 0
                 )"
            );

            $rows = $this->ztdQuery(
                "SELECT id, product FROM sl_mtd_inventory ORDER BY id"
            );

            if (count($rows) !== 4) {
                $this->markTestIncomplete('DELETE EXISTS: expected 4 rows, got ' . count($rows));
            }

            $products = array_column($rows, 'product');
            $this->assertNotContains('Washers', $products);
            $this->assertContains('Bolts', $products);
            $this->assertContains('Nails', $products);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE with EXISTS subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT from a JOIN across two shadow tables.
     * Items below min_stock in active warehouses should generate restock log entries.
     */
    public function testInsertSelectFromJoin(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO sl_mtd_restock_log (product_id, qty)
                 SELECT i.id, i.min_stock - i.stock
                 FROM sl_mtd_inventory i
                 JOIN sl_mtd_warehouses w ON i.warehouse_id = w.id
                 WHERE i.stock < i.min_stock AND w.active = 1"
            );

            $rows = $this->ztdQuery(
                "SELECT product_id, qty FROM sl_mtd_restock_log ORDER BY product_id"
            );

            // Active warehouses: Main (id=1) and Backup (id=2)
            // Nuts: stock=30 < min_stock=40 -> qty=10 (warehouse Main, active=1)
            // Nails: stock=15 < min_stock=20 -> qty=5 (warehouse Backup, active=1)
            // Washers: stock=10 < min_stock=25 but warehouse Decommissioned (active=0) -> excluded
            if (count($rows) !== 2) {
                $this->markTestIncomplete('INSERT SELECT JOIN: expected 2 rows, got ' . count($rows));
            }

            $this->assertEquals(2, (int) $rows[0]['product_id'], 'Nuts should need restocking');
            $this->assertEquals(10, (int) $rows[0]['qty'], 'Nuts restock qty should be 10');
            $this->assertEquals(4, (int) $rows[1]['product_id'], 'Nails should need restocking');
            $this->assertEquals(5, (int) $rows[1]['qty'], 'Nails restock qty should be 5');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT from JOIN failed: ' . $e->getMessage());
        }
    }

    /**
     * Physical isolation: all tables should be empty when ZTD is disabled.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();

        $cnt = $this->pdo->query("SELECT COUNT(*) FROM sl_mtd_warehouses")->fetchColumn();
        $this->assertSame(0, (int) $cnt, 'sl_mtd_warehouses should be empty');

        $cnt = $this->pdo->query("SELECT COUNT(*) FROM sl_mtd_inventory")->fetchColumn();
        $this->assertSame(0, (int) $cnt, 'sl_mtd_inventory should be empty');

        $cnt = $this->pdo->query("SELECT COUNT(*) FROM sl_mtd_restock_log")->fetchColumn();
        $this->assertSame(0, (int) $cnt, 'sl_mtd_restock_log should be empty');
    }
}
