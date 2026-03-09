<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests inter-warehouse stock transfers: warehouses, products, stock levels, and transfer records.
 * SQL patterns exercised: multi-table INSERT+UPDATE, self-join (source/dest warehouse),
 * GROUP BY SUM for balance, HAVING for threshold, prepared statement for transfer lookup (SQLite PDO).
 * @spec SPEC-10.2.144
 */
class SqliteWarehouseTransferTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_wt_warehouses (
                id INTEGER PRIMARY KEY,
                name TEXT,
                location TEXT
            )',
            'CREATE TABLE sl_wt_products (
                id INTEGER PRIMARY KEY,
                sku TEXT,
                name TEXT
            )',
            'CREATE TABLE sl_wt_stock (
                id INTEGER PRIMARY KEY,
                warehouse_id INTEGER,
                product_id INTEGER,
                quantity INTEGER
            )',
            'CREATE TABLE sl_wt_transfers (
                id INTEGER PRIMARY KEY,
                from_warehouse_id INTEGER,
                to_warehouse_id INTEGER,
                product_id INTEGER,
                quantity INTEGER,
                transfer_date TEXT,
                status TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_wt_transfers', 'sl_wt_stock', 'sl_wt_products', 'sl_wt_warehouses'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_wt_warehouses VALUES (1, 'Central', 'Chicago')");
        $this->pdo->exec("INSERT INTO sl_wt_warehouses VALUES (2, 'East', 'New York')");
        $this->pdo->exec("INSERT INTO sl_wt_warehouses VALUES (3, 'West', 'Los Angeles')");

        $this->pdo->exec("INSERT INTO sl_wt_products VALUES (1, 'WDG-001', 'Widget A')");
        $this->pdo->exec("INSERT INTO sl_wt_products VALUES (2, 'WDG-002', 'Widget B')");
        $this->pdo->exec("INSERT INTO sl_wt_products VALUES (3, 'GAD-001', 'Gadget X')");

        $this->pdo->exec("INSERT INTO sl_wt_stock VALUES (1, 1, 1, 500)");
        $this->pdo->exec("INSERT INTO sl_wt_stock VALUES (2, 1, 2, 300)");
        $this->pdo->exec("INSERT INTO sl_wt_stock VALUES (3, 1, 3, 200)");
        $this->pdo->exec("INSERT INTO sl_wt_stock VALUES (4, 2, 1, 150)");
        $this->pdo->exec("INSERT INTO sl_wt_stock VALUES (5, 2, 2, 80)");
        $this->pdo->exec("INSERT INTO sl_wt_stock VALUES (6, 3, 1, 100)");
        $this->pdo->exec("INSERT INTO sl_wt_stock VALUES (7, 3, 3, 250)");

        $this->pdo->exec("INSERT INTO sl_wt_transfers VALUES (1, 1, 2, 1, 50, '2025-10-01', 'completed')");
        $this->pdo->exec("INSERT INTO sl_wt_transfers VALUES (2, 1, 3, 2, 30, '2025-10-03', 'completed')");
        $this->pdo->exec("INSERT INTO sl_wt_transfers VALUES (3, 2, 3, 1, 20, '2025-10-05', 'pending')");
        $this->pdo->exec("INSERT INTO sl_wt_transfers VALUES (4, 3, 1, 3, 100, '2025-10-07', 'completed')");
        $this->pdo->exec("INSERT INTO sl_wt_transfers VALUES (5, 1, 2, 3, 75, '2025-10-08', 'cancelled')");
    }

    public function testStockSummaryByWarehouse(): void
    {
        $rows = $this->ztdQuery(
            "SELECT w.name AS warehouse, p.sku, p.name AS product, s.quantity
             FROM sl_wt_stock s
             JOIN sl_wt_warehouses w ON w.id = s.warehouse_id
             JOIN sl_wt_products p ON p.id = s.product_id
             ORDER BY w.name, p.sku"
        );

        $this->assertCount(7, $rows);
        $this->assertSame('Central', $rows[0]['warehouse']);
        $this->assertSame('GAD-001', $rows[0]['sku']);
        $this->assertEquals(200, (int) $rows[0]['quantity']);
    }

    public function testTotalStockPerProduct(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.sku, p.name, SUM(s.quantity) AS total_qty
             FROM sl_wt_stock s
             JOIN sl_wt_products p ON p.id = s.product_id
             GROUP BY p.sku, p.name
             ORDER BY p.sku"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('GAD-001', $rows[0]['sku']);
        $this->assertEquals(450, (int) $rows[0]['total_qty']);
        $this->assertSame('WDG-001', $rows[1]['sku']);
        $this->assertEquals(750, (int) $rows[1]['total_qty']);
        $this->assertSame('WDG-002', $rows[2]['sku']);
        $this->assertEquals(380, (int) $rows[2]['total_qty']);
    }

    public function testTransferWithWarehouseNames(): void
    {
        $rows = $this->ztdQuery(
            "SELECT t.id, wf.name AS from_warehouse, wt.name AS to_warehouse,
                    p.name AS product, t.quantity, t.status
             FROM sl_wt_transfers t
             JOIN sl_wt_warehouses wf ON wf.id = t.from_warehouse_id
             JOIN sl_wt_warehouses wt ON wt.id = t.to_warehouse_id
             JOIN sl_wt_products p ON p.id = t.product_id
             ORDER BY t.id"
        );

        $this->assertCount(5, $rows);
        $this->assertSame('Central', $rows[0]['from_warehouse']);
        $this->assertSame('East', $rows[0]['to_warehouse']);
        $this->assertSame('Widget A', $rows[0]['product']);
        $this->assertEquals(50, (int) $rows[0]['quantity']);

        $this->assertSame('West', $rows[3]['from_warehouse']);
        $this->assertSame('Central', $rows[3]['to_warehouse']);
        $this->assertEquals(100, (int) $rows[3]['quantity']);
    }

    public function testTransferVolumeByRouteWithHaving(): void
    {
        $rows = $this->ztdQuery(
            "SELECT wf.name AS from_wh, wt.name AS to_wh,
                    SUM(t.quantity) AS total_transferred
             FROM sl_wt_transfers t
             JOIN sl_wt_warehouses wf ON wf.id = t.from_warehouse_id
             JOIN sl_wt_warehouses wt ON wt.id = t.to_warehouse_id
             WHERE t.status = 'completed'
             GROUP BY wf.name, wt.name
             HAVING SUM(t.quantity) >= 50
             ORDER BY total_transferred DESC"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('West', $rows[0]['from_wh']);
        $this->assertEquals(100, (int) $rows[0]['total_transferred']);
        $this->assertSame('Central', $rows[1]['from_wh']);
        $this->assertEquals(50, (int) $rows[1]['total_transferred']);
    }

    public function testLowStockWarehouses(): void
    {
        $rows = $this->ztdQuery(
            "SELECT w.name AS warehouse, SUM(s.quantity) AS total_stock
             FROM sl_wt_stock s
             JOIN sl_wt_warehouses w ON w.id = s.warehouse_id
             GROUP BY w.name
             HAVING SUM(s.quantity) < 400
             ORDER BY total_stock"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('East', $rows[0]['warehouse']);
        $this->assertEquals(230, (int) $rows[0]['total_stock']);
        $this->assertSame('West', $rows[1]['warehouse']);
        $this->assertEquals(350, (int) $rows[1]['total_stock']);
    }

    public function testPreparedTransferLookupByStatus(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT t.id, p.name AS product, t.quantity, t.transfer_date
             FROM sl_wt_transfers t
             JOIN sl_wt_products p ON p.id = t.product_id
             WHERE t.status = ?
             ORDER BY t.id",
            ['completed']
        );

        $this->assertCount(3, $rows);
        $this->assertEquals(1, (int) $rows[0]['id']);
        $this->assertSame('Widget A', $rows[0]['product']);
        $this->assertEquals(4, (int) $rows[2]['id']);
        $this->assertSame('Gadget X', $rows[2]['product']);
    }

    public function testRecordTransferAndVerify(): void
    {
        $this->pdo->exec("INSERT INTO sl_wt_transfers VALUES (6, 2, 1, 2, 40, '2025-10-10', 'completed')");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_wt_transfers WHERE status = 'completed'");
        $this->assertEquals(4, (int) $rows[0]['cnt']);

        $this->pdo->exec("UPDATE sl_wt_stock SET quantity = quantity - 40 WHERE warehouse_id = 2 AND product_id = 2");
        $this->pdo->exec("UPDATE sl_wt_stock SET quantity = quantity + 40 WHERE warehouse_id = 1 AND product_id = 2");

        $rows = $this->ztdQuery("SELECT s.quantity FROM sl_wt_stock s WHERE s.warehouse_id = 2 AND s.product_id = 2");
        $this->assertEquals(40, (int) $rows[0]['quantity']);

        $rows = $this->ztdQuery("SELECT s.quantity FROM sl_wt_stock s WHERE s.warehouse_id = 1 AND s.product_id = 2");
        $this->assertEquals(340, (int) $rows[0]['quantity']);
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO sl_wt_transfers VALUES (6, 1, 3, 1, 200, '2025-10-12', 'pending')");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_wt_transfers");
        $this->assertEquals(6, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_wt_transfers")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
