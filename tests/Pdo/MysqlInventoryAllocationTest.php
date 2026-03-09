<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests an inventory allocation workflow through ZTD shadow store (MySQL PDO).
 * Covers stock reservation, sale conversion, capacity guards, warehouse reporting,
 * and physical isolation.
 * @spec SPEC-10.2.72
 */
class MysqlInventoryAllocationTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_ia_warehouses (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                location VARCHAR(255)
            )',
            'CREATE TABLE mp_ia_products (
                id INT PRIMARY KEY,
                warehouse_id INT,
                sku VARCHAR(50),
                name VARCHAR(255),
                total_stock INT,
                reserved INT,
                sold INT
            )',
            'CREATE TABLE mp_ia_reservations (
                id INT PRIMARY KEY,
                product_id INT,
                customer_name VARCHAR(255),
                quantity INT,
                status VARCHAR(20),
                reserved_at DATETIME
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_ia_reservations', 'mp_ia_products', 'mp_ia_warehouses'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 2 warehouses
        $this->pdo->exec("INSERT INTO mp_ia_warehouses VALUES (1, 'Main Warehouse', 'New York')");
        $this->pdo->exec("INSERT INTO mp_ia_warehouses VALUES (2, 'West Coast', 'Los Angeles')");

        // Products: total_stock, reserved, sold
        $this->pdo->exec("INSERT INTO mp_ia_products VALUES (1, 1, 'SKU-001', 'Widget A', 100, 10, 20)");
        $this->pdo->exec("INSERT INTO mp_ia_products VALUES (2, 1, 'SKU-002', 'Widget B', 50, 5, 30)");
        $this->pdo->exec("INSERT INTO mp_ia_products VALUES (3, 2, 'SKU-001', 'Widget A', 80, 0, 10)");
        $this->pdo->exec("INSERT INTO mp_ia_products VALUES (4, 2, 'SKU-003', 'Gadget C', 200, 25, 50)");

        // Existing reservations
        $this->pdo->exec("INSERT INTO mp_ia_reservations VALUES (1, 1, 'Alice', 5, 'active', '2026-03-01 09:00:00')");
        $this->pdo->exec("INSERT INTO mp_ia_reservations VALUES (2, 1, 'Bob', 5, 'active', '2026-03-02 10:00:00')");
        $this->pdo->exec("INSERT INTO mp_ia_reservations VALUES (3, 4, 'Charlie', 25, 'active', '2026-03-03 11:00:00')");
    }

    /**
     * Calculate available stock: total - reserved - sold.
     */
    public function testAvailableStockCalculation(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, sku, name,
                    total_stock, reserved, sold,
                    (total_stock - reserved - sold) AS available
             FROM mp_ia_products
             ORDER BY id"
        );

        $this->assertCount(4, $rows);
        $this->assertEquals(70, (int) $rows[0]['available']);  // 100 - 10 - 20
        $this->assertEquals(15, (int) $rows[1]['available']);  // 50 - 5 - 30
        $this->assertEquals(70, (int) $rows[2]['available']);  // 80 - 0 - 10
        $this->assertEquals(125, (int) $rows[3]['available']); // 200 - 25 - 50
    }

    /**
     * Reserve stock: INSERT reservation and UPDATE product reserved count.
     */
    public function testReserveStock(): void
    {
        // Reserve 3 units of product 2
        $this->pdo->exec("INSERT INTO mp_ia_reservations VALUES (4, 2, 'Diana', 3, 'active', '2026-03-04 09:00:00')");
        $this->pdo->exec("UPDATE mp_ia_products SET reserved = reserved + 3 WHERE id = 2");

        // Verify reservation exists
        $rows = $this->ztdQuery("SELECT quantity, status FROM mp_ia_reservations WHERE id = 4");
        $this->assertEquals(3, (int) $rows[0]['quantity']);
        $this->assertSame('active', $rows[0]['status']);

        // Verify reserved count updated
        $rows = $this->ztdQuery("SELECT reserved, (total_stock - reserved - sold) AS available FROM mp_ia_products WHERE id = 2");
        $this->assertEquals(8, (int) $rows[0]['reserved']);   // 5 + 3
        $this->assertEquals(12, (int) $rows[0]['available']); // 50 - 8 - 30
    }

    /**
     * Convert reservation to sale: UPDATE reservation status and product sold/reserved counts.
     */
    public function testConvertReservationToSale(): void
    {
        // Convert Alice's reservation (5 units of product 1) to a sale
        $affected = $this->pdo->exec("UPDATE mp_ia_reservations SET status = 'converted' WHERE id = 1 AND status = 'active'");
        $this->assertSame(1, $affected);

        $this->pdo->exec("UPDATE mp_ia_products SET reserved = reserved - 5, sold = sold + 5 WHERE id = 1");

        // Verify reservation converted
        $rows = $this->ztdQuery("SELECT status FROM mp_ia_reservations WHERE id = 1");
        $this->assertSame('converted', $rows[0]['status']);

        // Verify product counts: available should stay the same (reserved moved to sold)
        $rows = $this->ztdQuery(
            "SELECT total_stock, reserved, sold, (total_stock - reserved - sold) AS available
             FROM mp_ia_products WHERE id = 1"
        );
        $this->assertEquals(5, (int) $rows[0]['reserved']);   // 10 - 5
        $this->assertEquals(25, (int) $rows[0]['sold']);       // 20 + 5
        $this->assertEquals(70, (int) $rows[0]['available']);   // unchanged: 100 - 5 - 25
    }

    /**
     * Cancel reservation: UPDATE status and restore reserved count.
     */
    public function testCancelReservation(): void
    {
        $affected = $this->pdo->exec("UPDATE mp_ia_reservations SET status = 'cancelled' WHERE id = 2 AND status = 'active'");
        $this->assertSame(1, $affected);

        $this->pdo->exec("UPDATE mp_ia_products SET reserved = reserved - 5 WHERE id = 1");

        // Verify counts
        $rows = $this->ztdQuery(
            "SELECT reserved, (total_stock - reserved - sold) AS available
             FROM mp_ia_products WHERE id = 1"
        );
        $this->assertEquals(5, (int) $rows[0]['reserved']);   // 10 - 5
        $this->assertEquals(75, (int) $rows[0]['available']); // 100 - 5 - 20
    }

    /**
     * Warehouse stock report: GROUP BY warehouse with SUM of stock metrics.
     */
    public function testWarehouseStockReport(): void
    {
        $rows = $this->ztdQuery(
            "SELECT w.name AS warehouse,
                    COUNT(p.id) AS product_count,
                    SUM(p.total_stock) AS total_stock,
                    SUM(p.reserved) AS total_reserved,
                    SUM(p.sold) AS total_sold,
                    SUM(p.total_stock - p.reserved - p.sold) AS total_available
             FROM mp_ia_warehouses w
             LEFT JOIN mp_ia_products p ON p.warehouse_id = w.id
             GROUP BY w.id, w.name
             ORDER BY w.id"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Main Warehouse', $rows[0]['warehouse']);
        $this->assertEquals(2, (int) $rows[0]['product_count']);
        $this->assertEquals(150, (int) $rows[0]['total_stock']);
        $this->assertEquals(85, (int) $rows[0]['total_available']); // 70 + 15

        $this->assertSame('West Coast', $rows[1]['warehouse']);
        $this->assertEquals(2, (int) $rows[1]['product_count']);
        $this->assertEquals(280, (int) $rows[1]['total_stock']);
    }

    /**
     * Prepared statement: stock lookup by SKU across all warehouses.
     */
    public function testStockLookupBySku(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT p.id, w.name AS warehouse, p.total_stock, p.reserved, p.sold,
                    (p.total_stock - p.reserved - p.sold) AS available
             FROM mp_ia_products p
             JOIN mp_ia_warehouses w ON w.id = p.warehouse_id
             WHERE p.sku = ?
             ORDER BY w.name",
            ['SKU-001']
        );

        $this->assertCount(2, $rows);
        // Main Warehouse
        $this->assertEquals(70, (int) $rows[0]['available']);
        // West Coast
        $this->assertEquals(70, (int) $rows[1]['available']);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO mp_ia_reservations VALUES (4, 3, 'Eve', 10, 'active', '2026-03-05 09:00:00')");
        $this->pdo->exec("UPDATE mp_ia_products SET reserved = reserved + 10 WHERE id = 3");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_ia_reservations");
        $this->assertEquals(4, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM mp_ia_reservations')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
