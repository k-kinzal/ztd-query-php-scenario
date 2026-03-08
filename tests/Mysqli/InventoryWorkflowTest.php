<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests inventory management workflow patterns through ZTD shadow store.
 * Simulates stock tracking, order fulfillment, and reorder point checks.
 * @spec SPEC-4.2
 */
class InventoryWorkflowTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_inv_products (
                id INT PRIMARY KEY,
                sku VARCHAR(20) UNIQUE,
                name VARCHAR(255),
                stock INT,
                reorder_point INT,
                price DECIMAL(10,2)
            )',
            'CREATE TABLE mi_inv_orders (
                id INT PRIMARY KEY,
                product_id INT,
                quantity INT,
                order_type VARCHAR(20),
                created_at DATE
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_inv_orders', 'mi_inv_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_inv_products VALUES (1, 'SKU-001', 'Widget A', 100, 20, 9.99)");
        $this->mysqli->query("INSERT INTO mi_inv_products VALUES (2, 'SKU-002', 'Widget B', 50, 15, 19.99)");
        $this->mysqli->query("INSERT INTO mi_inv_products VALUES (3, 'SKU-003', 'Gadget C', 5, 10, 49.99)");
        $this->mysqli->query("INSERT INTO mi_inv_products VALUES (4, 'SKU-004', 'Gadget D', 0, 5, 99.99)");
        $this->mysqli->query("INSERT INTO mi_inv_products VALUES (5, 'SKU-005', 'Gizmo E', 200, 50, 4.99)");
    }

    /**
     * Decrement stock on order fulfillment using self-referencing UPDATE.
     */
    public function testDecrementStockOnOrder(): void
    {
        $this->mysqli->query("UPDATE mi_inv_products SET stock = stock - 10 WHERE id = 1");

        $rows = $this->ztdQuery("SELECT stock FROM mi_inv_products WHERE id = 1");
        $this->assertEquals(90, (int) $rows[0]['stock']);
    }

    /**
     * Prevent overselling: check stock before decrementing (application-level).
     */
    public function testConditionalStockDecrement(): void
    {
        // Only decrement if sufficient stock exists
        $this->mysqli->query(
            "UPDATE mi_inv_products SET stock = stock - 3 WHERE id = 3 AND stock >= 3"
        );
        $affected = $this->mysqli->lastAffectedRows();
        $this->assertSame(1, $affected);

        $rows = $this->ztdQuery("SELECT stock FROM mi_inv_products WHERE id = 3");
        $this->assertEquals(2, (int) $rows[0]['stock']);

        // Try to order more than remaining stock
        $this->mysqli->query(
            "UPDATE mi_inv_products SET stock = stock - 5 WHERE id = 3 AND stock >= 5"
        );
        $affected = $this->mysqli->lastAffectedRows();
        $this->assertSame(0, $affected); // No rows matched

        // Stock unchanged
        $rows = $this->ztdQuery("SELECT stock FROM mi_inv_products WHERE id = 3");
        $this->assertEquals(2, (int) $rows[0]['stock']);
    }

    /**
     * Find products below reorder point.
     */
    public function testLowStockAlert(): void
    {
        $rows = $this->ztdQuery(
            "SELECT sku, name, stock, reorder_point
             FROM mi_inv_products
             WHERE stock < reorder_point
             ORDER BY stock ASC"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('SKU-004', $rows[0]['sku']); // stock 0 < reorder 5
        $this->assertSame('SKU-003', $rows[1]['sku']); // stock 5 < reorder 10
    }

    /**
     * Total inventory value calculation.
     */
    public function testTotalInventoryValue(): void
    {
        $rows = $this->ztdQuery(
            "SELECT SUM(stock * price) AS total_value,
                    SUM(stock) AS total_units,
                    COUNT(*) AS product_count
             FROM mi_inv_products"
        );

        // 100*9.99 + 50*19.99 + 5*49.99 + 0*99.99 + 200*4.99
        // = 999 + 999.5 + 249.95 + 0 + 998 = 3246.45
        $this->assertEqualsWithDelta(3246.45, (float) $rows[0]['total_value'], 0.01);
        $this->assertEquals(355, (int) $rows[0]['total_units']);
    }

    /**
     * Order fulfillment workflow: record order, decrement stock, verify.
     */
    public function testOrderFulfillmentWorkflow(): void
    {
        // Step 1: Check stock
        $rows = $this->ztdQuery("SELECT stock FROM mi_inv_products WHERE id = 2");
        $initialStock = (int) $rows[0]['stock'];
        $this->assertEquals(50, $initialStock);

        // Step 2: Record order
        $this->mysqli->query("INSERT INTO mi_inv_orders VALUES (1, 2, 5, 'sale', '2024-03-15')");

        // Step 3: Decrement stock
        $this->mysqli->query("UPDATE mi_inv_products SET stock = stock - 5 WHERE id = 2");

        // Step 4: Verify stock decreased
        $rows = $this->ztdQuery("SELECT stock FROM mi_inv_products WHERE id = 2");
        $this->assertEquals(45, (int) $rows[0]['stock']);

        // Step 5: Verify order recorded
        $rows = $this->ztdQuery("SELECT quantity, order_type FROM mi_inv_orders WHERE product_id = 2");
        $this->assertCount(1, $rows);
        $this->assertEquals(5, (int) $rows[0]['quantity']);
    }

    /**
     * Bulk stock adjustment using CASE expression.
     */
    public function testBulkStockAdjustment(): void
    {
        $this->mysqli->query(
            "UPDATE mi_inv_products SET stock = CASE
                WHEN stock > 100 THEN stock - 50
                WHEN stock > 20 THEN stock - 10
                ELSE stock
             END
             WHERE stock > 0"
        );

        $rows = $this->ztdQuery(
            "SELECT id, stock FROM mi_inv_products ORDER BY id"
        );

        $this->assertEquals(90, (int) $rows[0]['stock']);  // 100 > 20 -> 100-10=90
        $this->assertEquals(40, (int) $rows[1]['stock']);  // 50 > 20 -> 50-10=40
        $this->assertEquals(5, (int) $rows[2]['stock']);   // 5 <= 20 -> unchanged
        $this->assertEquals(0, (int) $rows[3]['stock']);   // 0 not > 0 -> not updated
        $this->assertEquals(150, (int) $rows[4]['stock']); // 200 > 100 -> 200-50=150
    }

    /**
     * Multiple sequential stock decrements track correctly.
     */
    public function testSequentialDecrements(): void
    {
        for ($i = 0; $i < 5; $i++) {
            $this->mysqli->query("UPDATE mi_inv_products SET stock = stock - 1 WHERE id = 1");
        }

        $rows = $this->ztdQuery("SELECT stock FROM mi_inv_products WHERE id = 1");
        $this->assertEquals(95, (int) $rows[0]['stock']);
    }

    /**
     * Physical isolation: inventory changes don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("UPDATE mi_inv_products SET stock = stock - 50 WHERE id = 1");
        $this->mysqli->query("INSERT INTO mi_inv_orders VALUES (1, 1, 50, 'sale', '2024-03-15')");

        // Verify in ZTD
        $rows = $this->ztdQuery("SELECT stock FROM mi_inv_products WHERE id = 1");
        $this->assertEquals(50, (int) $rows[0]['stock']);

        // Physical table unchanged
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_inv_products');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
