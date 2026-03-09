<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests a multi-stage order fulfillment workflow through ZTD shadow store (MySQL PDO).
 * Covers line item fulfillment, partial/complete status transitions,
 * order summary reporting, and physical isolation.
 * @spec SPEC-10.2.73
 */
class MysqlOrderFulfillmentTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_of_orders (
                id INT PRIMARY KEY,
                customer_name VARCHAR(255),
                status VARCHAR(20),
                order_date DATETIME
            )',
            'CREATE TABLE mp_of_items (
                id INT PRIMARY KEY,
                order_id INT,
                product_name VARCHAR(255),
                quantity INT,
                unit_price DECIMAL(10,2),
                status VARCHAR(20)
            )',
            'CREATE TABLE mp_of_fulfillments (
                id INT PRIMARY KEY,
                item_id INT,
                shipped_quantity INT,
                shipped_at DATETIME,
                tracking_code VARCHAR(50)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_of_fulfillments', 'mp_of_items', 'mp_of_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 2 orders
        $this->pdo->exec("INSERT INTO mp_of_orders VALUES (1, 'Alice', 'processing', '2026-03-01 09:00:00')");
        $this->pdo->exec("INSERT INTO mp_of_orders VALUES (2, 'Bob', 'processing', '2026-03-02 10:00:00')");

        // Order 1: 3 items
        $this->pdo->exec("INSERT INTO mp_of_items VALUES (1, 1, 'Laptop', 1, 999.99, 'pending')");
        $this->pdo->exec("INSERT INTO mp_of_items VALUES (2, 1, 'Mouse', 2, 29.99, 'pending')");
        $this->pdo->exec("INSERT INTO mp_of_items VALUES (3, 1, 'Keyboard', 1, 79.99, 'pending')");

        // Order 2: 2 items
        $this->pdo->exec("INSERT INTO mp_of_items VALUES (4, 2, 'Monitor', 2, 349.99, 'pending')");
        $this->pdo->exec("INSERT INTO mp_of_items VALUES (5, 2, 'Webcam', 1, 89.99, 'pending')");
    }

    /**
     * List orders with item counts and total values.
     */
    public function testOrderSummaryList(): void
    {
        $rows = $this->ztdQuery(
            "SELECT o.id, o.customer_name, o.status,
                    COUNT(i.id) AS item_count,
                    SUM(i.quantity * i.unit_price) AS total_value
             FROM mp_of_orders o
             LEFT JOIN mp_of_items i ON i.order_id = o.id
             GROUP BY o.id, o.customer_name, o.status
             ORDER BY o.id"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['customer_name']);
        $this->assertEquals(3, (int) $rows[0]['item_count']);
        $this->assertEquals(1139.96, round((float) $rows[0]['total_value'], 2));

        $this->assertSame('Bob', $rows[1]['customer_name']);
        $this->assertEquals(2, (int) $rows[1]['item_count']);
        $this->assertEquals(789.97, round((float) $rows[1]['total_value'], 2));
    }

    /**
     * Fulfill a single item: INSERT fulfillment, UPDATE item status.
     */
    public function testFulfillSingleItem(): void
    {
        // Ship the laptop
        $this->pdo->exec("INSERT INTO mp_of_fulfillments VALUES (1, 1, 1, '2026-03-05 14:00:00', 'TRACK-001')");
        $affected = $this->pdo->exec("UPDATE mp_of_items SET status = 'shipped' WHERE id = 1 AND status = 'pending'");
        $this->assertSame(1, $affected);

        // Verify fulfillment
        $rows = $this->ztdQuery(
            "SELECT i.product_name, i.status, f.tracking_code
             FROM mp_of_items i
             JOIN mp_of_fulfillments f ON f.item_id = i.id
             WHERE i.id = 1"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('shipped', $rows[0]['status']);
        $this->assertSame('TRACK-001', $rows[0]['tracking_code']);
    }

    /**
     * Partial fulfillment: some items shipped, order stays 'processing'.
     */
    public function testPartialFulfillment(): void
    {
        // Ship laptop only
        $this->pdo->exec("INSERT INTO mp_of_fulfillments VALUES (1, 1, 1, '2026-03-05 14:00:00', 'TRACK-001')");
        $this->pdo->exec("UPDATE mp_of_items SET status = 'shipped' WHERE id = 1");

        // Check fulfillment status per order
        $rows = $this->ztdQuery(
            "SELECT o.id,
                    COUNT(i.id) AS total_items,
                    SUM(CASE WHEN i.status = 'shipped' THEN 1 ELSE 0 END) AS shipped_items,
                    SUM(CASE WHEN i.status = 'pending' THEN 1 ELSE 0 END) AS pending_items
             FROM mp_of_orders o
             JOIN mp_of_items i ON i.order_id = o.id
             WHERE o.id = 1
             GROUP BY o.id"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(3, (int) $rows[0]['total_items']);
        $this->assertEquals(1, (int) $rows[0]['shipped_items']);
        $this->assertEquals(2, (int) $rows[0]['pending_items']);
    }

    /**
     * Complete fulfillment: all items shipped, update order status.
     */
    public function testCompleteFulfillment(): void
    {
        // Ship all 3 items of order 1
        $this->pdo->exec("INSERT INTO mp_of_fulfillments VALUES (1, 1, 1, '2026-03-05 14:00:00', 'TRACK-001')");
        $this->pdo->exec("INSERT INTO mp_of_fulfillments VALUES (2, 2, 2, '2026-03-05 14:00:00', 'TRACK-001')");
        $this->pdo->exec("INSERT INTO mp_of_fulfillments VALUES (3, 3, 1, '2026-03-05 14:00:00', 'TRACK-002')");
        $this->pdo->exec("UPDATE mp_of_items SET status = 'shipped' WHERE order_id = 1");

        // Check all items shipped
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS pending_count
             FROM mp_of_items
             WHERE order_id = 1 AND status != 'shipped'"
        );
        $this->assertEquals(0, (int) $rows[0]['pending_count']);

        // Update order status
        $this->pdo->exec("UPDATE mp_of_orders SET status = 'shipped' WHERE id = 1");

        $rows = $this->ztdQuery("SELECT status FROM mp_of_orders WHERE id = 1");
        $this->assertSame('shipped', $rows[0]['status']);
    }

    /**
     * Fulfillment report: summary across all orders with conditional counts.
     */
    public function testFulfillmentReport(): void
    {
        // Partially fulfill order 1 (laptop only), fully fulfill order 2
        $this->pdo->exec("INSERT INTO mp_of_fulfillments VALUES (1, 1, 1, '2026-03-05 14:00:00', 'TRACK-001')");
        $this->pdo->exec("UPDATE mp_of_items SET status = 'shipped' WHERE id = 1");
        $this->pdo->exec("INSERT INTO mp_of_fulfillments VALUES (2, 4, 2, '2026-03-06 09:00:00', 'TRACK-003')");
        $this->pdo->exec("INSERT INTO mp_of_fulfillments VALUES (3, 5, 1, '2026-03-06 09:00:00', 'TRACK-003')");
        $this->pdo->exec("UPDATE mp_of_items SET status = 'shipped' WHERE order_id = 2");

        $rows = $this->ztdQuery(
            "SELECT o.id, o.customer_name,
                    COUNT(i.id) AS total_items,
                    SUM(CASE WHEN i.status = 'shipped' THEN 1 ELSE 0 END) AS shipped,
                    SUM(CASE WHEN i.status = 'pending' THEN 1 ELSE 0 END) AS pending,
                    SUM(i.quantity * i.unit_price) AS order_value
             FROM mp_of_orders o
             JOIN mp_of_items i ON i.order_id = o.id
             GROUP BY o.id, o.customer_name
             ORDER BY o.id"
        );

        $this->assertCount(2, $rows);
        // Order 1: 1 shipped, 2 pending
        $this->assertEquals(1, (int) $rows[0]['shipped']);
        $this->assertEquals(2, (int) $rows[0]['pending']);
        // Order 2: 2 shipped, 0 pending
        $this->assertEquals(2, (int) $rows[1]['shipped']);
        $this->assertEquals(0, (int) $rows[1]['pending']);
    }

    /**
     * Prepared statement: look up an order by customer name.
     */
    public function testOrderLookupPrepared(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT o.id, o.status,
                    COUNT(i.id) AS item_count,
                    SUM(i.quantity * i.unit_price) AS total_value
             FROM mp_of_orders o
             JOIN mp_of_items i ON i.order_id = o.id
             WHERE o.customer_name = ?
             GROUP BY o.id, o.status",
            ['Bob']
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(2, (int) $rows[0]['item_count']);
        $this->assertEquals(789.97, round((float) $rows[0]['total_value'], 2));
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO mp_of_fulfillments VALUES (1, 1, 1, '2026-03-05 14:00:00', 'TRACK-001')");
        $this->pdo->exec("UPDATE mp_of_items SET status = 'shipped' WHERE id = 1");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_of_fulfillments");
        $this->assertEquals(1, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mp_of_fulfillments")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
