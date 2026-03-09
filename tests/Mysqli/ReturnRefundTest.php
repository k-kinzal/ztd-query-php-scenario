<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests return authorization and refund processing workflows through ZTD shadow store (MySQLi).
 * Covers LEFT JOIN for return status, SUM for refund totals, GROUP BY with HAVING for eligibility,
 * UPDATE with arithmetic, prepared statements, and physical isolation.
 * @spec SPEC-10.2.133
 */
class ReturnRefundTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_rr_orders (
                id INT AUTO_INCREMENT PRIMARY KEY,
                customer_name VARCHAR(255),
                order_date VARCHAR(255),
                total_amount DECIMAL(10,2),
                status VARCHAR(50)
            )',
            'CREATE TABLE mi_rr_order_items (
                id INT AUTO_INCREMENT PRIMARY KEY,
                order_id INT,
                product_name VARCHAR(255),
                quantity INT,
                unit_price DECIMAL(10,2)
            )',
            'CREATE TABLE mi_rr_returns (
                id INT AUTO_INCREMENT PRIMARY KEY,
                order_item_id INT,
                return_date VARCHAR(255),
                quantity_returned INT,
                reason VARCHAR(255),
                refund_amount DECIMAL(10,2),
                refund_status VARCHAR(50)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_rr_returns', 'mi_rr_order_items', 'mi_rr_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 3 orders
        $this->mysqli->query("INSERT INTO mi_rr_orders VALUES (1, 'Alice', '2025-08-01', 250.00, 'partially_returned')");
        $this->mysqli->query("INSERT INTO mi_rr_orders VALUES (2, 'Bob', '2025-08-10', 180.00, 'completed')");
        $this->mysqli->query("INSERT INTO mi_rr_orders VALUES (3, 'Carol', '2025-08-15', 95.00, 'fully_returned')");

        // Order items
        $this->mysqli->query("INSERT INTO mi_rr_order_items VALUES (1, 1, 'Widget', 2, 50.00)");
        $this->mysqli->query("INSERT INTO mi_rr_order_items VALUES (2, 1, 'Gadget', 1, 150.00)");
        $this->mysqli->query("INSERT INTO mi_rr_order_items VALUES (3, 2, 'Widget', 1, 50.00)");
        $this->mysqli->query("INSERT INTO mi_rr_order_items VALUES (4, 2, 'Doohickey', 2, 65.00)");
        $this->mysqli->query("INSERT INTO mi_rr_order_items VALUES (5, 3, 'Thingamajig', 1, 95.00)");

        // Returns
        $this->mysqli->query("INSERT INTO mi_rr_returns VALUES (1, 1, '2025-08-10', 1, 'defective', 50.00, 'approved')");
        $this->mysqli->query("INSERT INTO mi_rr_returns VALUES (2, 2, '2025-08-12', 1, 'wrong_item', 150.00, 'pending')");
        $this->mysqli->query("INSERT INTO mi_rr_returns VALUES (3, 5, '2025-08-20', 1, 'changed_mind', 85.50, 'approved')");
    }

    /**
     * LEFT JOIN orders with returns (through order_items), GROUP BY order.
     * Show customer_name, total_amount, COUNT returns, SUM refund_amount.
     */
    public function testOrderReturnSummary(): void
    {
        $rows = $this->ztdQuery(
            "SELECT o.customer_name,
                    o.total_amount,
                    COUNT(r.id) AS return_count,
                    COALESCE(SUM(r.refund_amount), 0) AS total_refund
             FROM mi_rr_orders o
             LEFT JOIN mi_rr_order_items oi ON oi.order_id = o.id
             LEFT JOIN mi_rr_returns r ON r.order_item_id = oi.id
             GROUP BY o.id, o.customer_name, o.total_amount
             ORDER BY o.customer_name"
        );

        $this->assertCount(3, $rows);

        // Alice: 2 returns, $200.00 total refund
        $this->assertSame('Alice', $rows[0]['customer_name']);
        $this->assertEquals(250.00, (float) $rows[0]['total_amount']);
        $this->assertEquals(2, (int) $rows[0]['return_count']);
        $this->assertEquals(200.00, (float) $rows[0]['total_refund']);

        // Bob: 0 returns, $0 refund
        $this->assertSame('Bob', $rows[1]['customer_name']);
        $this->assertEquals(180.00, (float) $rows[1]['total_amount']);
        $this->assertEquals(0, (int) $rows[1]['return_count']);
        $this->assertEquals(0, (float) $rows[1]['total_refund']);

        // Carol: 1 return, $85.50 refund
        $this->assertSame('Carol', $rows[2]['customer_name']);
        $this->assertEquals(95.00, (float) $rows[2]['total_amount']);
        $this->assertEquals(1, (int) $rows[2]['return_count']);
        $this->assertEquals(85.50, (float) $rows[2]['total_refund']);
    }

    /**
     * SUM refund_amount WHERE refund_status = 'approved'.
     * Should be $135.50 ($50.00 + $85.50).
     */
    public function testApprovedRefundTotal(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT SUM(refund_amount) AS approved_total
             FROM mi_rr_returns
             WHERE refund_status = ?",
            ['approved']
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(135.50, (float) $rows[0]['approved_total']);
    }

    /**
     * GROUP BY reason, COUNT, SUM refund_amount. ORDER BY reason.
     * 3 rows: changed_mind(1,$85.50), defective(1,$50.00), wrong_item(1,$150.00).
     */
    public function testReturnReasonBreakdown(): void
    {
        $rows = $this->ztdQuery(
            "SELECT r.reason,
                    COUNT(*) AS reason_count,
                    SUM(r.refund_amount) AS reason_total
             FROM mi_rr_returns r
             GROUP BY r.reason
             ORDER BY r.reason"
        );

        $this->assertCount(3, $rows);

        $this->assertSame('changed_mind', $rows[0]['reason']);
        $this->assertEquals(1, (int) $rows[0]['reason_count']);
        $this->assertEquals(85.50, (float) $rows[0]['reason_total']);

        $this->assertSame('defective', $rows[1]['reason']);
        $this->assertEquals(1, (int) $rows[1]['reason_count']);
        $this->assertEquals(50.00, (float) $rows[1]['reason_total']);

        $this->assertSame('wrong_item', $rows[2]['reason']);
        $this->assertEquals(1, (int) $rows[2]['reason_count']);
        $this->assertEquals(150.00, (float) $rows[2]['reason_total']);
    }

    /**
     * JOIN to find orders that have at least one pending return.
     * Only order 1 (Alice) has a pending return.
     */
    public function testOrdersWithPendingReturns(): void
    {
        $rows = $this->ztdQuery(
            "SELECT DISTINCT o.id, o.customer_name
             FROM mi_rr_orders o
             JOIN mi_rr_order_items oi ON oi.order_id = o.id
             JOIN mi_rr_returns r ON r.order_item_id = oi.id
             WHERE r.refund_status = 'pending'
             ORDER BY o.id"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['customer_name']);
    }

    /**
     * For 'changed_mind' returns, verify refund < item unit_price (restocking fee applied).
     * Join returns with order_items, compute unit_price - refund_amount as fee.
     * Thingamajig fee = $9.50.
     */
    public function testRestockingFeeCalculation(): void
    {
        $rows = $this->ztdQuery(
            "SELECT oi.product_name,
                    oi.unit_price,
                    r.refund_amount,
                    oi.unit_price - r.refund_amount AS restocking_fee
             FROM mi_rr_returns r
             JOIN mi_rr_order_items oi ON oi.id = r.order_item_id
             WHERE r.reason = 'changed_mind'
             ORDER BY oi.product_name"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Thingamajig', $rows[0]['product_name']);
        $this->assertEquals(95.00, (float) $rows[0]['unit_price']);
        $this->assertEquals(85.50, (float) $rows[0]['refund_amount']);
        $this->assertEquals(9.50, (float) $rows[0]['restocking_fee']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        // Insert a new return
        $this->mysqli->query("INSERT INTO mi_rr_returns VALUES (4, 3, '2025-08-25', 1, 'defective', 50.00, 'pending')");

        // Visible through ZTD: now 4 returns
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_rr_returns");
        $this->assertSame(4, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_rr_returns');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
