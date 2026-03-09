<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests a split payment scenario: payment splitting across methods (credit card,
 * gift card, store credit) with partial refunds and SUM integrity validation (MySQLi).
 * @spec SPEC-10.2.115
 */
class SplitPaymentTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_sp_orders (
                id INT AUTO_INCREMENT PRIMARY KEY,
                customer_name VARCHAR(255),
                total_amount DECIMAL(10,2),
                status VARCHAR(255)
            )',
            'CREATE TABLE mi_sp_payment_splits (
                id INT AUTO_INCREMENT PRIMARY KEY,
                order_id INT,
                method VARCHAR(255),
                amount DECIMAL(10,2),
                refunded_amount DECIMAL(10,2) DEFAULT 0.00
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_sp_payment_splits', 'mi_sp_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 3 orders
        $this->mysqli->query("INSERT INTO mi_sp_orders VALUES (1, 'Alice', 200.00, 'paid')");
        $this->mysqli->query("INSERT INTO mi_sp_orders VALUES (2, 'Bob', 150.00, 'paid')");
        $this->mysqli->query("INSERT INTO mi_sp_orders VALUES (3, 'Carol', 80.00, 'pending')");

        // Payment splits for order 1 (sum=200)
        $this->mysqli->query("INSERT INTO mi_sp_payment_splits VALUES (1, 1, 'credit_card', 120.00, 0.00)");
        $this->mysqli->query("INSERT INTO mi_sp_payment_splits VALUES (2, 1, 'gift_card', 50.00, 0.00)");
        $this->mysqli->query("INSERT INTO mi_sp_payment_splits VALUES (3, 1, 'store_credit', 30.00, 0.00)");

        // Payment splits for order 2 (sum=150)
        $this->mysqli->query("INSERT INTO mi_sp_payment_splits VALUES (4, 2, 'credit_card', 150.00, 0.00)");

        // Payment splits for order 3 (sum=80)
        $this->mysqli->query("INSERT INTO mi_sp_payment_splits VALUES (5, 3, 'credit_card', 50.00, 0.00)");
        $this->mysqli->query("INSERT INTO mi_sp_payment_splits VALUES (6, 3, 'gift_card', 30.00, 0.00)");
    }

    /**
     * SUM of splits must equal order total for every order.
     */
    public function testSplitSumMatchesOrderTotal(): void
    {
        $rows = $this->ztdQuery(
            "SELECT o.id, o.total_amount, SUM(ps.amount) AS split_total
             FROM mi_sp_orders o
             JOIN mi_sp_payment_splits ps ON ps.order_id = o.id
             GROUP BY o.id, o.total_amount
             HAVING SUM(ps.amount) = o.total_amount
             ORDER BY o.id"
        );

        $this->assertCount(3, $rows);
        $this->assertEquals(1, (int) $rows[0]['id']);
        $this->assertEqualsWithDelta(200.0, (float) $rows[0]['split_total'], 0.01);
        $this->assertEquals(2, (int) $rows[1]['id']);
        $this->assertEqualsWithDelta(150.0, (float) $rows[1]['split_total'], 0.01);
        $this->assertEquals(3, (int) $rows[2]['id']);
        $this->assertEqualsWithDelta(80.0, (float) $rows[2]['split_total'], 0.01);
    }

    /**
     * Payment method breakdown: total amount per method across all orders.
     */
    public function testPaymentMethodBreakdown(): void
    {
        $rows = $this->ztdQuery(
            "SELECT method, SUM(amount) AS method_total
             FROM mi_sp_payment_splits
             GROUP BY method
             ORDER BY method_total DESC"
        );

        $this->assertCount(3, $rows);
        // credit_card: 120+150+50 = 320
        $this->assertSame('credit_card', $rows[0]['method']);
        $this->assertEqualsWithDelta(320.0, (float) $rows[0]['method_total'], 0.01);
        // gift_card: 50+30 = 80
        $this->assertSame('gift_card', $rows[1]['method']);
        $this->assertEqualsWithDelta(80.0, (float) $rows[1]['method_total'], 0.01);
        // store_credit: 30
        $this->assertSame('store_credit', $rows[2]['method']);
        $this->assertEqualsWithDelta(30.0, (float) $rows[2]['method_total'], 0.01);
    }

    /**
     * Partial refund: refund 20.00 on order 1 gift_card split, update order status.
     */
    public function testPartialRefund(): void
    {
        $this->mysqli->query("UPDATE mi_sp_payment_splits SET refunded_amount = 20.00 WHERE order_id = 1 AND method = 'gift_card'");
        $this->mysqli->query("UPDATE mi_sp_orders SET status = 'partially_refunded' WHERE id = 1");

        // Net amounts for order 1: credit_card 120-0=120, gift_card 50-20=30, store_credit 30-0=30
        $rows = $this->ztdQuery(
            "SELECT SUM(ps.amount - ps.refunded_amount) AS net_total
             FROM mi_sp_payment_splits ps
             WHERE ps.order_id = 1"
        );

        $this->assertCount(1, $rows);
        // 120 + 30 + 30 = 180
        $this->assertEqualsWithDelta(180.0, (float) $rows[0]['net_total'], 0.01);

        // Verify order status updated
        $rows = $this->ztdQuery("SELECT status FROM mi_sp_orders WHERE id = 1");
        $this->assertSame('partially_refunded', $rows[0]['status']);
    }

    /**
     * Net revenue per order after partial refund.
     */
    public function testNetRevenuePerOrder(): void
    {
        // Apply the refund first
        $this->mysqli->query("UPDATE mi_sp_payment_splits SET refunded_amount = 20.00 WHERE order_id = 1 AND method = 'gift_card'");

        $rows = $this->ztdQuery(
            "SELECT o.id, o.total_amount, SUM(ps.amount - ps.refunded_amount) AS net
             FROM mi_sp_orders o
             JOIN mi_sp_payment_splits ps ON ps.order_id = o.id
             GROUP BY o.id, o.total_amount
             ORDER BY o.id"
        );

        $this->assertCount(3, $rows);
        // Order 1: 120+30+30 = 180 net
        $this->assertEquals(1, (int) $rows[0]['id']);
        $this->assertEqualsWithDelta(180.0, (float) $rows[0]['net'], 0.01);
        // Order 2: 150 net
        $this->assertEquals(2, (int) $rows[1]['id']);
        $this->assertEqualsWithDelta(150.0, (float) $rows[1]['net'], 0.01);
        // Order 3: 80 net
        $this->assertEquals(3, (int) $rows[2]['id']);
        $this->assertEqualsWithDelta(80.0, (float) $rows[2]['net'], 0.01);
    }

    /**
     * Percentage of each payment method for order 1.
     */
    public function testMethodPercentagePerOrder(): void
    {
        $rows = $this->ztdQuery(
            "SELECT ps.method, ROUND(ps.amount * 100.0 / o.total_amount, 1) AS pct
             FROM mi_sp_payment_splits ps
             JOIN mi_sp_orders o ON o.id = ps.order_id
             WHERE o.id = 1
             ORDER BY pct DESC"
        );

        $this->assertCount(3, $rows);
        // credit_card: 120/200*100 = 60.0%
        $this->assertSame('credit_card', $rows[0]['method']);
        $this->assertEqualsWithDelta(60.0, (float) $rows[0]['pct'], 0.1);
        // gift_card: 50/200*100 = 25.0%
        $this->assertSame('gift_card', $rows[1]['method']);
        $this->assertEqualsWithDelta(25.0, (float) $rows[1]['pct'], 0.1);
        // store_credit: 30/200*100 = 15.0%
        $this->assertSame('store_credit', $rows[2]['method']);
        $this->assertEqualsWithDelta(15.0, (float) $rows[2]['pct'], 0.1);
    }

    /**
     * Add a zero-amount split and verify integrity (count increases, SUM unchanged).
     */
    public function testAddSplitAndVerifyIntegrity(): void
    {
        $this->mysqli->query("INSERT INTO mi_sp_payment_splits VALUES (7, 3, 'store_credit', 0.00, 0.00)");

        // Count of splits for order 3 should be 3
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt FROM mi_sp_payment_splits WHERE order_id = 3"
        );
        $this->assertEquals(3, (int) $rows[0]['cnt']);

        // SUM should still be 80
        $rows = $this->ztdQuery(
            "SELECT SUM(amount) AS total FROM mi_sp_payment_splits WHERE order_id = 3"
        );
        $this->assertEqualsWithDelta(80.0, (float) $rows[0]['total'], 0.01);
    }

    /**
     * Orders with more than one payment method.
     */
    public function testOrdersWithMultipleMethods(): void
    {
        $rows = $this->ztdQuery(
            "SELECT o.id, COUNT(ps.id) AS method_count
             FROM mi_sp_orders o
             JOIN mi_sp_payment_splits ps ON ps.order_id = o.id
             GROUP BY o.id
             HAVING COUNT(ps.id) > 1
             ORDER BY o.id"
        );

        $this->assertCount(2, $rows);
        // Order 1: 3 methods
        $this->assertEquals(1, (int) $rows[0]['id']);
        $this->assertEquals(3, (int) $rows[0]['method_count']);
        // Order 3: 2 methods
        $this->assertEquals(3, (int) $rows[1]['id']);
        $this->assertEquals(2, (int) $rows[1]['method_count']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_sp_payment_splits VALUES (7, 3, 'store_credit', 10.00, 0.00)");

        // ZTD sees the new split
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_sp_payment_splits");
        $this->assertEquals(7, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_sp_payment_splits');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
