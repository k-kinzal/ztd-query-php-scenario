<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests a subscription billing workflow through ZTD shadow store (PostgreSQL PDO).
 * Covers billing record generation, credit application, balance tracking,
 * conditional status updates, and physical isolation.
 * @spec SPEC-10.2.70
 */
class PostgresSubscriptionBillingTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_sb_subscriptions (
                id INTEGER PRIMARY KEY,
                customer_name VARCHAR(255),
                plan VARCHAR(50),
                monthly_rate NUMERIC(10,2),
                status VARCHAR(20),
                started_at TIMESTAMP
            )',
            'CREATE TABLE pg_sb_billing (
                id INTEGER PRIMARY KEY,
                subscription_id INTEGER,
                amount NUMERIC(10,2),
                billing_date DATE,
                paid SMALLINT
            )',
            'CREATE TABLE pg_sb_credits (
                id INTEGER PRIMARY KEY,
                subscription_id INTEGER,
                amount NUMERIC(10,2),
                reason VARCHAR(255),
                applied_at TIMESTAMP
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_sb_credits', 'pg_sb_billing', 'pg_sb_subscriptions'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 3 subscriptions
        $this->pdo->exec("INSERT INTO pg_sb_subscriptions VALUES (1, 'Alice', 'premium', 99.99, 'active', '2025-01-01 00:00:00')");
        $this->pdo->exec("INSERT INTO pg_sb_subscriptions VALUES (2, 'Bob', 'basic', 29.99, 'active', '2025-02-01 00:00:00')");
        $this->pdo->exec("INSERT INTO pg_sb_subscriptions VALUES (3, 'Charlie', 'premium', 99.99, 'suspended', '2025-03-01 00:00:00')");

        // Billing records
        $this->pdo->exec("INSERT INTO pg_sb_billing VALUES (1, 1, 99.99, '2025-02-01', 1)");
        $this->pdo->exec("INSERT INTO pg_sb_billing VALUES (2, 1, 99.99, '2025-03-01', 1)");
        $this->pdo->exec("INSERT INTO pg_sb_billing VALUES (3, 1, 99.99, '2025-04-01', 0)");
        $this->pdo->exec("INSERT INTO pg_sb_billing VALUES (4, 2, 29.99, '2025-03-01', 1)");
        $this->pdo->exec("INSERT INTO pg_sb_billing VALUES (5, 2, 29.99, '2025-04-01', 0)");

        // Credits
        $this->pdo->exec("INSERT INTO pg_sb_credits VALUES (1, 1, 25.00, 'Referral bonus', '2025-03-15 10:00:00')");
        $this->pdo->exec("INSERT INTO pg_sb_credits VALUES (2, 1, 10.00, 'Service outage', '2025-04-01 12:00:00')");
    }

    /**
     * List active subscriptions with total billed and total paid amounts.
     */
    public function testActiveSubscriptionsWithBilling(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.id, s.customer_name, s.plan,
                    COUNT(b.id) AS bill_count,
                    SUM(b.amount) AS total_billed,
                    SUM(CASE WHEN b.paid = 1 THEN b.amount ELSE 0 END) AS total_paid
             FROM pg_sb_subscriptions s
             LEFT JOIN pg_sb_billing b ON b.subscription_id = s.id
             WHERE s.status = 'active'
             GROUP BY s.id, s.customer_name, s.plan
             ORDER BY s.id"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['customer_name']);
        $this->assertEquals(3, (int) $rows[0]['bill_count']);
        $this->assertEquals(299.97, round((float) $rows[0]['total_billed'], 2));
        $this->assertEquals(199.98, round((float) $rows[0]['total_paid'], 2));
    }

    /**
     * Generate a new billing record for a subscription.
     */
    public function testGenerateBillingRecord(): void
    {
        $this->pdo->exec("INSERT INTO pg_sb_billing VALUES (6, 2, 29.99, '2025-05-01', 0)");

        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt, SUM(amount) AS total
             FROM pg_sb_billing
             WHERE subscription_id = 2"
        );

        $this->assertEquals(3, (int) $rows[0]['cnt']);
        $this->assertEquals(89.97, round((float) $rows[0]['total'], 2));
    }

    /**
     * Calculate effective balance: total billed (unpaid) minus credits.
     */
    public function testBalanceWithCredits(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.id, s.customer_name,
                    COALESCE((SELECT SUM(b.amount) FROM pg_sb_billing b WHERE b.subscription_id = s.id AND b.paid = 0), 0) AS total_unpaid,
                    COALESCE((SELECT SUM(c.amount) FROM pg_sb_credits c WHERE c.subscription_id = s.id), 0) AS total_credits
             FROM pg_sb_subscriptions s
             WHERE s.id = 1"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(99.99, round((float) $rows[0]['total_unpaid'], 2));
        $this->assertEquals(35.00, round((float) $rows[0]['total_credits'], 2));
    }

    /**
     * Apply a new credit and verify updated totals.
     */
    public function testApplyCredit(): void
    {
        $this->pdo->exec("INSERT INTO pg_sb_credits VALUES (3, 2, 15.00, 'Loyalty discount', '2025-04-10 09:00:00')");

        $rows = $this->ztdQuery(
            "SELECT SUM(amount) AS total_credits
             FROM pg_sb_credits
             WHERE subscription_id = 2"
        );

        $this->assertEquals(15.00, round((float) $rows[0]['total_credits'], 2));
    }

    /**
     * Mark unpaid billing as paid and verify via conditional aggregation.
     */
    public function testMarkBillingPaid(): void
    {
        // Pay the outstanding bill for subscription 1
        $affected = $this->pdo->exec("UPDATE pg_sb_billing SET paid = 1 WHERE id = 3 AND paid = 0");
        $this->assertSame(1, $affected);

        // All bills for subscription 1 should now be paid
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS total_bills,
                    SUM(CASE WHEN paid = 1 THEN 1 ELSE 0 END) AS paid_bills,
                    SUM(CASE WHEN paid = 0 THEN 1 ELSE 0 END) AS unpaid_bills
             FROM pg_sb_billing
             WHERE subscription_id = 1"
        );

        $this->assertEquals(3, (int) $rows[0]['total_bills']);
        $this->assertEquals(3, (int) $rows[0]['paid_bills']);
        $this->assertEquals(0, (int) $rows[0]['unpaid_bills']);
    }

    /**
     * Prepared statement: billing history for a subscription filtered by date range.
     */
    public function testBillingHistoryPrepared(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT b.id, b.amount, b.billing_date, b.paid
             FROM pg_sb_billing b
             WHERE b.subscription_id = ?
               AND b.billing_date >= ?
               AND b.billing_date <= ?
             ORDER BY b.billing_date",
            [1, '2025-02-01', '2025-03-31']
        );

        $this->assertCount(2, $rows);
        $this->assertEquals(99.99, round((float) $rows[0]['amount'], 2));
        $this->assertEquals(99.99, round((float) $rows[1]['amount'], 2));
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_sb_billing VALUES (6, 3, 99.99, '2025-04-01', 0)");
        $this->pdo->exec("UPDATE pg_sb_subscriptions SET status = 'cancelled' WHERE id = 3");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_sb_billing");
        $this->assertEquals(6, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT status FROM pg_sb_subscriptions WHERE id = 3");
        $this->assertSame('cancelled', $rows[0]['status']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_sb_billing")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
