<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests a SaaS subscription billing scenario: CASE expressions for status mapping,
 * LEFT JOIN for optional payment data, GROUP BY with SUM for revenue aggregation,
 * date string comparisons for overdue detection, COALESCE for defaults,
 * prepared statement for subscriber lookup, and physical isolation check (MySQL PDO).
 * @spec SPEC-10.2.149
 */
class MysqlSubscriptionBillingTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_sb_plans (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(50),
                price_monthly DECIMAL(10,2),
                features TEXT
            )',
            'CREATE TABLE mp_sb_subscribers (
                id INT AUTO_INCREMENT PRIMARY KEY,
                email VARCHAR(100),
                name VARCHAR(100),
                plan_id INT,
                status VARCHAR(20),
                start_date TEXT
            )',
            'CREATE TABLE mp_sb_invoices (
                id INT AUTO_INCREMENT PRIMARY KEY,
                subscriber_id INT,
                amount DECIMAL(10,2),
                due_date TEXT,
                status VARCHAR(20)
            )',
            'CREATE TABLE mp_sb_payments (
                id INT AUTO_INCREMENT PRIMARY KEY,
                invoice_id INT,
                amount_paid DECIMAL(10,2),
                payment_date TEXT,
                method VARCHAR(50)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_sb_payments', 'mp_sb_invoices', 'mp_sb_subscribers', 'mp_sb_plans'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 3 plans
        $this->pdo->exec("INSERT INTO mp_sb_plans VALUES (1, 'Basic', 9.99, 'Email support, 1 user')");
        $this->pdo->exec("INSERT INTO mp_sb_plans VALUES (2, 'Pro', 29.99, 'Priority support, 5 users, API access')");
        $this->pdo->exec("INSERT INTO mp_sb_plans VALUES (3, 'Enterprise', 99.99, 'Dedicated support, unlimited users, SLA')");

        // 5 subscribers
        $this->pdo->exec("INSERT INTO mp_sb_subscribers VALUES (1, 'alice@example.com', 'Alice Johnson', 1, 'active', '2025-01-15')");
        $this->pdo->exec("INSERT INTO mp_sb_subscribers VALUES (2, 'bob@example.com', 'Bob Smith', 2, 'active', '2025-02-01')");
        $this->pdo->exec("INSERT INTO mp_sb_subscribers VALUES (3, 'carol@example.com', 'Carol White', 3, 'active', '2025-03-10')");
        $this->pdo->exec("INSERT INTO mp_sb_subscribers VALUES (4, 'dan@example.com', 'Dan Brown', 1, 'cancelled', '2025-01-20')");
        $this->pdo->exec("INSERT INTO mp_sb_subscribers VALUES (5, 'eve@example.com', 'Eve Davis', 2, 'active', '2025-04-01')");

        // 8 invoices
        $this->pdo->exec("INSERT INTO mp_sb_invoices VALUES (1, 1, 9.99, '2025-02-15', 'paid')");
        $this->pdo->exec("INSERT INTO mp_sb_invoices VALUES (2, 1, 9.99, '2025-03-15', 'paid')");
        $this->pdo->exec("INSERT INTO mp_sb_invoices VALUES (3, 2, 29.99, '2025-03-01', 'paid')");
        $this->pdo->exec("INSERT INTO mp_sb_invoices VALUES (4, 2, 29.99, '2025-04-01', 'unpaid')");
        $this->pdo->exec("INSERT INTO mp_sb_invoices VALUES (5, 3, 99.99, '2025-04-10', 'paid')");
        $this->pdo->exec("INSERT INTO mp_sb_invoices VALUES (6, 3, 99.99, '2025-05-10', 'unpaid')");
        $this->pdo->exec("INSERT INTO mp_sb_invoices VALUES (7, 4, 9.99, '2025-02-20', 'overdue')");
        $this->pdo->exec("INSERT INTO mp_sb_invoices VALUES (8, 5, 29.99, '2025-05-01', 'unpaid')");

        // 5 payments (invoices 4, 6, 7, 8 have no payment)
        $this->pdo->exec("INSERT INTO mp_sb_payments VALUES (1, 1, 9.99, '2025-02-14', 'credit_card')");
        $this->pdo->exec("INSERT INTO mp_sb_payments VALUES (2, 2, 9.99, '2025-03-14', 'credit_card')");
        $this->pdo->exec("INSERT INTO mp_sb_payments VALUES (3, 3, 29.99, '2025-02-28', 'paypal')");
        $this->pdo->exec("INSERT INTO mp_sb_payments VALUES (4, 5, 99.99, '2025-04-09', 'bank_transfer')");
        $this->pdo->exec("INSERT INTO mp_sb_payments VALUES (5, 1, 5.00, '2025-02-15', 'credit_card')");
    }

    /**
     * JOIN plans and subscribers, filter active, ORDER BY subscriber name.
     */
    public function testActiveSubscriptionSummary(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.id, s.name AS subscriber_name, s.email, p.name AS plan_name, p.price_monthly, s.start_date
             FROM mp_sb_subscribers s
             JOIN mp_sb_plans p ON p.id = s.plan_id
             WHERE s.status = 'active'
             ORDER BY s.name"
        );

        $this->assertCount(4, $rows);

        $this->assertSame('Alice Johnson', $rows[0]['subscriber_name']);
        $this->assertSame('Basic', $rows[0]['plan_name']);
        $this->assertEquals(9.99, round((float) $rows[0]['price_monthly'], 2));

        $this->assertSame('Bob Smith', $rows[1]['subscriber_name']);
        $this->assertSame('Pro', $rows[1]['plan_name']);

        $this->assertSame('Carol White', $rows[2]['subscriber_name']);
        $this->assertSame('Enterprise', $rows[2]['plan_name']);

        $this->assertSame('Eve Davis', $rows[3]['subscriber_name']);
        $this->assertSame('Pro', $rows[3]['plan_name']);
    }

    /**
     * GROUP BY plan name with SUM of invoice amounts to show revenue per plan.
     */
    public function testRevenueByPlan(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.name AS plan_name, COUNT(i.id) AS invoice_count, SUM(i.amount) AS total_revenue
             FROM mp_sb_plans p
             JOIN mp_sb_subscribers s ON s.plan_id = p.id
             JOIN mp_sb_invoices i ON i.subscriber_id = s.id
             GROUP BY p.name
             ORDER BY total_revenue DESC"
        );

        $this->assertCount(3, $rows);

        // Enterprise: 2 invoices x 99.99 = 199.98
        $this->assertSame('Enterprise', $rows[0]['plan_name']);
        $this->assertEquals(2, (int) $rows[0]['invoice_count']);
        $this->assertEquals(199.98, round((float) $rows[0]['total_revenue'], 2));

        // Pro: 3 invoices (29.99 + 29.99 + 29.99) = 89.97
        $this->assertSame('Pro', $rows[1]['plan_name']);
        $this->assertEquals(3, (int) $rows[1]['invoice_count']);
        $this->assertEquals(89.97, round((float) $rows[1]['total_revenue'], 2));

        // Basic: 3 invoices (9.99 + 9.99 + 9.99) = 29.97
        $this->assertSame('Basic', $rows[2]['plan_name']);
        $this->assertEquals(3, (int) $rows[2]['invoice_count']);
        $this->assertEquals(29.97, round((float) $rows[2]['total_revenue'], 2));
    }

    /**
     * LEFT JOIN payments on invoices; COALESCE unpaid amounts to 'none' for display.
     * Shows invoice history for subscriber 1 (Alice) with payment info.
     */
    public function testSubscriberInvoiceHistory(): void
    {
        $rows = $this->ztdQuery(
            "SELECT i.id AS invoice_id, i.amount, i.due_date, i.status AS invoice_status,
                    COALESCE(p.method, 'none') AS payment_method,
                    COALESCE(p.payment_date, 'N/A') AS payment_date
             FROM mp_sb_invoices i
             LEFT JOIN mp_sb_payments p ON p.invoice_id = i.id
             WHERE i.subscriber_id = 1
             ORDER BY i.due_date, p.id"
        );

        // Alice has 2 invoices; invoice 1 has 2 payments, invoice 2 has 1 payment
        $this->assertCount(3, $rows);

        $this->assertEquals(1, (int) $rows[0]['invoice_id']);
        $this->assertSame('credit_card', $rows[0]['payment_method']);
        $this->assertSame('2025-02-14', $rows[0]['payment_date']);

        $this->assertEquals(1, (int) $rows[1]['invoice_id']);
        $this->assertSame('credit_card', $rows[1]['payment_method']);
        $this->assertSame('2025-02-15', $rows[1]['payment_date']);

        $this->assertEquals(2, (int) $rows[2]['invoice_id']);
        $this->assertSame('credit_card', $rows[2]['payment_method']);
    }

    /**
     * Date comparison for overdue invoices; CASE expression for days-overdue category.
     * Invoices with due_date < '2025-11-01' and status in ('unpaid','overdue').
     */
    public function testOverdueInvoices(): void
    {
        $rows = $this->ztdQuery(
            "SELECT i.id, s.name AS subscriber_name, i.amount, i.due_date, i.status,
                    CASE
                        WHEN i.due_date < '2025-03-01' THEN 'critical'
                        WHEN i.due_date < '2025-05-01' THEN 'warning'
                        ELSE 'recent'
                    END AS overdue_category
             FROM mp_sb_invoices i
             JOIN mp_sb_subscribers s ON s.id = i.subscriber_id
             WHERE i.due_date < '2025-11-01'
               AND i.status IN ('unpaid', 'overdue')
             ORDER BY i.due_date"
        );

        $this->assertCount(4, $rows);

        // Invoice 7: Dan, overdue, due 2025-02-20 => critical
        $this->assertEquals(7, (int) $rows[0]['id']);
        $this->assertSame('Dan Brown', $rows[0]['subscriber_name']);
        $this->assertSame('critical', $rows[0]['overdue_category']);

        // Invoice 4: Bob, unpaid, due 2025-04-01 => warning
        $this->assertEquals(4, (int) $rows[1]['id']);
        $this->assertSame('Bob Smith', $rows[1]['subscriber_name']);
        $this->assertSame('warning', $rows[1]['overdue_category']);

        // Invoice 8: Eve, unpaid, due 2025-05-01 => recent
        $this->assertEquals(8, (int) $rows[2]['id']);
        $this->assertSame('Eve Davis', $rows[2]['subscriber_name']);
        $this->assertSame('recent', $rows[2]['overdue_category']);

        // Invoice 6: Carol, unpaid, due 2025-05-10 => recent
        $this->assertEquals(6, (int) $rows[3]['id']);
        $this->assertSame('Carol White', $rows[3]['subscriber_name']);
        $this->assertSame('recent', $rows[3]['overdue_category']);
    }

    /**
     * Prepared statement: look up a subscriber by email with plan details.
     */
    public function testPreparedSubscriberLookup(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT s.name, s.email, p.name AS plan_name, p.price_monthly, s.status, s.start_date
             FROM mp_sb_subscribers s
             JOIN mp_sb_plans p ON p.id = s.plan_id
             WHERE s.email = ?",
            ['bob@example.com']
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Bob Smith', $rows[0]['name']);
        $this->assertSame('Pro', $rows[0]['plan_name']);
        $this->assertEquals(29.99, round((float) $rows[0]['price_monthly'], 2));
        $this->assertSame('active', $rows[0]['status']);
    }

    /**
     * Upgrade a subscriber's plan and add a new invoice; verify via shadow reads.
     */
    public function testUpgradeSubscriptionAndVerify(): void
    {
        // Upgrade Alice from Basic (plan 1) to Pro (plan 2)
        $affected = $this->pdo->exec("UPDATE mp_sb_subscribers SET plan_id = 2 WHERE id = 1");
        $this->assertSame(1, $affected);

        // Generate a new invoice for the upgraded plan
        $this->pdo->exec("INSERT INTO mp_sb_invoices VALUES (9, 1, 29.99, '2025-04-15', 'unpaid')");

        // Verify plan change is visible
        $rows = $this->ztdQuery(
            "SELECT s.name, p.name AS plan_name
             FROM mp_sb_subscribers s
             JOIN mp_sb_plans p ON p.id = s.plan_id
             WHERE s.id = 1"
        );
        $this->assertCount(1, $rows);
        $this->assertSame('Alice Johnson', $rows[0]['name']);
        $this->assertSame('Pro', $rows[0]['plan_name']);

        // Verify new invoice is visible
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt, SUM(amount) AS total
             FROM mp_sb_invoices
             WHERE subscriber_id = 1"
        );
        $this->assertEquals(3, (int) $rows[0]['cnt']);
        $this->assertEquals(49.97, round((float) $rows[0]['total'], 2));
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        // Insert an extra payment via shadow
        $this->pdo->exec("INSERT INTO mp_sb_payments VALUES (6, 4, 29.99, '2025-04-02', 'credit_card')");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_sb_payments");
        $this->assertEquals(6, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mp_sb_payments")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
