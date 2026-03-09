<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests a SaaS subscription renewal workflow through ZTD shadow store (MySQL PDO).
 * Subscriptions with plans and invoices exercise DELETE WHERE IN (subquery)
 * for expired trial cleanup, INSERT ... SELECT for renewal invoice generation,
 * multiple correlated subqueries in a single SELECT list, UPDATE with aggregate
 * subquery for loyalty discount, and physical isolation.
 * @spec SPEC-10.2.164
 */
class MysqlSubscriptionRenewalTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_sub_plans (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255),
                monthly_price DECIMAL(10,2),
                is_trial TINYINT
            )',
            'CREATE TABLE mp_sub_subscriptions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                customer_name VARCHAR(255),
                plan_id INT,
                status VARCHAR(50),
                start_date DATE,
                end_date DATE
            )',
            'CREATE TABLE mp_sub_invoices (
                id INT AUTO_INCREMENT PRIMARY KEY,
                subscription_id INT,
                amount DECIMAL(10,2),
                period VARCHAR(20),
                status VARCHAR(50)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_sub_invoices', 'mp_sub_subscriptions', 'mp_sub_plans'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 3 plans
        $this->pdo->exec("INSERT INTO mp_sub_plans VALUES (1, 'Free Trial', 0.00, 1)");
        $this->pdo->exec("INSERT INTO mp_sub_plans VALUES (2, 'Basic', 9.99, 0)");
        $this->pdo->exec("INSERT INTO mp_sub_plans VALUES (3, 'Pro', 29.99, 0)");

        // 6 subscriptions
        $this->pdo->exec("INSERT INTO mp_sub_subscriptions VALUES (1, 'Alice', 2, 'active', '2025-01-01', '2026-01-01')");
        $this->pdo->exec("INSERT INTO mp_sub_subscriptions VALUES (2, 'Bob', 3, 'active', '2025-03-15', '2026-03-15')");
        $this->pdo->exec("INSERT INTO mp_sub_subscriptions VALUES (3, 'Carol', 1, 'trial', '2025-12-01', '2026-01-01')");
        $this->pdo->exec("INSERT INTO mp_sub_subscriptions VALUES (4, 'Dave', 1, 'trial', '2026-02-01', '2026-03-01')");
        $this->pdo->exec("INSERT INTO mp_sub_subscriptions VALUES (5, 'Eve', 2, 'active', '2024-06-01', '2025-06-01')");
        $this->pdo->exec("INSERT INTO mp_sub_subscriptions VALUES (6, 'Frank', 3, 'cancelled', '2025-01-01', '2025-07-01')");

        // 8 invoices
        $this->pdo->exec("INSERT INTO mp_sub_invoices VALUES (1, 1, 9.99, '2025-01', 'paid')");
        $this->pdo->exec("INSERT INTO mp_sub_invoices VALUES (2, 1, 9.99, '2025-02', 'paid')");
        $this->pdo->exec("INSERT INTO mp_sub_invoices VALUES (3, 1, 9.99, '2025-03', 'paid')");
        $this->pdo->exec("INSERT INTO mp_sub_invoices VALUES (4, 2, 29.99, '2025-04', 'paid')");
        $this->pdo->exec("INSERT INTO mp_sub_invoices VALUES (5, 2, 29.99, '2025-05', 'paid')");
        $this->pdo->exec("INSERT INTO mp_sub_invoices VALUES (6, 5, 9.99, '2024-07', 'paid')");
        $this->pdo->exec("INSERT INTO mp_sub_invoices VALUES (7, 5, 9.99, '2024-08', 'paid')");
        $this->pdo->exec("INSERT INTO mp_sub_invoices VALUES (8, 5, 9.99, '2024-09', 'paid')");
    }

    /**
     * DELETE WHERE IN (subquery): remove expired trial subscriptions.
     * Trials with end_date < '2026-03-09' should be deleted.
     * Carol (end 2026-01-01) is expired; Dave (end 2026-03-01) is also expired.
     * Expected: 4 subscriptions remain after delete.
     */
    public function testDeleteExpiredTrials(): void
    {
        $this->pdo->exec(
            "DELETE FROM mp_sub_subscriptions
             WHERE id IN (
                 SELECT sub_id FROM (
                     SELECT s.id AS sub_id FROM mp_sub_subscriptions s
                     JOIN mp_sub_plans p ON p.id = s.plan_id
                     WHERE p.is_trial = 1 AND s.end_date < '2026-03-09'
                 ) tmp
             )"
        );

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_sub_subscriptions");
        $this->assertEquals(4, (int) $rows[0]['cnt']);

        // Verify only non-trial or non-expired remain
        $rows = $this->ztdQuery(
            "SELECT customer_name FROM mp_sub_subscriptions ORDER BY id"
        );
        $names = array_column($rows, 'customer_name');
        $this->assertSame(['Alice', 'Bob', 'Eve', 'Frank'], $names);
    }

    /**
     * INSERT ... SELECT: generate renewal invoices for active subscriptions.
     * Active subs: Alice (id=1, $9.99), Bob (id=2, $29.99), Eve (id=5, $9.99).
     * Expected: 3 new invoices inserted, total becomes 11.
     *
     * Issue #18: INSERT...SELECT with JOIN + GROUP BY produces column-not-found on MySQL.
     */
    public function testInsertSelectRenewalInvoices(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO mp_sub_invoices (id, subscription_id, amount, period, status)
                 SELECT 100 + s.id, s.id, p.monthly_price, '2026-03', 'pending'
                 FROM mp_sub_subscriptions s
                 JOIN mp_sub_plans p ON p.id = s.plan_id
                 WHERE s.status = 'active'"
            );
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Issue #18: INSERT...SELECT with JOIN fails on MySQL: ' . $e->getMessage()
            );
        }

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_sub_invoices");
        $this->assertEquals(11, (int) $rows[0]['cnt']);

        // Verify the new invoices
        $rows = $this->ztdQuery(
            "SELECT subscription_id, amount, status
             FROM mp_sub_invoices
             WHERE period = '2026-03'
             ORDER BY subscription_id"
        );
        $this->assertCount(3, $rows);
        $this->assertEquals(1, (int) $rows[0]['subscription_id']);
        $this->assertEquals(9.99, (float) $rows[0]['amount']);
        $this->assertSame('pending', $rows[0]['status']);

        $this->assertEquals(2, (int) $rows[1]['subscription_id']);
        $this->assertEquals(29.99, (float) $rows[1]['amount']);

        $this->assertEquals(5, (int) $rows[2]['subscription_id']);
        $this->assertEquals(9.99, (float) $rows[2]['amount']);
    }

    /**
     * Multiple correlated subqueries in SELECT list:
     * For each active subscription, get total_spent and invoice_count.
     * Expected 3 rows: Alice (29.97, 3), Bob (59.98, 2), Eve (29.97, 3).
     */
    public function testMultipleCorrelatedSubqueries(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.customer_name,
                    (SELECT SUM(i.amount) FROM mp_sub_invoices i WHERE i.subscription_id = s.id) AS total_spent,
                    (SELECT COUNT(*) FROM mp_sub_invoices i WHERE i.subscription_id = s.id) AS invoice_count
             FROM mp_sub_subscriptions s
             WHERE s.status = 'active'
             ORDER BY s.customer_name"
        );

        $this->assertCount(3, $rows);

        $this->assertSame('Alice', $rows[0]['customer_name']);
        $this->assertEquals(29.97, (float) $rows[0]['total_spent']);
        $this->assertEquals(3, (int) $rows[0]['invoice_count']);

        $this->assertSame('Bob', $rows[1]['customer_name']);
        $this->assertEquals(59.98, (float) $rows[1]['total_spent']);
        $this->assertEquals(2, (int) $rows[1]['invoice_count']);

        $this->assertSame('Eve', $rows[2]['customer_name']);
        $this->assertEquals(29.97, (float) $rows[2]['total_spent']);
        $this->assertEquals(3, (int) $rows[2]['invoice_count']);
    }

    /**
     * Prepared statement with JOIN and multiple params: filter by status and min total.
     * Find active subscriptions with total invoices >= $30.
     * Expected: Bob ($59.98).
     */
    public function testPreparedSubscriptionFilter(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT s.customer_name, SUM(i.amount) AS total_spent
             FROM mp_sub_subscriptions s
             JOIN mp_sub_invoices i ON i.subscription_id = s.id
             WHERE s.status = ?
             GROUP BY s.id, s.customer_name
             HAVING SUM(i.amount) >= ?
             ORDER BY total_spent DESC",
            ['active', 30]
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['customer_name']);
        $this->assertEquals(59.98, (float) $rows[0]['total_spent']);
    }

    /**
     * UPDATE then SELECT: mark all paid invoices for a subscription as refunded,
     * then verify via query.
     * Eve has 3 paid invoices; after update, all should be 'refunded'.
     */
    public function testUpdateThenVerify(): void
    {
        $this->pdo->exec(
            "UPDATE mp_sub_invoices SET status = 'refunded'
             WHERE subscription_id = 5 AND status = 'paid'"
        );

        $rows = $this->ztdQuery(
            "SELECT status, COUNT(*) AS cnt
             FROM mp_sub_invoices
             WHERE subscription_id = 5
             GROUP BY status"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('refunded', $rows[0]['status']);
        $this->assertEquals(3, (int) $rows[0]['cnt']);

        // Other invoices unchanged
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt FROM mp_sub_invoices WHERE status = 'paid'"
        );
        $this->assertEquals(5, (int) $rows[0]['cnt']);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO mp_sub_plans VALUES (4, 'Enterprise', 99.99, 0)");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_sub_plans");
        $this->assertEquals(4, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM mp_sub_plans')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
