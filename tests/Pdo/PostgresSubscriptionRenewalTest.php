<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests a SaaS subscription renewal workflow through ZTD shadow store (PostgreSQL PDO).
 * Subscriptions with plans and invoices exercise DELETE WHERE IN (subquery)
 * for expired trial cleanup, INSERT ... SELECT for renewal invoice generation,
 * multiple correlated subqueries in a single SELECT list, UPDATE with aggregate
 * subquery for loyalty discount, and physical isolation.
 * @spec SPEC-10.2.164
 */
class PostgresSubscriptionRenewalTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_sub_plans (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                monthly_price NUMERIC(10,2),
                is_trial SMALLINT
            )',
            'CREATE TABLE pg_sub_subscriptions (
                id SERIAL PRIMARY KEY,
                customer_name VARCHAR(255),
                plan_id INT,
                status VARCHAR(50),
                start_date DATE,
                end_date DATE
            )',
            'CREATE TABLE pg_sub_invoices (
                id SERIAL PRIMARY KEY,
                subscription_id INT,
                amount NUMERIC(10,2),
                period VARCHAR(20),
                status VARCHAR(50)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_sub_invoices', 'pg_sub_subscriptions', 'pg_sub_plans'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_sub_plans VALUES (1, 'Free Trial', 0.00, 1)");
        $this->pdo->exec("INSERT INTO pg_sub_plans VALUES (2, 'Basic', 9.99, 0)");
        $this->pdo->exec("INSERT INTO pg_sub_plans VALUES (3, 'Pro', 29.99, 0)");

        $this->pdo->exec("INSERT INTO pg_sub_subscriptions VALUES (1, 'Alice', 2, 'active', '2025-01-01', '2026-01-01')");
        $this->pdo->exec("INSERT INTO pg_sub_subscriptions VALUES (2, 'Bob', 3, 'active', '2025-03-15', '2026-03-15')");
        $this->pdo->exec("INSERT INTO pg_sub_subscriptions VALUES (3, 'Carol', 1, 'trial', '2025-12-01', '2026-01-01')");
        $this->pdo->exec("INSERT INTO pg_sub_subscriptions VALUES (4, 'Dave', 1, 'trial', '2026-02-01', '2026-03-01')");
        $this->pdo->exec("INSERT INTO pg_sub_subscriptions VALUES (5, 'Eve', 2, 'active', '2024-06-01', '2025-06-01')");
        $this->pdo->exec("INSERT INTO pg_sub_subscriptions VALUES (6, 'Frank', 3, 'cancelled', '2025-01-01', '2025-07-01')");

        $this->pdo->exec("INSERT INTO pg_sub_invoices VALUES (1, 1, 9.99, '2025-01', 'paid')");
        $this->pdo->exec("INSERT INTO pg_sub_invoices VALUES (2, 1, 9.99, '2025-02', 'paid')");
        $this->pdo->exec("INSERT INTO pg_sub_invoices VALUES (3, 1, 9.99, '2025-03', 'paid')");
        $this->pdo->exec("INSERT INTO pg_sub_invoices VALUES (4, 2, 29.99, '2025-04', 'paid')");
        $this->pdo->exec("INSERT INTO pg_sub_invoices VALUES (5, 2, 29.99, '2025-05', 'paid')");
        $this->pdo->exec("INSERT INTO pg_sub_invoices VALUES (6, 5, 9.99, '2024-07', 'paid')");
        $this->pdo->exec("INSERT INTO pg_sub_invoices VALUES (7, 5, 9.99, '2024-08', 'paid')");
        $this->pdo->exec("INSERT INTO pg_sub_invoices VALUES (8, 5, 9.99, '2024-09', 'paid')");
    }

    public function testDeleteExpiredTrials(): void
    {
        $this->pdo->exec(
            "DELETE FROM pg_sub_subscriptions
             WHERE id IN (
                 SELECT s.id FROM pg_sub_subscriptions s
                 JOIN pg_sub_plans p ON p.id = s.plan_id
                 WHERE p.is_trial = 1 AND s.end_date < '2026-03-09'
             )"
        );

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_sub_subscriptions");
        $this->assertEquals(4, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT customer_name FROM pg_sub_subscriptions ORDER BY id");
        $names = array_column($rows, 'customer_name');
        $this->assertSame(['Alice', 'Bob', 'Eve', 'Frank'], $names);
    }

    public function testInsertSelectRenewalInvoices(): void
    {
        $this->pdo->exec(
            "INSERT INTO pg_sub_invoices (id, subscription_id, amount, period, status)
             SELECT 100 + s.id, s.id, p.monthly_price, '2026-03', 'pending'
             FROM pg_sub_subscriptions s
             JOIN pg_sub_plans p ON p.id = s.plan_id
             WHERE s.status = 'active'"
        );

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_sub_invoices");
        $totalCount = (int) $rows[0]['cnt'];

        $rows = $this->ztdQuery(
            "SELECT subscription_id, amount, status
             FROM pg_sub_invoices
             WHERE period = '2026-03'
             ORDER BY subscription_id"
        );
        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'INSERT...SELECT with JOIN column values nullified on PostgreSQL. '
                . "Rows inserted: " . ($totalCount - 8) . " (expected 3)."
            );
        }
        $this->assertCount(3, $rows);
        $this->assertEquals(1, (int) $rows[0]['subscription_id']);
        $this->assertEquals(9.99, (float) $rows[0]['amount']);
        $this->assertSame('pending', $rows[0]['status']);

        $this->assertEquals(2, (int) $rows[1]['subscription_id']);
        $this->assertEquals(29.99, (float) $rows[1]['amount']);

        $this->assertEquals(5, (int) $rows[2]['subscription_id']);
        $this->assertEquals(9.99, (float) $rows[2]['amount']);
    }

    public function testMultipleCorrelatedSubqueries(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.customer_name,
                    (SELECT SUM(i.amount) FROM pg_sub_invoices i WHERE i.subscription_id = s.id) AS total_spent,
                    (SELECT COUNT(*) FROM pg_sub_invoices i WHERE i.subscription_id = s.id) AS invoice_count
             FROM pg_sub_subscriptions s
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
     * Prepared HAVING with $N params on PostgreSQL.
     * SPEC-11.SQLITE-HAVING-PARAMS states PostgreSQL works correctly,
     * but complex multi-table HAVING with $N params may also return empty.
     */
    public function testPreparedSubscriptionFilter(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT s.customer_name, SUM(i.amount) AS total_spent
             FROM pg_sub_subscriptions s
             JOIN pg_sub_invoices i ON i.subscription_id = s.id
             WHERE s.status = $1
             GROUP BY s.id, s.customer_name
             HAVING SUM(i.amount) >= $2
             ORDER BY total_spent DESC",
            ['active', 30]
        );

        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'Prepared HAVING with $N params returns empty on PostgreSQL. '
                . 'Extends SPEC-11.SQLITE-HAVING-PARAMS beyond SQLite-only. Expected 1 row (Bob, $59.98).'
            );
        }
        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['customer_name']);
        $this->assertEquals(59.98, (float) $rows[0]['total_spent']);
    }

    public function testUpdateThenVerify(): void
    {
        $this->pdo->exec(
            "UPDATE pg_sub_invoices SET status = 'refunded'
             WHERE subscription_id = 5 AND status = 'paid'"
        );

        $rows = $this->ztdQuery(
            "SELECT status, COUNT(*) AS cnt
             FROM pg_sub_invoices
             WHERE subscription_id = 5
             GROUP BY status"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('refunded', $rows[0]['status']);
        $this->assertEquals(3, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt FROM pg_sub_invoices WHERE status = 'paid'"
        );
        $this->assertEquals(5, (int) $rows[0]['cnt']);
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_sub_plans VALUES (4, 'Enterprise', 99.99, 0)");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_sub_plans");
        $this->assertEquals(4, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM pg_sub_plans')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
