<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests conditional aggregates (SUM/COUNT with CASE) without GROUP BY on PostgreSQL via PDO.
 * When no GROUP BY is present, the entire table is treated as a single group.
 * Also tests HAVING without GROUP BY (aggregate over entire table).
 * @spec SPEC-10.2.102
 */
class PostgresConditionalAggregateNoGroupByTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_ca_transactions (
            id INTEGER PRIMARY KEY,
            type VARCHAR(50),
            amount NUMERIC(10,2),
            status VARCHAR(50)
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_ca_transactions'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_ca_transactions VALUES (1, 'credit', 100.00, 'completed')");
        $this->pdo->exec("INSERT INTO pg_ca_transactions VALUES (2, 'debit', 50.00, 'completed')");
        $this->pdo->exec("INSERT INTO pg_ca_transactions VALUES (3, 'credit', 200.00, 'pending')");
        $this->pdo->exec("INSERT INTO pg_ca_transactions VALUES (4, 'debit', 75.00, 'completed')");
        $this->pdo->exec("INSERT INTO pg_ca_transactions VALUES (5, 'credit', 150.00, 'completed')");
        $this->pdo->exec("INSERT INTO pg_ca_transactions VALUES (6, 'debit', 30.00, 'failed')");
        $this->pdo->exec("INSERT INTO pg_ca_transactions VALUES (7, 'credit', 300.00, 'completed')");
        $this->pdo->exec("INSERT INTO pg_ca_transactions VALUES (8, 'debit', 25.00, 'pending')");
    }

    /**
     * Simple whole-table aggregate without GROUP BY.
     */
    public function testWholeTableAggregate(): void
    {
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS total_count,
                    SUM(amount) AS total_amount,
                    AVG(amount) AS avg_amount,
                    MIN(amount) AS min_amount,
                    MAX(amount) AS max_amount
             FROM pg_ca_transactions"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(8, (int) $rows[0]['total_count']);
        $this->assertEquals(930.00, (float) $rows[0]['total_amount']);
        $this->assertEqualsWithDelta(116.25, (float) $rows[0]['avg_amount'], 0.01);
        $this->assertEquals(25.00, (float) $rows[0]['min_amount']);
        $this->assertEquals(300.00, (float) $rows[0]['max_amount']);
    }

    /**
     * Conditional COUNT with CASE expression (no GROUP BY).
     */
    public function testConditionalCountWithCase(): void
    {
        $rows = $this->ztdQuery(
            "SELECT
                COUNT(CASE WHEN type = 'credit' THEN 1 END) AS credit_count,
                COUNT(CASE WHEN type = 'debit' THEN 1 END) AS debit_count,
                COUNT(CASE WHEN status = 'completed' THEN 1 END) AS completed_count,
                COUNT(CASE WHEN status = 'pending' THEN 1 END) AS pending_count,
                COUNT(CASE WHEN status = 'failed' THEN 1 END) AS failed_count
             FROM pg_ca_transactions"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(4, (int) $rows[0]['credit_count']);
        $this->assertEquals(4, (int) $rows[0]['debit_count']);
        $this->assertEquals(5, (int) $rows[0]['completed_count']);
        $this->assertEquals(2, (int) $rows[0]['pending_count']);
        $this->assertEquals(1, (int) $rows[0]['failed_count']);
    }

    /**
     * SUM with CASE expression for conditional totals (no GROUP BY).
     */
    public function testSumWithCaseExpression(): void
    {
        $rows = $this->ztdQuery(
            "SELECT
                SUM(CASE WHEN type = 'credit' THEN amount ELSE 0 END) AS total_credits,
                SUM(CASE WHEN type = 'debit' THEN amount ELSE 0 END) AS total_debits,
                SUM(CASE WHEN type = 'credit' THEN amount ELSE -amount END) AS net_balance
             FROM pg_ca_transactions"
        );

        $this->assertCount(1, $rows);
        // Credits: 100 + 200 + 150 + 300 = 750
        $this->assertEquals(750.00, (float) $rows[0]['total_credits']);
        // Debits: 50 + 75 + 30 + 25 = 180
        $this->assertEquals(180.00, (float) $rows[0]['total_debits']);
        // Net: 750 - 180 = 570
        $this->assertEquals(570.00, (float) $rows[0]['net_balance']);
    }

    /**
     * HAVING without GROUP BY: aggregate condition on entire table (match).
     */
    public function testHavingWithoutGroupBy(): void
    {
        $rows = $this->ztdQuery(
            "SELECT SUM(amount) AS total
             FROM pg_ca_transactions
             HAVING SUM(amount) > 500"
        );

        // Total is 930, which is > 500, so one row should be returned
        $this->assertCount(1, $rows);
        $this->assertEquals(930.00, (float) $rows[0]['total']);
    }

    /**
     * HAVING without GROUP BY: aggregate condition on entire table (no match).
     */
    public function testHavingWithoutGroupByNoMatch(): void
    {
        $rows = $this->ztdQuery(
            "SELECT SUM(amount) AS total
             FROM pg_ca_transactions
             HAVING SUM(amount) > 10000"
        );

        // Total is 930, which is < 10000, so no rows should be returned
        $this->assertCount(0, $rows);
    }

    /**
     * Multiple aggregates with FILTER clause (PostgreSQL-specific alternative to CASE).
     */
    public function testMultipleAggregatesWithFilter(): void
    {
        $rows = $this->ztdQuery(
            "SELECT
                COUNT(*) FILTER (WHERE type = 'credit') AS credit_count,
                COUNT(*) FILTER (WHERE type = 'debit') AS debit_count,
                SUM(amount) FILTER (WHERE status = 'completed') AS completed_total,
                AVG(amount) FILTER (WHERE type = 'credit') AS avg_credit
             FROM pg_ca_transactions"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(4, (int) $rows[0]['credit_count']);
        $this->assertEquals(4, (int) $rows[0]['debit_count']);
        // Completed: 100 + 50 + 75 + 150 + 300 = 675
        $this->assertEquals(675.00, (float) $rows[0]['completed_total']);
        // Avg credit: (100 + 200 + 150 + 300) / 4 = 187.50
        $this->assertEqualsWithDelta(187.50, (float) $rows[0]['avg_credit'], 0.01);
    }

    /**
     * Aggregates reflect newly inserted data.
     */
    public function testAggregateAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO pg_ca_transactions VALUES (9, 'credit', 1000.00, 'completed')");

        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt, SUM(amount) AS total
             FROM pg_ca_transactions"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(9, (int) $rows[0]['cnt']);
        $this->assertEquals(1930.00, (float) $rows[0]['total']);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_ca_transactions VALUES (9, 'credit', 500.00, 'completed')");
        $this->pdo->exec("UPDATE pg_ca_transactions SET amount = 999.00 WHERE id = 1");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_ca_transactions");
        $this->assertEquals(9, (int) $rows[0]['cnt']);

        $this->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM pg_ca_transactions')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
