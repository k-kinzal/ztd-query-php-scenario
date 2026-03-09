<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests conditional aggregation without GROUP BY through the ZTD CTE rewriter on MySQLi.
 * Covers whole-table aggregates, CASE-based counting and summing, HAVING without GROUP BY,
 * multiple aggregates with filters, and post-mutation behavior.
 * @spec SPEC-10.2.102
 */
class ConditionalAggregateNoGroupByTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_ca_transactions (
            id INT PRIMARY KEY,
            type VARCHAR(50),
            amount DECIMAL(10,2),
            status VARCHAR(50)
        )';
    }

    protected function getTableNames(): array
    {
        return ['mi_ca_transactions'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_ca_transactions VALUES (1, 'credit',  500.00,  'completed')");
        $this->mysqli->query("INSERT INTO mi_ca_transactions VALUES (2, 'debit',   200.00,  'completed')");
        $this->mysqli->query("INSERT INTO mi_ca_transactions VALUES (3, 'credit',  300.00,  'pending')");
        $this->mysqli->query("INSERT INTO mi_ca_transactions VALUES (4, 'debit',   150.00,  'failed')");
        $this->mysqli->query("INSERT INTO mi_ca_transactions VALUES (5, 'credit', 1000.00,  'completed')");
        $this->mysqli->query("INSERT INTO mi_ca_transactions VALUES (6, 'debit',    75.00,  'completed')");
        $this->mysqli->query("INSERT INTO mi_ca_transactions VALUES (7, 'credit',  250.00,  'pending')");
        $this->mysqli->query("INSERT INTO mi_ca_transactions VALUES (8, 'debit',   400.00,  'completed')");
    }

    /**
     * Whole-table aggregate without GROUP BY.
     * @spec SPEC-10.2.102
     */
    public function testWholeTableAggregate(): void
    {
        $rows = $this->ztdQuery("
            SELECT COUNT(*) AS total_count,
                   SUM(amount) AS total_amount,
                   AVG(amount) AS avg_amount,
                   MIN(amount) AS min_amount,
                   MAX(amount) AS max_amount
            FROM mi_ca_transactions
        ");

        $this->assertCount(1, $rows);
        $this->assertEquals(8, (int) $rows[0]['total_count']);
        // 500+200+300+150+1000+75+250+400 = 2875
        $this->assertEqualsWithDelta(2875.00, (float) $rows[0]['total_amount'], 0.01);
        $this->assertEqualsWithDelta(359.375, (float) $rows[0]['avg_amount'], 0.01);
        $this->assertEqualsWithDelta(75.00, (float) $rows[0]['min_amount'], 0.01);
        $this->assertEqualsWithDelta(1000.00, (float) $rows[0]['max_amount'], 0.01);
    }

    /**
     * Conditional COUNT using CASE expression without GROUP BY.
     * @spec SPEC-10.2.102
     */
    public function testConditionalCountWithCase(): void
    {
        $rows = $this->ztdQuery("
            SELECT
                COUNT(CASE WHEN type = 'credit' THEN 1 END) AS credit_count,
                COUNT(CASE WHEN type = 'debit' THEN 1 END) AS debit_count,
                COUNT(CASE WHEN status = 'completed' THEN 1 END) AS completed_count,
                COUNT(CASE WHEN status = 'pending' THEN 1 END) AS pending_count,
                COUNT(CASE WHEN status = 'failed' THEN 1 END) AS failed_count
            FROM mi_ca_transactions
        ");

        $this->assertCount(1, $rows);
        $this->assertEquals(4, (int) $rows[0]['credit_count']);
        $this->assertEquals(4, (int) $rows[0]['debit_count']);
        $this->assertEquals(5, (int) $rows[0]['completed_count']);
        $this->assertEquals(2, (int) $rows[0]['pending_count']);
        $this->assertEquals(1, (int) $rows[0]['failed_count']);
    }

    /**
     * SUM with CASE expression to compute conditional totals without GROUP BY.
     * @spec SPEC-10.2.102
     */
    public function testSumWithCaseExpression(): void
    {
        $rows = $this->ztdQuery("
            SELECT
                SUM(CASE WHEN type = 'credit' THEN amount ELSE 0 END) AS total_credits,
                SUM(CASE WHEN type = 'debit' THEN amount ELSE 0 END) AS total_debits,
                SUM(CASE WHEN type = 'credit' THEN amount ELSE -amount END) AS net_balance
            FROM mi_ca_transactions
        ");

        $this->assertCount(1, $rows);
        // Credits: 500+300+1000+250 = 2050
        $this->assertEqualsWithDelta(2050.00, (float) $rows[0]['total_credits'], 0.01);
        // Debits: 200+150+75+400 = 825
        $this->assertEqualsWithDelta(825.00, (float) $rows[0]['total_debits'], 0.01);
        // Net: 2050 - 825 = 1225
        $this->assertEqualsWithDelta(1225.00, (float) $rows[0]['net_balance'], 0.01);
    }

    /**
     * HAVING without GROUP BY — aggregate over the entire table must satisfy HAVING.
     * @spec SPEC-10.2.102
     */
    public function testHavingWithoutGroupBy(): void
    {
        $rows = $this->ztdQuery("
            SELECT SUM(amount) AS total
            FROM mi_ca_transactions
            HAVING SUM(amount) > 1000
        ");

        // Total is 2875, which is > 1000, so one row is returned
        $this->assertCount(1, $rows);
        $this->assertEqualsWithDelta(2875.00, (float) $rows[0]['total'], 0.01);
    }

    /**
     * HAVING without GROUP BY that filters out the single aggregate row.
     * @spec SPEC-10.2.102
     */
    public function testHavingWithoutGroupByNoMatch(): void
    {
        $rows = $this->ztdQuery("
            SELECT SUM(amount) AS total
            FROM mi_ca_transactions
            HAVING SUM(amount) > 999999
        ");

        // Total is 2875, which is < 999999, so no rows returned
        $this->assertCount(0, $rows);
    }

    /**
     * Multiple different aggregates with conditional filters in a single query.
     * @spec SPEC-10.2.102
     */
    public function testMultipleAggregatesWithFilter(): void
    {
        $rows = $this->ztdQuery("
            SELECT
                SUM(CASE WHEN status = 'completed' AND type = 'credit' THEN amount ELSE 0 END) AS completed_credits,
                SUM(CASE WHEN status = 'completed' AND type = 'debit' THEN amount ELSE 0 END) AS completed_debits,
                AVG(CASE WHEN status = 'completed' THEN amount END) AS avg_completed,
                COUNT(CASE WHEN status != 'completed' THEN 1 END) AS non_completed_count
            FROM mi_ca_transactions
        ");

        $this->assertCount(1, $rows);
        // Completed credits: 500 + 1000 = 1500
        $this->assertEqualsWithDelta(1500.00, (float) $rows[0]['completed_credits'], 0.01);
        // Completed debits: 200 + 75 + 400 = 675
        $this->assertEqualsWithDelta(675.00, (float) $rows[0]['completed_debits'], 0.01);
        // Avg completed: (500+200+1000+75+400)/5 = 435
        $this->assertEqualsWithDelta(435.00, (float) $rows[0]['avg_completed'], 0.01);
        // Non-completed: pending(2) + failed(1) = 3
        $this->assertEquals(3, (int) $rows[0]['non_completed_count']);
    }

    /**
     * Aggregate results correctly reflect data inserted after seed.
     * @spec SPEC-10.2.102
     */
    public function testAggregateAfterInsert(): void
    {
        $this->mysqli->query("INSERT INTO mi_ca_transactions VALUES (9, 'credit', 600.00, 'completed')");

        $rows = $this->ztdQuery("
            SELECT
                COUNT(*) AS total_count,
                SUM(amount) AS total_amount,
                COUNT(CASE WHEN type = 'credit' THEN 1 END) AS credit_count
            FROM mi_ca_transactions
        ");

        $this->assertCount(1, $rows);
        $this->assertEquals(9, (int) $rows[0]['total_count']);
        // 2875 + 600 = 3475
        $this->assertEqualsWithDelta(3475.00, (float) $rows[0]['total_amount'], 0.01);
        $this->assertEquals(5, (int) $rows[0]['credit_count']);
    }

    /**
     * Physical table remains empty — all mutations are in ZTD shadow store.
     * @spec SPEC-10.2.102
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_ca_transactions VALUES (9, 'credit', 100.00, 'completed')");
        $this->mysqli->query("UPDATE mi_ca_transactions SET amount = 9999.00 WHERE id = 1");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_ca_transactions");
        $this->assertEquals(9, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_ca_transactions');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
