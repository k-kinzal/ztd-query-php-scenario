<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests conditional aggregate queries without GROUP BY through the ZTD CTE
 * rewriter on MySQL via PDO.
 * Covers whole-table aggregates, conditional COUNT/SUM with CASE,
 * HAVING without GROUP BY, multiple filtered aggregates, and reads after INSERT.
 * @spec SPEC-10.2.102
 */
class MysqlConditionalAggregateNoGroupByTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mp_ca_transactions (
            id INT PRIMARY KEY,
            type VARCHAR(50),
            amount DECIMAL(10,2),
            status VARCHAR(50)
        )';
    }

    protected function getTableNames(): array
    {
        return ['mp_ca_transactions'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_ca_transactions VALUES (1, 'credit',  100.00, 'completed')");
        $this->pdo->exec("INSERT INTO mp_ca_transactions VALUES (2, 'debit',    50.00, 'completed')");
        $this->pdo->exec("INSERT INTO mp_ca_transactions VALUES (3, 'credit',  200.00, 'pending')");
        $this->pdo->exec("INSERT INTO mp_ca_transactions VALUES (4, 'debit',    75.00, 'completed')");
        $this->pdo->exec("INSERT INTO mp_ca_transactions VALUES (5, 'credit',  300.00, 'completed')");
        $this->pdo->exec("INSERT INTO mp_ca_transactions VALUES (6, 'refund',   25.00, 'completed')");
        $this->pdo->exec("INSERT INTO mp_ca_transactions VALUES (7, 'debit',   150.00, 'failed')");
    }

    /**
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
            FROM mp_ca_transactions
        ");

        $this->assertCount(1, $rows);
        $this->assertSame(7, (int) $rows[0]['total_count']);
        $this->assertEqualsWithDelta(900.00, (float) $rows[0]['total_amount'], 0.01);
        $this->assertEqualsWithDelta(128.57, (float) $rows[0]['avg_amount'], 0.01);
        $this->assertEqualsWithDelta(25.00, (float) $rows[0]['min_amount'], 0.01);
        $this->assertEqualsWithDelta(300.00, (float) $rows[0]['max_amount'], 0.01);
    }

    /**
     * @spec SPEC-10.2.102
     */
    public function testConditionalCountWithCase(): void
    {
        $rows = $this->ztdQuery("
            SELECT
                COUNT(CASE WHEN type = 'credit' THEN 1 END) AS credit_count,
                COUNT(CASE WHEN type = 'debit' THEN 1 END) AS debit_count,
                COUNT(CASE WHEN type = 'refund' THEN 1 END) AS refund_count
            FROM mp_ca_transactions
        ");

        $this->assertCount(1, $rows);
        $this->assertSame(3, (int) $rows[0]['credit_count']);
        $this->assertSame(3, (int) $rows[0]['debit_count']);
        $this->assertSame(1, (int) $rows[0]['refund_count']);
    }

    /**
     * @spec SPEC-10.2.102
     */
    public function testSumWithCaseExpression(): void
    {
        $rows = $this->ztdQuery("
            SELECT
                SUM(CASE WHEN type = 'credit' THEN amount ELSE 0 END) AS total_credits,
                SUM(CASE WHEN type = 'debit' THEN amount ELSE 0 END) AS total_debits,
                SUM(CASE WHEN type = 'credit' THEN amount ELSE 0 END)
                  - SUM(CASE WHEN type = 'debit' THEN amount ELSE 0 END) AS net
            FROM mp_ca_transactions
        ");

        $this->assertCount(1, $rows);
        $this->assertEqualsWithDelta(600.00, (float) $rows[0]['total_credits'], 0.01);
        $this->assertEqualsWithDelta(275.00, (float) $rows[0]['total_debits'], 0.01);
        $this->assertEqualsWithDelta(325.00, (float) $rows[0]['net'], 0.01);
    }

    /**
     * @spec SPEC-10.2.102
     */
    public function testHavingWithoutGroupBy(): void
    {
        // HAVING without GROUP BY treats entire result set as one group
        $rows = $this->ztdQuery("
            SELECT SUM(amount) AS total
            FROM mp_ca_transactions
            HAVING SUM(amount) > 500
        ");

        $this->assertCount(1, $rows);
        $this->assertEqualsWithDelta(900.00, (float) $rows[0]['total'], 0.01);
    }

    /**
     * @spec SPEC-10.2.102
     */
    public function testHavingWithoutGroupByNoMatch(): void
    {
        // When HAVING condition is not met, no rows should be returned
        $rows = $this->ztdQuery("
            SELECT SUM(amount) AS total
            FROM mp_ca_transactions
            HAVING SUM(amount) > 99999
        ");

        $this->assertCount(0, $rows);
    }

    /**
     * @spec SPEC-10.2.102
     */
    public function testMultipleAggregatesWithFilter(): void
    {
        $rows = $this->ztdQuery("
            SELECT
                COUNT(CASE WHEN status = 'completed' THEN 1 END) AS completed_count,
                SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) AS completed_total,
                COUNT(CASE WHEN status = 'pending' THEN 1 END) AS pending_count,
                SUM(CASE WHEN status = 'pending' THEN amount ELSE 0 END) AS pending_total,
                COUNT(CASE WHEN status = 'failed' THEN 1 END) AS failed_count,
                SUM(CASE WHEN status = 'failed' THEN amount ELSE 0 END) AS failed_total
            FROM mp_ca_transactions
        ");

        $this->assertCount(1, $rows);
        $this->assertSame(5, (int) $rows[0]['completed_count']);
        $this->assertEqualsWithDelta(550.00, (float) $rows[0]['completed_total'], 0.01);
        $this->assertSame(1, (int) $rows[0]['pending_count']);
        $this->assertEqualsWithDelta(200.00, (float) $rows[0]['pending_total'], 0.01);
        $this->assertSame(1, (int) $rows[0]['failed_count']);
        $this->assertEqualsWithDelta(150.00, (float) $rows[0]['failed_total'], 0.01);
    }

    /**
     * @spec SPEC-10.2.102
     */
    public function testAggregateAfterInsert(): void
    {
        $this->ztdExec("INSERT INTO mp_ca_transactions VALUES (8, 'credit', 1000.00, 'completed')");

        $rows = $this->ztdQuery("
            SELECT
                COUNT(*) AS total_count,
                SUM(CASE WHEN type = 'credit' THEN amount ELSE 0 END) AS total_credits
            FROM mp_ca_transactions
        ");

        $this->assertCount(1, $rows);
        $this->assertSame(8, (int) $rows[0]['total_count']);
        $this->assertEqualsWithDelta(1600.00, (float) $rows[0]['total_credits'], 0.01);
    }

    /**
     * @spec SPEC-10.2.102
     */
    public function testPhysicalIsolation(): void
    {
        $this->ztdExec("INSERT INTO mp_ca_transactions VALUES (8, 'credit', 999.99, 'completed')");
        $this->ztdExec("UPDATE mp_ca_transactions SET status = 'cancelled' WHERE id = 7");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_ca_transactions");
        $this->assertSame(8, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM mp_ca_transactions')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
