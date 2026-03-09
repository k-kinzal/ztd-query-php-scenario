<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests aggregate queries without GROUP BY (whole-table aggregation) and HAVING without GROUP BY.
 * Edge case for CTE rewriter.
 * @spec SPEC-10.2.102
 */
class SqliteConditionalAggregateNoGroupByTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_ca_transactions (
            id INTEGER PRIMARY KEY,
            type TEXT,
            amount REAL,
            status TEXT
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_ca_transactions'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_ca_transactions VALUES (1, 'credit', 100.00, 'completed')");
        $this->pdo->exec("INSERT INTO sl_ca_transactions VALUES (2, 'debit', 50.00, 'completed')");
        $this->pdo->exec("INSERT INTO sl_ca_transactions VALUES (3, 'credit', 200.00, 'completed')");
        $this->pdo->exec("INSERT INTO sl_ca_transactions VALUES (4, 'debit', 75.00, 'pending')");
        $this->pdo->exec("INSERT INTO sl_ca_transactions VALUES (5, 'credit', 150.00, 'completed')");
        $this->pdo->exec("INSERT INTO sl_ca_transactions VALUES (6, 'debit', 30.00, 'failed')");
        $this->pdo->exec("INSERT INTO sl_ca_transactions VALUES (7, 'credit', 300.00, 'pending')");
        $this->pdo->exec("INSERT INTO sl_ca_transactions VALUES (8, 'debit', 120.00, 'completed')");
    }

    /**
     * Whole-table aggregate: COUNT, SUM, AVG without GROUP BY.
     */
    public function testWholeTableAggregate(): void
    {
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt, SUM(amount) AS total, AVG(amount) AS avg_amt
             FROM sl_ca_transactions"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(8, (int) $rows[0]['cnt']);
        // 100+50+200+75+150+30+300+120 = 1025
        $this->assertEquals(1025.00, (float) $rows[0]['total']);
        $this->assertEquals(128.125, (float) $rows[0]['avg_amt']);
    }

    /**
     * Conditional COUNT using CASE expressions.
     */
    public function testConditionalCountWithCase(): void
    {
        $rows = $this->ztdQuery(
            "SELECT COUNT(CASE WHEN type = 'credit' THEN 1 END) AS credits,
                    COUNT(CASE WHEN type = 'debit' THEN 1 END) AS debits
             FROM sl_ca_transactions"
        );

        $this->assertCount(1, $rows);
        // Credits: ids 1,3,5,7 = 4
        $this->assertEquals(4, (int) $rows[0]['credits']);
        // Debits: ids 2,4,6,8 = 4
        $this->assertEquals(4, (int) $rows[0]['debits']);
    }

    /**
     * SUM with CASE expression: net balance for completed transactions.
     */
    public function testSumWithCaseExpression(): void
    {
        $rows = $this->ztdQuery(
            "SELECT SUM(CASE WHEN type = 'credit' THEN amount ELSE -amount END) AS balance
             FROM sl_ca_transactions
             WHERE status = 'completed'"
        );

        $this->assertCount(1, $rows);
        // Completed credits: 100+200+150=450; completed debits: 50+120=170
        // Balance: 450 - 170 = 280
        $this->assertEquals(280.00, (float) $rows[0]['balance']);
    }

    /**
     * HAVING without GROUP BY: valid SQL that returns 1 row if condition met.
     */
    public function testHavingWithoutGroupBy(): void
    {
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt FROM sl_ca_transactions HAVING COUNT(*) > 5"
        );

        // 8 rows > 5, so should return 1 row with cnt=8
        $this->assertCount(1, $rows);
        $this->assertEquals(8, (int) $rows[0]['cnt']);
    }

    /**
     * HAVING without GROUP BY where condition is not met: returns 0 rows.
     */
    public function testHavingWithoutGroupByNoMatch(): void
    {
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt FROM sl_ca_transactions HAVING COUNT(*) > 100"
        );

        // 8 rows is not > 100, so should return 0 rows
        $this->assertCount(0, $rows);
    }

    /**
     * Multiple aggregates with WHERE filter.
     */
    public function testMultipleAggregatesWithFilter(): void
    {
        $rows = $this->ztdQuery(
            "SELECT MIN(amount) AS min_amt, MAX(amount) AS max_amt, SUM(amount) AS total
             FROM sl_ca_transactions
             WHERE status != 'failed'"
        );

        $this->assertCount(1, $rows);
        // Non-failed: 100,50,200,75,150,300,120
        // Min: 50, Max: 300, Sum: 995
        $this->assertEquals(50.00, (float) $rows[0]['min_amt']);
        $this->assertEquals(300.00, (float) $rows[0]['max_amt']);
        $this->assertEquals(995.00, (float) $rows[0]['total']);
    }

    /**
     * Insert new row, re-run aggregate, verify counts updated.
     */
    public function testAggregateAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO sl_ca_transactions VALUES (9, 'credit', 500.00, 'completed')");

        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt, SUM(amount) AS total FROM sl_ca_transactions"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(9, (int) $rows[0]['cnt']);
        // 1025 + 500 = 1525
        $this->assertEquals(1525.00, (float) $rows[0]['total']);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO sl_ca_transactions VALUES (9, 'credit', 500.00, 'completed')");
        $this->pdo->exec("DELETE FROM sl_ca_transactions WHERE id = 1");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_ca_transactions");
        $this->assertEquals(8, (int) $rows[0]['cnt']); // 8 original - 1 deleted + 1 inserted

        $this->pdo->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM sl_ca_transactions')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
