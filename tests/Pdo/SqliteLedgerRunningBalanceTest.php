<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests financial ledger / running balance patterns through ZTD shadow store.
 * Simulates credits, debits, running totals via window functions, and balance consistency checks.
 * @spec SPEC-10.2.23
 * @spec SPEC-3.3
 */
class SqliteLedgerRunningBalanceTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_led_accounts (
                id INTEGER PRIMARY KEY,
                name TEXT,
                balance REAL
            )',
            'CREATE TABLE sl_led_transactions (
                id INTEGER PRIMARY KEY,
                account_id INTEGER,
                amount REAL,
                type TEXT,
                description TEXT,
                created_at TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_led_transactions', 'sl_led_accounts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_led_accounts VALUES (1, 'Checking', 1000.00)");
        $this->pdo->exec("INSERT INTO sl_led_accounts VALUES (2, 'Savings', 5000.00)");

        $this->pdo->exec("INSERT INTO sl_led_transactions VALUES (1, 1, 500.00, 'credit', 'Deposit', '2024-01-01')");
        $this->pdo->exec("INSERT INTO sl_led_transactions VALUES (2, 1, -200.00, 'debit', 'Rent', '2024-01-05')");
        $this->pdo->exec("INSERT INTO sl_led_transactions VALUES (3, 1, -50.00, 'debit', 'Groceries', '2024-01-10')");
        $this->pdo->exec("INSERT INTO sl_led_transactions VALUES (4, 1, 1500.00, 'credit', 'Salary', '2024-01-15')");
        $this->pdo->exec("INSERT INTO sl_led_transactions VALUES (5, 1, -100.00, 'debit', 'Utilities', '2024-01-20')");
        $this->pdo->exec("INSERT INTO sl_led_transactions VALUES (6, 2, 200.00, 'credit', 'Transfer in', '2024-01-02')");
        $this->pdo->exec("INSERT INTO sl_led_transactions VALUES (7, 2, -50.00, 'debit', 'Fee', '2024-01-15')");
    }

    /**
     * Running balance using SUM() OVER (ORDER BY ...) window function.
     */
    public function testRunningBalanceWithWindowFunction(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, description, amount,
                    SUM(amount) OVER (ORDER BY created_at, id ROWS UNBOUNDED PRECEDING) AS running_balance
             FROM sl_led_transactions
             WHERE account_id = 1
             ORDER BY created_at, id"
        );

        $this->assertCount(5, $rows);
        $this->assertEqualsWithDelta(500.00, (float) $rows[0]['running_balance'], 0.01);
        $this->assertEqualsWithDelta(300.00, (float) $rows[1]['running_balance'], 0.01);
        $this->assertEqualsWithDelta(250.00, (float) $rows[2]['running_balance'], 0.01);
        $this->assertEqualsWithDelta(1750.00, (float) $rows[3]['running_balance'], 0.01);
        $this->assertEqualsWithDelta(1650.00, (float) $rows[4]['running_balance'], 0.01);
    }

    /**
     * Running balance after inserting new transactions.
     */
    public function testRunningBalanceAfterNewTransaction(): void
    {
        $this->pdo->exec("INSERT INTO sl_led_transactions VALUES (8, 1, -300.00, 'debit', 'Car payment', '2024-01-25')");

        $rows = $this->ztdQuery(
            "SELECT id, amount,
                    SUM(amount) OVER (ORDER BY created_at, id ROWS UNBOUNDED PRECEDING) AS running_balance
             FROM sl_led_transactions
             WHERE account_id = 1
             ORDER BY created_at, id"
        );

        $this->assertCount(6, $rows);
        $last = end($rows);
        // 500 - 200 - 50 + 1500 - 100 - 300 = 1350
        $this->assertEqualsWithDelta(1350.00, (float) $last['running_balance'], 0.01);
    }

    /**
     * Self-referencing UPDATE: credit account balance.
     */
    public function testCreditAccountBalance(): void
    {
        $this->pdo->exec("UPDATE sl_led_accounts SET balance = balance + 500 WHERE id = 1");

        $rows = $this->ztdQuery("SELECT balance FROM sl_led_accounts WHERE id = 1");
        $this->assertEqualsWithDelta(1500.00, (float) $rows[0]['balance'], 0.01);
    }

    /**
     * Self-referencing UPDATE: debit account balance.
     */
    public function testDebitAccountBalance(): void
    {
        $this->pdo->exec("UPDATE sl_led_accounts SET balance = balance - 200 WHERE id = 1");

        $rows = $this->ztdQuery("SELECT balance FROM sl_led_accounts WHERE id = 1");
        $this->assertEqualsWithDelta(800.00, (float) $rows[0]['balance'], 0.01);
    }

    /**
     * Balance consistency: SUM of transactions should match account balance changes.
     */
    public function testBalanceConsistencyCheck(): void
    {
        // Compute expected balance change from transactions
        $rows = $this->ztdQuery(
            "SELECT SUM(amount) AS net_change FROM sl_led_transactions WHERE account_id = 1"
        );
        $netChange = (float) $rows[0]['net_change'];
        // 500 - 200 - 50 + 1500 - 100 = 1650
        $this->assertEqualsWithDelta(1650.00, $netChange, 0.01);
    }

    /**
     * Multiple window functions in same query: running balance + row number.
     */
    public function testMultipleWindowFunctions(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, amount,
                    ROW_NUMBER() OVER (ORDER BY created_at, id) AS seq,
                    SUM(amount) OVER (ORDER BY created_at, id ROWS UNBOUNDED PRECEDING) AS running_total,
                    COUNT(*) OVER () AS total_count
             FROM sl_led_transactions
             WHERE account_id = 1
             ORDER BY created_at, id"
        );

        $this->assertCount(5, $rows);
        $this->assertEquals(1, (int) $rows[0]['seq']);
        $this->assertEquals(5, (int) $rows[0]['total_count']);
        $this->assertEqualsWithDelta(500.00, (float) $rows[0]['running_total'], 0.01);
    }

    /**
     * PARTITION BY: running balance per account using window function.
     */
    public function testRunningBalancePartitionedByAccount(): void
    {
        $rows = $this->ztdQuery(
            "SELECT account_id, id, amount,
                    SUM(amount) OVER (PARTITION BY account_id ORDER BY created_at, id ROWS UNBOUNDED PRECEDING) AS running_balance
             FROM sl_led_transactions
             ORDER BY account_id, created_at, id"
        );

        $this->assertCount(7, $rows);

        // Account 1 last running balance: 500 - 200 - 50 + 1500 - 100 = 1650
        $acct1Rows = array_values(array_filter($rows, fn($r) => $r['account_id'] == 1));
        $this->assertEqualsWithDelta(1650.00, (float) end($acct1Rows)['running_balance'], 0.01);

        // Account 2 last running balance: 200 - 50 = 150
        $acct2Rows = array_values(array_filter($rows, fn($r) => $r['account_id'] == 2));
        $this->assertEqualsWithDelta(150.00, (float) end($acct2Rows)['running_balance'], 0.01);
    }

    /**
     * Aggregate credit vs debit totals using CASE.
     */
    public function testCreditDebitTotals(): void
    {
        $rows = $this->ztdQuery(
            "SELECT account_id,
                    SUM(CASE WHEN type = 'credit' THEN amount ELSE 0 END) AS total_credits,
                    SUM(CASE WHEN type = 'debit' THEN ABS(amount) ELSE 0 END) AS total_debits,
                    SUM(amount) AS net
             FROM sl_led_transactions
             GROUP BY account_id
             ORDER BY account_id"
        );

        $this->assertCount(2, $rows);
        // Account 1: credits = 500 + 1500 = 2000, debits = 200 + 50 + 100 = 350
        $this->assertEqualsWithDelta(2000.00, (float) $rows[0]['total_credits'], 0.01);
        $this->assertEqualsWithDelta(350.00, (float) $rows[0]['total_debits'], 0.01);
    }

    /**
     * Full workflow: insert transaction, update balance, verify running total reflects both.
     */
    public function testFullTransactionWorkflow(): void
    {
        // Step 1: Record a new transaction
        $this->pdo->exec("INSERT INTO sl_led_transactions VALUES (8, 1, 750.00, 'credit', 'Bonus', '2024-01-25')");

        // Step 2: Update account balance
        $this->pdo->exec("UPDATE sl_led_accounts SET balance = balance + 750 WHERE id = 1");

        // Step 3: Verify account balance
        $rows = $this->ztdQuery("SELECT balance FROM sl_led_accounts WHERE id = 1");
        $this->assertEqualsWithDelta(1750.00, (float) $rows[0]['balance'], 0.01);

        // Step 4: Verify running balance includes new transaction
        $rows = $this->ztdQuery(
            "SELECT amount,
                    SUM(amount) OVER (ORDER BY created_at, id ROWS UNBOUNDED PRECEDING) AS running
             FROM sl_led_transactions
             WHERE account_id = 1
             ORDER BY created_at, id"
        );

        $this->assertCount(6, $rows);
        $last = end($rows);
        // 500 - 200 - 50 + 1500 - 100 + 750 = 2400
        $this->assertEqualsWithDelta(2400.00, (float) $last['running'], 0.01);
    }

    /**
     * Prepared statement: parameterized transaction query with running balance.
     */
    public function testPreparedRunningBalance(): void
    {
        $stmt = $this->pdo->prepare(
            'SELECT id, amount,
                    SUM(amount) OVER (ORDER BY created_at, id ROWS UNBOUNDED PRECEDING) AS running
             FROM sl_led_transactions
             WHERE account_id = ?
             ORDER BY created_at, id'
        );

        $stmt->execute([1]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(5, $rows);
        $this->assertEqualsWithDelta(1650.00, (float) end($rows)['running'], 0.01);

        $stmt->execute([2]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertEqualsWithDelta(150.00, (float) end($rows)['running'], 0.01);
    }

    /**
     * Physical isolation: ledger changes don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO sl_led_transactions VALUES (8, 1, 9999.00, 'credit', 'Big deposit', '2024-02-01')");
        $this->pdo->exec("UPDATE sl_led_accounts SET balance = balance + 9999 WHERE id = 1");

        $rows = $this->ztdQuery("SELECT balance FROM sl_led_accounts WHERE id = 1");
        $this->assertEqualsWithDelta(10999.00, (float) $rows[0]['balance'], 0.01);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT balance FROM sl_led_accounts WHERE id = 1")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(0, $rows);
    }
}
