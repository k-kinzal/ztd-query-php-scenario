<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests financial ledger / running balance patterns through ZTD shadow store.
 * Simulates credits, debits, running totals via window functions, and balance consistency checks.
 * @spec SPEC-10.2.23
 * @spec SPEC-3.3
 */
class LedgerRunningBalanceTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_led_accounts (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                balance DECIMAL(10,2)
            )',
            'CREATE TABLE mi_led_transactions (
                id INT PRIMARY KEY,
                account_id INT,
                amount DECIMAL(10,2),
                type VARCHAR(255),
                description VARCHAR(255),
                created_at VARCHAR(255)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_led_transactions', 'mi_led_accounts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_led_accounts VALUES (1, 'Checking', 1000.00)");
        $this->mysqli->query("INSERT INTO mi_led_accounts VALUES (2, 'Savings', 5000.00)");

        $this->mysqli->query("INSERT INTO mi_led_transactions VALUES (1, 1, 500.00, 'credit', 'Deposit', '2024-01-01')");
        $this->mysqli->query("INSERT INTO mi_led_transactions VALUES (2, 1, -200.00, 'debit', 'Rent', '2024-01-05')");
        $this->mysqli->query("INSERT INTO mi_led_transactions VALUES (3, 1, -50.00, 'debit', 'Groceries', '2024-01-10')");
        $this->mysqli->query("INSERT INTO mi_led_transactions VALUES (4, 1, 1500.00, 'credit', 'Salary', '2024-01-15')");
        $this->mysqli->query("INSERT INTO mi_led_transactions VALUES (5, 1, -100.00, 'debit', 'Utilities', '2024-01-20')");
        $this->mysqli->query("INSERT INTO mi_led_transactions VALUES (6, 2, 200.00, 'credit', 'Transfer in', '2024-01-02')");
        $this->mysqli->query("INSERT INTO mi_led_transactions VALUES (7, 2, -50.00, 'debit', 'Fee', '2024-01-15')");
    }

    /**
     * Running balance using SUM() OVER (ORDER BY ...) window function.
     */
    public function testRunningBalanceWithWindowFunction(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, description, amount,
                    SUM(amount) OVER (ORDER BY created_at, id ROWS UNBOUNDED PRECEDING) AS running_balance
             FROM mi_led_transactions
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
        $this->mysqli->query("INSERT INTO mi_led_transactions VALUES (8, 1, -300.00, 'debit', 'Car payment', '2024-01-25')");

        $rows = $this->ztdQuery(
            "SELECT id, amount,
                    SUM(amount) OVER (ORDER BY created_at, id ROWS UNBOUNDED PRECEDING) AS running_balance
             FROM mi_led_transactions
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
        $this->mysqli->query("UPDATE mi_led_accounts SET balance = balance + 500 WHERE id = 1");

        $rows = $this->ztdQuery("SELECT balance FROM mi_led_accounts WHERE id = 1");
        $this->assertEqualsWithDelta(1500.00, (float) $rows[0]['balance'], 0.01);
    }

    /**
     * Self-referencing UPDATE: debit account balance.
     */
    public function testDebitAccountBalance(): void
    {
        $this->mysqli->query("UPDATE mi_led_accounts SET balance = balance - 200 WHERE id = 1");

        $rows = $this->ztdQuery("SELECT balance FROM mi_led_accounts WHERE id = 1");
        $this->assertEqualsWithDelta(800.00, (float) $rows[0]['balance'], 0.01);
    }

    /**
     * Balance consistency: SUM of transactions should match account balance changes.
     */
    public function testBalanceConsistencyCheck(): void
    {
        // Compute expected balance change from transactions
        $rows = $this->ztdQuery(
            "SELECT SUM(amount) AS net_change FROM mi_led_transactions WHERE account_id = 1"
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
             FROM mi_led_transactions
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
             FROM mi_led_transactions
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
             FROM mi_led_transactions
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
        $this->mysqli->query("INSERT INTO mi_led_transactions VALUES (8, 1, 750.00, 'credit', 'Bonus', '2024-01-25')");

        // Step 2: Update account balance
        $this->mysqli->query("UPDATE mi_led_accounts SET balance = balance + 750 WHERE id = 1");

        // Step 3: Verify account balance
        $rows = $this->ztdQuery("SELECT balance FROM mi_led_accounts WHERE id = 1");
        $this->assertEqualsWithDelta(1750.00, (float) $rows[0]['balance'], 0.01);

        // Step 4: Verify running balance includes new transaction
        $rows = $this->ztdQuery(
            "SELECT amount,
                    SUM(amount) OVER (ORDER BY created_at, id ROWS UNBOUNDED PRECEDING) AS running
             FROM mi_led_transactions
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
        $stmt = $this->mysqli->prepare(
            'SELECT id, amount,
                    SUM(amount) OVER (ORDER BY created_at, id ROWS UNBOUNDED PRECEDING) AS running
             FROM mi_led_transactions
             WHERE account_id = ?
             ORDER BY created_at, id'
        );

        $acctId = 1;
        $stmt->bind_param('i', $acctId);
        $stmt->execute();
        $rows = $stmt->get_result()->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(5, $rows);
        $this->assertEqualsWithDelta(1650.00, (float) end($rows)['running'], 0.01);

        $acctId = 2;
        $stmt->bind_param('i', $acctId);
        $stmt->execute();
        $rows = $stmt->get_result()->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertEqualsWithDelta(150.00, (float) end($rows)['running'], 0.01);
    }

    /**
     * Physical isolation: ledger changes don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_led_transactions VALUES (8, 1, 9999.00, 'credit', 'Big deposit', '2024-02-01')");
        $this->mysqli->query("UPDATE mi_led_accounts SET balance = balance + 9999 WHERE id = 1");

        $rows = $this->ztdQuery("SELECT balance FROM mi_led_accounts WHERE id = 1");
        $this->assertEqualsWithDelta(10999.00, (float) $rows[0]['balance'], 0.01);

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_led_accounts');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
