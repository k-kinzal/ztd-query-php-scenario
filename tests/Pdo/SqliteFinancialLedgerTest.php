<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests a double-entry financial ledger through ZTD shadow store (SQLite PDO).
 * Covers debit/credit entries, balance calculations with sign multiplier,
 * reversals, point-in-time queries, and physical isolation.
 * @spec SPEC-10.2.71
 */
class SqliteFinancialLedgerTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_fl_accounts (
                id INTEGER PRIMARY KEY,
                name TEXT,
                account_type TEXT
            )',
            'CREATE TABLE sl_fl_entries (
                id INTEGER PRIMARY KEY,
                account_id INTEGER,
                transaction_ref TEXT,
                entry_type TEXT,
                amount REAL,
                description TEXT,
                created_at TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_fl_entries', 'sl_fl_accounts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 3 accounts
        $this->pdo->exec("INSERT INTO sl_fl_accounts VALUES (1, 'Operating', 'asset')");
        $this->pdo->exec("INSERT INTO sl_fl_accounts VALUES (2, 'Revenue', 'income')");
        $this->pdo->exec("INSERT INTO sl_fl_accounts VALUES (3, 'Expenses', 'expense')");

        // Transaction TXN-001: Revenue received ($500 debit to Operating, credit to Revenue)
        $this->pdo->exec("INSERT INTO sl_fl_entries VALUES (1, 1, 'TXN-001', 'debit', 500.00, 'Client payment', '2026-01-15 09:00:00')");
        $this->pdo->exec("INSERT INTO sl_fl_entries VALUES (2, 2, 'TXN-001', 'credit', 500.00, 'Client payment', '2026-01-15 09:00:00')");

        // Transaction TXN-002: Expense paid ($150 credit from Operating, debit to Expenses)
        $this->pdo->exec("INSERT INTO sl_fl_entries VALUES (3, 1, 'TXN-002', 'credit', 150.00, 'Office supplies', '2026-01-20 14:00:00')");
        $this->pdo->exec("INSERT INTO sl_fl_entries VALUES (4, 3, 'TXN-002', 'debit', 150.00, 'Office supplies', '2026-01-20 14:00:00')");

        // Transaction TXN-003: Another revenue ($300)
        $this->pdo->exec("INSERT INTO sl_fl_entries VALUES (5, 1, 'TXN-003', 'debit', 300.00, 'Consulting fee', '2026-02-01 10:00:00')");
        $this->pdo->exec("INSERT INTO sl_fl_entries VALUES (6, 2, 'TXN-003', 'credit', 300.00, 'Consulting fee', '2026-02-01 10:00:00')");
    }

    /**
     * Calculate account balances using SUM with debit/credit sign logic.
     * Asset/Expense: debits increase (+), credits decrease (-)
     * Income: credits increase (+), debits decrease (-)
     */
    public function testAccountBalances(): void
    {
        $rows = $this->ztdQuery(
            "SELECT a.id, a.name, a.account_type,
                    SUM(CASE WHEN e.entry_type = 'debit' THEN e.amount ELSE -e.amount END) AS balance
             FROM sl_fl_accounts a
             LEFT JOIN sl_fl_entries e ON e.account_id = a.id
             GROUP BY a.id, a.name, a.account_type
             ORDER BY a.id"
        );

        $this->assertCount(3, $rows);
        // Operating: +500 -150 +300 = 650
        $this->assertEquals(650.00, round((float) $rows[0]['balance'], 2));
        // Revenue: -500 -300 = -800 (negative because income accounts track credits as positive)
        $this->assertEquals(-800.00, round((float) $rows[1]['balance'], 2));
        // Expenses: +150
        $this->assertEquals(150.00, round((float) $rows[2]['balance'], 2));
    }

    /**
     * Record a new transaction with paired entries.
     */
    public function testRecordTransaction(): void
    {
        // TXN-004: Expense $75.50
        $this->pdo->exec("INSERT INTO sl_fl_entries VALUES (7, 1, 'TXN-004', 'credit', 75.50, 'Internet bill', '2026-02-10 09:00:00')");
        $this->pdo->exec("INSERT INTO sl_fl_entries VALUES (8, 3, 'TXN-004', 'debit', 75.50, 'Internet bill', '2026-02-10 09:00:00')");

        // Verify the transaction entries exist
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt, SUM(amount) AS total
             FROM sl_fl_entries
             WHERE transaction_ref = 'TXN-004'"
        );

        $this->assertEquals(2, (int) $rows[0]['cnt']);
        $this->assertEquals(151.00, round((float) $rows[0]['total'], 2));
    }

    /**
     * Verify double-entry integrity: sum of all debits equals sum of all credits.
     */
    public function testDoubleEntryIntegrity(): void
    {
        $rows = $this->ztdQuery(
            "SELECT SUM(CASE WHEN entry_type = 'debit' THEN amount ELSE 0 END) AS total_debits,
                    SUM(CASE WHEN entry_type = 'credit' THEN amount ELSE 0 END) AS total_credits
             FROM sl_fl_entries"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(
            round((float) $rows[0]['total_debits'], 2),
            round((float) $rows[0]['total_credits'], 2)
        );
    }

    /**
     * Post a reversal entry and verify the balance adjusts correctly.
     */
    public function testReversalEntry(): void
    {
        // Reverse TXN-002 ($150 expense)
        $this->pdo->exec("INSERT INTO sl_fl_entries VALUES (7, 1, 'TXN-002-REV', 'debit', 150.00, 'Reverse: Office supplies', '2026-02-15 11:00:00')");
        $this->pdo->exec("INSERT INTO sl_fl_entries VALUES (8, 3, 'TXN-002-REV', 'credit', 150.00, 'Reverse: Office supplies', '2026-02-15 11:00:00')");

        // Operating balance should be back to 800 (650 + 150 reversal)
        $rows = $this->ztdQuery(
            "SELECT SUM(CASE WHEN entry_type = 'debit' THEN amount ELSE -amount END) AS balance
             FROM sl_fl_entries
             WHERE account_id = 1"
        );
        $this->assertEquals(800.00, round((float) $rows[0]['balance'], 2));

        // Expenses should be 0 (150 - 150)
        $rows = $this->ztdQuery(
            "SELECT SUM(CASE WHEN entry_type = 'debit' THEN amount ELSE -amount END) AS balance
             FROM sl_fl_entries
             WHERE account_id = 3"
        );
        $this->assertEquals(0.00, round((float) $rows[0]['balance'], 2));
    }

    /**
     * Prepared statement: account statement filtered by date range.
     */
    public function testAccountStatementPrepared(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT e.id, e.transaction_ref, e.entry_type, e.amount, e.description
             FROM sl_fl_entries e
             WHERE e.account_id = ?
               AND e.created_at >= ?
               AND e.created_at <= ?
             ORDER BY e.created_at",
            [1, '2026-01-01 00:00:00', '2026-01-31 23:59:59']
        );

        $this->assertCount(2, $rows);
        $this->assertSame('TXN-001', $rows[0]['transaction_ref']);
        $this->assertSame('debit', $rows[0]['entry_type']);
        $this->assertSame('TXN-002', $rows[1]['transaction_ref']);
        $this->assertSame('credit', $rows[1]['entry_type']);
    }

    /**
     * Transaction-level summary: group entries by transaction_ref.
     */
    public function testTransactionSummary(): void
    {
        $rows = $this->ztdQuery(
            "SELECT transaction_ref,
                    COUNT(*) AS entry_count,
                    SUM(amount) AS total_amount,
                    MIN(created_at) AS txn_date
             FROM sl_fl_entries
             GROUP BY transaction_ref
             ORDER BY MIN(created_at)"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('TXN-001', $rows[0]['transaction_ref']);
        $this->assertEquals(2, (int) $rows[0]['entry_count']);
        $this->assertEquals(1000.00, round((float) $rows[0]['total_amount'], 2));
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO sl_fl_entries VALUES (7, 1, 'TXN-005', 'debit', 999.99, 'Test entry', '2026-03-01 09:00:00')");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_fl_entries");
        $this->assertEquals(7, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_fl_entries")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
