<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests a realistic batch processing workflow that accumulates many shadow
 * operations — inserts, updates, deletes, and complex queries — to verify
 * the shadow store correctly tracks all changes in sequence.
 * @spec SPEC-4.1
 */
class SqliteBatchProcessingWorkflowTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE bp_accounts (id INTEGER PRIMARY KEY, name TEXT, balance REAL, status TEXT)',
            'CREATE TABLE bp_transactions (id INTEGER PRIMARY KEY, account_id INTEGER, amount REAL, type TEXT, processed INTEGER DEFAULT 0)',
            'CREATE TABLE bp_audit (id INTEGER PRIMARY KEY, action TEXT, details TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['bp_accounts', 'bp_transactions', 'bp_audit'];
    }


    public function testFullBatchProcessingPipeline(): void
    {
        // Step 1: Seed accounts
        $this->pdo->exec("INSERT INTO bp_accounts (id, name, balance, status) VALUES (1, 'Alice', 1000.00, 'active')");
        $this->pdo->exec("INSERT INTO bp_accounts (id, name, balance, status) VALUES (2, 'Bob', 500.00, 'active')");
        $this->pdo->exec("INSERT INTO bp_accounts (id, name, balance, status) VALUES (3, 'Carol', 250.00, 'frozen')");
        $this->pdo->exec("INSERT INTO bp_accounts (id, name, balance, status) VALUES (4, 'Dave', 0.00, 'active')");

        // Step 2: Seed unprocessed transactions (explicit processed=0 since shadow store doesn't apply defaults)
        $this->pdo->exec("INSERT INTO bp_transactions (id, account_id, amount, type, processed) VALUES (1, 1, -200.00, 'debit', 0)");
        $this->pdo->exec("INSERT INTO bp_transactions (id, account_id, amount, type, processed) VALUES (2, 1, 50.00, 'credit', 0)");
        $this->pdo->exec("INSERT INTO bp_transactions (id, account_id, amount, type, processed) VALUES (3, 2, -100.00, 'debit', 0)");
        $this->pdo->exec("INSERT INTO bp_transactions (id, account_id, amount, type, processed) VALUES (4, 3, 100.00, 'credit', 0)");
        $this->pdo->exec("INSERT INTO bp_transactions (id, account_id, amount, type, processed) VALUES (5, 4, -50.00, 'debit', 0)");

        // Step 3: Process only transactions for active accounts
        // Mark transactions for frozen accounts as skipped
        $this->pdo->exec("
            UPDATE bp_transactions SET processed = -1
            WHERE account_id IN (SELECT id FROM bp_accounts WHERE status = 'frozen')
        ");

        // Step 4: Apply debit transactions (balance >= amount)
        $stmt = $this->pdo->query("
            SELECT t.id, t.account_id, t.amount
            FROM bp_transactions t
            JOIN bp_accounts a ON a.id = t.account_id
            WHERE t.type = 'debit' AND t.processed = 0 AND a.balance + t.amount >= 0
        ");
        $debits = $stmt->fetchAll(PDO::FETCH_ASSOC);

        foreach ($debits as $debit) {
            $this->pdo->exec("UPDATE bp_accounts SET balance = balance + ({$debit['amount']}) WHERE id = {$debit['account_id']}");
            $this->pdo->exec("UPDATE bp_transactions SET processed = 1 WHERE id = {$debit['id']}");
        }

        // Step 5: Reject debits that would overdraw
        $this->pdo->exec("
            UPDATE bp_transactions SET processed = -2
            WHERE type = 'debit' AND processed = 0
        ");

        // Step 6: Apply credit transactions
        $stmt = $this->pdo->query("
            SELECT t.id, t.account_id, t.amount
            FROM bp_transactions t
            WHERE t.type = 'credit' AND t.processed = 0
        ");
        $credits = $stmt->fetchAll(PDO::FETCH_ASSOC);

        foreach ($credits as $credit) {
            $this->pdo->exec("UPDATE bp_accounts SET balance = balance + {$credit['amount']} WHERE id = {$credit['account_id']}");
            $this->pdo->exec("UPDATE bp_transactions SET processed = 1 WHERE id = {$credit['id']}");
        }

        // Step 7: Log audit entries
        $this->pdo->exec("INSERT INTO bp_audit (id, action, details) VALUES (1, 'batch_complete', 'Processed 5 transactions')");

        // Verify final state

        // Account balances
        $stmt = $this->pdo->query("SELECT id, balance FROM bp_accounts ORDER BY id");
        $accounts = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(850.00, (float) $accounts[0]['balance'], 0.01); // Alice: 1000-200+50
        $this->assertEqualsWithDelta(400.00, (float) $accounts[1]['balance'], 0.01); // Bob: 500-100
        $this->assertEqualsWithDelta(250.00, (float) $accounts[2]['balance'], 0.01); // Carol: unchanged (frozen)
        $this->assertEqualsWithDelta(0.00, (float) $accounts[3]['balance'], 0.01);   // Dave: unchanged (rejected)

        // Transaction processing status
        $stmt = $this->pdo->query("SELECT id, processed FROM bp_transactions ORDER BY id");
        $txns = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(1, (int) $txns[0]['processed']);  // Alice debit: processed
        $this->assertSame(1, (int) $txns[1]['processed']);  // Alice credit: processed
        $this->assertSame(1, (int) $txns[2]['processed']);  // Bob debit: processed
        $this->assertSame(-1, (int) $txns[3]['processed']); // Carol credit: skipped (frozen)
        $this->assertSame(-2, (int) $txns[4]['processed']); // Dave debit: rejected (overdraw)

        // Aggregation query on accumulated state
        $stmt = $this->pdo->query("
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN processed = 1 THEN 1 ELSE 0 END) AS success,
                SUM(CASE WHEN processed = -1 THEN 1 ELSE 0 END) AS skipped,
                SUM(CASE WHEN processed = -2 THEN 1 ELSE 0 END) AS rejected
            FROM bp_transactions
        ");
        $summary = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(5, (int) $summary['total']);
        $this->assertSame(3, (int) $summary['success']);
        $this->assertSame(1, (int) $summary['skipped']);
        $this->assertSame(1, (int) $summary['rejected']);

        // Audit log
        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM bp_audit");
        $this->assertSame(1, (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt']);
    }

    public function testIncrementalBatchWithPreparedStatements(): void
    {
        // Seed
        for ($i = 1; $i <= 20; $i++) {
            $this->pdo->exec("INSERT INTO bp_accounts (id, name, balance, status) VALUES ($i, 'User$i', " . ($i * 100) . ", 'active')");
        }

        // Batch update using prepared statement
        $updateStmt = $this->pdo->prepare("UPDATE bp_accounts SET balance = balance * 1.05 WHERE id = ?");
        for ($i = 1; $i <= 20; $i++) {
            $updateStmt->execute([$i]);
        }

        // Verify all got 5% increase
        $stmt = $this->pdo->query("SELECT SUM(balance) AS total FROM bp_accounts");
        $total = (float) $stmt->fetch(PDO::FETCH_ASSOC)['total'];
        // Original total: 100+200+...+2000 = 21000, after 5%: 22050
        $this->assertEqualsWithDelta(22050.00, $total, 0.01);

        // Delete low-balance accounts
        $this->pdo->exec("DELETE FROM bp_accounts WHERE balance < 600");

        // Verify count
        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM bp_accounts");
        $cnt = (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt'];
        // Accounts with original balance >= 572 (600/1.05) → ids 6-20 = 15 accounts
        $this->assertSame(15, $cnt);
    }

    public function testCrossTableAggregationAfterMutations(): void
    {
        // Setup
        $this->pdo->exec("INSERT INTO bp_accounts (id, name, balance, status) VALUES (1, 'Alice', 1000, 'active')");
        $this->pdo->exec("INSERT INTO bp_accounts (id, name, balance, status) VALUES (2, 'Bob', 500, 'active')");

        for ($i = 1; $i <= 10; $i++) {
            $accountId = ($i % 2) + 1;
            $amount = $i * 10;
            $this->pdo->exec("INSERT INTO bp_transactions (id, account_id, amount, type, processed) VALUES ($i, $accountId, $amount, 'credit', 1)");
        }

        // Cross-table aggregation
        $stmt = $this->pdo->query("
            SELECT a.name,
                   COUNT(t.id) AS txn_count,
                   SUM(t.amount) AS txn_total,
                   a.balance + COALESCE(SUM(t.amount), 0) AS projected_balance
            FROM bp_accounts a
            LEFT JOIN bp_transactions t ON t.account_id = a.id
            GROUP BY a.id, a.name, a.balance
            ORDER BY a.id
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        // Alice (id=1): transactions with odd ids (2,4,6,8,10 → amounts 20,40,60,80,100 = 300)
        // Wait, account_id = (i%2)+1: i=1→2, i=2→1, i=3→2, i=4→1, i=5→2, i=6→1, i=7→2, i=8→1, i=9→2, i=10→1
        // Alice(id=1): i=2,4,6,8,10 → amounts 20,40,60,80,100 = 300
        // Bob(id=2): i=1,3,5,7,9 → amounts 10,30,50,70,90 = 250
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame(5, (int) $rows[0]['txn_count']);
        $this->assertEqualsWithDelta(300.0, (float) $rows[0]['txn_total'], 0.01);
        $this->assertEqualsWithDelta(1300.0, (float) $rows[0]['projected_balance'], 0.01);

        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertSame(5, (int) $rows[1]['txn_count']);
        $this->assertEqualsWithDelta(250.0, (float) $rows[1]['txn_total'], 0.01);

        // Delete some transactions and verify aggregation updates
        $this->pdo->exec("DELETE FROM bp_transactions WHERE amount > 60");

        $stmt = $this->pdo->query("
            SELECT a.name, COUNT(t.id) AS txn_count
            FROM bp_accounts a
            LEFT JOIN bp_transactions t ON t.account_id = a.id
            GROUP BY a.id, a.name
            ORDER BY a.id
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // Remaining transactions: amounts 10,20,30,40,50,60
        // Alice: 20,40,60 = 3 txns
        // Bob: 10,30,50 = 3 txns
        $this->assertSame(3, (int) $rows[0]['txn_count']);
        $this->assertSame(3, (int) $rows[1]['txn_count']);
    }
}
