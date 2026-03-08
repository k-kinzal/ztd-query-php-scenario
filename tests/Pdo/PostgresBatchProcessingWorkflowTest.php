<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests batch processing workflow on PostgreSQL PDO.
 * @spec SPEC-4.1
 */
class PostgresBatchProcessingWorkflowTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE bp_accounts (id INT PRIMARY KEY, name VARCHAR(50), balance DECIMAL(10,2), status VARCHAR(20))',
            'CREATE TABLE bp_transactions (id INT PRIMARY KEY, account_id INT, amount DECIMAL(10,2), type VARCHAR(20), processed INT DEFAULT 0)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['bp_transactions', 'bp_accounts'];
    }


    public function testBatchDebitCreditProcessing(): void
    {
        $this->pdo->exec("INSERT INTO bp_accounts (id, name, balance, status) VALUES (1, 'Alice', 1000.00, 'active')");
        $this->pdo->exec("INSERT INTO bp_accounts (id, name, balance, status) VALUES (2, 'Bob', 500.00, 'active')");
        $this->pdo->exec("INSERT INTO bp_accounts (id, name, balance, status) VALUES (3, 'Carol', 250.00, 'frozen')");

        $this->pdo->exec("INSERT INTO bp_transactions (id, account_id, amount, type, processed) VALUES (1, 1, -200.00, 'debit', 0)");
        $this->pdo->exec("INSERT INTO bp_transactions (id, account_id, amount, type, processed) VALUES (2, 1, 50.00, 'credit', 0)");
        $this->pdo->exec("INSERT INTO bp_transactions (id, account_id, amount, type, processed) VALUES (3, 2, -100.00, 'debit', 0)");
        $this->pdo->exec("INSERT INTO bp_transactions (id, account_id, amount, type, processed) VALUES (4, 3, 100.00, 'credit', 0)");

        $this->pdo->exec("UPDATE bp_transactions SET processed = -1 WHERE account_id IN (SELECT id FROM bp_accounts WHERE status = 'frozen')");

        $stmt = $this->pdo->query("
            SELECT t.id, t.account_id, t.amount
            FROM bp_transactions t
            JOIN bp_accounts a ON a.id = t.account_id
            WHERE t.type = 'debit' AND t.processed = 0 AND a.balance + t.amount >= 0
        ");
        foreach ($stmt->fetchAll(PDO::FETCH_ASSOC) as $d) {
            $this->pdo->exec("UPDATE bp_accounts SET balance = balance + ({$d['amount']}) WHERE id = {$d['account_id']}");
            $this->pdo->exec("UPDATE bp_transactions SET processed = 1 WHERE id = {$d['id']}");
        }

        $stmt = $this->pdo->query("SELECT t.id, t.account_id, t.amount FROM bp_transactions t WHERE t.type = 'credit' AND t.processed = 0");
        foreach ($stmt->fetchAll(PDO::FETCH_ASSOC) as $c) {
            $this->pdo->exec("UPDATE bp_accounts SET balance = balance + {$c['amount']} WHERE id = {$c['account_id']}");
            $this->pdo->exec("UPDATE bp_transactions SET processed = 1 WHERE id = {$c['id']}");
        }

        $stmt = $this->pdo->query("SELECT id, balance FROM bp_accounts ORDER BY id");
        $accounts = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(850.00, (float) $accounts[0]['balance'], 0.01);
        $this->assertEqualsWithDelta(400.00, (float) $accounts[1]['balance'], 0.01);
        $this->assertEqualsWithDelta(250.00, (float) $accounts[2]['balance'], 0.01);
    }

    public function testIncrementalBatchWithPreparedStatements(): void
    {
        for ($i = 1; $i <= 20; $i++) {
            $this->pdo->exec("INSERT INTO bp_accounts (id, name, balance, status) VALUES ($i, 'User$i', " . ($i * 100) . ", 'active')");
        }

        $stmt = $this->pdo->prepare("UPDATE bp_accounts SET balance = balance * 1.05 WHERE id = ?");
        for ($i = 1; $i <= 20; $i++) {
            $stmt->execute([$i]);
        }

        $stmt = $this->pdo->query("SELECT SUM(balance) AS total FROM bp_accounts");
        $this->assertEqualsWithDelta(22050.00, (float) $stmt->fetch(PDO::FETCH_ASSOC)['total'], 0.01);
    }
}
