<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests batch processing workflow on MySQLi.
 */
class BatchProcessingWorkflowTest extends TestCase
{
    private ZtdMysqli $mysqli;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS mi_bp_transactions');
        $raw->query('DROP TABLE IF EXISTS mi_bp_accounts');
        $raw->query('CREATE TABLE mi_bp_accounts (id INT PRIMARY KEY, name VARCHAR(50), balance DECIMAL(10,2), status VARCHAR(20))');
        $raw->query('CREATE TABLE mi_bp_transactions (id INT PRIMARY KEY, account_id INT, amount DECIMAL(10,2), type VARCHAR(20), processed INT DEFAULT 0)');
        $raw->close();
    }

    protected function setUp(): void
    {
        $this->mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
    }

    public function testBatchDebitCreditProcessing(): void
    {
        $this->mysqli->query("INSERT INTO mi_bp_accounts (id, name, balance, status) VALUES (1, 'Alice', 1000.00, 'active')");
        $this->mysqli->query("INSERT INTO mi_bp_accounts (id, name, balance, status) VALUES (2, 'Bob', 500.00, 'active')");
        $this->mysqli->query("INSERT INTO mi_bp_accounts (id, name, balance, status) VALUES (3, 'Carol', 250.00, 'frozen')");

        $this->mysqli->query("INSERT INTO mi_bp_transactions (id, account_id, amount, type, processed) VALUES (1, 1, -200.00, 'debit', 0)");
        $this->mysqli->query("INSERT INTO mi_bp_transactions (id, account_id, amount, type, processed) VALUES (2, 1, 50.00, 'credit', 0)");
        $this->mysqli->query("INSERT INTO mi_bp_transactions (id, account_id, amount, type, processed) VALUES (3, 2, -100.00, 'debit', 0)");
        $this->mysqli->query("INSERT INTO mi_bp_transactions (id, account_id, amount, type, processed) VALUES (4, 3, 100.00, 'credit', 0)");

        $this->mysqli->query("UPDATE mi_bp_transactions SET processed = -1 WHERE account_id IN (SELECT id FROM mi_bp_accounts WHERE status = 'frozen')");

        $result = $this->mysqli->query("
            SELECT t.id, t.account_id, t.amount
            FROM mi_bp_transactions t
            JOIN mi_bp_accounts a ON a.id = t.account_id
            WHERE t.type = 'debit' AND t.processed = 0 AND a.balance + t.amount >= 0
        ");
        $debits = $result->fetch_all(MYSQLI_ASSOC);
        foreach ($debits as $d) {
            $this->mysqli->query("UPDATE mi_bp_accounts SET balance = balance + ({$d['amount']}) WHERE id = {$d['account_id']}");
            $this->mysqli->query("UPDATE mi_bp_transactions SET processed = 1 WHERE id = {$d['id']}");
        }

        $result = $this->mysqli->query("SELECT t.id, t.account_id, t.amount FROM mi_bp_transactions t WHERE t.type = 'credit' AND t.processed = 0");
        foreach ($result->fetch_all(MYSQLI_ASSOC) as $c) {
            $this->mysqli->query("UPDATE mi_bp_accounts SET balance = balance + {$c['amount']} WHERE id = {$c['account_id']}");
            $this->mysqli->query("UPDATE mi_bp_transactions SET processed = 1 WHERE id = {$c['id']}");
        }

        $result = $this->mysqli->query("SELECT id, balance FROM mi_bp_accounts ORDER BY id");
        $accounts = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertEqualsWithDelta(850.00, (float) $accounts[0]['balance'], 0.01);
        $this->assertEqualsWithDelta(400.00, (float) $accounts[1]['balance'], 0.01);
        $this->assertEqualsWithDelta(250.00, (float) $accounts[2]['balance'], 0.01);
    }

    protected function tearDown(): void
    {
        $this->mysqli->close();
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS mi_bp_transactions');
        $raw->query('DROP TABLE IF EXISTS mi_bp_accounts');
        $raw->close();
    }
}
