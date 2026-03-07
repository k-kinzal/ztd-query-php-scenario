<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

class SqliteTransactionTest extends TestCase
{
    private PDO $raw;
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $this->raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $this->raw->exec('CREATE TABLE tx_test (id INTEGER PRIMARY KEY, val TEXT)');

        $this->pdo = ZtdPdo::fromPdo($this->raw);
    }

    public function testBeginTransactionAndCommit(): void
    {
        $this->assertTrue($this->pdo->beginTransaction());
        $this->assertTrue($this->pdo->inTransaction());

        $this->pdo->exec("INSERT INTO tx_test (id, val) VALUES (1, 'hello')");

        $this->assertTrue($this->pdo->commit());
        $this->assertFalse($this->pdo->inTransaction());

        $stmt = $this->pdo->query('SELECT * FROM tx_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
    }

    public function testBeginTransactionAndRollback(): void
    {
        $this->pdo->exec("INSERT INTO tx_test (id, val) VALUES (1, 'before_tx')");

        $this->assertTrue($this->pdo->beginTransaction());
        $this->assertTrue($this->pdo->rollBack());

        $stmt = $this->pdo->query('SELECT * FROM tx_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
    }

    public function testQuote(): void
    {
        $quoted = $this->pdo->quote("it's a test");
        $this->assertIsString($quoted);
        $this->assertStringContainsString('it', $quoted);
    }
}
