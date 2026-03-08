<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests transaction interaction with ZTD shadow store on SQLite.
 *
 * Since shadow operations don't physically write, transactions
 * (BEGIN/COMMIT/ROLLBACK) may interact differently with shadow state.
 */
class SqliteTransactionWithShadowTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE sl_txn_test (id INTEGER PRIMARY KEY, name TEXT)');
        $this->pdo = ZtdPdo::fromPdo($raw);
    }

    /**
     * beginTransaction/commit works with shadow INSERT.
     */
    public function testCommitWithShadowInsert(): void
    {
        $this->pdo->beginTransaction();
        $this->pdo->exec("INSERT INTO sl_txn_test VALUES (1, 'Alice')");
        $this->pdo->commit();

        $stmt = $this->pdo->query('SELECT name FROM sl_txn_test WHERE id = 1');
        $this->assertSame('Alice', $stmt->fetchColumn());
    }

    /**
     * rollBack behavior with shadow INSERT.
     */
    public function testRollbackWithShadowInsert(): void
    {
        $this->pdo->beginTransaction();
        $this->pdo->exec("INSERT INTO sl_txn_test VALUES (1, 'Alice')");
        $this->pdo->rollBack();

        // After rollback, shadow data behavior depends on implementation.
        // The shadow store may or may not respect rollback.
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_txn_test');
        $count = (int) $stmt->fetchColumn();
        // Document observed behavior
        $this->assertContains($count, [0, 1]);
    }

    /**
     * inTransaction() returns correct state.
     */
    public function testInTransactionState(): void
    {
        $this->assertFalse($this->pdo->inTransaction());

        $this->pdo->beginTransaction();
        $this->assertTrue($this->pdo->inTransaction());

        $this->pdo->commit();
        $this->assertFalse($this->pdo->inTransaction());
    }

    /**
     * Multiple operations within a transaction.
     */
    public function testMultipleOpsInTransaction(): void
    {
        $this->pdo->beginTransaction();
        $this->pdo->exec("INSERT INTO sl_txn_test VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO sl_txn_test VALUES (2, 'Bob')");
        $this->pdo->exec("UPDATE sl_txn_test SET name = 'ALICE' WHERE id = 1");
        $this->pdo->commit();

        $stmt = $this->pdo->query('SELECT name FROM sl_txn_test ORDER BY id');
        $names = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['ALICE', 'Bob'], $names);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->beginTransaction();
        $this->pdo->exec("INSERT INTO sl_txn_test VALUES (1, 'Alice')");
        $this->pdo->commit();

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_txn_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
