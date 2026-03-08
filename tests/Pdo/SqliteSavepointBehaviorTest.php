<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests SAVEPOINT behavior on SQLite with ZTD.
 *
 * SAVEPOINT, RELEASE SAVEPOINT, and ROLLBACK TO SAVEPOINT are not supported.
 * On SQLite, all three throw UnsupportedSqlException.
 */
class SqliteSavepointBehaviorTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:');
        $raw->exec('CREATE TABLE sp_test (id INT PRIMARY KEY, name VARCHAR(50))');
        $this->pdo = ZtdPdo::fromPdo($raw);

        $this->pdo->exec("INSERT INTO sp_test VALUES (1, 'Alice')");
    }

    /**
     * SAVEPOINT throws exception.
     */
    public function testSavepointThrows(): void
    {
        $this->expectException(\Throwable::class);
        $this->pdo->exec('SAVEPOINT sp1');
    }

    /**
     * RELEASE SAVEPOINT throws exception.
     */
    public function testReleaseSavepointThrows(): void
    {
        $this->expectException(\Throwable::class);
        $this->pdo->exec('RELEASE SAVEPOINT sp1');
    }

    /**
     * ROLLBACK TO SAVEPOINT throws exception.
     */
    public function testRollbackToSavepointThrows(): void
    {
        $this->expectException(\Throwable::class);
        $this->pdo->exec('ROLLBACK TO SAVEPOINT sp1');
    }

    /**
     * Shadow data unaffected by failed SAVEPOINT commands.
     */
    public function testShadowDataUnaffectedByFailedSavepoint(): void
    {
        try {
            $this->pdo->exec('SAVEPOINT sp1');
        } catch (\Throwable $e) {
            // Expected
        }

        // Shadow data still intact
        $stmt = $this->pdo->query('SELECT name FROM sp_test WHERE id = 1');
        $this->assertSame('Alice', $stmt->fetchColumn());
    }
}
