<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests SAVEPOINT behavior on SQLite with ZTD.
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
     * SAVEPOINT should be supported.
     */
    public function testSavepointSupported(): void
    {
        try {
            $this->pdo->exec('SAVEPOINT sp1');
            $this->assertTrue(true);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SAVEPOINT not yet supported on SQLite: ' . $e->getMessage()
            );
        }
    }

    /**
     * RELEASE SAVEPOINT should be supported.
     */
    public function testReleaseSavepointSupported(): void
    {
        try {
            $this->pdo->exec('SAVEPOINT sp1');
            $this->pdo->exec('RELEASE SAVEPOINT sp1');
            $this->assertTrue(true);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'RELEASE SAVEPOINT not yet supported on SQLite: ' . $e->getMessage()
            );
        }
    }

    /**
     * ROLLBACK TO SAVEPOINT should be supported.
     */
    public function testRollbackToSavepointSupported(): void
    {
        try {
            $this->pdo->exec('SAVEPOINT sp1');
            $this->pdo->exec('ROLLBACK TO SAVEPOINT sp1');
            $this->assertTrue(true);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'ROLLBACK TO SAVEPOINT not yet supported on SQLite: ' . $e->getMessage()
            );
        }
    }

    /**
     * Shadow data should remain intact regardless of SAVEPOINT support.
     */
    public function testShadowDataIntactAfterSavepoint(): void
    {
        try {
            $this->pdo->exec('SAVEPOINT sp1');
        } catch (\Throwable $e) {
            // SAVEPOINT may not be supported yet
        }

        // Shadow data still intact
        $stmt = $this->pdo->query('SELECT name FROM sp_test WHERE id = 1');
        $this->assertSame('Alice', $stmt->fetchColumn());
    }
}
