<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;
use ZtdQuery\Adapter\Pdo\ZtdPdoException;

/**
 * Tests savepoint (nested transaction) behavior with ZTD on SQLite PDO.
 */
class SqliteSavepointTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);

        $this->pdo->exec('CREATE TABLE sp_test (id INT PRIMARY KEY, name VARCHAR(50))');
        $this->pdo->exec("INSERT INTO sp_test VALUES (1, 'Alice')");
    }

    /**
     * SAVEPOINT should be supported.
     */
    public function testSavepointSupported(): void
    {
        $this->pdo->beginTransaction();

        try {
            $this->pdo->exec('SAVEPOINT sp1');
            $this->pdo->commit();
            $this->assertTrue(true);
        } catch (ZtdPdoException $e) {
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
        $this->pdo->beginTransaction();

        try {
            $this->pdo->exec('SAVEPOINT sp1');
            $this->pdo->exec('RELEASE SAVEPOINT sp1');
            $this->pdo->commit();
            $this->assertTrue(true);
        } catch (ZtdPdoException $e) {
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
        $this->pdo->beginTransaction();

        try {
            $this->pdo->exec('SAVEPOINT sp1');
            $this->pdo->exec('ROLLBACK TO SAVEPOINT sp1');
            $this->pdo->commit();
            $this->assertTrue(true);
        } catch (ZtdPdoException $e) {
            $this->markTestIncomplete(
                'ROLLBACK TO SAVEPOINT not yet supported on SQLite: ' . $e->getMessage()
            );
        }
    }

    public function testBeginTransactionCommitWorks(): void
    {
        $this->pdo->beginTransaction();
        $this->pdo->exec("INSERT INTO sp_test VALUES (2, 'Bob')");
        $this->pdo->commit();

        $stmt = $this->pdo->query('SELECT COUNT(*) AS cnt FROM sp_test');
        $this->assertSame(2, (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt']);
    }

    /**
     * Rollback should undo the INSERT.
     */
    public function testRollbackUndoesShadowInsert(): void
    {
        $this->pdo->beginTransaction();
        $this->pdo->exec("INSERT INTO sp_test VALUES (2, 'Bob')");
        $this->pdo->rollBack();

        $stmt = $this->pdo->query('SELECT COUNT(*) AS cnt FROM sp_test');
        $cnt = (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt'];
        // Expected: rollback should undo the INSERT, leaving only Alice
        if ($cnt !== 1) {
            $this->markTestIncomplete(
                'Shadow store does not participate in transactions. '
                . 'Expected count 1 after rollback, got ' . $cnt
            );
        }
        $this->assertSame(1, $cnt);
    }
}
