<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;
use ZtdQuery\Adapter\Pdo\ZtdPdoException;

/**
 * Tests savepoint (nested transaction) behavior with ZTD on SQLite PDO.
 *
 * Discovery: SAVEPOINT, RELEASE SAVEPOINT, and ROLLBACK TO SAVEPOINT
 * are not supported SQL statements in ZTD mode and throw ZtdPdoException.
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

    public function testSavepointThrowsUnsupportedSqlException(): void
    {
        $this->pdo->beginTransaction();

        $this->expectException(ZtdPdoException::class);
        $this->expectExceptionMessage('Statement type not supported');
        $this->pdo->exec('SAVEPOINT sp1');
    }

    public function testReleaseSavepointThrowsUnsupportedSqlException(): void
    {
        $this->expectException(ZtdPdoException::class);
        $this->expectExceptionMessage('Statement type not supported');
        $this->pdo->exec('RELEASE SAVEPOINT sp1');
    }

    public function testRollbackToSavepointThrowsUnsupportedSqlException(): void
    {
        $this->expectException(ZtdPdoException::class);
        $this->expectExceptionMessage('Statement type not supported');
        $this->pdo->exec('ROLLBACK TO SAVEPOINT sp1');
    }

    public function testBeginTransactionCommitWorks(): void
    {
        $this->pdo->beginTransaction();
        $this->pdo->exec("INSERT INTO sp_test VALUES (2, 'Bob')");
        $this->pdo->commit();

        $stmt = $this->pdo->query('SELECT COUNT(*) AS cnt FROM sp_test');
        $this->assertSame(2, (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt']);
    }

    public function testRollbackDoesNotUndoShadowInsert(): void
    {
        $this->pdo->beginTransaction();
        $this->pdo->exec("INSERT INTO sp_test VALUES (2, 'Bob')");
        $this->pdo->rollBack();

        $stmt = $this->pdo->query('SELECT COUNT(*) AS cnt FROM sp_test');
        $cnt = (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt'];
        // Shadow data persists after rollback — shadow store is not transactional
        $this->assertSame(2, $cnt);
    }
}
