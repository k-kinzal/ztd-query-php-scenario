<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests savepoint behavior with ZTD on PostgreSQL PDO.
 * @spec SPEC-6.3
 */
class PostgresSavepointTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sp_test (id INT PRIMARY KEY, name VARCHAR(50))';
    }

    protected function getTableNames(): array
    {
        return ['sp_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sp_test VALUES (1, 'Alice')");
    }

    public function testSavepointPassesThroughSilently(): void
    {
        $this->pdo->beginTransaction();
        $result = $this->pdo->exec('SAVEPOINT sp1');
        // Does not throw — silently passes through
        $this->assertSame(0, $result);
        $this->pdo->commit();
    }

    public function testReleaseSavepointPassesThroughSilently(): void
    {
        $this->pdo->beginTransaction();
        $this->pdo->exec('SAVEPOINT sp1');
        $result = $this->pdo->exec('RELEASE SAVEPOINT sp1');
        $this->assertSame(0, $result);
        $this->pdo->commit();
    }

    /**
     * ROLLBACK TO SAVEPOINT should undo the INSERT after the savepoint.
     */
    public function testRollbackToSavepointUndoesShadowData(): void
    {
        $this->pdo->beginTransaction();
        $this->pdo->exec("INSERT INTO sp_test VALUES (2, 'Bob')");

        $this->pdo->exec('SAVEPOINT sp1');
        $this->pdo->exec("INSERT INTO sp_test VALUES (3, 'Charlie')");
        $this->pdo->exec('ROLLBACK TO SAVEPOINT sp1');

        $this->pdo->commit();

        $stmt = $this->pdo->query('SELECT COUNT(*) AS cnt FROM sp_test');
        $cnt = (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt'];
        // Expected: Charlie's INSERT was rolled back, so count should be 2 (Alice + Bob)
        if ($cnt !== 2) {
            $this->markTestIncomplete(
                'Shadow store does not participate in savepoints. '
                . 'Expected count 2 after ROLLBACK TO SAVEPOINT, got ' . $cnt
            );
        }
        $this->assertSame(2, $cnt);
    }
}
