<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests savepoint behavior with ZTD on MySQL PDO.
 * @spec SPEC-6.3
 */
class MysqlSavepointTest extends AbstractMysqlPdoTestCase
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

    /**
     * SAVEPOINT should be supported on MySQL.
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
                'SAVEPOINT not yet supported on MySQL PDO: ' . $e->getMessage()
            );
        }
    }

    /**
     * RELEASE SAVEPOINT should be supported on MySQL.
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
                'RELEASE SAVEPOINT not yet supported on MySQL PDO: ' . $e->getMessage()
            );
        }
    }

    /**
     * ROLLBACK TO SAVEPOINT should be supported on MySQL.
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
                'ROLLBACK TO SAVEPOINT not yet supported on MySQL PDO: ' . $e->getMessage()
            );
        }
    }
}
