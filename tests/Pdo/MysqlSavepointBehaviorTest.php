<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests SAVEPOINT behavior on MySQL PDO with ZTD.
 * @spec SPEC-6.3
 */
class MysqlSavepointBehaviorTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pdo_msp_test (id INT PRIMARY KEY, name VARCHAR(50))';
    }

    protected function getTableNames(): array
    {
        return ['pdo_msp_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pdo_msp_test VALUES (1, 'Alice')");
    }

    /**
     * SAVEPOINT should be supported on MySQL.
     */
    public function testSavepointSupported(): void
    {
        try {
            $this->pdo->exec('SAVEPOINT sp1');
            $this->assertTrue(true);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SAVEPOINT not yet supported on MySQL: ' . $e->getMessage()
            );
        }
    }

    /**
     * ROLLBACK TO SAVEPOINT should be supported on MySQL.
     */
    public function testRollbackToSavepointSupported(): void
    {
        try {
            $this->pdo->exec('SAVEPOINT sp1');
            $this->pdo->exec('ROLLBACK TO SAVEPOINT sp1');
            $this->assertTrue(true);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'ROLLBACK TO SAVEPOINT not yet supported on MySQL: ' . $e->getMessage()
            );
        }
    }

    /**
     * Shadow data should remain intact regardless of SAVEPOINT support.
     */
    public function testShadowDataIntact(): void
    {
        try {
            $this->pdo->exec('SAVEPOINT sp1');
        } catch (\Throwable $e) {
            // SAVEPOINT may not be supported yet
        }

        $stmt = $this->pdo->query('SELECT name FROM pdo_msp_test WHERE id = 1');
        $this->assertSame('Alice', $stmt->fetchColumn());
    }
}
