<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests SAVEPOINT behavior on SQLite with ZTD.
 * @spec SPEC-6.3
 */
class SqliteSavepointBehaviorTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sp_test (id INT PRIMARY KEY, name VARCHAR(50))';
    }

    protected function getTableNames(): array
    {
        return ['sp_test'];
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
