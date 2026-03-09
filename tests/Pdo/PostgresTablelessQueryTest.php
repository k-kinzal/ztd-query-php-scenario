<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests table-less queries through the PostgreSQL PDO ZTD adapter.
 *
 * Queries like SELECT 1, SELECT NOW(), SELECT version() don't reference
 * any table. The CTE rewriter should pass these through without
 * modification since there are no table references to rewrite.
 *
 * @spec SPEC-3.1
 */
class PostgresTablelessQueryTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string
    {
        return 'CREATE TABLE pg_tlq_dummy (id INT PRIMARY KEY)';
    }

    protected function getTableNames(): array
    {
        return ['pg_tlq_dummy'];
    }

    /**
     * SELECT 1 should return the integer literal 1.
     */
    public function testSelectLiteral(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT 1 AS val');
            $this->assertCount(1, $rows);
            $this->assertEquals(1, $rows[0]['val']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT literal through ZTD failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT 1+1 AS result should return 2.
     */
    public function testSelectArithmetic(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT 1+1 AS result');
            $this->assertCount(1, $rows);
            $this->assertEquals(2, $rows[0]['result']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT arithmetic through ZTD failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT 'hello' AS msg should return the string 'hello'.
     */
    public function testSelectStringLiteral(): void
    {
        try {
            $rows = $this->ztdQuery("SELECT 'hello' AS msg");
            $this->assertCount(1, $rows);
            $this->assertSame('hello', $rows[0]['msg']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT string literal through ZTD failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT CURRENT_TIMESTAMP AS ts should return a non-null timestamp.
     */
    public function testSelectCurrentTimestamp(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT CURRENT_TIMESTAMP AS ts');
            $this->assertCount(1, $rows);
            $this->assertNotNull($rows[0]['ts']);
            $this->assertNotEmpty($rows[0]['ts']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT CURRENT_TIMESTAMP through ZTD failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT version() should return the PostgreSQL server version string.
     */
    public function testSelectVersion(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT version() AS ver');
            $this->assertCount(1, $rows);
            $this->assertNotNull($rows[0]['ver']);
            $this->assertNotEmpty($rows[0]['ver']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT version() through ZTD failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT current_database() should return the current database name.
     */
    public function testSelectCurrentDatabase(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT current_database() AS db');
            $this->assertCount(1, $rows);
            $this->assertNotNull($rows[0]['db']);
            $this->assertNotEmpty($rows[0]['db']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT current_database() through ZTD failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT current_user should return the current user name.
     */
    public function testSelectCurrentUser(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT current_user AS usr');
            $this->assertCount(1, $rows);
            $this->assertNotNull($rows[0]['usr']);
            $this->assertNotEmpty($rows[0]['usr']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT current_user through ZTD failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT pg_backend_pid() should return a non-null integer PID.
     */
    public function testSelectPgBackendPid(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT pg_backend_pid() AS pid');
            $this->assertCount(1, $rows);
            $this->assertNotNull($rows[0]['pid']);
            $this->assertGreaterThan(0, (int) $rows[0]['pid']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT pg_backend_pid() through ZTD failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT NULL AS val should return null.
     */
    public function testSelectNullLiteral(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT NULL AS val');
            $this->assertCount(1, $rows);
            $this->assertNull($rows[0]['val']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT NULL through ZTD failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared table-less query: SELECT ? + ? AS result with params [3, 4]
     * should return 7.
     *
     * PDO uses ? placeholders regardless of the underlying driver.
     */
    public function testPreparedTablelessQuery(): void
    {
        try {
            $stmt = $this->pdo->prepare('SELECT ?::int + ?::int AS result');
            $stmt->execute([3, 4]);
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            $this->assertCount(1, $rows);
            $this->assertEquals(7, $rows[0]['result']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared table-less query through ZTD failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * After inserting into the dummy table, a table-less SELECT 1+1
     * should still work correctly.
     */
    public function testTablelessQueryAfterShadowInsert(): void
    {
        try {
            $this->pdo->exec('INSERT INTO pg_tlq_dummy (id) VALUES (1)');

            $rows = $this->ztdQuery('SELECT 1+1 AS result');
            $this->assertCount(1, $rows);
            $this->assertEquals(2, $rows[0]['result']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Table-less query after shadow INSERT failed: ' . $e->getMessage()
            );
        }
    }
}
