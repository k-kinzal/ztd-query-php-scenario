<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests ALTER TABLE operations in ZTD mode on SQLite.
 *
 * SQLite's mutation resolver accepts ALTER TABLE (ADD/DROP/RENAME COLUMN, RENAME TO)
 * without throwing exceptions, but the CTE rewriter does NOT reflect schema changes:
 * - ADD COLUMN: new column is silently dropped from SELECT results
 * - DROP COLUMN: column still appears in SELECT results
 * - RENAME COLUMN: old column name is still used in SELECT results
 *
 * This differs from MySQL where ALTER TABLE fully updates the shadow schema.
 * @spec SPEC-5.1a
 */
class SqliteAlterTableTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE alter_test (id INTEGER PRIMARY KEY, name TEXT)';
    }

    protected function getTableNames(): array
    {
        return ['alter_test'];
    }


    public function testAddColumnDoesNotThrow(): void
    {
        // ALTER TABLE ADD COLUMN is accepted without error
        $this->pdo->exec('ALTER TABLE alter_test ADD COLUMN age INTEGER');

        // INSERT with new column also succeeds
        $this->pdo->exec("INSERT INTO alter_test (id, name, age) VALUES (3, 'Charlie', 30)");

        // However, the CTE rewriter uses the original schema — new column is NOT in results
        $stmt = $this->pdo->query('SELECT * FROM alter_test WHERE id = 3');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Charlie', $rows[0]['name']);
        $this->assertArrayNotHasKey('age', $rows[0]);
    }

    public function testDropColumnDoesNotRemoveFromResults(): void
    {
        // ALTER TABLE DROP COLUMN is accepted without error
        $this->pdo->exec('ALTER TABLE alter_test DROP COLUMN name');
        $this->pdo->exec("INSERT INTO alter_test (id) VALUES (3)");

        // But the CTE rewriter still includes 'name' in results (original schema)
        $stmt = $this->pdo->query('SELECT * FROM alter_test WHERE id = 3');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertArrayHasKey('name', $rows[0]);
    }

    public function testRenameColumnDoesNotTakeEffect(): void
    {
        // ALTER TABLE RENAME COLUMN is accepted without error
        $this->pdo->exec('ALTER TABLE alter_test RENAME COLUMN name TO full_name');
        $this->pdo->exec("INSERT INTO alter_test (id, full_name) VALUES (3, 'Charlie')");

        // CTE rewriter still uses old column name
        $stmt = $this->pdo->query('SELECT * FROM alter_test WHERE id = 3');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertArrayHasKey('name', $rows[0]);
        $this->assertArrayNotHasKey('full_name', $rows[0]);
    }

    public function testOriginalColumnsStillWork(): void
    {
        // After ALTER TABLE, original columns still work as expected
        $this->pdo->exec('ALTER TABLE alter_test ADD COLUMN age INTEGER');
        $this->pdo->exec("INSERT INTO alter_test (id, name) VALUES (3, 'Charlie')");

        $stmt = $this->pdo->query('SELECT id, name FROM alter_test WHERE id = 3');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Charlie', $rows[0]['name']);
    }
}
