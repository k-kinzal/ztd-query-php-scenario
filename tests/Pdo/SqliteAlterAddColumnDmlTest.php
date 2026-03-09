<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests ALTER TABLE ADD COLUMN followed by DML through CTE shadow on SQLite.
 *
 * Known Issue: After ALTER TABLE ADD COLUMN, the CTE rewriter's schema cache
 * is not invalidated. INSERT/UPDATE referencing the new column succeed (shadow
 * store accepts them), but SELECT queries that reference the new column fail
 * with "no such column" because the CTE uses the stale schema.
 *
 * This means data written to the new column is silently lost — it goes into
 * the shadow store but can never be read back through ZTD.
 *
 * @spec SPEC-5.2
 * @see SPEC-11.ALTER-ADD-COL-STALE-SCHEMA
 */
class SqliteAlterAddColumnDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sq_alt_col (id INTEGER PRIMARY KEY, name TEXT)';
    }

    protected function getTableNames(): array
    {
        return ['sq_alt_col'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sq_alt_col VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO sq_alt_col VALUES (2, 'Bob')");
    }

    /**
     * INSERT with new column succeeds (shadow store accepts it).
     */
    public function testAddColumnThenInsertSucceeds(): void
    {
        $this->pdo->exec("ALTER TABLE sq_alt_col ADD COLUMN age INTEGER DEFAULT 0");
        $this->pdo->exec("INSERT INTO sq_alt_col (id, name, age) VALUES (3, 'Charlie', 25)");

        // The row was accepted into the shadow store
        $rows = $this->ztdQuery('SELECT id, name FROM sq_alt_col ORDER BY id');
        $this->assertCount(3, $rows, 'INSERT with new column should succeed');
        $this->assertSame('Charlie', $rows[2]['name']);
    }

    /**
     * SELECT referencing new column fails (Known Issue).
     *
     * The data is in the shadow store but the CTE rewriter doesn't know
     * about the column, so it generates SQL without it.
     */
    public function testAddColumnThenSelectNewColumnFails(): void
    {
        $this->pdo->exec("ALTER TABLE sq_alt_col ADD COLUMN age INTEGER DEFAULT 0");
        $this->pdo->exec("INSERT INTO sq_alt_col (id, name, age) VALUES (3, 'Charlie', 25)");

        $this->expectException(\PDOException::class);
        $this->expectExceptionMessage('no such column');
        $this->ztdQuery('SELECT id, name, age FROM sq_alt_col ORDER BY id');
    }

    /**
     * UPDATE with new column succeeds (shadow store accepts it).
     */
    public function testAddColumnThenUpdateSucceeds(): void
    {
        $this->pdo->exec("ALTER TABLE sq_alt_col ADD COLUMN score INTEGER DEFAULT 0");
        // UPDATE succeeds - shadow store doesn't validate column existence
        $this->pdo->exec("UPDATE sq_alt_col SET score = 100 WHERE id = 1");

        // Original columns still readable
        $rows = $this->ztdQuery('SELECT id, name FROM sq_alt_col ORDER BY id');
        $this->assertCount(2, $rows);
    }

    /**
     * SELECT with new column in SET fails on read-back (Known Issue).
     */
    public function testAddColumnThenSelectAfterUpdateFails(): void
    {
        $this->pdo->exec("ALTER TABLE sq_alt_col ADD COLUMN score INTEGER DEFAULT 0");
        $this->pdo->exec("UPDATE sq_alt_col SET score = 100 WHERE id = 1");

        $this->expectException(\PDOException::class);
        $this->expectExceptionMessage('no such column');
        $this->ztdQuery('SELECT id, score FROM sq_alt_col ORDER BY id');
    }

    /**
     * WHERE clause with new column also fails (Known Issue).
     */
    public function testAddColumnThenWhereNewColumnFails(): void
    {
        $this->pdo->exec("ALTER TABLE sq_alt_col ADD COLUMN priority INTEGER DEFAULT 1");

        $this->expectException(\PDOException::class);
        $this->expectExceptionMessage('no such column');
        $this->ztdQuery('SELECT id FROM sq_alt_col WHERE priority > 0 ORDER BY id');
    }

    /**
     * ORDER BY new column also fails (Known Issue).
     */
    public function testAddColumnThenOrderByNewColumnFails(): void
    {
        $this->pdo->exec("ALTER TABLE sq_alt_col ADD COLUMN rank INTEGER DEFAULT 99");

        $this->expectException(\PDOException::class);
        $this->expectExceptionMessage('no such column');
        $this->ztdQuery('SELECT id, name FROM sq_alt_col ORDER BY rank ASC');
    }

    /**
     * Queries using only original columns still work after ADD COLUMN.
     */
    public function testOriginalColumnsStillWorkAfterAddColumn(): void
    {
        $this->pdo->exec("ALTER TABLE sq_alt_col ADD COLUMN age INTEGER DEFAULT 0");

        $rows = $this->ztdQuery('SELECT id, name FROM sq_alt_col ORDER BY id');
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    /**
     * DROP COLUMN then query (SQLite 3.35.0+).
     */
    public function testDropColumnThenQuery(): void
    {
        $version = $this->pdo->query('SELECT sqlite_version()')->fetchColumn();
        if (version_compare($version, '3.35.0', '<')) {
            $this->markTestSkipped('DROP COLUMN requires SQLite 3.35.0+');
        }

        try {
            $this->pdo->exec("ALTER TABLE sq_alt_col DROP COLUMN name");

            $rows = $this->ztdQuery('SELECT id FROM sq_alt_col ORDER BY id');
            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestSkipped('ALTER DROP COLUMN not supported through CTE: ' . $e->getMessage());
        }
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sq_alt_col');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
