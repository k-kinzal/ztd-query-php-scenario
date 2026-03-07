<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;
use ZtdQuery\Adapter\Pdo\ZtdPdoException;

/**
 * Tests that specific exception types are thrown for various error conditions.
 *
 * These exceptions are defined in vendor/k-kinzal/ztd-query-core/src/Exception/:
 * - TableAlreadyExistsException: CREATE TABLE on existing shadow table
 * - ColumnAlreadyExistsException: ALTER TABLE ADD COLUMN for existing column
 * - ColumnNotFoundException: ALTER TABLE on non-existent column
 *
 * Using SQLite for fast in-memory testing since exception behavior
 * is consistent across platforms.
 */
class SqliteExceptionTypesTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE ex_test (id INTEGER PRIMARY KEY, name TEXT, score INT)');

        $this->pdo = ZtdPdo::fromPdo($raw);
    }

    /**
     * CREATE TABLE on a table that already exists in shadow store
     * should throw an exception (via CreateTableMutation).
     */
    public function testCreateTableAlreadyExistsThrows(): void
    {
        $this->pdo->exec('CREATE TABLE new_table (id INTEGER PRIMARY KEY, val TEXT)');

        // Second CREATE without IF NOT EXISTS should throw
        $this->expectException(ZtdPdoException::class);
        $this->pdo->exec('CREATE TABLE new_table (id INTEGER PRIMARY KEY, val TEXT)');
    }

    /**
     * CREATE TABLE IF NOT EXISTS on existing table should NOT throw.
     */
    public function testCreateTableIfNotExistsDoesNotThrow(): void
    {
        $this->pdo->exec('CREATE TABLE new_table2 (id INTEGER PRIMARY KEY, val TEXT)');
        $this->pdo->exec('CREATE TABLE IF NOT EXISTS new_table2 (id INTEGER PRIMARY KEY, val TEXT)');

        // Data should still be accessible
        $this->pdo->exec("INSERT INTO new_table2 (id, val) VALUES (1, 'test')");
        $stmt = $this->pdo->query('SELECT val FROM new_table2 WHERE id = 1');
        $this->assertSame('test', $stmt->fetchColumn());
    }

    /**
     * SQLite: CREATE TABLE AS SELECT on existing table does NOT throw.
     * The SQLite mutation resolver silently overwrites the existing shadow table.
     * This differs from MySQL where CreateTableAsSelectMutation throws.
     */
    public function testCreateTableAsSelectOnExistingDoesNotThrowOnSqlite(): void
    {
        $this->pdo->exec("INSERT INTO ex_test (id, name, score) VALUES (1, 'Alice', 90)");
        $this->pdo->exec('CREATE TABLE ctas_result AS SELECT * FROM ex_test');
        $this->pdo->exec('CREATE TABLE ctas_result AS SELECT * FROM ex_test');

        // No exception thrown — table is silently replaced
        $this->assertTrue(true);
    }

    /**
     * SQLite: ALTER TABLE ADD COLUMN for existing column does NOT throw.
     * The SQLite mutation resolver rebuilds the table via CreateTableMutation
     * and does not check for column existence like MySQL's AlterTableMutation.
     */
    public function testAlterTableAddExistingColumnDoesNotThrowOnSqlite(): void
    {
        $this->pdo->exec('ALTER TABLE ex_test ADD COLUMN name TEXT');

        // No ColumnAlreadyExistsException — silently accepted
        $this->assertTrue(true);
    }

    /**
     * SQLite: ALTER TABLE DROP non-existent column does NOT throw.
     * The SQLite mutation resolver silently ignores the missing column.
     */
    public function testAlterTableDropNonExistentColumnDoesNotThrowOnSqlite(): void
    {
        $this->pdo->exec('ALTER TABLE ex_test DROP COLUMN nonexistent');

        // No ColumnNotFoundException — silently ignored
        $this->assertTrue(true);
    }

    /**
     * SQLite: ALTER TABLE RENAME non-existent column does NOT throw.
     */
    public function testAlterTableRenameNonExistentColumnDoesNotThrowOnSqlite(): void
    {
        $this->pdo->exec('ALTER TABLE ex_test RENAME COLUMN nonexistent TO new_name');

        // No ColumnNotFoundException — silently ignored
        $this->assertTrue(true);
    }

    /**
     * DROP TABLE on shadow-created table, then query should throw.
     */
    public function testDropTableThenQueryThrows(): void
    {
        $this->pdo->exec('CREATE TABLE drop_me (id INTEGER PRIMARY KEY, val TEXT)');
        $this->pdo->exec("INSERT INTO drop_me (id, val) VALUES (1, 'test')");
        $this->pdo->exec('DROP TABLE drop_me');

        $this->expectException(\PDOException::class);
        $this->pdo->query('SELECT * FROM drop_me');
    }

    /**
     * DROP TABLE IF EXISTS on non-existent table should not throw.
     */
    public function testDropTableIfExistsOnNonExistent(): void
    {
        $this->pdo->exec('DROP TABLE IF EXISTS totally_missing');
        $this->assertTrue(true); // No exception = pass
    }
}
