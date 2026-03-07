<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;
use ZtdQuery\Adapter\Pdo\ZtdPdoException;

/**
 * Tests DDL edge cases in ZTD mode on SQLite:
 * CREATE TABLE IF NOT EXISTS, DROP TABLE IF EXISTS, DROP non-existent table.
 */
class DdlEdgeCasesTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE existing_table (id INTEGER PRIMARY KEY, val TEXT)');

        $this->pdo = ZtdPdo::fromPdo($raw);
    }

    public function testCreateTableIfNotExistsOnExistingTable(): void
    {
        // Should NOT throw because IF NOT EXISTS is specified
        $this->pdo->exec('CREATE TABLE IF NOT EXISTS existing_table (id INTEGER PRIMARY KEY, val TEXT)');

        // Table should still be usable
        $this->pdo->exec("INSERT INTO existing_table (id, val) VALUES (1, 'test')");
        $stmt = $this->pdo->query('SELECT * FROM existing_table WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('test', $rows[0]['val']);
    }

    public function testCreateTableWithoutIfNotExistsOnExistingTableThrows(): void
    {
        $this->expectException(ZtdPdoException::class);
        $this->expectExceptionMessageMatches('/already exists/i');
        $this->pdo->exec('CREATE TABLE existing_table (id INTEGER PRIMARY KEY, val TEXT)');
    }

    public function testCreateTableIfNotExistsOnNewTable(): void
    {
        $this->pdo->exec('CREATE TABLE IF NOT EXISTS new_table (id INTEGER PRIMARY KEY, name TEXT)');

        $this->pdo->exec("INSERT INTO new_table (id, name) VALUES (1, 'hello')");
        $stmt = $this->pdo->query('SELECT * FROM new_table WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('hello', $rows[0]['name']);
    }

    public function testDropTableIfExistsOnExistingTable(): void
    {
        $this->pdo->exec("INSERT INTO existing_table (id, val) VALUES (1, 'test')");

        // Should succeed
        $this->pdo->exec('DROP TABLE IF EXISTS existing_table');

        // Shadow data should be cleared
        $stmt = $this->pdo->query('SELECT * FROM existing_table');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(0, $rows);
    }

    public function testDropTableIfExistsOnNonExistentTable(): void
    {
        // Should NOT throw because IF EXISTS is specified
        $this->pdo->exec('DROP TABLE IF EXISTS nonexistent_table');
        $this->assertTrue(true); // No exception = pass
    }

    public function testDropTableWithoutIfExistsOnNonExistentTableThrows(): void
    {
        $this->expectException(\Throwable::class);
        $this->pdo->exec('DROP TABLE nonexistent_table');
    }

    public function testCreateDropCreateCycle(): void
    {
        // Create shadow table
        $this->pdo->exec('CREATE TABLE cycle_table (id INTEGER PRIMARY KEY, val TEXT)');
        $this->pdo->exec("INSERT INTO cycle_table (id, val) VALUES (1, 'first')");

        // Drop it
        $this->pdo->exec('DROP TABLE IF EXISTS cycle_table');

        // Recreate it
        $this->pdo->exec('CREATE TABLE cycle_table (id INTEGER PRIMARY KEY, val TEXT)');
        $this->pdo->exec("INSERT INTO cycle_table (id, val) VALUES (2, 'second')");

        $stmt = $this->pdo->query('SELECT * FROM cycle_table');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('second', $rows[0]['val']);
    }

    public function testDropTableThenQueryFallsThrough(): void
    {
        // Drop existing table's shadow
        $this->pdo->exec('DROP TABLE existing_table');

        // Queries should now fall through to physical table (which is empty)
        $stmt = $this->pdo->query('SELECT * FROM existing_table');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(0, $rows);
    }

    public function testTruncateOnSqliteThrows(): void
    {
        // SQLite does not have native TRUNCATE TABLE syntax.
        // ZTD should throw since TRUNCATE is unsupported on SQLite.
        $this->expectException(\Throwable::class);
        $this->pdo->exec('TRUNCATE TABLE existing_table');
    }
}
