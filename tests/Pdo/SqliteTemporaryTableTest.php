<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests CREATE TEMPORARY TABLE on SQLite ZTD.
 *
 * The SqliteParser recognizes TEMPORARY in CREATE TABLE statements.
 * In ZTD shadow mode, temporary tables behave the same as regular shadow tables.
 * @spec SPEC-5.1
 */
class SqliteTemporaryTableTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [];
    }

    protected function getTableNames(): array
    {
        return ['statements'];
    }


    public function testCreateTemporaryTable(): void
    {
        $this->pdo->exec('CREATE TEMPORARY TABLE temp_test (id INTEGER PRIMARY KEY, val TEXT)');
        $this->pdo->exec("INSERT INTO temp_test (id, val) VALUES (1, 'hello')");

        $stmt = $this->pdo->query('SELECT val FROM temp_test WHERE id = 1');
        $this->assertSame('hello', $stmt->fetchColumn());
    }

    public function testCreateTemporaryTableIfNotExists(): void
    {
        $this->pdo->exec('CREATE TEMPORARY TABLE IF NOT EXISTS temp_ine (id INTEGER PRIMARY KEY, val TEXT)');
        $this->pdo->exec("INSERT INTO temp_ine (id, val) VALUES (1, 'test')");

        // Second CREATE should not error
        $this->pdo->exec('CREATE TEMPORARY TABLE IF NOT EXISTS temp_ine (id INTEGER PRIMARY KEY, val TEXT)');

        $stmt = $this->pdo->query('SELECT val FROM temp_ine WHERE id = 1');
        $this->assertSame('test', $stmt->fetchColumn());
    }

    public function testDropTemporaryTable(): void
    {
        $this->pdo->exec('CREATE TEMPORARY TABLE temp_drop (id INTEGER PRIMARY KEY, val TEXT)');
        $this->pdo->exec("INSERT INTO temp_drop (id, val) VALUES (1, 'bye')");
        $this->pdo->exec('DROP TABLE temp_drop');

        $this->expectException(\PDOException::class);
        $this->pdo->query('SELECT * FROM temp_drop');
    }

    public function testTemporaryTableUpdateDelete(): void
    {
        $this->pdo->exec('CREATE TEMPORARY TABLE temp_crud (id INTEGER PRIMARY KEY, val TEXT)');
        $this->pdo->exec("INSERT INTO temp_crud (id, val) VALUES (1, 'a')");
        $this->pdo->exec("INSERT INTO temp_crud (id, val) VALUES (2, 'b')");

        $this->pdo->exec("UPDATE temp_crud SET val = 'updated' WHERE id = 1");
        $this->pdo->exec("DELETE FROM temp_crud WHERE id = 2");

        $stmt = $this->pdo->query('SELECT val FROM temp_crud WHERE id = 1');
        $this->assertSame('updated', $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM temp_crud');
        $this->assertSame(1, (int) $stmt->fetchColumn());
    }
}
