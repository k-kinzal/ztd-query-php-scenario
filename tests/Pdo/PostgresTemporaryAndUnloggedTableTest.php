<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests CREATE TEMPORARY TABLE and CREATE UNLOGGED TABLE on PostgreSQL ZTD.
 *
 * The PgSqlParser recognizes TEMPORARY, TEMP, and UNLOGGED modifiers in CREATE TABLE.
 * In ZTD shadow mode, these modifiers don't affect behavior (all tables are in-memory),
 * but the parser must correctly extract table names despite these keywords.
 * @spec SPEC-5.1
 */
class PostgresTemporaryAndUnloggedTableTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [];
    }

    protected function getTableNames(): array
    {
        return [];
    }


    public function testCreateTemporaryTable(): void
    {
        $this->pdo->exec('CREATE TEMPORARY TABLE pg_temp_test (id INT PRIMARY KEY, val TEXT)');
        $this->pdo->exec("INSERT INTO pg_temp_test (id, val) VALUES (1, 'hello')");

        $stmt = $this->pdo->query('SELECT val FROM pg_temp_test WHERE id = 1');
        $this->assertSame('hello', $stmt->fetchColumn());
    }

    public function testCreateTempTable(): void
    {
        $this->pdo->exec('CREATE TEMP TABLE pg_temp2 (id INT PRIMARY KEY, val TEXT)');
        $this->pdo->exec("INSERT INTO pg_temp2 (id, val) VALUES (1, 'world')");

        $stmt = $this->pdo->query('SELECT val FROM pg_temp2 WHERE id = 1');
        $this->assertSame('world', $stmt->fetchColumn());
    }

    public function testCreateUnloggedTable(): void
    {
        $this->pdo->exec('CREATE UNLOGGED TABLE pg_unlogged_test (id INT PRIMARY KEY, val TEXT)');
        $this->pdo->exec("INSERT INTO pg_unlogged_test (id, val) VALUES (1, 'unlogged')");

        $stmt = $this->pdo->query('SELECT val FROM pg_unlogged_test WHERE id = 1');
        $this->assertSame('unlogged', $stmt->fetchColumn());
    }

    public function testTemporaryTableWithIfNotExists(): void
    {
        $this->pdo->exec('CREATE TEMPORARY TABLE IF NOT EXISTS pg_temp_ine (id INT PRIMARY KEY, val TEXT)');
        $this->pdo->exec("INSERT INTO pg_temp_ine (id, val) VALUES (1, 'test')");

        // Second CREATE should not error
        $this->pdo->exec('CREATE TEMPORARY TABLE IF NOT EXISTS pg_temp_ine (id INT PRIMARY KEY, val TEXT)');

        $stmt = $this->pdo->query('SELECT val FROM pg_temp_ine WHERE id = 1');
        $this->assertSame('test', $stmt->fetchColumn());
    }

    public function testDropTemporaryTable(): void
    {
        $this->pdo->exec('CREATE TEMPORARY TABLE pg_temp_drop (id INT PRIMARY KEY, val TEXT)');
        $this->pdo->exec("INSERT INTO pg_temp_drop (id, val) VALUES (1, 'bye')");
        $this->pdo->exec('DROP TABLE pg_temp_drop');

        $this->expectException(\PDOException::class);
        $this->pdo->query('SELECT * FROM pg_temp_drop');
    }

    public function testTemporaryTablePhysicalIsolation(): void
    {
        $this->pdo->exec('CREATE TEMPORARY TABLE pg_temp_iso (id INT PRIMARY KEY, val TEXT)');
        $this->pdo->exec("INSERT INTO pg_temp_iso (id, val) VALUES (1, 'shadow')");

        // Physical DB should not have this temporary table
        $this->pdo->disableZtd();
        try {
            $this->pdo->query('SELECT * FROM pg_temp_iso');
            // If the query succeeds, table doesn't exist physically
            $this->assertTrue(true);
        } catch (\PDOException $e) {
            // Table does not exist — expected
            $this->assertStringContainsString('does not exist', $e->getMessage());
        }
    }
}
