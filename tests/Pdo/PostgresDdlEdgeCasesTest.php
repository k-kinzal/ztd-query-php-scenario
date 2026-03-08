<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests DDL edge cases on PostgreSQL:
 * CREATE TABLE IF NOT EXISTS, DROP TABLE IF EXISTS, TRUNCATE isolation.
 * @spec pending
 */
class PostgresDdlEdgeCasesTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_ddl_edge (id INT PRIMARY KEY, val VARCHAR(255))',
            'CREATE TABLE IF NOT EXISTS pg_ddl_edge (id INT PRIMARY KEY, val VARCHAR(255))',
            'CREATE TABLE pg_ddl_edge (id INT PRIMARY KEY)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_ddl_edge', 'pg_nonexistent_ddl_table', 'IF'];
    }


    public function testCreateTableIfNotExistsOnExistingTable(): void
    {
        // Should NOT throw
        $this->pdo->exec('CREATE TABLE IF NOT EXISTS pg_ddl_edge (id INT PRIMARY KEY, val VARCHAR(255))');

        $this->pdo->exec("INSERT INTO pg_ddl_edge (id, val) VALUES (1, 'test')");
        $stmt = $this->pdo->query('SELECT * FROM pg_ddl_edge WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
    }

    public function testCreateTableWithoutIfNotExistsThrows(): void
    {
        $this->expectException(ZtdPdoException::class);
        $this->expectExceptionMessageMatches('/already exists/i');
        $this->pdo->exec('CREATE TABLE pg_ddl_edge (id INT PRIMARY KEY)');
    }

    public function testDropTableIfExistsOnNonExistent(): void
    {
        // Should NOT throw
        $this->pdo->exec('DROP TABLE IF EXISTS pg_nonexistent_ddl_table');
        $this->assertTrue(true);
    }

    public function testTruncateIsolation(): void
    {
        // Insert into shadow
        $this->pdo->exec("INSERT INTO pg_ddl_edge (id, val) VALUES (1, 'shadow_data')");

        // Truncate clears shadow data
        $this->pdo->exec('TRUNCATE TABLE pg_ddl_edge');

        $stmt = $this->pdo->query('SELECT * FROM pg_ddl_edge');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));

        // Physical table should still exist and be empty (it was already empty)
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT * FROM pg_ddl_edge');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));
    }

    public function testTruncateThenInsert(): void
    {
        $this->pdo->exec("INSERT INTO pg_ddl_edge (id, val) VALUES (1, 'before')");
        $this->pdo->exec('TRUNCATE TABLE pg_ddl_edge');
        $this->pdo->exec("INSERT INTO pg_ddl_edge (id, val) VALUES (2, 'after')");

        $stmt = $this->pdo->query('SELECT * FROM pg_ddl_edge');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('after', $rows[0]['val']);
    }
}
