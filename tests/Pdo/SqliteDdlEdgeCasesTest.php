<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests DDL edge cases on SQLite:
 * CREATE TABLE IF NOT EXISTS, DROP TABLE IF EXISTS.
 * Note: SQLite does not support TRUNCATE TABLE syntax.
 * @spec SPEC-5.1
 */
class SqliteDdlEdgeCasesTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE ddl_edge (id INTEGER PRIMARY KEY, val TEXT)',
            'CREATE TABLE IF NOT EXISTS ddl_edge (id INTEGER PRIMARY KEY, val TEXT)',
            'CREATE TABLE ddl_edge (id INTEGER PRIMARY KEY)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['nonexistent_ddl_table', 'IF', 'ddl_edge'];
    }


    public function testCreateTableIfNotExistsOnExistingTable(): void
    {
        // Should NOT throw because IF NOT EXISTS is specified
        $this->pdo->exec('CREATE TABLE IF NOT EXISTS ddl_edge (id INTEGER PRIMARY KEY, val TEXT)');

        $this->pdo->exec("INSERT INTO ddl_edge (id, val) VALUES (1, 'test')");
        $stmt = $this->pdo->query('SELECT * FROM ddl_edge WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
    }

    public function testCreateTableWithoutIfNotExistsThrows(): void
    {
        $this->expectException(ZtdPdoException::class);
        $this->expectExceptionMessageMatches('/already exists/i');
        $this->pdo->exec('CREATE TABLE ddl_edge (id INTEGER PRIMARY KEY)');
    }

    public function testDropTableIfExistsOnNonExistent(): void
    {
        // Should NOT throw because IF EXISTS is specified
        $this->pdo->exec('DROP TABLE IF EXISTS nonexistent_ddl_table');
        $this->assertTrue(true);
    }

    public function testDeleteFromAsWorkaroundForTruncate(): void
    {
        // SQLite does not support TRUNCATE TABLE syntax
        // Use DELETE FROM ... WHERE 1=1 as workaround
        $this->pdo->exec("INSERT INTO ddl_edge (id, val) VALUES (1, 'shadow_data')");

        $this->pdo->exec('DELETE FROM ddl_edge WHERE 1=1');

        $stmt = $this->pdo->query('SELECT * FROM ddl_edge');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));

        // Physical table should still exist and be empty
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT * FROM ddl_edge');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));
    }

    public function testDeleteAllThenInsert(): void
    {
        $this->pdo->exec("INSERT INTO ddl_edge (id, val) VALUES (1, 'before')");
        $this->pdo->exec('DELETE FROM ddl_edge WHERE 1=1');
        $this->pdo->exec("INSERT INTO ddl_edge (id, val) VALUES (2, 'after')");

        $stmt = $this->pdo->query('SELECT * FROM ddl_edge');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('after', $rows[0]['val']);
    }
}
