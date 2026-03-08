<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests DDL edge cases on MySQL via PDO adapter:
 * CREATE TABLE IF NOT EXISTS, DROP TABLE IF EXISTS, TRUNCATE isolation.
 * @spec pending
 */
class MysqlDdlEdgeCasesTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mysql_pdo_ddl_edge (id INT PRIMARY KEY, val VARCHAR(255))',
            'CREATE TABLE IF NOT EXISTS mysql_pdo_ddl_edge (id INT PRIMARY KEY, val VARCHAR(255))',
            'CREATE TABLE mysql_pdo_ddl_edge (id INT PRIMARY KEY)',
            'CREATE TABLE IF NOT EXISTS mysql_pdo_ddl_edge_new (id INT PRIMARY KEY, name VARCHAR(255))',
            'CREATE TABLE mysql_pdo_ddl_cycle (id INT PRIMARY KEY, val VARCHAR(255))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mysql_pdo_ddl_edge', 'mysql_pdo_nonexistent_ddl_table', 'mysql_pdo_ddl_cycle', 'IF', 'mysql_pdo_ddl_edge_new'];
    }


    public function testCreateTableIfNotExistsOnExistingTable(): void
    {
        // Should NOT throw because IF NOT EXISTS is specified
        $this->pdo->exec('CREATE TABLE IF NOT EXISTS mysql_pdo_ddl_edge (id INT PRIMARY KEY, val VARCHAR(255))');

        $this->pdo->exec("INSERT INTO mysql_pdo_ddl_edge (id, val) VALUES (1, 'test')");
        $stmt = $this->pdo->query('SELECT * FROM mysql_pdo_ddl_edge WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
    }

    public function testCreateTableWithoutIfNotExistsThrows(): void
    {
        $this->expectException(ZtdPdoException::class);
        $this->expectExceptionMessageMatches('/already exists/i');
        $this->pdo->exec('CREATE TABLE mysql_pdo_ddl_edge (id INT PRIMARY KEY)');
    }

    public function testDropTableIfExistsOnNonExistent(): void
    {
        // Should NOT throw because IF EXISTS is specified
        $this->pdo->exec('DROP TABLE IF EXISTS mysql_pdo_nonexistent_ddl_table');
        $this->assertTrue(true);
    }

    public function testCreateTableIfNotExistsOnNewTable(): void
    {
        $this->pdo->exec('CREATE TABLE IF NOT EXISTS mysql_pdo_ddl_edge_new (id INT PRIMARY KEY, name VARCHAR(255))');

        $this->pdo->exec("INSERT INTO mysql_pdo_ddl_edge_new (id, name) VALUES (1, 'hello')");
        $stmt = $this->pdo->query('SELECT * FROM mysql_pdo_ddl_edge_new WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('hello', $rows[0]['name']);
    }

    public function testDropTableIfExistsOnExistingTable(): void
    {
        $this->pdo->exec("INSERT INTO mysql_pdo_ddl_edge (id, val) VALUES (1, 'test')");

        // Should succeed
        $this->pdo->exec('DROP TABLE IF EXISTS mysql_pdo_ddl_edge');

        // Shadow data should be cleared
        $stmt = $this->pdo->query('SELECT * FROM mysql_pdo_ddl_edge');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(0, $rows);
    }

    public function testDropTableWithoutIfExistsOnNonExistentTableThrows(): void
    {
        $this->expectException(\Throwable::class);
        $this->pdo->exec('DROP TABLE mysql_pdo_nonexistent_ddl_table');
    }

    public function testCreateDropCreateCycle(): void
    {
        // Create shadow table
        $this->pdo->exec('CREATE TABLE mysql_pdo_ddl_cycle (id INT PRIMARY KEY, val VARCHAR(255))');
        $this->pdo->exec("INSERT INTO mysql_pdo_ddl_cycle (id, val) VALUES (1, 'first')");

        // Drop it
        $this->pdo->exec('DROP TABLE IF EXISTS mysql_pdo_ddl_cycle');

        // Recreate it
        $this->pdo->exec('CREATE TABLE mysql_pdo_ddl_cycle (id INT PRIMARY KEY, val VARCHAR(255))');
        $this->pdo->exec("INSERT INTO mysql_pdo_ddl_cycle (id, val) VALUES (2, 'second')");

        $stmt = $this->pdo->query('SELECT * FROM mysql_pdo_ddl_cycle');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('second', $rows[0]['val']);
    }

    public function testTruncateIsolation(): void
    {
        // Insert into shadow
        $this->pdo->exec("INSERT INTO mysql_pdo_ddl_edge (id, val) VALUES (1, 'shadow_data')");

        // Truncate clears shadow data
        $this->pdo->exec('TRUNCATE TABLE mysql_pdo_ddl_edge');

        $stmt = $this->pdo->query('SELECT * FROM mysql_pdo_ddl_edge');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));

        // Physical table should still exist and be empty (it was already empty)
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT * FROM mysql_pdo_ddl_edge');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));
    }

    public function testTruncateThenInsert(): void
    {
        $this->pdo->exec("INSERT INTO mysql_pdo_ddl_edge (id, val) VALUES (1, 'before')");
        $this->pdo->exec('TRUNCATE TABLE mysql_pdo_ddl_edge');
        $this->pdo->exec("INSERT INTO mysql_pdo_ddl_edge (id, val) VALUES (2, 'after')");

        $stmt = $this->pdo->query('SELECT * FROM mysql_pdo_ddl_edge');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('after', $rows[0]['val']);
    }
}
