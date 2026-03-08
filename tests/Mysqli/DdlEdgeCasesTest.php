<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests DDL edge cases on MySQL via MySQLi adapter:
 * CREATE TABLE IF NOT EXISTS, DROP TABLE IF EXISTS, TRUNCATE isolation.
 * @spec pending
 */
class DdlEdgeCasesTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mysqli_ddl_edge (id INT PRIMARY KEY, val VARCHAR(255))',
            'CREATE TABLE IF NOT EXISTS mysqli_ddl_edge (id INT PRIMARY KEY, val VARCHAR(255))',
            'CREATE TABLE mysqli_ddl_edge (id INT PRIMARY KEY)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mysqli_ddl_edge', 'mysqli_nonexistent_ddl_table', 'IF'];
    }


    public function testCreateTableIfNotExistsOnExistingTable(): void
    {
        // Should NOT throw because IF NOT EXISTS is specified
        $this->mysqli->query('CREATE TABLE IF NOT EXISTS mysqli_ddl_edge (id INT PRIMARY KEY, val VARCHAR(255))');

        $this->mysqli->query("INSERT INTO mysqli_ddl_edge (id, val) VALUES (1, 'test')");
        $result = $this->mysqli->query('SELECT * FROM mysqli_ddl_edge WHERE id = 1');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $rows);
    }

    public function testCreateTableWithoutIfNotExistsThrows(): void
    {
        $this->expectException(ZtdMysqliException::class);
        $this->expectExceptionMessageMatches('/already exists/i');
        $this->mysqli->query('CREATE TABLE mysqli_ddl_edge (id INT PRIMARY KEY)');
    }

    public function testDropTableIfExistsOnNonExistent(): void
    {
        // Should NOT throw because IF EXISTS is specified
        $this->mysqli->query('DROP TABLE IF EXISTS mysqli_nonexistent_ddl_table');
        $this->assertTrue(true);
    }

    public function testTruncateIsolation(): void
    {
        // Insert into shadow
        $this->mysqli->query("INSERT INTO mysqli_ddl_edge (id, val) VALUES (1, 'shadow_data')");

        // Truncate clears shadow data
        $this->mysqli->query('TRUNCATE TABLE mysqli_ddl_edge');

        $result = $this->mysqli->query('SELECT * FROM mysqli_ddl_edge');
        $this->assertCount(0, $result->fetch_all(MYSQLI_ASSOC));

        // Physical table should still exist and be empty (it was already empty)
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT * FROM mysqli_ddl_edge');
        $this->assertCount(0, $result->fetch_all(MYSQLI_ASSOC));
    }

    public function testTruncateThenInsert(): void
    {
        $this->mysqli->query("INSERT INTO mysqli_ddl_edge (id, val) VALUES (1, 'before')");
        $this->mysqli->query('TRUNCATE TABLE mysqli_ddl_edge');
        $this->mysqli->query("INSERT INTO mysqli_ddl_edge (id, val) VALUES (2, 'after')");

        $result = $this->mysqli->query('SELECT * FROM mysqli_ddl_edge');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('after', $rows[0]['val']);
    }
}
