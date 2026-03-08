<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests CREATE TEMPORARY TABLE on MySQL ZTD (MySQLi adapter).
 *
 * MySQL supports TEMPORARY tables. In ZTD shadow mode, the TEMPORARY modifier
 * should be handled by the parser, and the shadow table should behave normally.
 * @spec SPEC-5.1
 */
class TemporaryTableTest extends AbstractMysqliTestCase
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
        $this->mysqli->query('CREATE TEMPORARY TABLE mi_temp_test (id INT PRIMARY KEY, val VARCHAR(50))');
        $this->mysqli->query("INSERT INTO mi_temp_test (id, val) VALUES (1, 'hello')");

        $result = $this->mysqli->query('SELECT val FROM mi_temp_test WHERE id = 1');
        $this->assertSame('hello', $result->fetch_assoc()['val']);
    }

    public function testTemporaryTableUpdateDelete(): void
    {
        $this->mysqli->query('CREATE TEMPORARY TABLE mi_temp_crud (id INT PRIMARY KEY, val VARCHAR(50))');
        $this->mysqli->query("INSERT INTO mi_temp_crud (id, val) VALUES (1, 'a')");
        $this->mysqli->query("INSERT INTO mi_temp_crud (id, val) VALUES (2, 'b')");

        $this->mysqli->query("UPDATE mi_temp_crud SET val = 'updated' WHERE id = 1");
        $this->mysqli->query("DELETE FROM mi_temp_crud WHERE id = 2");

        $result = $this->mysqli->query('SELECT val FROM mi_temp_crud WHERE id = 1');
        $this->assertSame('updated', $result->fetch_assoc()['val']);

        $result = $this->mysqli->query('SELECT COUNT(*) as cnt FROM mi_temp_crud');
        $this->assertEquals(1, $result->fetch_assoc()['cnt']);
    }

    public function testDropTemporaryTable(): void
    {
        $this->mysqli->query('CREATE TEMPORARY TABLE mi_temp_drop (id INT PRIMARY KEY, val VARCHAR(50))');
        $this->mysqli->query("INSERT INTO mi_temp_drop (id, val) VALUES (1, 'bye')");
        $this->mysqli->query('DROP TABLE mi_temp_drop');

        $this->expectException(\mysqli_sql_exception::class);
        $this->mysqli->query('SELECT * FROM mi_temp_drop');
    }

    public function testTemporaryTablePhysicalIsolation(): void
    {
        $this->mysqli->query('CREATE TEMPORARY TABLE mi_temp_iso (id INT PRIMARY KEY, val VARCHAR(50))');
        $this->mysqli->query("INSERT INTO mi_temp_iso (id, val) VALUES (1, 'shadow')");

        $this->mysqli->disableZtd();
        try {
            $result = $this->mysqli->query('SELECT * FROM mi_temp_iso');
            // Temp table may exist in the physical session
            $this->assertSame(0, $result->num_rows);
        } catch (\mysqli_sql_exception $e) {
            // Table doesn't exist — expected
            $this->assertStringContainsString("doesn't exist", $e->getMessage());
        }
    }
}
