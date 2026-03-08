<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests CREATE TABLE LIKE and CREATE TABLE AS SELECT on MySQL via MySQLi adapter.
 * @spec SPEC-5.1b
 */
class CreateTableVariantsTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE ctv_source (id INT PRIMARY KEY, val VARCHAR(255))',
            'CREATE TABLE ctv_target LIKE ctv_source',
            'CREATE TABLE ctv_ctas AS SELECT * FROM ctv_source',
        ];
    }

    protected function getTableNames(): array
    {
        return ['ctv_target', 'ctv_ctas', 'ctv_source', 'LIKE', 'AS'];
    }


    public function testCreateTableLike(): void
    {
        $this->mysqli->query('CREATE TABLE ctv_target LIKE ctv_source');

        $this->mysqli->query("INSERT INTO ctv_target (id, val) VALUES (1, 'hello')");

        $result = $this->mysqli->query('SELECT * FROM ctv_target WHERE id = 1');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('hello', $rows[0]['val']);
    }

    public function testCreateTableAsSelect(): void
    {
        $this->mysqli->query("INSERT INTO ctv_source (id, val) VALUES (1, 'hello')");
        $this->mysqli->query("INSERT INTO ctv_source (id, val) VALUES (2, 'world')");

        $this->mysqli->query('CREATE TABLE ctv_ctas AS SELECT * FROM ctv_source');

        $result = $this->mysqli->query('SELECT * FROM ctv_ctas ORDER BY id');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('hello', $rows[0]['val']);
        $this->assertSame('world', $rows[1]['val']);
    }

    public function testCreateTableLikeIsolation(): void
    {
        $this->mysqli->query('CREATE TABLE ctv_target LIKE ctv_source');
        $this->mysqli->query("INSERT INTO ctv_target (id, val) VALUES (1, 'hello')");

        // Physical table doesn't exist — querying with ZTD disabled throws
        $this->mysqli->disableZtd();
        $this->expectException(\mysqli_sql_exception::class);
        $this->expectExceptionMessageMatches("/doesn't exist/");
        $this->mysqli->query('SELECT * FROM ctv_target');
    }
}
