<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests string edge cases, wide table handling, and value boundary patterns via MySQLi.
 *
 * Cross-platform parity with MysqlStringAndWidenessEdgeCaseTest (PDO).
 * @spec pending
 */
class StringAndWidenessEdgeCaseTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_sw_str (id INT PRIMARY KEY, val TEXT)',
            'CREATE TABLE mi_sw_long (id INT PRIMARY KEY, content LONGTEXT)',
            'CREATE TABLE mi_sw_int (id INT PRIMARY KEY, big_val BIGINT)',
            'CREATE TABLE mi_sw_wide (id INT PRIMARY KEY,',
            'CREATE TABLE mi_sw_cond (id INT PRIMARY KEY, a INT, b INT, c INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_sw_str', 'mi_sw_long', 'mi_sw_wide', 'mi_sw_int', 'mi_sw_cond'];
    }


    public function testEmptyStringVsNull(): void
    {
        $this->mysqli->query("INSERT INTO mi_sw_str VALUES (1, '')");
        $this->mysqli->query('INSERT INTO mi_sw_str VALUES (2, NULL)');

        $result = $this->mysqli->query('SELECT id FROM mi_sw_str WHERE val IS NOT NULL ORDER BY id');
        $rows = [];
        while ($row = $result->fetch_assoc()) {
            $rows[] = $row['id'];
        }
        $this->assertCount(1, $rows);
        $this->assertEquals(1, $rows[0]);
    }

    public function testVeryLongStringValue(): void
    {
        $longStr = str_repeat('a', 10000);
        $stmt = $this->mysqli->prepare('INSERT INTO mi_sw_long VALUES (1, ?)');
        $stmt->bind_param('s', $longStr);
        $stmt->execute();

        $result = $this->mysqli->query('SELECT content FROM mi_sw_long WHERE id = 1');
        $this->assertSame($longStr, $result->fetch_assoc()['content']);
    }

    public function testWideTable20Columns(): void
    {
        $vals = [];
        for ($i = 1; $i <= 20; $i++) {
            $vals[] = "'val{$i}'";
        }
        $this->mysqli->query('INSERT INTO mi_sw_wide VALUES (1, ' . implode(', ', $vals) . ')');

        $result = $this->mysqli->query('SELECT * FROM mi_sw_wide WHERE id = 1');
        $row = $result->fetch_assoc();

        $this->assertSame('val1', $row['col1']);
        $this->assertSame('val10', $row['col10']);
        $this->assertSame('val20', $row['col20']);
    }

    public function testInsertAndSelectMaxIntValues(): void
    {
        $this->mysqli->query('INSERT INTO mi_sw_int VALUES (1, 2147483647)');
        $this->mysqli->query('INSERT INTO mi_sw_int VALUES (2, -2147483648)');

        $result = $this->mysqli->query('SELECT big_val FROM mi_sw_int ORDER BY id');
        $rows = [];
        while ($row = $result->fetch_assoc()) {
            $rows[] = $row['big_val'];
        }
        $this->assertEquals(2147483647, $rows[0]);
        $this->assertEquals(-2147483648, $rows[1]);
    }

    public function testSelectWithNestedOrAnd(): void
    {
        $this->mysqli->query('CREATE TABLE mi_sw_cond (id INT PRIMARY KEY, a INT, b INT, c INT)');
        $this->mysqli->query('INSERT INTO mi_sw_cond VALUES (1, 1, 0, 0)');
        $this->mysqli->query('INSERT INTO mi_sw_cond VALUES (2, 0, 1, 1)');
        $this->mysqli->query('INSERT INTO mi_sw_cond VALUES (3, 1, 1, 0)');

        $result = $this->mysqli->query('SELECT id FROM mi_sw_cond WHERE (a = 1 AND b = 1) OR (b = 1 AND c = 1) ORDER BY id');
        $ids = [];
        while ($row = $result->fetch_assoc()) {
            $ids[] = $row['id'];
        }
        $this->assertCount(2, $ids);
        $this->assertEquals(2, $ids[0]);
        $this->assertEquals(3, $ids[1]);
    }
}
