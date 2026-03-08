<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests string edge cases, wide table handling, and value boundary patterns on MySQL.
 * @spec SPEC-4.11
 */
class MysqlStringAndWidenessEdgeCaseTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sw_str_m (id INT PRIMARY KEY, val TEXT)',
            'CREATE TABLE sw_long_m (id INT PRIMARY KEY, content LONGTEXT)',
            'CREATE TABLE sw_int_m (id INT PRIMARY KEY, big_val BIGINT)',
            'CREATE TABLE sw_wide_m (id INT PRIMARY KEY,',
            'CREATE TABLE sw_cond_m (id INT PRIMARY KEY, a INT, b INT, c INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sw_str_m', 'sw_long_m', 'sw_wide_m', 'sw_int_m', 'sw_cond_m'];
    }


    public function testEmptyStringVsNull(): void
    {
        $this->pdo->exec("INSERT INTO sw_str_m VALUES (1, '')");
        $this->pdo->exec('INSERT INTO sw_str_m VALUES (2, NULL)');

        $stmt = $this->pdo->query('SELECT id FROM sw_str_m WHERE val IS NOT NULL ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(1, $rows);
        $this->assertEquals(1, $rows[0]);
    }

    public function testVeryLongStringValue(): void
    {
        $longStr = str_repeat('a', 10000);
        $stmt = $this->pdo->prepare('INSERT INTO sw_long_m VALUES (1, ?)');
        $stmt->execute([$longStr]);

        $stmt = $this->pdo->query('SELECT content FROM sw_long_m WHERE id = 1');
        $result = $stmt->fetchColumn();
        $this->assertSame($longStr, $result);
    }

    public function testWideTable20Columns(): void
    {
        $vals = [];
        for ($i = 1; $i <= 20; $i++) {
            $vals[] = "'val{$i}'";
        }
        $this->pdo->exec('INSERT INTO sw_wide_m VALUES (1, ' . implode(', ', $vals) . ')');

        $stmt = $this->pdo->query('SELECT * FROM sw_wide_m WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->assertSame('val1', $row['col1']);
        $this->assertSame('val10', $row['col10']);
        $this->assertSame('val20', $row['col20']);
    }

    public function testInsertAndSelectMaxIntValues(): void
    {
        $this->pdo->exec('INSERT INTO sw_int_m VALUES (1, 2147483647)');
        $this->pdo->exec('INSERT INTO sw_int_m VALUES (2, -2147483648)');

        $stmt = $this->pdo->query('SELECT big_val FROM sw_int_m ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertEquals(2147483647, $rows[0]);
        $this->assertEquals(-2147483648, $rows[1]);
    }

    public function testSelectWithNestedOrAnd(): void
    {
        $this->pdo->exec('CREATE TABLE sw_cond_m (id INT PRIMARY KEY, a INT, b INT, c INT)');
        $this->pdo->exec('INSERT INTO sw_cond_m VALUES (1, 1, 0, 0)');
        $this->pdo->exec('INSERT INTO sw_cond_m VALUES (2, 0, 1, 1)');
        $this->pdo->exec('INSERT INTO sw_cond_m VALUES (3, 1, 1, 0)');

        $stmt = $this->pdo->query('SELECT id FROM sw_cond_m WHERE (a = 1 AND b = 1) OR (b = 1 AND c = 1) ORDER BY id');
        $ids = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(2, $ids);
        $this->assertEquals(2, $ids[0]);
        $this->assertEquals(3, $ids[1]);
    }
}
