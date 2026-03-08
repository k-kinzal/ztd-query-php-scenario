<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests string edge cases, wide table handling, and value boundary patterns.
 * @spec SPEC-4.11
 */
class SqliteStringAndWidenessEdgeCaseTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE str_test (id INT PRIMARY KEY, val VARCHAR(50))',
            'CREATE TABLE long_str (id INT PRIMARY KEY, content TEXT)',
            'CREATE TABLE quote_test (id INT PRIMARY KEY, val TEXT)',
            'CREATE TABLE dquote_test (id INT PRIMARY KEY, val TEXT)',
            'CREATE TABLE wide20 (id INT PRIMARY KEY,',
            'CREATE TABLE multi_upd (id INT PRIMARY KEY, a VARCHAR(20), b VARCHAR(20), c VARCHAR(20))',
            'CREATE TABLE int_edge (id INT PRIMARY KEY, big_val BIGINT)',
            'CREATE TABLE named_test (id INT PRIMARY KEY, name VARCHAR(50), score INT, active INT)',
            'CREATE TABLE del_multi (id INT PRIMARY KEY, cat VARCHAR(20), status INT)',
            'CREATE TABLE or_test (id INT PRIMARY KEY, name VARCHAR(50), role VARCHAR(20))',
            'CREATE TABLE nested_cond (id INT PRIMARY KEY, a INT, b INT, c INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['str_test', 'long_str', 'quote_test', 'dquote_test', 'wide20', 'multi_upd', 'int_edge', 'named_test', 'del_multi', 'or_test', 'nested_cond'];
    }


    public function testEmptyStringVsNull(): void
    {
        $this->pdo->exec('CREATE TABLE str_test (id INT PRIMARY KEY, val VARCHAR(50))');
        $this->pdo->exec("INSERT INTO str_test VALUES (1, '')");
        $this->pdo->exec('INSERT INTO str_test VALUES (2, NULL)');

        // Empty string is NOT NULL
        $stmt = $this->pdo->query("SELECT id FROM str_test WHERE val IS NOT NULL");
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame([1], array_map('intval', $rows));

        // NULL IS NULL
        $stmt = $this->pdo->query('SELECT id FROM str_test WHERE val IS NULL');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame([2], array_map('intval', $rows));

        // Empty string equals empty string
        $stmt = $this->pdo->query("SELECT id FROM str_test WHERE val = ''");
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(1, $rows);
        $this->assertEquals(1, $rows[0]);
    }

    public function testVeryLongStringValue(): void
    {
        $this->pdo->exec('CREATE TABLE long_str (id INT PRIMARY KEY, content TEXT)');

        $longStr = str_repeat('a', 10000);
        $stmt = $this->pdo->prepare('INSERT INTO long_str VALUES (1, ?)');
        $stmt->execute([$longStr]);

        $stmt = $this->pdo->query('SELECT content FROM long_str WHERE id = 1');
        $result = $stmt->fetchColumn();
        $this->assertSame($longStr, $result);
    }

    public function testStringWithSingleQuotes(): void
    {
        $this->pdo->exec('CREATE TABLE quote_test (id INT PRIMARY KEY, val TEXT)');

        $value = "It's a test with 'quotes' inside";
        $stmt = $this->pdo->prepare('INSERT INTO quote_test VALUES (1, ?)');
        $stmt->execute([$value]);

        $stmt = $this->pdo->query('SELECT val FROM quote_test WHERE id = 1');
        $this->assertSame($value, $stmt->fetchColumn());
    }

    public function testStringWithDoubleQuotes(): void
    {
        $this->pdo->exec('CREATE TABLE dquote_test (id INT PRIMARY KEY, val TEXT)');

        $value = 'He said "hello" to the world';
        $stmt = $this->pdo->prepare('INSERT INTO dquote_test VALUES (1, ?)');
        $stmt->execute([$value]);

        $stmt = $this->pdo->query('SELECT val FROM dquote_test WHERE id = 1');
        $this->assertSame($value, $stmt->fetchColumn());
    }

    public function testWideTable20Columns(): void
    {
        $cols = [];
        $vals = [];
        for ($i = 1; $i <= 20; $i++) {
            $cols[] = "col{$i} VARCHAR(50)";
            $vals[] = "'val{$i}'";
        }

        $this->pdo->exec('CREATE TABLE wide20 (id INT PRIMARY KEY, ' . implode(', ', $cols) . ')');
        $this->pdo->exec('INSERT INTO wide20 VALUES (1, ' . implode(', ', $vals) . ')');

        $stmt = $this->pdo->query('SELECT * FROM wide20 WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->assertSame('val1', $row['col1']);
        $this->assertSame('val10', $row['col10']);
        $this->assertSame('val20', $row['col20']);
        $this->assertSame(21, $stmt->columnCount()); // id + 20 cols
    }

    public function testUpdateMultipleColumnsAtOnce(): void
    {
        $this->pdo->exec('CREATE TABLE multi_upd (id INT PRIMARY KEY, a VARCHAR(20), b VARCHAR(20), c VARCHAR(20))');
        $this->pdo->exec("INSERT INTO multi_upd VALUES (1, 'old_a', 'old_b', 'old_c')");

        $this->pdo->exec("UPDATE multi_upd SET a = 'new_a', b = 'new_b', c = 'new_c' WHERE id = 1");

        $stmt = $this->pdo->query('SELECT a, b, c FROM multi_upd WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('new_a', $row['a']);
        $this->assertSame('new_b', $row['b']);
        $this->assertSame('new_c', $row['c']);
    }

    public function testInsertAndSelectMaxIntValues(): void
    {
        $this->pdo->exec('CREATE TABLE int_edge (id INT PRIMARY KEY, big_val BIGINT)');

        // Test with values near INT32 boundary
        $this->pdo->exec('INSERT INTO int_edge VALUES (1, 2147483647)');
        $this->pdo->exec('INSERT INTO int_edge VALUES (2, -2147483648)');

        $stmt = $this->pdo->query('SELECT big_val FROM int_edge ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertEquals(2147483647, $rows[0]);
        $this->assertEquals(-2147483648, $rows[1]);
    }

    public function testPreparedNamedParamsWithComplexWhere(): void
    {
        $this->pdo->exec('CREATE TABLE named_test (id INT PRIMARY KEY, name VARCHAR(50), score INT, active INT)');
        $this->pdo->exec("INSERT INTO named_test VALUES (1, 'Alice', 90, 1)");
        $this->pdo->exec("INSERT INTO named_test VALUES (2, 'Bob', 70, 1)");
        $this->pdo->exec("INSERT INTO named_test VALUES (3, 'Charlie', 85, 0)");

        $stmt = $this->pdo->prepare(
            'SELECT name FROM named_test WHERE score > :min_score AND active = :active ORDER BY name'
        );
        $stmt->execute([':min_score' => 60, ':active' => 1]);
        $names = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Alice', 'Bob'], $names);
    }

    public function testDeleteWithMultipleConditions(): void
    {
        $this->pdo->exec('CREATE TABLE del_multi (id INT PRIMARY KEY, cat VARCHAR(20), status INT)');
        $this->pdo->exec("INSERT INTO del_multi VALUES (1, 'a', 1)");
        $this->pdo->exec("INSERT INTO del_multi VALUES (2, 'b', 0)");
        $this->pdo->exec("INSERT INTO del_multi VALUES (3, 'a', 0)");
        $this->pdo->exec("INSERT INTO del_multi VALUES (4, 'b', 1)");

        $result = $this->pdo->exec("DELETE FROM del_multi WHERE cat = 'a' AND status = 0");
        $this->assertSame(1, $result);

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM del_multi');
        $this->assertEquals(3, $stmt->fetchColumn());
    }

    public function testSelectWithOrConditions(): void
    {
        $this->pdo->exec('CREATE TABLE or_test (id INT PRIMARY KEY, name VARCHAR(50), role VARCHAR(20))');
        $this->pdo->exec("INSERT INTO or_test VALUES (1, 'Alice', 'admin')");
        $this->pdo->exec("INSERT INTO or_test VALUES (2, 'Bob', 'user')");
        $this->pdo->exec("INSERT INTO or_test VALUES (3, 'Charlie', 'moderator')");

        $stmt = $this->pdo->query("SELECT name FROM or_test WHERE role = 'admin' OR role = 'moderator' ORDER BY name");
        $names = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Alice', 'Charlie'], $names);
    }

    public function testSelectWithNestedOrAnd(): void
    {
        $this->pdo->exec('CREATE TABLE nested_cond (id INT PRIMARY KEY, a INT, b INT, c INT)');
        $this->pdo->exec('INSERT INTO nested_cond VALUES (1, 1, 0, 0)');
        $this->pdo->exec('INSERT INTO nested_cond VALUES (2, 0, 1, 1)');
        $this->pdo->exec('INSERT INTO nested_cond VALUES (3, 1, 1, 0)');

        // (a = 1 AND b = 1) OR (b = 1 AND c = 1) → rows 2 and 3
        $stmt = $this->pdo->query('SELECT id FROM nested_cond WHERE (a = 1 AND b = 1) OR (b = 1 AND c = 1) ORDER BY id');
        $ids = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(2, $ids);
        $this->assertEquals(2, $ids[0]);
        $this->assertEquals(3, $ids[1]);
    }
}
