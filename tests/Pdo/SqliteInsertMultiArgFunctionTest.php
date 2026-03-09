<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT with multi-argument functions in VALUES clause.
 *
 * Functions like ROUND(x,y), SUBSTR(s,a,b), REPLACE(s,a,b) contain commas
 * inside parentheses. If the InsertTransformer splits VALUES by commas without
 * tracking parenthesis depth, it will miscount columns.
 *
 * @spec SPEC-4.1
 */
class SqliteInsertMultiArgFunctionTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE maf (id INTEGER PRIMARY KEY, name TEXT, val REAL)';
    }

    protected function getTableNames(): array
    {
        return ['maf'];
    }

    /**
     * ROUND(x, n) — two-argument function in VALUES.
     */
    public function testInsertWithRound(): void
    {
        $this->pdo->exec("INSERT INTO maf (id, name, val) VALUES (1, 'pi', ROUND(3.14159, 2))");
        $rows = $this->ztdQuery('SELECT val FROM maf WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertEquals(3.14, (float) $rows[0]['val'], '', 0.001);
    }

    /**
     * SUBSTR(s, start, length) — three-argument function in VALUES.
     */
    public function testInsertWithSubstr(): void
    {
        $this->pdo->exec("INSERT INTO maf (id, name, val) VALUES (1, SUBSTR('hello world', 1, 5), 0)");
        $rows = $this->ztdQuery('SELECT name FROM maf WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertSame('hello', $rows[0]['name']);
    }

    /**
     * REPLACE(s, from, to) — three-argument function in VALUES.
     */
    public function testInsertWithReplace(): void
    {
        $this->pdo->exec("INSERT INTO maf (id, name, val) VALUES (1, REPLACE('foo-bar', '-', '_'), 0)");
        $rows = $this->ztdQuery('SELECT name FROM maf WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertSame('foo_bar', $rows[0]['name']);
    }

    /**
     * INSTR(s, sub) — two-argument function returning integer.
     */
    public function testInsertWithInstr(): void
    {
        $this->pdo->exec("INSERT INTO maf (id, name, val) VALUES (1, 'test', INSTR('hello world', 'world'))");
        $rows = $this->ztdQuery('SELECT val FROM maf WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertEquals(7, (int) $rows[0]['val']);
    }

    /**
     * Multiple multi-arg functions in one INSERT.
     */
    public function testInsertWithMultipleMultiArgFunctions(): void
    {
        $this->pdo->exec(
            "INSERT INTO maf (id, name, val) VALUES (1, SUBSTR(REPLACE('foo-bar-baz', '-', ' '), 1, 7), ROUND(99.999, 1))"
        );
        $rows = $this->ztdQuery('SELECT name, val FROM maf WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertSame('foo bar', $rows[0]['name']);
        $this->assertEquals(100.0, (float) $rows[0]['val'], '', 0.001);
    }

    /**
     * Multi-row INSERT with multi-arg functions.
     */
    public function testMultiRowInsertWithMultiArgFunctions(): void
    {
        $this->pdo->exec(
            "INSERT INTO maf (id, name, val) VALUES
             (1, REPLACE('a-b', '-', '_'), ROUND(1.555, 2)),
             (2, SUBSTR('abcdef', 2, 3), ROUND(2.555, 1)),
             (3, REPLACE(SUBSTR('xy-z', 1, 4), '-', ''), ROUND(3.14, 0))"
        );
        $rows = $this->ztdQuery('SELECT id, name, val FROM maf ORDER BY id');
        $this->assertCount(3, $rows);
        $this->assertSame('a_b', $rows[0]['name']);
        $this->assertEqualsWithDelta(1.56, (float) $rows[0]['val'], 0.02);
        $this->assertSame('bcd', $rows[1]['name']);
        $this->assertEquals(2.6, (float) $rows[1]['val'], '', 0.1);
        $this->assertSame('xyz', $rows[2]['name']);
        $this->assertEquals(3.0, (float) $rows[2]['val'], '', 0.1);
    }

    /**
     * IIF with nested commas.
     */
    public function testInsertWithIifMultiArg(): void
    {
        $this->pdo->exec(
            "INSERT INTO maf (id, name, val) VALUES (1, IIF(1=1, REPLACE('a-b', '-', '_'), 'no'), ROUND(IIF(2>1, 3.456, 0), 2))"
        );
        $rows = $this->ztdQuery('SELECT name, val FROM maf WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertSame('a_b', $rows[0]['name']);
        $this->assertEquals(3.46, (float) $rows[0]['val'], '', 0.01);
    }

    /**
     * Prepared INSERT with multi-arg function mixed with parameters.
     */
    public function testPreparedInsertWithMultiArgFunction(): void
    {
        $stmt = $this->pdo->prepare("INSERT INTO maf (id, name, val) VALUES (?, REPLACE(?, '-', '_'), ROUND(?, 2))");
        $stmt->execute([1, 'hello-world', 3.14159]);

        $rows = $this->ztdQuery('SELECT name, val FROM maf WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertSame('hello_world', $rows[0]['name']);
        $this->assertEquals(3.14, (float) $rows[0]['val'], '', 0.01);
    }
}
