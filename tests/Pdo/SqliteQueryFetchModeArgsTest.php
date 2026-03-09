<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests PDO::query() with fetch mode arguments on shadow data.
 *
 * ZtdPdo::query() converts query() into prepare()+setFetchMode()+execute(),
 * which differs from native PDO's query() that sets fetch mode at driver level.
 * This tests whether fetch mode arguments are correctly forwarded.
 *
 * @spec SPEC-3.3
 */
class SqliteQueryFetchModeArgsTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE qfm (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)';
    }

    protected function getTableNames(): array
    {
        return ['qfm'];
    }

    /**
     * query() with FETCH_COLUMN fetch mode.
     */
    public function testQueryWithFetchColumn(): void
    {
        $this->pdo->exec("INSERT INTO qfm (id, name, score) VALUES (1, 'Alice', 100), (2, 'Bob', 85)");

        $stmt = $this->pdo->query('SELECT name FROM qfm ORDER BY id', PDO::FETCH_COLUMN, 0);
        $names = $stmt->fetchAll();
        $this->assertCount(2, $names);
        $this->assertSame('Alice', $names[0]);
        $this->assertSame('Bob', $names[1]);
    }

    /**
     * query() with FETCH_NUM.
     */
    public function testQueryWithFetchNum(): void
    {
        $this->pdo->exec("INSERT INTO qfm (id, name, score) VALUES (1, 'Alice', 100)");

        $stmt = $this->pdo->query('SELECT id, name, score FROM qfm WHERE id = 1', PDO::FETCH_NUM);
        $row = $stmt->fetch();
        $this->assertIsArray($row);
        $this->assertArrayHasKey(0, $row);
        $this->assertArrayHasKey(1, $row);
        $this->assertArrayHasKey(2, $row);
        $this->assertEquals(1, $row[0]);
        $this->assertSame('Alice', $row[1]);
    }

    /**
     * query() with FETCH_OBJ.
     */
    public function testQueryWithFetchObj(): void
    {
        $this->pdo->exec("INSERT INTO qfm (id, name, score) VALUES (1, 'Alice', 100)");

        $stmt = $this->pdo->query('SELECT name, score FROM qfm WHERE id = 1', PDO::FETCH_OBJ);
        $obj = $stmt->fetch();
        $this->assertIsObject($obj);
        $this->assertSame('Alice', $obj->name);
        $this->assertEquals(100, $obj->score);
    }

    /**
     * query() with FETCH_BOTH.
     */
    public function testQueryWithFetchBoth(): void
    {
        $this->pdo->exec("INSERT INTO qfm (id, name, score) VALUES (1, 'Alice', 100)");

        $stmt = $this->pdo->query('SELECT name, score FROM qfm WHERE id = 1', PDO::FETCH_BOTH);
        $row = $stmt->fetch();
        $this->assertSame('Alice', $row['name']);
        $this->assertSame('Alice', $row[0]);
        $this->assertEquals(100, $row['score']);
        $this->assertEquals(100, $row[1]);
    }

    /**
     * query() with FETCH_KEY_PAIR.
     */
    public function testQueryWithFetchKeyPair(): void
    {
        $this->pdo->exec("INSERT INTO qfm (id, name, score) VALUES (1, 'Alice', 100), (2, 'Bob', 85)");

        $stmt = $this->pdo->query('SELECT id, name FROM qfm ORDER BY id', PDO::FETCH_KEY_PAIR);
        $pairs = $stmt->fetchAll();
        $this->assertCount(2, $pairs);
        $this->assertSame('Alice', $pairs[1]);
        $this->assertSame('Bob', $pairs[2]);
    }

    /**
     * query() with FETCH_COLUMN after shadow mutation.
     */
    public function testQueryWithFetchColumnAfterMutation(): void
    {
        $this->pdo->exec("INSERT INTO qfm (id, name, score) VALUES (1, 'Alice', 100), (2, 'Bob', 85)");
        $this->pdo->exec("UPDATE qfm SET name = 'ALICE' WHERE id = 1");

        $stmt = $this->pdo->query('SELECT name FROM qfm ORDER BY id', PDO::FETCH_COLUMN, 0);
        $names = $stmt->fetchAll();
        $this->assertSame('ALICE', $names[0]);
        $this->assertSame('Bob', $names[1]);
    }

    /**
     * query() with FETCH_ASSOC after deletion.
     */
    public function testQueryWithFetchAssocAfterDelete(): void
    {
        $this->pdo->exec("INSERT INTO qfm (id, name, score) VALUES (1, 'Alice', 100), (2, 'Bob', 85), (3, 'Charlie', 70)");
        $this->pdo->exec("DELETE FROM qfm WHERE id = 2");

        $stmt = $this->pdo->query('SELECT name FROM qfm ORDER BY id', PDO::FETCH_ASSOC);
        $rows = $stmt->fetchAll();
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Charlie', $rows[1]['name']);
    }

    /**
     * fetchObject() on shadow data — standalone method test.
     */
    public function testFetchObjectOnShadowData(): void
    {
        $this->pdo->exec("INSERT INTO qfm (id, name, score) VALUES (1, 'Alice', 100)");

        $stmt = $this->pdo->query('SELECT name, score FROM qfm WHERE id = 1');
        $obj = $stmt->fetchObject();
        $this->assertIsObject($obj);
        $this->assertSame('Alice', $obj->name);
        $this->assertEquals(100, $obj->score);
    }

    /**
     * fetchObject() after shadow UPDATE.
     */
    public function testFetchObjectAfterUpdate(): void
    {
        $this->pdo->exec("INSERT INTO qfm (id, name, score) VALUES (1, 'Alice', 100)");
        $this->pdo->exec("UPDATE qfm SET name = 'Alice Updated' WHERE id = 1");

        $stmt = $this->pdo->query('SELECT name, score FROM qfm WHERE id = 1');
        $obj = $stmt->fetchObject();
        $this->assertSame('Alice Updated', $obj->name);
    }

    /**
     * fetchObject() on DML statement returns false.
     */
    public function testFetchObjectOnDmlReturnsFalse(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO qfm (id, name, score) VALUES (?, ?, ?)');
        $stmt->execute([1, 'Alice', 100]);
        $result = $stmt->fetchObject();
        $this->assertFalse($result);
    }
}
