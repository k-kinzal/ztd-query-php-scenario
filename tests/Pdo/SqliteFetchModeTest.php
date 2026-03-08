<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests various PDO fetch modes work correctly with ZTD shadow store on SQLite.
 * @spec pending
 */
class SqliteFetchModeTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE fetch_test (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)';
    }

    protected function getTableNames(): array
    {
        return ['fetch_test'];
    }


    public function testQueryWithFetchModeArg(): void
    {
        $stmt = $this->pdo->query('SELECT * FROM fetch_test ORDER BY id', PDO::FETCH_ASSOC);
        $rows = $stmt->fetchAll();

        $this->assertCount(3, $rows);
        $this->assertArrayHasKey('name', $rows[0]);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public function testFetchAssoc(): void
    {
        $stmt = $this->pdo->query('SELECT * FROM fetch_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->assertIsArray($row);
        $this->assertArrayHasKey('name', $row);
        $this->assertArrayNotHasKey(0, $row);
        $this->assertSame('Alice', $row['name']);
    }

    public function testFetchNum(): void
    {
        $stmt = $this->pdo->query('SELECT id, name, score FROM fetch_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_NUM);

        $this->assertIsArray($row);
        $this->assertArrayHasKey(0, $row);
        $this->assertArrayNotHasKey('name', $row);
        $this->assertSame(1, (int) $row[0]);
        $this->assertSame('Alice', $row[1]);
    }

    public function testFetchBoth(): void
    {
        $stmt = $this->pdo->query('SELECT id, name FROM fetch_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_BOTH);

        $this->assertIsArray($row);
        // Both numeric and associative keys
        $this->assertArrayHasKey(0, $row);
        $this->assertArrayHasKey('id', $row);
        $this->assertSame($row[0], $row['id']);
    }

    public function testFetchObject(): void
    {
        $stmt = $this->pdo->query('SELECT * FROM fetch_test WHERE id = 1');
        $obj = $stmt->fetch(PDO::FETCH_OBJ);

        $this->assertIsObject($obj);
        $this->assertSame('Alice', $obj->name);
        $this->assertSame(100, (int) $obj->score);
    }

    public function testFetchAllAssoc(): void
    {
        $stmt = $this->pdo->query('SELECT * FROM fetch_test ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertSame('Charlie', $rows[2]['name']);
    }

    public function testFetchAllNum(): void
    {
        $stmt = $this->pdo->query('SELECT id, name FROM fetch_test ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_NUM);

        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0][1]);
    }

    public function testFetchColumn(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM fetch_test ORDER BY id');

        $this->assertSame('Alice', $stmt->fetchColumn());
        $this->assertSame('Bob', $stmt->fetchColumn());
        $this->assertSame('Charlie', $stmt->fetchColumn());
        $this->assertFalse($stmt->fetchColumn());
    }

    public function testFetchColumnIndex(): void
    {
        $stmt = $this->pdo->query('SELECT id, name, score FROM fetch_test WHERE id = 1');
        $name = $stmt->fetchColumn(1);

        $this->assertSame('Alice', $name);
    }

    public function testFetchReturnsFalseWhenExhausted(): void
    {
        $stmt = $this->pdo->query('SELECT * FROM fetch_test WHERE id = 1');
        $row1 = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertIsArray($row1);

        $row2 = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertFalse($row2);
    }

    public function testForeachIteration(): void
    {
        $stmt = $this->pdo->query('SELECT * FROM fetch_test ORDER BY id');
        $stmt->setFetchMode(PDO::FETCH_ASSOC);

        $names = [];
        foreach ($stmt as $row) {
            $names[] = $row['name'];
        }

        $this->assertSame(['Alice', 'Bob', 'Charlie'], $names);
    }

    public function testColumnCount(): void
    {
        $stmt = $this->pdo->query('SELECT id, name, score FROM fetch_test LIMIT 1');
        $this->assertSame(3, $stmt->columnCount());
    }

    public function testRowCount(): void
    {
        $stmt = $this->pdo->prepare('UPDATE fetch_test SET score = score + 5 WHERE score < 100');
        $stmt->execute();

        $this->assertSame(2, $stmt->rowCount());
    }
}
