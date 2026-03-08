<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests fetchColumn(), closeCursor(), and FETCH_CLASS on SQLite PDO.
 * @spec pending
 */
class SqliteFetchColumnAndCursorTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE fc_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['fc_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec('CREATE TABLE fc_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
        $this->pdo->exec("INSERT INTO fc_test VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO fc_test VALUES (2, 'Bob', 85)");
        $this->pdo->exec("INSERT INTO fc_test VALUES (3, 'Charlie', 70)");

        }

    public function testFetchColumnDefaultFirstColumn(): void
    {
        $stmt = $this->pdo->query('SELECT name, score FROM fc_test WHERE id = 1');
        $value = $stmt->fetchColumn();
        $this->assertSame('Alice', $value);
    }

    public function testFetchColumnSecondColumn(): void
    {
        $stmt = $this->pdo->query('SELECT name, score FROM fc_test WHERE id = 1');
        $value = $stmt->fetchColumn(1);
        $this->assertSame(100, (int) $value); // score column
    }

    public function testFetchColumnIteration(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM fc_test ORDER BY id');
        $names = [];
        while (($name = $stmt->fetchColumn()) !== false) {
            $names[] = $name;
        }
        $this->assertSame(['Alice', 'Bob', 'Charlie'], $names);
    }

    public function testFetchColumnReturnsFalseWhenExhausted(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM fc_test WHERE id = 999');
        $value = $stmt->fetchColumn();
        $this->assertFalse($value);
    }

    public function testCloseCursorAllowsNewQuery(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM fc_test ORDER BY id');
        $stmt->fetch(PDO::FETCH_ASSOC); // partial fetch
        $stmt->closeCursor();

        // Should be able to run a new query after closing cursor
        $stmt2 = $this->pdo->query('SELECT COUNT(*) AS cnt FROM fc_test');
        $row = $stmt2->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(3, (int) $row['cnt']);
    }

    public function testFetchClassMode(): void
    {
        $stmt = $this->pdo->query('SELECT name, score FROM fc_test WHERE id = 1');
        $obj = $stmt->fetchObject();
        $this->assertInstanceOf(\stdClass::class, $obj);
        $this->assertSame('Alice', $obj->name);
        $this->assertSame(100, (int) $obj->score);
    }

    public function testFetchAllWithFetchColumn(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM fc_test ORDER BY id');
        $names = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Alice', 'Bob', 'Charlie'], $names);
    }

    public function testFetchKeyPairMode(): void
    {
        $stmt = $this->pdo->query('SELECT id, name FROM fc_test ORDER BY id');
        $pairs = $stmt->fetchAll(PDO::FETCH_KEY_PAIR);
        $this->assertSame([1 => 'Alice', 2 => 'Bob', 3 => 'Charlie'], $pairs);
    }

    public function testColumnCount(): void
    {
        $stmt = $this->pdo->query('SELECT name, score FROM fc_test WHERE id = 1');
        $this->assertSame(2, $stmt->columnCount());
    }

    public function testRowCountAfterSelect(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM fc_test');
        // rowCount() on SELECT is platform-dependent but should not error
        $count = $stmt->rowCount();
        $this->assertIsInt($count);
    }
}
