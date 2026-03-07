<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests getIterator()/foreach on ZtdPdoStatement and default fetch mode configuration.
 */
class SqliteIteratorAndDefaultFetchModeTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);

        $this->pdo->exec('CREATE TABLE iter_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
        $this->pdo->exec("INSERT INTO iter_test VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO iter_test VALUES (2, 'Bob', 85)");
        $this->pdo->exec("INSERT INTO iter_test VALUES (3, 'Charlie', 70)");
    }

    public function testForeachOnStatement(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM iter_test ORDER BY id');
        $names = [];
        foreach ($stmt as $row) {
            $names[] = $row['name'];
        }
        $this->assertSame(['Alice', 'Bob', 'Charlie'], $names);
    }

    public function testForeachAfterShadowInsert(): void
    {
        $this->pdo->exec("INSERT INTO iter_test VALUES (4, 'Diana', 95)");

        $stmt = $this->pdo->query('SELECT name FROM iter_test ORDER BY id');
        $names = [];
        foreach ($stmt as $row) {
            $names[] = $row['name'];
        }
        $this->assertCount(4, $names);
        $this->assertSame('Diana', $names[3]);
    }

    public function testForeachAfterShadowDelete(): void
    {
        $this->pdo->exec("DELETE FROM iter_test WHERE id = 2");

        $stmt = $this->pdo->query('SELECT name FROM iter_test ORDER BY id');
        $names = [];
        foreach ($stmt as $row) {
            $names[] = $row['name'];
        }
        $this->assertSame(['Alice', 'Charlie'], $names);
    }

    public function testDefaultFetchModeAssoc(): void
    {
        $this->pdo->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_ASSOC);

        $stmt = $this->pdo->query('SELECT name, score FROM iter_test WHERE id = 1');
        $row = $stmt->fetch();

        $this->assertArrayHasKey('name', $row);
        $this->assertArrayNotHasKey(0, $row);
    }

    public function testDefaultFetchModeNum(): void
    {
        $this->pdo->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_NUM);

        $stmt = $this->pdo->query('SELECT name, score FROM iter_test WHERE id = 1');
        $row = $stmt->fetch();

        $this->assertArrayHasKey(0, $row);
        $this->assertSame('Alice', $row[0]);
    }

    public function testDefaultFetchModeObj(): void
    {
        $this->pdo->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_OBJ);

        $stmt = $this->pdo->query('SELECT name, score FROM iter_test WHERE id = 1');
        $row = $stmt->fetch();

        $this->assertIsObject($row);
        $this->assertSame('Alice', $row->name);
    }

    public function testSetFetchModeOverridesDefault(): void
    {
        $this->pdo->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_NUM);

        $stmt = $this->pdo->query('SELECT name, score FROM iter_test WHERE id = 1');
        $stmt->setFetchMode(PDO::FETCH_ASSOC);
        $row = $stmt->fetch();

        // setFetchMode on statement overrides connection-level default
        $this->assertArrayHasKey('name', $row);
        $this->assertArrayNotHasKey(0, $row);
    }

    public function testQueryWithFetchModeArgument(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM iter_test ORDER BY id', PDO::FETCH_COLUMN, 0);
        $names = $stmt->fetchAll();
        $this->assertSame(['Alice', 'Bob', 'Charlie'], $names);
    }

    public function testForeachOnPreparedStatement(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM iter_test WHERE score > ?');
        $stmt->execute([75]);

        $names = [];
        foreach ($stmt as $row) {
            $names[] = $row['name'];
        }
        $this->assertSame(['Alice', 'Bob'], $names);
    }

    public function testGetIteratorExplicit(): void
    {
        $stmt = $this->pdo->query('SELECT id, name FROM iter_test ORDER BY id');
        $iterator = $stmt->getIterator();

        $this->assertInstanceOf(\Traversable::class, $iterator);
        $rows = iterator_to_array($iterator);
        $this->assertCount(3, $rows);
    }
}
