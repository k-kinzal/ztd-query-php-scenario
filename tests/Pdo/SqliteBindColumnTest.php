<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests bindColumn() and FETCH_BOUND mode on SQLite ZTD.
 * @spec SPEC-3.2
 */
class SqliteBindColumnTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE bc_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['bc_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO bc_test VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO bc_test VALUES (2, 'Bob', 85)");
        $this->pdo->exec("INSERT INTO bc_test VALUES (3, 'Charlie', 70)");

        }

    public function testBindColumnByNumber(): void
    {
        $stmt = $this->pdo->query('SELECT id, name, score FROM bc_test WHERE id = 1');
        $stmt->bindColumn(1, $id);
        $stmt->bindColumn(2, $name);
        $stmt->bindColumn(3, $score);
        $stmt->fetch(PDO::FETCH_BOUND);

        $this->assertSame(1, (int) $id);
        $this->assertSame('Alice', $name);
        $this->assertSame(100, (int) $score);
    }

    public function testBindColumnByName(): void
    {
        $stmt = $this->pdo->query('SELECT id, name, score FROM bc_test WHERE id = 1');
        $stmt->bindColumn('name', $name);
        $stmt->bindColumn('score', $score);
        $stmt->fetch(PDO::FETCH_BOUND);

        $this->assertSame('Alice', $name);
        $this->assertSame(100, (int) $score);
    }

    public function testBindColumnIteration(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM bc_test ORDER BY id');
        $stmt->bindColumn(1, $name);

        $names = [];
        while ($stmt->fetch(PDO::FETCH_BOUND)) {
            $names[] = $name;
        }
        $this->assertSame(['Alice', 'Bob', 'Charlie'], $names);
    }

    public function testBindColumnAfterShadowInsert(): void
    {
        $this->pdo->exec("INSERT INTO bc_test VALUES (4, 'Diana', 95)");

        $stmt = $this->pdo->query('SELECT name, score FROM bc_test WHERE id = 4');
        $stmt->bindColumn('name', $name);
        $stmt->bindColumn('score', $score);
        $stmt->fetch(PDO::FETCH_BOUND);

        $this->assertSame('Diana', $name);
        $this->assertSame(95, (int) $score);
    }

    public function testBindColumnAfterShadowUpdate(): void
    {
        $this->pdo->exec("UPDATE bc_test SET score = 999 WHERE id = 1");

        $stmt = $this->pdo->query('SELECT name, score FROM bc_test WHERE id = 1');
        $stmt->bindColumn('name', $name);
        $stmt->bindColumn('score', $score);
        $stmt->fetch(PDO::FETCH_BOUND);

        $this->assertSame('Alice', $name);
        $this->assertSame(999, (int) $score);
    }

    public function testBindColumnWithPreparedStatement(): void
    {
        $stmt = $this->pdo->prepare('SELECT name, score FROM bc_test WHERE score > ?');
        $stmt->execute([80]);
        $stmt->bindColumn('name', $name);
        $stmt->bindColumn('score', $score);

        $results = [];
        while ($stmt->fetch(PDO::FETCH_BOUND)) {
            $results[] = ['name' => $name, 'score' => (int) $score];
        }
        $this->assertCount(2, $results);
    }

    public function testBindColumnWithType(): void
    {
        $stmt = $this->pdo->query('SELECT id, name FROM bc_test WHERE id = 1');
        $stmt->bindColumn(1, $id, PDO::PARAM_INT);
        $stmt->bindColumn(2, $name, PDO::PARAM_STR);
        $stmt->fetch(PDO::FETCH_BOUND);

        $this->assertSame('Alice', $name);
    }
}
