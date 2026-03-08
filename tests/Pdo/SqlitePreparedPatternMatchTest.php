<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests LIKE, BETWEEN, and IS NULL with prepared statement parameters on SQLite.
 * @spec pending
 */
class SqlitePreparedPatternMatchTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pattern_test (id INT PRIMARY KEY, name VARCHAR(50), score INT, note TEXT)';
    }

    protected function getTableNames(): array
    {
        return ['pattern_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec('CREATE TABLE pattern_test (id INT PRIMARY KEY, name VARCHAR(50), score INT, note TEXT)');
        $this->pdo->exec("INSERT INTO pattern_test VALUES (1, 'Alice', 80, 'Good student')");
        $this->pdo->exec("INSERT INTO pattern_test VALUES (2, 'Bob', 60, NULL)");
        $this->pdo->exec("INSERT INTO pattern_test VALUES (3, 'Charlie', 90, 'Top performer')");
        $this->pdo->exec("INSERT INTO pattern_test VALUES (4, 'Alice Jr', 70, 'Good effort')");
        $this->pdo->exec("INSERT INTO pattern_test VALUES (5, 'Dave', 50, NULL)");

        }

    public function testLikeWithPreparedParam(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM pattern_test WHERE name LIKE ? ORDER BY name');
        $stmt->execute(['Alice%']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Alice Jr', $rows[1]['name']);
    }

    public function testLikeWithMiddleWildcard(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM pattern_test WHERE name LIKE ?');
        $stmt->execute(['%li%']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows); // Alice, Charlie, Alice Jr
    }

    public function testLikeWithNamedParam(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM pattern_test WHERE name LIKE :pattern ORDER BY name');
        $stmt->execute([':pattern' => '%ob']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
    }

    public function testNotLikeWithPreparedParam(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM pattern_test WHERE name NOT LIKE ? ORDER BY name');
        $stmt->execute(['Alice%']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows); // Bob, Charlie, Dave
    }

    public function testBetweenWithPreparedParams(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM pattern_test WHERE score BETWEEN ? AND ? ORDER BY name');
        $stmt->execute([60, 80]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows); // Alice (80), Bob (60), Alice Jr (70)
    }

    public function testBetweenWithNamedParams(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM pattern_test WHERE score BETWEEN :low AND :high ORDER BY score');
        $stmt->execute([':low' => 70, ':high' => 90]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows); // Alice Jr (70), Alice (80), Charlie (90)
    }

    public function testIsNullQuery(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM pattern_test WHERE note IS NULL ORDER BY name');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
        $this->assertSame('Dave', $rows[1]['name']);
    }

    public function testIsNotNullQuery(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM pattern_test WHERE note IS NOT NULL ORDER BY name');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
    }

    public function testUpdateSetNullWithPreparedParam(): void
    {
        $stmt = $this->pdo->prepare('UPDATE pattern_test SET note = ? WHERE id = ?');
        $stmt->execute([null, 1]);

        $select = $this->pdo->query('SELECT note FROM pattern_test WHERE id = 1');
        $row = $select->fetch(PDO::FETCH_ASSOC);
        $this->assertNull($row['note']);
    }

    public function testUpdateNullToValueWithPreparedParam(): void
    {
        $stmt = $this->pdo->prepare('UPDATE pattern_test SET note = ? WHERE id = ?');
        $stmt->execute(['New note', 2]);

        $select = $this->pdo->query('SELECT note FROM pattern_test WHERE id = 2');
        $row = $select->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('New note', $row['note']);
    }

    public function testLikeCombinedWithBetween(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM pattern_test WHERE name LIKE ? AND score BETWEEN ? AND ? ORDER BY name');
        $stmt->execute(['%li%', 70, 100]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // Alice (80), Charlie (90), Alice Jr (70)
        $this->assertCount(3, $rows);
    }

    public function testCountWhereIsNull(): void
    {
        $stmt = $this->pdo->query('SELECT COUNT(*) AS cnt FROM pattern_test WHERE note IS NULL');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(2, (int) $row['cnt']);
    }
}
