<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests LIKE, BETWEEN, and IS NULL with prepared statement parameters on PostgreSQL PDO.
 * @spec SPEC-3.2
 */
class PostgresPreparedPatternMatchTest extends AbstractPostgresPdoTestCase
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

    public function testBetweenWithPreparedParams(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM pattern_test WHERE score BETWEEN ? AND ? ORDER BY name');
        $stmt->execute([60, 80]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
    }

    public function testIsNullQuery(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM pattern_test WHERE note IS NULL ORDER BY name');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
        $this->assertSame('Dave', $rows[1]['name']);
    }

    public function testUpdateSetNullWithPreparedParam(): void
    {
        $stmt = $this->pdo->prepare('UPDATE pattern_test SET note = ? WHERE id = ?');
        $stmt->execute([null, 1]);

        $select = $this->pdo->query('SELECT note FROM pattern_test WHERE id = 1');
        $row = $select->fetch(PDO::FETCH_ASSOC);
        $this->assertNull($row['note']);
    }
}
