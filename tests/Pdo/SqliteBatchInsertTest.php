<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests batch insert operations and NULL handling in ZTD mode on SQLite.
 */
class SqliteBatchInsertTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE batch_test (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)');

        $this->pdo = ZtdPdo::fromPdo($raw);
    }

    public function testMultiRowInsert(): void
    {
        $this->pdo->exec("INSERT INTO batch_test (id, name, score) VALUES (1, 'Alice', 100), (2, 'Bob', 85), (3, 'Charlie', 70)");

        $stmt = $this->pdo->query('SELECT * FROM batch_test ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertSame('Charlie', $rows[2]['name']);
    }

    public function testMultiRowInsertAffectedCount(): void
    {
        $count = $this->pdo->exec("INSERT INTO batch_test (id, name, score) VALUES (1, 'Alice', 100), (2, 'Bob', 85)");
        $this->assertSame(2, $count);
    }

    public function testInsertWithNullValue(): void
    {
        $this->pdo->exec("INSERT INTO batch_test (id, name, score) VALUES (1, 'Alice', NULL)");

        $stmt = $this->pdo->query('SELECT * FROM batch_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertNull($row['score']);
    }

    public function testInsertNullAndQueryIsNull(): void
    {
        $this->pdo->exec("INSERT INTO batch_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO batch_test (id, name, score) VALUES (2, 'Bob', NULL)");
        $this->pdo->exec("INSERT INTO batch_test (id, name, score) VALUES (3, 'Charlie', 90)");

        $stmt = $this->pdo->query('SELECT name FROM batch_test WHERE score IS NULL');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
    }

    public function testInsertNullAndQueryIsNotNull(): void
    {
        $this->pdo->exec("INSERT INTO batch_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO batch_test (id, name, score) VALUES (2, 'Bob', NULL)");

        $stmt = $this->pdo->query('SELECT name FROM batch_test WHERE score IS NOT NULL');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public function testAggregationAfterBatchInsert(): void
    {
        $this->pdo->exec("INSERT INTO batch_test (id, name, score) VALUES (1, 'Alice', 100), (2, 'Bob', 85), (3, 'Charlie', 70)");

        $stmt = $this->pdo->query('SELECT AVG(score) as avg_score, MAX(score) as max_score, MIN(score) as min_score FROM batch_test');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->assertEquals(85.0, (float) $row['avg_score']);
        $this->assertSame(100, (int) $row['max_score']);
        $this->assertSame(70, (int) $row['min_score']);
    }

    public function testDeleteAfterBatchInsert(): void
    {
        $this->pdo->exec("INSERT INTO batch_test (id, name, score) VALUES (1, 'Alice', 100), (2, 'Bob', 30), (3, 'Charlie', 70)");

        $count = $this->pdo->exec('DELETE FROM batch_test WHERE score < 50');
        $this->assertSame(1, $count);

        $stmt = $this->pdo->query('SELECT * FROM batch_test ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Charlie', $rows[1]['name']);
    }
}
