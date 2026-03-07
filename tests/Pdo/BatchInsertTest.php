<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests batch insert operations and NULL handling in ZTD mode.
 */
class BatchInsertTest extends TestCase
{
    private PDO $raw;
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $this->raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $this->raw->exec('CREATE TABLE batch_test (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)');

        $this->pdo = ZtdPdo::fromPdo($this->raw);
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
        $this->pdo->exec("INSERT INTO batch_test (id, name, score) VALUES (1, NULL, NULL)");

        $stmt = $this->pdo->query('SELECT * FROM batch_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertNull($rows[0]['name']);
        $this->assertNull($rows[0]['score']);
    }

    public function testUpdateToNull(): void
    {
        $this->pdo->exec("INSERT INTO batch_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->pdo->exec("UPDATE batch_test SET name = NULL WHERE id = 1");

        $stmt = $this->pdo->query('SELECT * FROM batch_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertNull($rows[0]['name']);
    }

    public function testSelectWithNullComparison(): void
    {
        $this->pdo->exec("INSERT INTO batch_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO batch_test (id, name, score) VALUES (2, NULL, 85)");

        $stmt = $this->pdo->query('SELECT * FROM batch_test WHERE name IS NULL');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame(2, (int) $rows[0]['id']);

        $stmt = $this->pdo->query('SELECT * FROM batch_test WHERE name IS NOT NULL');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame(1, (int) $rows[0]['id']);
    }

    public function testMultiRowInsertIsolation(): void
    {
        $this->pdo->exec("INSERT INTO batch_test (id, name, score) VALUES (1, 'Alice', 100), (2, 'Bob', 85)");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT * FROM batch_test');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));
    }

    public function testSequentialInsertsAndSelect(): void
    {
        // Simulate a realistic user workflow: many individual inserts followed by queries
        for ($i = 1; $i <= 10; $i++) {
            $this->pdo->exec("INSERT INTO batch_test (id, name, score) VALUES ($i, 'User$i', " . ($i * 10) . ")");
        }

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM batch_test');
        $this->assertSame(10, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT SUM(score) FROM batch_test');
        $this->assertSame(550, (int) $stmt->fetchColumn());

        // Delete some
        $this->pdo->exec('DELETE FROM batch_test WHERE score < 50');
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM batch_test');
        $this->assertSame(6, (int) $stmt->fetchColumn());
    }
}
