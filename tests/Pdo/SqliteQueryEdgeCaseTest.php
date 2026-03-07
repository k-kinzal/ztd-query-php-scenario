<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests edge cases in query behavior with the shadow store:
 * NULL handling, ORDER BY, LIMIT, self-referencing updates, etc.
 */
class SqliteQueryEdgeCaseTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE edge_test (id INTEGER PRIMARY KEY, name TEXT, score INTEGER, category TEXT)');

        $this->pdo = ZtdPdo::fromPdo($raw);
    }

    public function testCountStarVsCountColumn(): void
    {
        $this->pdo->exec("INSERT INTO edge_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO edge_test (id, name, score) VALUES (2, 'Bob', NULL)");
        $this->pdo->exec("INSERT INTO edge_test (id, name, score) VALUES (3, 'Charlie', 90)");

        // COUNT(*) counts all rows including NULLs
        $stmt = $this->pdo->query('SELECT COUNT(*) as total FROM edge_test');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(3, (int) $row['total']);

        // COUNT(score) excludes NULL values
        $stmt = $this->pdo->query('SELECT COUNT(score) as non_null FROM edge_test');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(2, (int) $row['non_null']);
    }

    public function testSumWithNulls(): void
    {
        $this->pdo->exec("INSERT INTO edge_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO edge_test (id, name, score) VALUES (2, 'Bob', NULL)");
        $this->pdo->exec("INSERT INTO edge_test (id, name, score) VALUES (3, 'Charlie', 50)");

        // SUM ignores NULLs
        $stmt = $this->pdo->query('SELECT SUM(score) as total FROM edge_test');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(150, (int) $row['total']);
    }

    public function testOrderByWithNulls(): void
    {
        $this->pdo->exec("INSERT INTO edge_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO edge_test (id, name, score) VALUES (2, 'Bob', NULL)");
        $this->pdo->exec("INSERT INTO edge_test (id, name, score) VALUES (3, 'Charlie', 50)");

        // SQLite: NULLs sort first in ASC order
        $stmt = $this->pdo->query('SELECT name, score FROM edge_test ORDER BY score ASC');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertNull($rows[0]['score']);
        $this->assertSame('Bob', $rows[0]['name']);
    }

    public function testLimitZero(): void
    {
        $this->pdo->exec("INSERT INTO edge_test (id, name, score) VALUES (1, 'Alice', 100)");

        $stmt = $this->pdo->query('SELECT * FROM edge_test LIMIT 0');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(0, $rows);
    }

    public function testLimitWithOffset(): void
    {
        for ($i = 1; $i <= 5; $i++) {
            $this->pdo->exec("INSERT INTO edge_test (id, name, score) VALUES ($i, 'item_$i', $i)");
        }

        // Skip first 2, take next 2
        $stmt = $this->pdo->query('SELECT * FROM edge_test ORDER BY id LIMIT 2 OFFSET 2');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame(3, (int) $rows[0]['id']);
        $this->assertSame(4, (int) $rows[1]['id']);
    }

    public function testSelfReferencingUpdate(): void
    {
        $this->pdo->exec("INSERT INTO edge_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO edge_test (id, name, score) VALUES (2, 'Bob', 85)");

        // Self-referencing: increment score
        $this->pdo->exec('UPDATE edge_test SET score = score + 10');

        $stmt = $this->pdo->query('SELECT score FROM edge_test ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(110, (int) $rows[0]['score']);
        $this->assertSame(95, (int) $rows[1]['score']);
    }

    public function testUpdateWithConcatenation(): void
    {
        $this->pdo->exec("INSERT INTO edge_test (id, name, score) VALUES (1, 'Alice', 100)");

        // String concatenation in UPDATE
        $this->pdo->exec("UPDATE edge_test SET name = name || ' (updated)' WHERE id = 1");

        $stmt = $this->pdo->query('SELECT name FROM edge_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice (updated)', $row['name']);
    }

    public function testDistinctWithNulls(): void
    {
        $this->pdo->exec("INSERT INTO edge_test (id, name, category) VALUES (1, 'a', 'X')");
        $this->pdo->exec("INSERT INTO edge_test (id, name, category) VALUES (2, 'b', 'X')");
        $this->pdo->exec("INSERT INTO edge_test (id, name, category) VALUES (3, 'c', NULL)");
        $this->pdo->exec("INSERT INTO edge_test (id, name, category) VALUES (4, 'd', NULL)");

        $stmt = $this->pdo->query('SELECT DISTINCT category FROM edge_test ORDER BY category');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // DISTINCT treats all NULLs as equal — should return 2 rows: NULL, 'X'
        $this->assertCount(2, $rows);
    }

    public function testGroupByWithHavingCount(): void
    {
        $this->pdo->exec("INSERT INTO edge_test (id, name, category) VALUES (1, 'a', 'X')");
        $this->pdo->exec("INSERT INTO edge_test (id, name, category) VALUES (2, 'b', 'X')");
        $this->pdo->exec("INSERT INTO edge_test (id, name, category) VALUES (3, 'c', 'Y')");

        $stmt = $this->pdo->query('SELECT category, COUNT(*) as cnt FROM edge_test GROUP BY category HAVING COUNT(*) > 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('X', $rows[0]['category']);
        $this->assertSame(2, (int) $rows[0]['cnt']);
    }

    public function testMinMaxWithStrings(): void
    {
        $this->pdo->exec("INSERT INTO edge_test (id, name) VALUES (1, 'Charlie')");
        $this->pdo->exec("INSERT INTO edge_test (id, name) VALUES (2, 'Alice')");
        $this->pdo->exec("INSERT INTO edge_test (id, name) VALUES (3, 'Bob')");

        $stmt = $this->pdo->query('SELECT MIN(name) as first, MAX(name) as last FROM edge_test');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['first']);
        $this->assertSame('Charlie', $row['last']);
    }

    public function testDeleteAllWithoutWhereDoesNotWork(): void
    {
        // KNOWN LIMITATION: DELETE without WHERE clause does not clear the shadow store.
        // The CTE rewriter apparently requires a WHERE clause to track deletions.
        $this->pdo->exec("INSERT INTO edge_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO edge_test (id, name, score) VALUES (2, 'Bob', 85)");

        $this->pdo->exec('DELETE FROM edge_test');

        // Shadow store still returns the rows — DELETE without WHERE is not applied
        $stmt = $this->pdo->query('SELECT COUNT(*) as cnt FROM edge_test');
        $this->assertSame(2, (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt']);
    }

    public function testDeleteAllWithWhereOneEqualsOne(): void
    {
        // Workaround: use WHERE 1=1 to delete all rows
        $this->pdo->exec("INSERT INTO edge_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO edge_test (id, name, score) VALUES (2, 'Bob', 85)");

        $this->pdo->exec('DELETE FROM edge_test WHERE 1=1');

        $stmt = $this->pdo->query('SELECT COUNT(*) as cnt FROM edge_test');
        $this->assertSame(0, (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt']);

        // Insert new data after delete
        $this->pdo->exec("INSERT INTO edge_test (id, name, score) VALUES (3, 'Charlie', 90)");

        $stmt = $this->pdo->query('SELECT * FROM edge_test');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Charlie', $rows[0]['name']);
    }

    public function testMultipleUpdatesToSameRow(): void
    {
        $this->pdo->exec("INSERT INTO edge_test (id, name, score) VALUES (1, 'Alice', 100)");

        $this->pdo->exec("UPDATE edge_test SET score = 90 WHERE id = 1");
        $this->pdo->exec("UPDATE edge_test SET name = 'Alice Updated' WHERE id = 1");
        $this->pdo->exec("UPDATE edge_test SET score = 95 WHERE id = 1");

        $stmt = $this->pdo->query('SELECT * FROM edge_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice Updated', $row['name']);
        $this->assertSame(95, (int) $row['score']);
    }

    public function testInsertDeleteInsertSameId(): void
    {
        $this->pdo->exec("INSERT INTO edge_test (id, name) VALUES (1, 'first')");
        $this->pdo->exec('DELETE FROM edge_test WHERE id = 1');
        $this->pdo->exec("INSERT INTO edge_test (id, name) VALUES (1, 'second')");

        $stmt = $this->pdo->query('SELECT * FROM edge_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('second', $rows[0]['name']);
    }
}
