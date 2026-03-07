<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests edge cases in query behavior with the shadow store on MySQL via MySQLi:
 * NULL handling, ORDER BY, LIMIT, self-referencing updates, etc.
 */
class QueryEdgeCaseTest extends TestCase
{
    private ZtdMysqli $mysqli;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS mysqli_edge_test');
        $raw->query('CREATE TABLE mysqli_edge_test (id INT PRIMARY KEY, name VARCHAR(255), score INT, category VARCHAR(255))');
        $raw->close();
    }

    protected function setUp(): void
    {
        $this->mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
    }

    public function testCountStarVsCountColumn(): void
    {
        $this->mysqli->query("INSERT INTO mysqli_edge_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->mysqli->query("INSERT INTO mysqli_edge_test (id, name, score) VALUES (2, 'Bob', NULL)");
        $this->mysqli->query("INSERT INTO mysqli_edge_test (id, name, score) VALUES (3, 'Charlie', 90)");

        $result = $this->mysqli->query('SELECT COUNT(*) as total FROM mysqli_edge_test');
        $row = $result->fetch_assoc();
        $this->assertSame(3, (int) $row['total']);

        $result = $this->mysqli->query('SELECT COUNT(score) as non_null FROM mysqli_edge_test');
        $row = $result->fetch_assoc();
        $this->assertSame(2, (int) $row['non_null']);
    }

    public function testSumWithNulls(): void
    {
        $this->mysqli->query("INSERT INTO mysqli_edge_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->mysqli->query("INSERT INTO mysqli_edge_test (id, name, score) VALUES (2, 'Bob', NULL)");
        $this->mysqli->query("INSERT INTO mysqli_edge_test (id, name, score) VALUES (3, 'Charlie', 50)");

        $result = $this->mysqli->query('SELECT SUM(score) as total FROM mysqli_edge_test');
        $row = $result->fetch_assoc();
        $this->assertEquals(150, $row['total']);
    }

    public function testOrderByWithNulls(): void
    {
        $this->mysqli->query("INSERT INTO mysqli_edge_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->mysqli->query("INSERT INTO mysqli_edge_test (id, name, score) VALUES (2, 'Bob', NULL)");
        $this->mysqli->query("INSERT INTO mysqli_edge_test (id, name, score) VALUES (3, 'Charlie', 50)");

        // MySQL: NULLs sort first in ASC order
        $result = $this->mysqli->query('SELECT name, score FROM mysqli_edge_test ORDER BY score ASC');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertNull($rows[0]['score']);
        $this->assertSame('Bob', $rows[0]['name']);
    }

    public function testLimitZero(): void
    {
        $this->mysqli->query("INSERT INTO mysqli_edge_test (id, name, score) VALUES (1, 'Alice', 100)");

        $result = $this->mysqli->query('SELECT * FROM mysqli_edge_test LIMIT 0');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(0, $rows);
    }

    public function testLimitWithOffset(): void
    {
        for ($i = 1; $i <= 5; $i++) {
            $this->mysqli->query("INSERT INTO mysqli_edge_test (id, name, score) VALUES ($i, 'item_$i', $i)");
        }

        $result = $this->mysqli->query('SELECT * FROM mysqli_edge_test ORDER BY id LIMIT 2 OFFSET 2');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame(3, (int) $rows[0]['id']);
        $this->assertSame(4, (int) $rows[1]['id']);
    }

    public function testSelfReferencingUpdate(): void
    {
        $this->mysqli->query("INSERT INTO mysqli_edge_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->mysqli->query("INSERT INTO mysqli_edge_test (id, name, score) VALUES (2, 'Bob', 85)");

        $this->mysqli->query('UPDATE mysqli_edge_test SET score = score + 10');

        $result = $this->mysqli->query('SELECT score FROM mysqli_edge_test ORDER BY id');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertSame(110, (int) $rows[0]['score']);
        $this->assertSame(95, (int) $rows[1]['score']);
    }

    public function testUpdateWithConcatenation(): void
    {
        $this->mysqli->query("INSERT INTO mysqli_edge_test (id, name, score) VALUES (1, 'Alice', 100)");

        $this->mysqli->query("UPDATE mysqli_edge_test SET name = CONCAT(name, ' (updated)') WHERE id = 1");

        $result = $this->mysqli->query('SELECT name FROM mysqli_edge_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Alice (updated)', $row['name']);
    }

    public function testDistinctWithNulls(): void
    {
        $this->mysqli->query("INSERT INTO mysqli_edge_test (id, name, category) VALUES (1, 'a', 'X')");
        $this->mysqli->query("INSERT INTO mysqli_edge_test (id, name, category) VALUES (2, 'b', 'X')");
        $this->mysqli->query("INSERT INTO mysqli_edge_test (id, name, category) VALUES (3, 'c', NULL)");
        $this->mysqli->query("INSERT INTO mysqli_edge_test (id, name, category) VALUES (4, 'd', NULL)");

        $result = $this->mysqli->query('SELECT DISTINCT category FROM mysqli_edge_test ORDER BY category');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
    }

    public function testGroupByWithHavingCount(): void
    {
        $this->mysqli->query("INSERT INTO mysqli_edge_test (id, name, category) VALUES (1, 'a', 'X')");
        $this->mysqli->query("INSERT INTO mysqli_edge_test (id, name, category) VALUES (2, 'b', 'X')");
        $this->mysqli->query("INSERT INTO mysqli_edge_test (id, name, category) VALUES (3, 'c', 'Y')");

        $result = $this->mysqli->query('SELECT category, COUNT(*) as cnt FROM mysqli_edge_test GROUP BY category HAVING COUNT(*) > 1');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('X', $rows[0]['category']);
        $this->assertSame(2, (int) $rows[0]['cnt']);
    }

    public function testMinMaxWithStrings(): void
    {
        $this->mysqli->query("INSERT INTO mysqli_edge_test (id, name) VALUES (1, 'Charlie')");
        $this->mysqli->query("INSERT INTO mysqli_edge_test (id, name) VALUES (2, 'Alice')");
        $this->mysqli->query("INSERT INTO mysqli_edge_test (id, name) VALUES (3, 'Bob')");

        $result = $this->mysqli->query('SELECT MIN(name) as first, MAX(name) as last FROM mysqli_edge_test');
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['first']);
        $this->assertSame('Charlie', $row['last']);
    }

    public function testDeleteAllWithoutWhere(): void
    {
        // DELETE without WHERE works on MySQL (unlike SQLite where it's ignored)
        $this->mysqli->query("INSERT INTO mysqli_edge_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->mysqli->query("INSERT INTO mysqli_edge_test (id, name, score) VALUES (2, 'Bob', 85)");

        $this->mysqli->query('DELETE FROM mysqli_edge_test');

        $result = $this->mysqli->query('SELECT COUNT(*) as cnt FROM mysqli_edge_test');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }

    public function testDeleteAllThenInsert(): void
    {
        $this->mysqli->query("INSERT INTO mysqli_edge_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->mysqli->query("INSERT INTO mysqli_edge_test (id, name, score) VALUES (2, 'Bob', 85)");

        $this->mysqli->query('DELETE FROM mysqli_edge_test WHERE 1=1');

        $result = $this->mysqli->query('SELECT COUNT(*) as cnt FROM mysqli_edge_test');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);

        $this->mysqli->query("INSERT INTO mysqli_edge_test (id, name, score) VALUES (3, 'Charlie', 90)");

        $result = $this->mysqli->query('SELECT * FROM mysqli_edge_test');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Charlie', $rows[0]['name']);
    }

    public function testMultipleUpdatesToSameRow(): void
    {
        $this->mysqli->query("INSERT INTO mysqli_edge_test (id, name, score) VALUES (1, 'Alice', 100)");

        $this->mysqli->query("UPDATE mysqli_edge_test SET score = 90 WHERE id = 1");
        $this->mysqli->query("UPDATE mysqli_edge_test SET name = 'Alice Updated' WHERE id = 1");
        $this->mysqli->query("UPDATE mysqli_edge_test SET score = 95 WHERE id = 1");

        $result = $this->mysqli->query('SELECT * FROM mysqli_edge_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Alice Updated', $row['name']);
        $this->assertSame(95, (int) $row['score']);
    }

    public function testInsertDeleteInsertSameId(): void
    {
        $this->mysqli->query("INSERT INTO mysqli_edge_test (id, name) VALUES (1, 'first')");
        $this->mysqli->query('DELETE FROM mysqli_edge_test WHERE id = 1');
        $this->mysqli->query("INSERT INTO mysqli_edge_test (id, name) VALUES (1, 'second')");

        $result = $this->mysqli->query('SELECT * FROM mysqli_edge_test WHERE id = 1');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('second', $rows[0]['name']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS mysqli_edge_test');
        $raw->close();
    }

    protected function tearDown(): void
    {
        $this->mysqli->close();
    }
}
