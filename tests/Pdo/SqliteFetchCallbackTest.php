<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests fetchAll with FETCH_FUNC callback mode and other advanced fetch patterns on SQLite ZTD.
 */
class SqliteFetchCallbackTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);

        $this->pdo->exec('CREATE TABLE cb_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
        $this->pdo->exec("INSERT INTO cb_test VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO cb_test VALUES (2, 'Bob', 85)");
        $this->pdo->exec("INSERT INTO cb_test VALUES (3, 'Charlie', 70)");
    }

    public function testFetchAllWithFetchFunc(): void
    {
        $stmt = $this->pdo->query('SELECT name, score FROM cb_test ORDER BY id');
        $results = $stmt->fetchAll(PDO::FETCH_FUNC, function ($name, $score) {
            return "$name: $score";
        });

        $this->assertSame(['Alice: 100', 'Bob: 85', 'Charlie: 70'], $results);
    }

    public function testFetchFuncAfterShadowMutation(): void
    {
        $this->pdo->exec("UPDATE cb_test SET score = 999 WHERE id = 1");
        $this->pdo->exec("DELETE FROM cb_test WHERE id = 3");

        $stmt = $this->pdo->query('SELECT name, score FROM cb_test ORDER BY id');
        $results = $stmt->fetchAll(PDO::FETCH_FUNC, function ($name, $score) {
            return "$name: $score";
        });

        $this->assertSame(['Alice: 999', 'Bob: 85'], $results);
    }

    public function testFetchFuncWithPreparedStatement(): void
    {
        $stmt = $this->pdo->prepare('SELECT name, score FROM cb_test WHERE score > ?');
        $stmt->execute([80]);
        $results = $stmt->fetchAll(PDO::FETCH_FUNC, function ($name, $score) {
            return strtoupper($name) . '=' . $score;
        });

        $this->assertCount(2, $results);
        $this->assertContains('ALICE=100', $results);
        $this->assertContains('BOB=85', $results);
    }

    public function testFetchColumnWithGroupByAggregation(): void
    {
        $this->pdo->exec('CREATE TABLE grp_test (id INT PRIMARY KEY, category VARCHAR(10), amount INT)');
        $this->pdo->exec("INSERT INTO grp_test VALUES (1, 'A', 100)");
        $this->pdo->exec("INSERT INTO grp_test VALUES (2, 'A', 200)");
        $this->pdo->exec("INSERT INTO grp_test VALUES (3, 'B', 150)");
        $this->pdo->exec("INSERT INTO grp_test VALUES (4, 'B', 50)");

        $stmt = $this->pdo->query('SELECT SUM(amount) FROM grp_test GROUP BY category ORDER BY category');
        $sums = $stmt->fetchAll(PDO::FETCH_COLUMN);

        $this->assertSame([300, 200], array_map('intval', $sums));
    }

    public function testFetchGroupWithCallback(): void
    {
        $this->pdo->exec('CREATE TABLE gc_test (id INT PRIMARY KEY, category VARCHAR(10), name VARCHAR(50))');
        $this->pdo->exec("INSERT INTO gc_test VALUES (1, 'A', 'Alice')");
        $this->pdo->exec("INSERT INTO gc_test VALUES (2, 'A', 'Bob')");
        $this->pdo->exec("INSERT INTO gc_test VALUES (3, 'B', 'Charlie')");

        $stmt = $this->pdo->query('SELECT category, name FROM gc_test ORDER BY id');
        $groups = $stmt->fetchAll(PDO::FETCH_GROUP | PDO::FETCH_COLUMN);

        $this->assertArrayHasKey('A', $groups);
        $this->assertSame(['Alice', 'Bob'], $groups['A']);
        $this->assertSame(['Charlie'], $groups['B']);
    }

    public function testFetchKeyPairAfterMutation(): void
    {
        $this->pdo->exec("UPDATE cb_test SET name = 'UpdatedAlice' WHERE id = 1");
        $this->pdo->exec("DELETE FROM cb_test WHERE id = 3");

        $stmt = $this->pdo->query('SELECT id, name FROM cb_test ORDER BY id');
        $pairs = $stmt->fetchAll(PDO::FETCH_KEY_PAIR);

        $this->assertSame([1 => 'UpdatedAlice', 2 => 'Bob'], $pairs);
    }

    public function testFetchUniqueAfterMutation(): void
    {
        $this->pdo->exec("INSERT INTO cb_test VALUES (4, 'Diana', 95)");

        $stmt = $this->pdo->query('SELECT id, name, score FROM cb_test ORDER BY id');
        $unique = $stmt->fetchAll(PDO::FETCH_UNIQUE | PDO::FETCH_ASSOC);

        $this->assertCount(4, $unique);
        $this->assertArrayHasKey(4, $unique);
        $this->assertSame('Diana', $unique[4]['name']);
    }

    public function testMultipleFetchAllModes(): void
    {
        // Test that we can use different fetchAll modes sequentially
        $stmt1 = $this->pdo->query('SELECT name FROM cb_test ORDER BY id');
        $columns = $stmt1->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Alice', 'Bob', 'Charlie'], $columns);

        $stmt2 = $this->pdo->query('SELECT name FROM cb_test ORDER BY id');
        $objects = $stmt2->fetchAll(PDO::FETCH_OBJ);
        $this->assertCount(3, $objects);
        $this->assertSame('Alice', $objects[0]->name);
    }
}
