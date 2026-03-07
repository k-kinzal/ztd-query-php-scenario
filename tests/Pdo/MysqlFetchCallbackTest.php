<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests fetchAll with FETCH_FUNC callback mode and other advanced fetch patterns on MySQL ZTD PDO.
 */
class MysqlFetchCallbackTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS gc_test_m');
        $raw->exec('DROP TABLE IF EXISTS grp_test_m');
        $raw->exec('DROP TABLE IF EXISTS cb_test_m');
        $raw->exec('CREATE TABLE cb_test_m (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
        $raw->exec('CREATE TABLE grp_test_m (id INT PRIMARY KEY, category VARCHAR(10), amount INT)');
        $raw->exec('CREATE TABLE gc_test_m (id INT PRIMARY KEY, category VARCHAR(10), name VARCHAR(50))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO cb_test_m VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO cb_test_m VALUES (2, 'Bob', 85)");
        $this->pdo->exec("INSERT INTO cb_test_m VALUES (3, 'Charlie', 70)");
    }

    public function testFetchAllWithFetchFunc(): void
    {
        $stmt = $this->pdo->query('SELECT name, score FROM cb_test_m ORDER BY id');
        $results = $stmt->fetchAll(PDO::FETCH_FUNC, function ($name, $score) {
            return "$name: $score";
        });

        $this->assertSame(['Alice: 100', 'Bob: 85', 'Charlie: 70'], $results);
    }

    public function testFetchFuncAfterShadowMutation(): void
    {
        $this->pdo->exec("UPDATE cb_test_m SET score = 999 WHERE id = 1");
        $this->pdo->exec("DELETE FROM cb_test_m WHERE id = 3");

        $stmt = $this->pdo->query('SELECT name, score FROM cb_test_m ORDER BY id');
        $results = $stmt->fetchAll(PDO::FETCH_FUNC, function ($name, $score) {
            return "$name: $score";
        });

        $this->assertSame(['Alice: 999', 'Bob: 85'], $results);
    }

    public function testFetchFuncWithPreparedStatement(): void
    {
        $stmt = $this->pdo->prepare('SELECT name, score FROM cb_test_m WHERE score > ?');
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
        $this->pdo->exec("INSERT INTO grp_test_m VALUES (1, 'A', 100)");
        $this->pdo->exec("INSERT INTO grp_test_m VALUES (2, 'A', 200)");
        $this->pdo->exec("INSERT INTO grp_test_m VALUES (3, 'B', 150)");
        $this->pdo->exec("INSERT INTO grp_test_m VALUES (4, 'B', 50)");

        $stmt = $this->pdo->query('SELECT SUM(amount) FROM grp_test_m GROUP BY category ORDER BY category');
        $sums = $stmt->fetchAll(PDO::FETCH_COLUMN);

        $this->assertSame([300, 200], array_map('intval', $sums));
    }

    public function testFetchGroupWithColumn(): void
    {
        $this->pdo->exec("INSERT INTO gc_test_m VALUES (1, 'A', 'Alice')");
        $this->pdo->exec("INSERT INTO gc_test_m VALUES (2, 'A', 'Bob')");
        $this->pdo->exec("INSERT INTO gc_test_m VALUES (3, 'B', 'Charlie')");

        $stmt = $this->pdo->query('SELECT category, name FROM gc_test_m ORDER BY id');
        $groups = $stmt->fetchAll(PDO::FETCH_GROUP | PDO::FETCH_COLUMN);

        $this->assertArrayHasKey('A', $groups);
        $this->assertSame(['Alice', 'Bob'], $groups['A']);
        $this->assertSame(['Charlie'], $groups['B']);
    }

    public function testFetchKeyPairAfterMutation(): void
    {
        $this->pdo->exec("UPDATE cb_test_m SET name = 'UpdatedAlice' WHERE id = 1");
        $this->pdo->exec("DELETE FROM cb_test_m WHERE id = 3");

        $stmt = $this->pdo->query('SELECT id, name FROM cb_test_m ORDER BY id');
        $pairs = $stmt->fetchAll(PDO::FETCH_KEY_PAIR);

        $this->assertSame([1 => 'UpdatedAlice', 2 => 'Bob'], $pairs);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS gc_test_m');
        $raw->exec('DROP TABLE IF EXISTS grp_test_m');
        $raw->exec('DROP TABLE IF EXISTS cb_test_m');
    }
}
