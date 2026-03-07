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
 * Tests that database constraints are NOT enforced in the shadow store on MySQL via PDO.
 */
class MysqlConstraintBehaviorTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS mysql_constraint_test');
        $raw->exec('CREATE TABLE mysql_constraint_test (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL, email VARCHAR(255) UNIQUE)');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
    }

    public function testDuplicatePrimaryKeyNotEnforcedInShadow(): void
    {
        $this->pdo->exec("INSERT INTO mysql_constraint_test (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");
        $this->pdo->exec("INSERT INTO mysql_constraint_test (id, name, email) VALUES (1, 'Bob', 'bob@example.com')");

        $stmt = $this->pdo->query('SELECT * FROM mysql_constraint_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertGreaterThanOrEqual(1, count($rows));
    }

    public function testNotNullNotEnforcedInShadow(): void
    {
        $this->pdo->exec("INSERT INTO mysql_constraint_test (id, name, email) VALUES (1, NULL, 'test@test.com')");

        $stmt = $this->pdo->query('SELECT * FROM mysql_constraint_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertNull($rows[0]['name']);
    }

    public function testUniqueConstraintNotEnforcedInShadow(): void
    {
        $this->pdo->exec("INSERT INTO mysql_constraint_test (id, name, email) VALUES (1, 'Alice', 'same@email.com')");
        $this->pdo->exec("INSERT INTO mysql_constraint_test (id, name, email) VALUES (2, 'Bob', 'same@email.com')");

        $stmt = $this->pdo->query('SELECT * FROM mysql_constraint_test ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mysql_constraint_test');
    }
}
