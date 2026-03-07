<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

class ConstraintBehaviorTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS constraint_child');
        $raw->exec('DROP TABLE IF EXISTS constraint_test');
        $raw->exec('CREATE TABLE constraint_test (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL, email VARCHAR(255) UNIQUE)');
        $raw->exec('CREATE TABLE constraint_child (id INT PRIMARY KEY, parent_id INT, FOREIGN KEY (parent_id) REFERENCES constraint_test(id))');
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
        $this->pdo->exec("INSERT INTO constraint_test (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");
        $this->pdo->exec("INSERT INTO constraint_test (id, name, email) VALUES (1, 'Bob', 'bob@example.com')");

        // Shadow store does not enforce PRIMARY KEY uniqueness
        $stmt = $this->pdo->query('SELECT * FROM constraint_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertGreaterThanOrEqual(1, count($rows));
    }

    public function testNotNullNotEnforcedInShadow(): void
    {
        // Shadow store does not enforce NOT NULL constraints
        $this->pdo->exec("INSERT INTO constraint_test (id, name, email) VALUES (1, NULL, 'test@test.com')");

        $stmt = $this->pdo->query('SELECT * FROM constraint_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertNull($rows[0]['name']);
    }

    public function testUniqueConstraintNotEnforcedInShadow(): void
    {
        $this->pdo->exec("INSERT INTO constraint_test (id, name, email) VALUES (1, 'Alice', 'same@email.com')");
        $this->pdo->exec("INSERT INTO constraint_test (id, name, email) VALUES (2, 'Bob', 'same@email.com')");

        // Shadow store does not enforce UNIQUE constraints
        $stmt = $this->pdo->query('SELECT * FROM constraint_test ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
    }

    public function testForeignKeyNotEnforcedInShadow(): void
    {
        // Shadow store does not enforce FOREIGN KEY constraints
        $this->pdo->exec("INSERT INTO constraint_child (id, parent_id) VALUES (1, 999)");

        $stmt = $this->pdo->query('SELECT * FROM constraint_child WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(999, (int) $rows[0]['parent_id']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS constraint_child');
        $raw->exec('DROP TABLE IF EXISTS constraint_test');
    }
}
