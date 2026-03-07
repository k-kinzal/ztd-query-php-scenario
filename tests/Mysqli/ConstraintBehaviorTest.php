<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

class ConstraintBehaviorTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS constraint_child');
        $raw->query('DROP TABLE IF EXISTS constraint_test');
        $raw->query('CREATE TABLE constraint_test (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL, email VARCHAR(255) UNIQUE)');
        $raw->query('CREATE TABLE constraint_child (id INT PRIMARY KEY, parent_id INT, FOREIGN KEY (parent_id) REFERENCES constraint_test(id))');
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

    public function testDuplicatePrimaryKeyNotEnforcedInShadow(): void
    {
        $this->mysqli->query("INSERT INTO constraint_test (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");
        $this->mysqli->query("INSERT INTO constraint_test (id, name, email) VALUES (1, 'Bob', 'bob@example.com')");

        // Shadow store does not enforce PRIMARY KEY uniqueness
        $result = $this->mysqli->query('SELECT * FROM constraint_test WHERE id = 1');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertGreaterThanOrEqual(1, count($rows));
    }

    public function testNotNullNotEnforcedInShadow(): void
    {
        // Shadow store does not enforce NOT NULL constraints
        $this->mysqli->query("INSERT INTO constraint_test (id, name, email) VALUES (1, NULL, 'test@test.com')");

        $result = $this->mysqli->query('SELECT * FROM constraint_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertNotNull($row);
        $this->assertNull($row['name']);
    }

    public function testUniqueConstraintNotEnforcedInShadow(): void
    {
        $this->mysqli->query("INSERT INTO constraint_test (id, name, email) VALUES (1, 'Alice', 'same@email.com')");
        $this->mysqli->query("INSERT INTO constraint_test (id, name, email) VALUES (2, 'Bob', 'same@email.com')");

        // Shadow store does not enforce UNIQUE constraints
        $result = $this->mysqli->query('SELECT * FROM constraint_test ORDER BY id');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
    }

    public function testForeignKeyNotEnforcedInShadow(): void
    {
        // Shadow store does not enforce FOREIGN KEY constraints
        $this->mysqli->query("INSERT INTO constraint_child (id, parent_id) VALUES (1, 999)");

        $result = $this->mysqli->query('SELECT * FROM constraint_child WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame(999, (int) $row['parent_id']);
    }

    protected function tearDown(): void
    {
        $this->mysqli->close();
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
        $raw->query('DROP TABLE IF EXISTS constraint_child');
        $raw->query('DROP TABLE IF EXISTS constraint_test');
        $raw->close();
    }
}
