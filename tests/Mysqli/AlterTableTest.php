<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

class AlterTableTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS alter_test');
        $raw->query('CREATE TABLE alter_test (id INT PRIMARY KEY, name VARCHAR(255))');
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

        $this->mysqli->query("INSERT INTO alter_test (id, name) VALUES (1, 'Alice')");
        $this->mysqli->query("INSERT INTO alter_test (id, name) VALUES (2, 'Bob')");
    }

    public function testAddColumn(): void
    {
        $this->mysqli->query('ALTER TABLE alter_test ADD COLUMN age INT');

        // Insert with the new column
        $this->mysqli->query("INSERT INTO alter_test (id, name, age) VALUES (3, 'Charlie', 30)");

        $result = $this->mysqli->query('SELECT * FROM alter_test WHERE id = 3');
        $row = $result->fetch_assoc();
        $this->assertSame('Charlie', $row['name']);
        $this->assertSame(30, (int) $row['age']);
    }

    public function testDropColumn(): void
    {
        $this->mysqli->query('ALTER TABLE alter_test DROP COLUMN name');

        $result = $this->mysqli->query('SELECT * FROM alter_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertArrayHasKey('id', $row);
        $this->assertArrayNotHasKey('name', $row);
    }

    public function testModifyColumn(): void
    {
        $this->mysqli->query('ALTER TABLE alter_test MODIFY COLUMN name TEXT');

        // Should still be able to query
        $result = $this->mysqli->query('SELECT * FROM alter_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);
    }

    public function testChangeColumn(): void
    {
        $this->mysqli->query('ALTER TABLE alter_test CHANGE COLUMN name full_name VARCHAR(500)');

        // Insert with new column name
        $this->mysqli->query("INSERT INTO alter_test (id, full_name) VALUES (3, 'Charlie Brown')");

        $result = $this->mysqli->query('SELECT * FROM alter_test WHERE id = 3');
        $row = $result->fetch_assoc();
        $this->assertSame('Charlie Brown', $row['full_name']);
    }

    public function testAddAndDropColumnSequence(): void
    {
        $this->mysqli->query('ALTER TABLE alter_test ADD COLUMN email VARCHAR(255)');
        $this->mysqli->query("INSERT INTO alter_test (id, name, email) VALUES (3, 'Charlie', 'charlie@example.com')");

        $result = $this->mysqli->query('SELECT email FROM alter_test WHERE id = 3');
        $row = $result->fetch_assoc();
        $this->assertSame('charlie@example.com', $row['email']);

        // Drop the column we just added
        $this->mysqli->query('ALTER TABLE alter_test DROP COLUMN email');

        $result = $this->mysqli->query('SELECT * FROM alter_test WHERE id = 3');
        $row = $result->fetch_assoc();
        $this->assertArrayNotHasKey('email', $row);
    }

    public function testAlterTableIsolation(): void
    {
        $this->mysqli->query('ALTER TABLE alter_test ADD COLUMN age INT');

        // Physical table should be unchanged
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SHOW COLUMNS FROM alter_test');
        $columns = [];
        while ($row = $result->fetch_assoc()) {
            $columns[] = $row['Field'];
        }
        $this->assertNotContains('age', $columns);
        $this->mysqli->enableZtd();
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
        $raw->query('DROP TABLE IF EXISTS alter_test');
        $raw->close();
    }
}
