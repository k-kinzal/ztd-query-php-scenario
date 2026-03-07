<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

class AlterTableTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS alter_test');
        $raw->exec('CREATE TABLE alter_test (id INT PRIMARY KEY, name VARCHAR(255))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO alter_test (id, name) VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO alter_test (id, name) VALUES (2, 'Bob')");
    }

    public function testAddColumn(): void
    {
        $this->pdo->exec('ALTER TABLE alter_test ADD COLUMN age INT');

        $this->pdo->exec("INSERT INTO alter_test (id, name, age) VALUES (3, 'Charlie', 30)");

        $stmt = $this->pdo->query('SELECT * FROM alter_test WHERE id = 3');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Charlie', $rows[0]['name']);
        $this->assertSame(30, (int) $rows[0]['age']);
    }

    public function testDropColumn(): void
    {
        $this->pdo->exec('ALTER TABLE alter_test DROP COLUMN name');

        $stmt = $this->pdo->query('SELECT * FROM alter_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertArrayHasKey('id', $rows[0]);
        $this->assertArrayNotHasKey('name', $rows[0]);
    }

    public function testModifyColumn(): void
    {
        $this->pdo->exec('ALTER TABLE alter_test MODIFY COLUMN name TEXT');

        $stmt = $this->pdo->query('SELECT * FROM alter_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public function testChangeColumn(): void
    {
        $this->pdo->exec('ALTER TABLE alter_test CHANGE COLUMN name full_name VARCHAR(500)');

        $this->pdo->exec("INSERT INTO alter_test (id, full_name) VALUES (3, 'Charlie Brown')");

        $stmt = $this->pdo->query('SELECT * FROM alter_test WHERE id = 3');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('Charlie Brown', $rows[0]['full_name']);
    }

    public function testAlterTableIsolation(): void
    {
        $this->pdo->exec('ALTER TABLE alter_test ADD COLUMN age INT');

        // Physical table should be unchanged
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SHOW COLUMNS FROM alter_test');
        $columns = array_column($stmt->fetchAll(PDO::FETCH_ASSOC), 'Field');
        $this->assertNotContains('age', $columns);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS alter_test');
    }
}
