<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

class CreateTableVariantsTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS source_table');
        $raw->exec('CREATE TABLE source_table (id INT PRIMARY KEY, val VARCHAR(255))');
        $raw->exec('DROP TABLE IF EXISTS ctas_target');
        $raw->exec('DROP TABLE IF EXISTS ctlike_target');
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

    public function testCreateTableLike(): void
    {
        $this->pdo->exec('CREATE TABLE ctlike_target LIKE source_table');

        // Should be able to insert and select from the LIKE-created table
        $this->pdo->exec("INSERT INTO ctlike_target (id, val) VALUES (1, 'hello')");

        $stmt = $this->pdo->query('SELECT * FROM ctlike_target WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('hello', $rows[0]['val']);
    }

    public function testCreateTableAsSelect(): void
    {
        // Insert data into source via shadow
        $this->pdo->exec("INSERT INTO source_table (id, val) VALUES (1, 'hello')");
        $this->pdo->exec("INSERT INTO source_table (id, val) VALUES (2, 'world')");

        // Create table as select from shadow data
        $this->pdo->exec('CREATE TABLE ctas_target AS SELECT * FROM source_table');

        $stmt = $this->pdo->query('SELECT * FROM ctas_target ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('hello', $rows[0]['val']);
        $this->assertSame('world', $rows[1]['val']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS source_table');
        $raw->exec('DROP TABLE IF EXISTS ctas_target');
        $raw->exec('DROP TABLE IF EXISTS ctlike_target');
    }
}
