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
 * Tests CREATE TABLE IF NOT EXISTS on MySQL PDO.
 */
class MysqlCreateTableIfNotExistsTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
        $raw->exec('DROP TABLE IF EXISTS pdo_mctine_test');
        $raw->exec('CREATE TABLE pdo_mctine_test (id INT PRIMARY KEY, name VARCHAR(50))');
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

    /**
     * CREATE TABLE IF NOT EXISTS on existing table is no-op.
     */
    public function testCreateIfNotExistsOnExisting(): void
    {
        $this->pdo->exec("INSERT INTO pdo_mctine_test VALUES (1, 'Alice')");

        $this->pdo->exec('CREATE TABLE IF NOT EXISTS pdo_mctine_test (id INT PRIMARY KEY, name VARCHAR(50))');

        $stmt = $this->pdo->query('SELECT name FROM pdo_mctine_test WHERE id = 1');
        $this->assertSame('Alice', $stmt->fetchColumn());
    }

    /**
     * CREATE TABLE IF NOT EXISTS creates new table.
     */
    public function testCreateIfNotExistsNew(): void
    {
        $this->pdo->exec('CREATE TABLE IF NOT EXISTS pdo_mctine_new (id INT PRIMARY KEY, val VARCHAR(50))');
        $this->pdo->exec("INSERT INTO pdo_mctine_new VALUES (1, 'test')");

        $stmt = $this->pdo->query('SELECT val FROM pdo_mctine_new WHERE id = 1');
        $this->assertSame('test', $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
            $raw->exec('DROP TABLE IF EXISTS pdo_mctine_test');
            $raw->exec('DROP TABLE IF EXISTS pdo_mctine_new');
        } catch (\Exception $e) {
        }
    }
}
