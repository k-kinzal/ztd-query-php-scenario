<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\PostgreSQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests JSON data handling and CROSS JOIN patterns on PostgreSQL PDO.
 */
class PostgresJsonAndCrossJoinTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_jcj_products');
        $raw->exec('DROP TABLE IF EXISTS pg_jcj_colors');
        $raw->exec('DROP TABLE IF EXISTS pg_jcj_sizes');
        $raw->exec('CREATE TABLE pg_jcj_products (id INT PRIMARY KEY, name VARCHAR(255), metadata JSONB)');
        $raw->exec('CREATE TABLE pg_jcj_colors (id INT PRIMARY KEY, color VARCHAR(50))');
        $raw->exec('CREATE TABLE pg_jcj_sizes (id INT PRIMARY KEY, size VARCHAR(50))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
    }

    public function testInsertAndSelectJsonData(): void
    {
        $this->pdo->exec("INSERT INTO pg_jcj_products (id, name, metadata) VALUES (1, 'Widget', '{\"color\":\"red\",\"weight\":1.5}')");
        $this->pdo->exec("INSERT INTO pg_jcj_products (id, name, metadata) VALUES (2, 'Gadget', '{\"color\":\"blue\",\"weight\":2.0}')");

        $stmt = $this->pdo->query('SELECT name, metadata FROM pg_jcj_products ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);

        $meta1 = json_decode($rows[0]['metadata'], true);
        $this->assertSame('red', $meta1['color']);
    }

    public function testJsonExtractFunction(): void
    {
        $this->pdo->exec("INSERT INTO pg_jcj_products (id, name, metadata) VALUES (1, 'Widget', '{\"color\":\"red\"}')");
        $this->pdo->exec("INSERT INTO pg_jcj_products (id, name, metadata) VALUES (2, 'Gadget', '{\"color\":\"blue\"}')");

        // PostgreSQL JSONB ->> operator
        $stmt = $this->pdo->query("SELECT name, metadata->>'color' AS color FROM pg_jcj_products ORDER BY id");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('red', $rows[0]['color']);
        $this->assertSame('blue', $rows[1]['color']);
    }

    public function testCrossJoin(): void
    {
        $this->pdo->exec("INSERT INTO pg_jcj_colors (id, color) VALUES (1, 'Red')");
        $this->pdo->exec("INSERT INTO pg_jcj_colors (id, color) VALUES (2, 'Blue')");
        $this->pdo->exec("INSERT INTO pg_jcj_sizes (id, size) VALUES (1, 'Small')");
        $this->pdo->exec("INSERT INTO pg_jcj_sizes (id, size) VALUES (2, 'Medium')");
        $this->pdo->exec("INSERT INTO pg_jcj_sizes (id, size) VALUES (3, 'Large')");

        $stmt = $this->pdo->query("
            SELECT c.color, s.size
            FROM pg_jcj_colors c
            CROSS JOIN pg_jcj_sizes s
            ORDER BY c.color, s.size
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(6, $rows);
    }

    public function testImplicitCrossJoin(): void
    {
        $this->pdo->exec("INSERT INTO pg_jcj_colors (id, color) VALUES (1, 'Red')");
        $this->pdo->exec("INSERT INTO pg_jcj_colors (id, color) VALUES (2, 'Blue')");
        $this->pdo->exec("INSERT INTO pg_jcj_sizes (id, size) VALUES (1, 'S')");
        $this->pdo->exec("INSERT INTO pg_jcj_sizes (id, size) VALUES (2, 'M')");

        $stmt = $this->pdo->query("
            SELECT c.color, s.size
            FROM pg_jcj_colors c, pg_jcj_sizes s
            ORDER BY c.color, s.size
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(4, $rows);
    }

    public function testCrossJoinAfterMutations(): void
    {
        $this->pdo->exec("INSERT INTO pg_jcj_colors (id, color) VALUES (1, 'Red')");
        $this->pdo->exec("INSERT INTO pg_jcj_colors (id, color) VALUES (2, 'Blue')");
        $this->pdo->exec("INSERT INTO pg_jcj_sizes (id, size) VALUES (1, 'S')");
        $this->pdo->exec("INSERT INTO pg_jcj_sizes (id, size) VALUES (2, 'M')");

        $this->pdo->exec("DELETE FROM pg_jcj_colors WHERE id = 2");

        $stmt = $this->pdo->query("
            SELECT c.color, s.size
            FROM pg_jcj_colors c
            CROSS JOIN pg_jcj_sizes s
            ORDER BY c.color, s.size
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Red', $rows[0]['color']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_jcj_products');
        $raw->exec('DROP TABLE IF EXISTS pg_jcj_colors');
        $raw->exec('DROP TABLE IF EXISTS pg_jcj_sizes');
    }
}
