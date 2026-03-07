<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests JSON data handling and CROSS JOIN patterns on MySQLi.
 */
class JsonAndCrossJoinTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_jcj_products');
        $raw->query('DROP TABLE IF EXISTS mi_jcj_colors');
        $raw->query('DROP TABLE IF EXISTS mi_jcj_sizes');
        $raw->query('CREATE TABLE mi_jcj_products (id INT PRIMARY KEY, name VARCHAR(255), metadata JSON)');
        $raw->query('CREATE TABLE mi_jcj_colors (id INT PRIMARY KEY, color VARCHAR(50))');
        $raw->query('CREATE TABLE mi_jcj_sizes (id INT PRIMARY KEY, size VARCHAR(50))');
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

    public function testInsertAndSelectJsonData(): void
    {
        $this->mysqli->query("INSERT INTO mi_jcj_products (id, name, metadata) VALUES (1, 'Widget', '{\"color\":\"red\",\"weight\":1.5}')");
        $this->mysqli->query("INSERT INTO mi_jcj_products (id, name, metadata) VALUES (2, 'Gadget', '{\"color\":\"blue\",\"weight\":2.0}')");

        $result = $this->mysqli->query('SELECT name, metadata FROM mi_jcj_products ORDER BY id');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);

        $meta1 = json_decode($rows[0]['metadata'], true);
        $this->assertSame('red', $meta1['color']);
    }

    public function testJsonExtractFunction(): void
    {
        $this->mysqli->query("INSERT INTO mi_jcj_products (id, name, metadata) VALUES (1, 'Widget', '{\"color\":\"red\"}')");
        $this->mysqli->query("INSERT INTO mi_jcj_products (id, name, metadata) VALUES (2, 'Gadget', '{\"color\":\"blue\"}')");

        $result = $this->mysqli->query("SELECT name, JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.color')) AS color FROM mi_jcj_products ORDER BY id");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertSame('red', $rows[0]['color']);
        $this->assertSame('blue', $rows[1]['color']);
    }

    public function testCrossJoin(): void
    {
        $this->mysqli->query("INSERT INTO mi_jcj_colors (id, color) VALUES (1, 'Red')");
        $this->mysqli->query("INSERT INTO mi_jcj_colors (id, color) VALUES (2, 'Blue')");
        $this->mysqli->query("INSERT INTO mi_jcj_sizes (id, size) VALUES (1, 'Small')");
        $this->mysqli->query("INSERT INTO mi_jcj_sizes (id, size) VALUES (2, 'Medium')");
        $this->mysqli->query("INSERT INTO mi_jcj_sizes (id, size) VALUES (3, 'Large')");

        $result = $this->mysqli->query("
            SELECT c.color, s.size
            FROM mi_jcj_colors c
            CROSS JOIN mi_jcj_sizes s
            ORDER BY c.color, s.size
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(6, $rows);
    }

    public function testImplicitCrossJoin(): void
    {
        $this->mysqli->query("INSERT INTO mi_jcj_colors (id, color) VALUES (1, 'Red')");
        $this->mysqli->query("INSERT INTO mi_jcj_colors (id, color) VALUES (2, 'Blue')");
        $this->mysqli->query("INSERT INTO mi_jcj_sizes (id, size) VALUES (1, 'S')");
        $this->mysqli->query("INSERT INTO mi_jcj_sizes (id, size) VALUES (2, 'M')");

        $result = $this->mysqli->query("
            SELECT c.color, s.size
            FROM mi_jcj_colors c, mi_jcj_sizes s
            ORDER BY c.color, s.size
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(4, $rows);
    }

    public function testCrossJoinAfterMutations(): void
    {
        $this->mysqli->query("INSERT INTO mi_jcj_colors (id, color) VALUES (1, 'Red')");
        $this->mysqli->query("INSERT INTO mi_jcj_colors (id, color) VALUES (2, 'Blue')");
        $this->mysqli->query("INSERT INTO mi_jcj_sizes (id, size) VALUES (1, 'S')");
        $this->mysqli->query("INSERT INTO mi_jcj_sizes (id, size) VALUES (2, 'M')");

        $this->mysqli->query("DELETE FROM mi_jcj_colors WHERE id = 2");

        $result = $this->mysqli->query("
            SELECT c.color, s.size
            FROM mi_jcj_colors c
            CROSS JOIN mi_jcj_sizes s
            ORDER BY c.color, s.size
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Red', $rows[0]['color']);
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
        $raw->query('DROP TABLE IF EXISTS mi_jcj_products');
        $raw->query('DROP TABLE IF EXISTS mi_jcj_colors');
        $raw->query('DROP TABLE IF EXISTS mi_jcj_sizes');
        $raw->close();
    }
}
