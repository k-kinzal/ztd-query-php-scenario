<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests prepared statements with IN and NOT IN clauses on MySQLi.
 */
class PreparedInClauseTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_pic_items');
        $raw->query('CREATE TABLE mi_pic_items (id INT PRIMARY KEY, name VARCHAR(50), category VARCHAR(10), price DECIMAL(10,2))');
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

        $this->mysqli->query("INSERT INTO mi_pic_items (id, name, category, price) VALUES (1, 'Widget', 'A', 10.0)");
        $this->mysqli->query("INSERT INTO mi_pic_items (id, name, category, price) VALUES (2, 'Gadget', 'B', 25.0)");
        $this->mysqli->query("INSERT INTO mi_pic_items (id, name, category, price) VALUES (3, 'Doohickey', 'A', 5.0)");
        $this->mysqli->query("INSERT INTO mi_pic_items (id, name, category, price) VALUES (4, 'Thingamajig', 'C', 50.0)");
    }

    public function testInClauseWithParams(): void
    {
        $stmt = $this->mysqli->prepare('SELECT name FROM mi_pic_items WHERE id IN (?, ?) ORDER BY name');
        $id1 = 1;
        $id2 = 3;
        $stmt->bind_param('ii', $id1, $id2);
        $stmt->execute();
        $result = $stmt->get_result();
        $names = array_column($result->fetch_all(MYSQLI_ASSOC), 'name');
        $this->assertSame(['Doohickey', 'Widget'], $names);
    }

    public function testNotInClauseWithParams(): void
    {
        $stmt = $this->mysqli->prepare('SELECT name FROM mi_pic_items WHERE id NOT IN (?, ?) ORDER BY name');
        $id1 = 1;
        $id2 = 2;
        $stmt->bind_param('ii', $id1, $id2);
        $stmt->execute();
        $result = $stmt->get_result();
        $names = array_column($result->fetch_all(MYSQLI_ASSOC), 'name');
        $this->assertSame(['Doohickey', 'Thingamajig'], $names);
    }

    public function testInClauseWithSubquery(): void
    {
        $stmt = $this->mysqli->prepare('SELECT name FROM mi_pic_items WHERE category IN (SELECT category FROM mi_pic_items WHERE price > ?) ORDER BY name');
        $price = 20.0;
        $stmt->bind_param('d', $price);
        $stmt->execute();
        $result = $stmt->get_result();
        $names = array_column($result->fetch_all(MYSQLI_ASSOC), 'name');
        $this->assertSame(['Gadget', 'Thingamajig'], $names);
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
        $raw->query('DROP TABLE IF EXISTS mi_pic_items');
        $raw->close();
    }
}
