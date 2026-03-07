<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests column aliasing patterns on MySQLi.
 */
class ColumnAliasingTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_ca_items');
        $raw->query('CREATE TABLE mi_ca_items (id INT PRIMARY KEY, name VARCHAR(50), price DECIMAL(10,2), qty INT, category VARCHAR(10))');
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

        $this->mysqli->query("INSERT INTO mi_ca_items (id, name, price, qty, category) VALUES (1, 'Widget', 10.50, 100, 'A')");
        $this->mysqli->query("INSERT INTO mi_ca_items (id, name, price, qty, category) VALUES (2, 'Gadget', 25.00, 50, 'A')");
        $this->mysqli->query("INSERT INTO mi_ca_items (id, name, price, qty, category) VALUES (3, 'Doohickey', 5.75, 200, 'B')");
    }

    public function testExpressionAlias(): void
    {
        $result = $this->mysqli->query("SELECT name, price * qty AS total_value FROM mi_ca_items ORDER BY total_value DESC");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertSame('Gadget', $rows[0]['name']);
        $this->assertEqualsWithDelta(1250.0, (float) $rows[0]['total_value'], 0.01);
    }

    public function testCaseExpressionAlias(): void
    {
        $result = $this->mysqli->query("
            SELECT name,
                   CASE WHEN price > 20 THEN 'expensive' WHEN price > 8 THEN 'moderate' ELSE 'cheap' END AS tier
            FROM mi_ca_items ORDER BY id
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertSame('moderate', $rows[0]['tier']);
        $this->assertSame('expensive', $rows[1]['tier']);
        $this->assertSame('cheap', $rows[2]['tier']);
    }

    public function testCoalesceAlias(): void
    {
        $this->mysqli->query("INSERT INTO mi_ca_items (id, name, price, qty, category) VALUES (4, 'Unknown', 0, 0, NULL)");

        $result = $this->mysqli->query("SELECT name, COALESCE(category, 'Uncategorized') AS display_cat FROM mi_ca_items ORDER BY id");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertSame('A', $rows[0]['display_cat']);
        $this->assertSame('Uncategorized', $rows[3]['display_cat']);
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
        $raw->query('DROP TABLE IF EXISTS mi_ca_items');
        $raw->close();
    }
}
