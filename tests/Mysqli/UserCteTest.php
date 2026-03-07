<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests user-written CTE (WITH) queries and INSERT ... SELECT in ZTD mode.
 */
class UserCteTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS cte_products');
        $raw->query('DROP TABLE IF EXISTS cte_products_backup');
        $raw->query('CREATE TABLE cte_products (id INT PRIMARY KEY, name VARCHAR(255), category VARCHAR(255), price DECIMAL(10,2))');
        $raw->query('CREATE TABLE cte_products_backup (id INT PRIMARY KEY, name VARCHAR(255), category VARCHAR(255), price DECIMAL(10,2))');
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

        $this->mysqli->query("INSERT INTO cte_products (id, name, category, price) VALUES (1, 'Widget A', 'gadgets', 10.00)");
        $this->mysqli->query("INSERT INTO cte_products (id, name, category, price) VALUES (2, 'Widget B', 'gadgets', 20.00)");
        $this->mysqli->query("INSERT INTO cte_products (id, name, category, price) VALUES (3, 'Gizmo X', 'tools', 30.00)");
    }

    public function testUserCteSelect(): void
    {
        $result = $this->mysqli->query("
            WITH expensive AS (
                SELECT * FROM cte_products WHERE price > 15
            )
            SELECT name, price FROM expensive ORDER BY price
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Widget B', $rows[0]['name']);
        $this->assertSame('Gizmo X', $rows[1]['name']);
    }

    public function testInsertSelectWithExplicitColumns(): void
    {
        $this->mysqli->query("INSERT INTO cte_products_backup (id, name, category, price) SELECT id, name, category, price FROM cte_products");

        $result = $this->mysqli->query('SELECT * FROM cte_products_backup ORDER BY id');
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(3, $rows);
        $this->assertSame('Widget A', $rows[0]['name']);
    }

    public function testInsertSelectStarFailsOnMysql(): void
    {
        // INSERT ... SELECT * fails on MySQL because the InsertTransformer
        // counts SELECT * as 1 column instead of expanding it.
        // This works on SQLite but not on MySQL.
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('INSERT column count');
        $this->mysqli->query("INSERT INTO cte_products_backup SELECT * FROM cte_products");
    }

    public function testInsertSelectWithWhereExplicitColumns(): void
    {
        $this->mysqli->query("INSERT INTO cte_products_backup (id, name, category, price) SELECT id, name, category, price FROM cte_products WHERE category = 'gadgets'");

        $result = $this->mysqli->query('SELECT * FROM cte_products_backup ORDER BY id');
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(2, $rows);
    }

    public function testInsertSelectIsolation(): void
    {
        $this->mysqli->query("INSERT INTO cte_products_backup (id, name, category, price) SELECT id, name, category, price FROM cte_products");

        // Physical backup table should be empty
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT * FROM cte_products_backup');
        $this->assertSame(0, $result->num_rows);
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
        $raw->query('DROP TABLE IF EXISTS cte_products_backup');
        $raw->query('DROP TABLE IF EXISTS cte_products');
        $raw->close();
    }
}
