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
 * Tests user-written CTE queries and INSERT ... SELECT on MySQL via PDO.
 */
class MysqlUserCteTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS mysql_cte_backup');
        $raw->exec('DROP TABLE IF EXISTS mysql_cte_products');
        $raw->exec('CREATE TABLE mysql_cte_products (id INT PRIMARY KEY, name VARCHAR(255), category VARCHAR(255), price DECIMAL(10,2))');
        $raw->exec('CREATE TABLE mysql_cte_backup (id INT PRIMARY KEY, name VARCHAR(255), category VARCHAR(255), price DECIMAL(10,2))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO mysql_cte_products (id, name, category, price) VALUES (1, 'Widget A', 'gadgets', 10.00)");
        $this->pdo->exec("INSERT INTO mysql_cte_products (id, name, category, price) VALUES (2, 'Widget B', 'gadgets', 20.00)");
        $this->pdo->exec("INSERT INTO mysql_cte_products (id, name, category, price) VALUES (3, 'Gizmo X', 'tools', 30.00)");
    }

    public function testUserCteSelectReadsShadowData(): void
    {
        $stmt = $this->pdo->query("
            WITH expensive AS (
                SELECT * FROM mysql_cte_products WHERE price > 15
            )
            SELECT name, price FROM expensive ORDER BY price
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Widget B', $rows[0]['name']);
        $this->assertSame('Gizmo X', $rows[1]['name']);
    }

    public function testUserCteWithAggregation(): void
    {
        $stmt = $this->pdo->query("
            WITH category_summary AS (
                SELECT category, COUNT(*) as cnt, SUM(price) as total
                FROM mysql_cte_products
                GROUP BY category
            )
            SELECT * FROM category_summary ORDER BY category
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('gadgets', $rows[0]['category']);
        $this->assertSame(2, (int) $rows[0]['cnt']);
        $this->assertSame(30.0, (float) $rows[0]['total']);
        $this->assertSame('tools', $rows[1]['category']);
        $this->assertSame(1, (int) $rows[1]['cnt']);
    }

    public function testInsertSelectExplicitColumns(): void
    {
        $this->pdo->exec("INSERT INTO mysql_cte_backup (id, name, category, price) SELECT id, name, category, price FROM mysql_cte_products");

        $stmt = $this->pdo->query('SELECT * FROM mysql_cte_backup ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(3, $rows);
        $this->assertSame('Widget A', $rows[0]['name']);
    }

    public function testInsertSelectIsolation(): void
    {
        $this->pdo->exec("INSERT INTO mysql_cte_backup (id, name, category, price) SELECT id, name, category, price FROM mysql_cte_products");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT * FROM mysql_cte_backup');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mysql_cte_backup');
        $raw->exec('DROP TABLE IF EXISTS mysql_cte_products');
    }
}
