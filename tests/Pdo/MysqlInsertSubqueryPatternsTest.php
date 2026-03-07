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
 * Tests INSERT ... SELECT subquery patterns on MySQL PDO.
 * MySQL handles computed columns and GROUP BY aggregation correctly,
 * unlike SQLite/PostgreSQL where they produce NULLs.
 */
class MysqlInsertSubqueryPatternsTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS mysql_isp_stats');
        $raw->exec('DROP TABLE IF EXISTS mysql_isp_archive');
        $raw->exec('DROP TABLE IF EXISTS mysql_isp_products');
        $raw->exec('CREATE TABLE mysql_isp_products (id INT PRIMARY KEY, name VARCHAR(50), price DECIMAL(10,2), category VARCHAR(30))');
        $raw->exec('CREATE TABLE mysql_isp_archive (id INT PRIMARY KEY, name VARCHAR(50), price DECIMAL(10,2), category VARCHAR(30))');
        $raw->exec('CREATE TABLE mysql_isp_stats (category VARCHAR(30) PRIMARY KEY, product_count INT, avg_price DECIMAL(10,2))');
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

    public function testInsertSelectWithWhereFilter(): void
    {
        $this->pdo->exec("INSERT INTO mysql_isp_products VALUES (1, 'Widget', 29.99, 'electronics')");
        $this->pdo->exec("INSERT INTO mysql_isp_products VALUES (2, 'Gadget', 49.99, 'electronics')");
        $this->pdo->exec("INSERT INTO mysql_isp_products VALUES (3, 'Toy', 9.99, 'toys')");

        $affected = $this->pdo->exec("INSERT INTO mysql_isp_archive (id, name, price, category) SELECT id, name, price, category FROM mysql_isp_products WHERE category = 'electronics'");
        $this->assertSame(2, $affected);

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mysql_isp_archive");
        $this->assertSame(2, (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt']);
    }

    /**
     * Unlike SQLite/PostgreSQL, MySQL correctly transfers computed expressions.
     */
    public function testInsertSelectWithComputedColumnsWorks(): void
    {
        $this->pdo->exec("INSERT INTO mysql_isp_products VALUES (1, 'Widget', 29.99, 'electronics')");

        $this->pdo->exec("INSERT INTO mysql_isp_archive (id, name, price, category) SELECT id, name, price * 2, category FROM mysql_isp_products");

        $stmt = $this->pdo->query("SELECT price FROM mysql_isp_archive WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(59.98, (float) $row['price'], 0.01);
    }

    /**
     * Unlike SQLite/PostgreSQL, MySQL correctly transfers GROUP BY aggregations.
     */
    public function testInsertSelectWithGroupByAggregationWorks(): void
    {
        $this->pdo->exec("INSERT INTO mysql_isp_products VALUES (1, 'Widget', 29.99, 'electronics')");
        $this->pdo->exec("INSERT INTO mysql_isp_products VALUES (2, 'Gadget', 49.99, 'electronics')");
        $this->pdo->exec("INSERT INTO mysql_isp_products VALUES (3, 'Toy', 9.99, 'toys')");

        $affected = $this->pdo->exec("
            INSERT INTO mysql_isp_stats (category, product_count, avg_price)
            SELECT category, COUNT(*), AVG(price)
            FROM mysql_isp_products
            GROUP BY category
        ");
        $this->assertSame(2, $affected);

        $stmt = $this->pdo->query("SELECT * FROM mysql_isp_stats ORDER BY category");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('electronics', $rows[0]['category']);
        $this->assertSame(2, (int) $rows[0]['product_count']);
        $this->assertEqualsWithDelta(39.99, (float) $rows[0]['avg_price'], 0.01);
    }

    public function testInsertSelectWhereNotExists(): void
    {
        $this->pdo->exec("INSERT INTO mysql_isp_products VALUES (1, 'Widget', 29.99, 'electronics')");
        $this->pdo->exec("INSERT INTO mysql_isp_archive VALUES (1, 'Already Archived', 29.99, 'electronics')");

        $affected = $this->pdo->exec("
            INSERT INTO mysql_isp_archive (id, name, price, category)
            SELECT id, name, price, category
            FROM mysql_isp_products p
            WHERE NOT EXISTS (SELECT 1 FROM mysql_isp_archive a WHERE a.id = p.id)
        ");
        $this->assertSame(0, $affected);
    }

    public function testInsertSelectReflectsMutations(): void
    {
        $this->pdo->exec("INSERT INTO mysql_isp_products VALUES (1, 'Widget', 29.99, 'electronics')");
        $this->pdo->exec("INSERT INTO mysql_isp_products VALUES (2, 'Gadget', 49.99, 'electronics')");

        $this->pdo->exec("UPDATE mysql_isp_products SET price = 99.99 WHERE id = 1");
        $this->pdo->exec("DELETE FROM mysql_isp_products WHERE id = 2");

        $affected = $this->pdo->exec("INSERT INTO mysql_isp_archive (id, name, price, category) SELECT id, name, price, category FROM mysql_isp_products");
        $this->assertSame(1, $affected);

        $stmt = $this->pdo->query("SELECT price FROM mysql_isp_archive WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(99.99, (float) $row['price'], 0.01);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mysql_isp_stats');
        $raw->exec('DROP TABLE IF EXISTS mysql_isp_archive');
        $raw->exec('DROP TABLE IF EXISTS mysql_isp_products');
    }
}
