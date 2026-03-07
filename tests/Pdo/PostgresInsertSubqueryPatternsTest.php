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
 * Tests INSERT ... SELECT subquery patterns on PostgreSQL PDO.
 * Like SQLite, computed/aggregated columns produce NULLs (unlike MySQL which works).
 */
class PostgresInsertSubqueryPatternsTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS pg_isp_stats');
        $raw->exec('DROP TABLE IF EXISTS pg_isp_archive');
        $raw->exec('DROP TABLE IF EXISTS pg_isp_products');
        $raw->exec('CREATE TABLE pg_isp_products (id INT PRIMARY KEY, name VARCHAR(50), price NUMERIC(10,2), category VARCHAR(30))');
        $raw->exec('CREATE TABLE pg_isp_archive (id INT PRIMARY KEY, name VARCHAR(50), price NUMERIC(10,2), category VARCHAR(30))');
        $raw->exec('CREATE TABLE pg_isp_stats (category VARCHAR(30) PRIMARY KEY, product_count INT, avg_price NUMERIC(10,2))');
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

    public function testInsertSelectWithWhereFilter(): void
    {
        $this->pdo->exec("INSERT INTO pg_isp_products VALUES (1, 'Widget', 29.99, 'electronics')");
        $this->pdo->exec("INSERT INTO pg_isp_products VALUES (2, 'Gadget', 49.99, 'electronics')");
        $this->pdo->exec("INSERT INTO pg_isp_products VALUES (3, 'Toy', 9.99, 'toys')");

        $affected = $this->pdo->exec("INSERT INTO pg_isp_archive (id, name, price, category) SELECT id, name, price, category FROM pg_isp_products WHERE category = 'electronics'");
        $this->assertSame(2, $affected);
    }

    /**
     * Like SQLite, computed columns produce NULL on PostgreSQL.
     */
    public function testInsertSelectWithComputedColumnsProducesNull(): void
    {
        $this->pdo->exec("INSERT INTO pg_isp_products VALUES (1, 'Widget', 29.99, 'electronics')");

        $affected = $this->pdo->exec("INSERT INTO pg_isp_archive (id, name, price, category) SELECT id, name, price * 2, category FROM pg_isp_products");
        $this->assertSame(1, $affected);

        $stmt = $this->pdo->query("SELECT price FROM pg_isp_archive WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertNull($row['price']);
    }

    /**
     * Like SQLite, GROUP BY + aggregation produces NULL on PostgreSQL.
     */
    public function testInsertSelectWithGroupByAggregationProducesNull(): void
    {
        $this->pdo->exec("INSERT INTO pg_isp_products VALUES (1, 'Widget', 29.99, 'electronics')");
        $this->pdo->exec("INSERT INTO pg_isp_products VALUES (2, 'Gadget', 49.99, 'electronics')");
        $this->pdo->exec("INSERT INTO pg_isp_products VALUES (3, 'Toy', 9.99, 'toys')");

        $affected = $this->pdo->exec("
            INSERT INTO pg_isp_stats (category, product_count, avg_price)
            SELECT category, COUNT(*), AVG(price)
            FROM pg_isp_products
            GROUP BY category
        ");
        $this->assertSame(2, $affected);

        $stmt = $this->pdo->query("SELECT * FROM pg_isp_stats ORDER BY category");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('electronics', $rows[0]['category']);
        $this->assertNull($rows[0]['product_count']);
    }

    public function testInsertSelectWhereNotExists(): void
    {
        $this->pdo->exec("INSERT INTO pg_isp_products VALUES (1, 'Widget', 29.99, 'electronics')");
        $this->pdo->exec("INSERT INTO pg_isp_archive VALUES (1, 'Already Archived', 29.99, 'electronics')");

        $affected = $this->pdo->exec("
            INSERT INTO pg_isp_archive (id, name, price, category)
            SELECT id, name, price, category
            FROM pg_isp_products p
            WHERE NOT EXISTS (SELECT 1 FROM pg_isp_archive a WHERE a.id = p.id)
        ");
        $this->assertSame(0, $affected);
    }

    public function testInsertSelectReflectsMutations(): void
    {
        $this->pdo->exec("INSERT INTO pg_isp_products VALUES (1, 'Widget', 29.99, 'electronics')");
        $this->pdo->exec("INSERT INTO pg_isp_products VALUES (2, 'Gadget', 49.99, 'electronics')");

        $this->pdo->exec("UPDATE pg_isp_products SET price = 99.99 WHERE id = 1");
        $this->pdo->exec("DELETE FROM pg_isp_products WHERE id = 2");

        $affected = $this->pdo->exec("INSERT INTO pg_isp_archive (id, name, price, category) SELECT id, name, price, category FROM pg_isp_products");
        $this->assertSame(1, $affected);

        $stmt = $this->pdo->query("SELECT price FROM pg_isp_archive WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(99.99, (float) $row['price'], 0.01);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_isp_stats');
        $raw->exec('DROP TABLE IF EXISTS pg_isp_archive');
        $raw->exec('DROP TABLE IF EXISTS pg_isp_products');
    }
}
