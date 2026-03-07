<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests parameterized LIMIT/OFFSET, expression-based GROUP BY,
 * and INSERT...SELECT with filtering on MySQLi.
 */
class PaginationAndGroupingTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_pg_archive');
        $raw->query('DROP TABLE IF EXISTS mi_pg_products');
        $raw->query('CREATE TABLE mi_pg_products (id INT PRIMARY KEY, name VARCHAR(255), category VARCHAR(50), price DECIMAL(10,2), stock INT)');
        $raw->query('CREATE TABLE mi_pg_archive (id INT PRIMARY KEY, name VARCHAR(255), category VARCHAR(50), price DECIMAL(10,2), stock INT)');
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

        $this->mysqli->query("INSERT INTO mi_pg_products VALUES (1, 'Widget A', 'hardware', 9.99, 50)");
        $this->mysqli->query("INSERT INTO mi_pg_products VALUES (2, 'Widget B', 'hardware', 14.99, 30)");
        $this->mysqli->query("INSERT INTO mi_pg_products VALUES (3, 'Gadget X', 'electronics', 29.99, 10)");
        $this->mysqli->query("INSERT INTO mi_pg_products VALUES (4, 'Gadget Y', 'electronics', 49.99, 5)");
        $this->mysqli->query("INSERT INTO mi_pg_products VALUES (5, 'Gizmo', 'electronics', 19.99, 20)");
    }

    public function testPreparedLimitOffset(): void
    {
        $stmt = $this->mysqli->prepare("SELECT name FROM mi_pg_products ORDER BY id LIMIT ? OFFSET ?");
        $stmt->bind_param('ii', $limit, $offset);
        $limit = 2;
        $offset = 0;
        $stmt->execute();
        $result = $stmt->get_result();
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Widget A', $rows[0]['name']);

        $offset = 2;
        $stmt->execute();
        $result = $stmt->get_result();
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Gadget X', $rows[0]['name']);
    }

    public function testGroupByCase(): void
    {
        $result = $this->mysqli->query("
            SELECT
                CASE WHEN price < 20 THEN 'budget' WHEN price < 40 THEN 'mid' ELSE 'premium' END AS tier,
                COUNT(*) AS cnt
            FROM mi_pg_products
            GROUP BY CASE WHEN price < 20 THEN 'budget' WHEN price < 40 THEN 'mid' ELSE 'premium' END
            ORDER BY tier
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('budget', $rows[0]['tier']);
    }

    public function testInsertSelectWithWhere(): void
    {
        $this->mysqli->query("INSERT INTO mi_pg_archive (id, name, category, price, stock) SELECT id, name, category, price, stock FROM mi_pg_products WHERE stock <= 10");

        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_pg_archive");
        $row = $result->fetch_assoc();
        $this->assertSame(2, (int) $row['cnt']);
    }

    public function testPaginationAfterDelete(): void
    {
        $this->mysqli->query("DELETE FROM mi_pg_products WHERE id = 2");

        $stmt = $this->mysqli->prepare("SELECT name FROM mi_pg_products ORDER BY id LIMIT ? OFFSET ?");
        $stmt->bind_param('ii', $limit, $offset);
        $limit = 2;
        $offset = 0;
        $stmt->execute();
        $result = $stmt->get_result();
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Widget A', $rows[0]['name']);
        $this->assertSame('Gadget X', $rows[1]['name']);
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
        $raw->query('DROP TABLE IF EXISTS mi_pg_archive');
        $raw->query('DROP TABLE IF EXISTS mi_pg_products');
        $raw->close();
    }
}
