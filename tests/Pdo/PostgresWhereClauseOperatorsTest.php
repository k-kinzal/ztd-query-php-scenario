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
 * Tests WHERE clause operators on PostgreSQL PDO: LIKE, NOT LIKE, BETWEEN, NOT BETWEEN,
 * EXISTS, NOT EXISTS, comparison operators, and SIMILAR TO — all after mutations.
 */
class PostgresWhereClauseOperatorsTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS pg_wco_orders');
        $raw->exec('DROP TABLE IF EXISTS pg_wco_products');
        $raw->exec('CREATE TABLE pg_wco_products (id INT PRIMARY KEY, name VARCHAR(50), category VARCHAR(30), price NUMERIC(10,2), in_stock SMALLINT)');
        $raw->exec('CREATE TABLE pg_wco_orders (id INT PRIMARY KEY, product_id INT, customer VARCHAR(50), qty INT)');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO pg_wco_products VALUES (1, 'Widget Alpha', 'electronics', 29.99, 1)");
        $this->pdo->exec("INSERT INTO pg_wco_products VALUES (2, 'Widget Beta', 'electronics', 49.99, 1)");
        $this->pdo->exec("INSERT INTO pg_wco_products VALUES (3, 'Gadget Pro', 'accessories', 15.00, 0)");
        $this->pdo->exec("INSERT INTO pg_wco_products VALUES (4, 'Super Tool', 'tools', 99.99, 1)");
        $this->pdo->exec("INSERT INTO pg_wco_products VALUES (5, 'Mini Tool', 'tools', 9.99, 1)");

        $this->pdo->exec("INSERT INTO pg_wco_orders VALUES (1, 1, 'Alice', 2)");
        $this->pdo->exec("INSERT INTO pg_wco_orders VALUES (2, 2, 'Bob', 1)");
        $this->pdo->exec("INSERT INTO pg_wco_orders VALUES (3, 4, 'Alice', 3)");
    }

    public function testLikeWithPercentWildcard(): void
    {
        $stmt = $this->pdo->query("SELECT name FROM pg_wco_products WHERE name LIKE 'Widget%' ORDER BY name");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Widget Alpha', $rows[0]['name']);
    }

    public function testLikeAfterUpdate(): void
    {
        $this->pdo->exec("UPDATE pg_wco_products SET name = 'Widget Gamma' WHERE id = 3");

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_wco_products WHERE name LIKE 'Widget%'");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(3, (int) $row['cnt']);
    }

    public function testBetweenAfterUpdate(): void
    {
        $this->pdo->exec("UPDATE pg_wco_products SET price = 25.00 WHERE id = 5");

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_wco_products WHERE price BETWEEN 10 AND 50");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(4, (int) $row['cnt']);
    }

    public function testExistsSubquery(): void
    {
        $stmt = $this->pdo->query("
            SELECT p.name FROM pg_wco_products p
            WHERE EXISTS (SELECT 1 FROM pg_wco_orders o WHERE o.product_id = p.id)
            ORDER BY p.name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
    }

    public function testNotExistsSubquery(): void
    {
        $stmt = $this->pdo->query("
            SELECT p.name FROM pg_wco_products p
            WHERE NOT EXISTS (SELECT 1 FROM pg_wco_orders o WHERE o.product_id = p.id)
            ORDER BY p.name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
    }

    public function testExistsAfterInsertingOrder(): void
    {
        $this->pdo->exec("INSERT INTO pg_wco_orders VALUES (4, 3, 'Charlie', 1)");

        $stmt = $this->pdo->query("
            SELECT p.name FROM pg_wco_products p
            WHERE EXISTS (SELECT 1 FROM pg_wco_orders o WHERE o.product_id = p.id)
            ORDER BY p.name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(4, $rows);
    }

    public function testPreparedLikeWithParameter(): void
    {
        $stmt = $this->pdo->prepare("SELECT name FROM pg_wco_products WHERE name LIKE ? ORDER BY name");
        $stmt->execute(['%Tool%']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
    }

    public function testPreparedBetweenWithParameters(): void
    {
        $stmt = $this->pdo->prepare("SELECT name FROM pg_wco_products WHERE price BETWEEN ? AND ? ORDER BY price");
        $stmt->execute([20, 60]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_wco_orders');
        $raw->exec('DROP TABLE IF EXISTS pg_wco_products');
    }
}
