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
 * Tests subqueries in various SQL positions on MySQL PDO.
 */
class MysqlSubqueryPositionsTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS sp_orders');
        $raw->exec('DROP TABLE IF EXISTS sp_products');
        $raw->exec('CREATE TABLE sp_products (id INT PRIMARY KEY, name VARCHAR(50), category VARCHAR(10), price DECIMAL(10,2))');
        $raw->exec('CREATE TABLE sp_orders (id INT PRIMARY KEY, product_id INT, qty INT)');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO sp_products (id, name, category, price) VALUES (1, 'Widget', 'A', 10.00)");
        $this->pdo->exec("INSERT INTO sp_products (id, name, category, price) VALUES (2, 'Gadget', 'A', 25.00)");
        $this->pdo->exec("INSERT INTO sp_products (id, name, category, price) VALUES (3, 'Doohickey', 'B', 5.00)");
        $this->pdo->exec("INSERT INTO sp_products (id, name, category, price) VALUES (4, 'Thingamajig', 'B', 50.00)");

        $this->pdo->exec("INSERT INTO sp_orders (id, product_id, qty) VALUES (1, 1, 3)");
        $this->pdo->exec("INSERT INTO sp_orders (id, product_id, qty) VALUES (2, 2, 1)");
        $this->pdo->exec("INSERT INTO sp_orders (id, product_id, qty) VALUES (3, 1, 2)");
        $this->pdo->exec("INSERT INTO sp_orders (id, product_id, qty) VALUES (4, 3, 5)");
    }

    public function testScalarSubqueryInSelectList(): void
    {
        $stmt = $this->pdo->query("
            SELECT p.name,
                   (SELECT SUM(o.qty) FROM sp_orders o WHERE o.product_id = p.id) AS total_qty
            FROM sp_products p
            ORDER BY p.id
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(5, (int) $rows[0]['total_qty']);
        $this->assertSame(1, (int) $rows[1]['total_qty']);
        $this->assertSame(5, (int) $rows[2]['total_qty']);
        $this->assertNull($rows[3]['total_qty']);
    }

    public function testSubqueryInOrderBy(): void
    {
        $stmt = $this->pdo->query("
            SELECT p.name
            FROM sp_products p
            ORDER BY (SELECT COALESCE(SUM(o.qty), 0) FROM sp_orders o WHERE o.product_id = p.id) DESC
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $names = array_column($rows, 'name');
        $this->assertSame('Thingamajig', $names[3]);
        $this->assertSame('Gadget', $names[2]);
    }

    public function testNestedWhereWithMultipleOperators(): void
    {
        $stmt = $this->pdo->query("
            SELECT p.name FROM sp_products p
            WHERE (p.price > 8 AND p.category = 'A')
               OR (p.price < 10 AND p.id IN (SELECT o.product_id FROM sp_orders o WHERE o.qty >= 5))
            ORDER BY p.id
        ");
        $names = array_column($stmt->fetchAll(PDO::FETCH_ASSOC), 'name');
        $this->assertSame(['Widget', 'Gadget', 'Doohickey'], $names);
    }

    public function testExistsNotExistsCombined(): void
    {
        $stmt = $this->pdo->query("
            SELECT p.name FROM sp_products p
            WHERE EXISTS (SELECT 1 FROM sp_orders o WHERE o.product_id = p.id)
              AND NOT EXISTS (SELECT 1 FROM sp_orders o WHERE o.product_id = p.id AND o.qty > 4)
            ORDER BY p.id
        ");
        $names = array_column($stmt->fetchAll(PDO::FETCH_ASSOC), 'name');
        $this->assertSame(['Widget', 'Gadget'], $names);
    }

    public function testSubqueryReflectsMutations(): void
    {
        $this->pdo->exec("DELETE FROM sp_orders WHERE product_id = 1");

        $stmt = $this->pdo->query("
            SELECT (SELECT SUM(o.qty) FROM sp_orders o WHERE o.product_id = p.id) AS total_qty
            FROM sp_products p WHERE p.id = 1
        ");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertNull($row['total_qty']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS sp_orders');
        $raw->exec('DROP TABLE IF EXISTS sp_products');
    }
}
