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
 * Tests advanced PDO fetch modes (FETCH_GROUP, FETCH_UNIQUE, FETCH_COLUMN|FETCH_GROUP)
 * with ZTD shadow store on PostgreSQL.
 */
class PostgresFetchModeAdvancedTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS orders_pg');
        $raw->exec('CREATE TABLE orders_pg (id INT PRIMARY KEY, customer VARCHAR(50), product VARCHAR(50), amount INT)');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec('DELETE FROM orders_pg');
        $this->pdo->exec("INSERT INTO orders_pg VALUES (1, 'Alice', 'Widget', 100)");
        $this->pdo->exec("INSERT INTO orders_pg VALUES (2, 'Alice', 'Gadget', 200)");
        $this->pdo->exec("INSERT INTO orders_pg VALUES (3, 'Bob', 'Widget', 150)");
        $this->pdo->exec("INSERT INTO orders_pg VALUES (4, 'Charlie', 'Gizmo', 300)");
        $this->pdo->exec("INSERT INTO orders_pg VALUES (5, 'Alice', 'Gizmo', 50)");
    }

    public function testFetchGroupByCustomer(): void
    {
        $stmt = $this->pdo->query('SELECT customer, product, amount FROM orders_pg ORDER BY id');
        $groups = $stmt->fetchAll(PDO::FETCH_GROUP | PDO::FETCH_ASSOC);

        $this->assertArrayHasKey('Alice', $groups);
        $this->assertArrayHasKey('Bob', $groups);
        $this->assertArrayHasKey('Charlie', $groups);
        $this->assertCount(3, $groups['Alice']);
        $this->assertCount(1, $groups['Bob']);
    }

    public function testFetchUniqueById(): void
    {
        $stmt = $this->pdo->query('SELECT id, customer, product FROM orders_pg ORDER BY id');
        $unique = $stmt->fetchAll(PDO::FETCH_UNIQUE | PDO::FETCH_ASSOC);

        $this->assertArrayHasKey(1, $unique);
        $this->assertArrayHasKey(5, $unique);
        $this->assertSame('Alice', $unique[1]['customer']);
        $this->assertSame('Gizmo', $unique[5]['product']);
    }

    public function testFetchColumnGroup(): void
    {
        $stmt = $this->pdo->query('SELECT customer, product FROM orders_pg ORDER BY id');
        $groups = $stmt->fetchAll(PDO::FETCH_GROUP | PDO::FETCH_COLUMN);

        $this->assertArrayHasKey('Alice', $groups);
        $this->assertSame(['Widget', 'Gadget', 'Gizmo'], $groups['Alice']);
        $this->assertSame(['Widget'], $groups['Bob']);
    }

    public function testFetchNumericMode(): void
    {
        $stmt = $this->pdo->query('SELECT customer, product FROM orders_pg WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_NUM);
        $this->assertSame('Alice', $row[0]);
        $this->assertSame('Widget', $row[1]);
    }

    public function testFetchBothMode(): void
    {
        $stmt = $this->pdo->query('SELECT customer, product FROM orders_pg WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_BOTH);
        // Both numeric and associative keys
        $this->assertSame('Alice', $row[0]);
        $this->assertSame('Alice', $row['customer']);
        $this->assertSame('Widget', $row[1]);
        $this->assertSame('Widget', $row['product']);
    }

    public function testFetchAllNumMode(): void
    {
        $stmt = $this->pdo->query('SELECT product FROM orders_pg ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_NUM);
        $this->assertCount(5, $rows);
        $this->assertSame('Widget', $rows[0][0]);
    }

    public function testFetchObjectWithClassName(): void
    {
        $stmt = $this->pdo->query('SELECT customer, product, amount FROM orders_pg WHERE id = 1');
        $obj = $stmt->fetchObject();
        $this->assertSame('Alice', $obj->customer);
        $this->assertSame('Widget', $obj->product);
        $this->assertSame(100, (int) $obj->amount);
    }

    public function testFetchAllObjects(): void
    {
        $stmt = $this->pdo->query('SELECT customer, product FROM orders_pg ORDER BY id');
        $objects = $stmt->fetchAll(PDO::FETCH_OBJ);
        $this->assertCount(5, $objects);
        $this->assertSame('Alice', $objects[0]->customer);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS orders_pg');
    }
}
