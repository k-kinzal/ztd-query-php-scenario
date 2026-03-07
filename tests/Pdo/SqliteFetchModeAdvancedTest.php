<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests advanced PDO fetch modes (FETCH_GROUP, FETCH_UNIQUE, FETCH_COLUMN|FETCH_GROUP)
 * with ZTD shadow store on SQLite.
 */
class SqliteFetchModeAdvancedTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);

        $this->pdo->exec('CREATE TABLE orders (id INT PRIMARY KEY, customer VARCHAR(50), product VARCHAR(50), amount INT)');
        $this->pdo->exec("INSERT INTO orders VALUES (1, 'Alice', 'Widget', 100)");
        $this->pdo->exec("INSERT INTO orders VALUES (2, 'Alice', 'Gadget', 200)");
        $this->pdo->exec("INSERT INTO orders VALUES (3, 'Bob', 'Widget', 150)");
        $this->pdo->exec("INSERT INTO orders VALUES (4, 'Charlie', 'Gizmo', 300)");
        $this->pdo->exec("INSERT INTO orders VALUES (5, 'Alice', 'Gizmo', 50)");
    }

    public function testFetchGroupByCustomer(): void
    {
        $stmt = $this->pdo->query('SELECT customer, product, amount FROM orders ORDER BY id');
        $groups = $stmt->fetchAll(PDO::FETCH_GROUP | PDO::FETCH_ASSOC);

        $this->assertArrayHasKey('Alice', $groups);
        $this->assertArrayHasKey('Bob', $groups);
        $this->assertArrayHasKey('Charlie', $groups);
        $this->assertCount(3, $groups['Alice']);
        $this->assertCount(1, $groups['Bob']);
    }

    public function testFetchUniqueById(): void
    {
        $stmt = $this->pdo->query('SELECT id, customer, product FROM orders ORDER BY id');
        $unique = $stmt->fetchAll(PDO::FETCH_UNIQUE | PDO::FETCH_ASSOC);

        $this->assertArrayHasKey(1, $unique);
        $this->assertArrayHasKey(5, $unique);
        $this->assertSame('Alice', $unique[1]['customer']);
        $this->assertSame('Gizmo', $unique[5]['product']);
    }

    public function testFetchColumnGroup(): void
    {
        $stmt = $this->pdo->query('SELECT customer, product FROM orders ORDER BY id');
        $groups = $stmt->fetchAll(PDO::FETCH_GROUP | PDO::FETCH_COLUMN);

        $this->assertArrayHasKey('Alice', $groups);
        $this->assertSame(['Widget', 'Gadget', 'Gizmo'], $groups['Alice']);
        $this->assertSame(['Widget'], $groups['Bob']);
    }

    public function testFetchNumericMode(): void
    {
        $stmt = $this->pdo->query('SELECT customer, product FROM orders WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_NUM);
        $this->assertSame('Alice', $row[0]);
        $this->assertSame('Widget', $row[1]);
    }

    public function testFetchBothMode(): void
    {
        $stmt = $this->pdo->query('SELECT customer, product FROM orders WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_BOTH);
        // Both numeric and associative keys
        $this->assertSame('Alice', $row[0]);
        $this->assertSame('Alice', $row['customer']);
        $this->assertSame('Widget', $row[1]);
        $this->assertSame('Widget', $row['product']);
    }

    public function testFetchAllNumMode(): void
    {
        $stmt = $this->pdo->query('SELECT product FROM orders ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_NUM);
        $this->assertCount(5, $rows);
        $this->assertSame('Widget', $rows[0][0]);
    }

    public function testFetchObjectWithClassName(): void
    {
        $stmt = $this->pdo->query('SELECT customer, product, amount FROM orders WHERE id = 1');
        $obj = $stmt->fetchObject();
        $this->assertSame('Alice', $obj->customer);
        $this->assertSame('Widget', $obj->product);
        $this->assertSame(100, (int) $obj->amount);
    }

    public function testFetchAllObjects(): void
    {
        $stmt = $this->pdo->query('SELECT customer, product FROM orders ORDER BY id');
        $objects = $stmt->fetchAll(PDO::FETCH_OBJ);
        $this->assertCount(5, $objects);
        $this->assertSame('Alice', $objects[0]->customer);
    }
}
