<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests WHERE clause operators: LIKE, NOT LIKE, BETWEEN, NOT BETWEEN,
 * EXISTS, NOT EXISTS, IN with subquery, comparison operators — all after mutations.
 */
class SqliteWhereClauseOperatorsTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, category TEXT, price REAL, in_stock INTEGER)');
        $raw->exec('CREATE TABLE orders (id INTEGER PRIMARY KEY, product_id INTEGER, customer TEXT, qty INTEGER)');

        $this->pdo = ZtdPdo::fromPdo($raw);

        $this->pdo->exec("INSERT INTO products VALUES (1, 'Widget Alpha', 'electronics', 29.99, 1)");
        $this->pdo->exec("INSERT INTO products VALUES (2, 'Widget Beta', 'electronics', 49.99, 1)");
        $this->pdo->exec("INSERT INTO products VALUES (3, 'Gadget Pro', 'accessories', 15.00, 0)");
        $this->pdo->exec("INSERT INTO products VALUES (4, 'Super Tool', 'tools', 99.99, 1)");
        $this->pdo->exec("INSERT INTO products VALUES (5, 'Mini Tool', 'tools', 9.99, 1)");

        $this->pdo->exec("INSERT INTO orders VALUES (1, 1, 'Alice', 2)");
        $this->pdo->exec("INSERT INTO orders VALUES (2, 2, 'Bob', 1)");
        $this->pdo->exec("INSERT INTO orders VALUES (3, 4, 'Alice', 3)");
    }

    public function testLikeWithPercentWildcard(): void
    {
        $stmt = $this->pdo->query("SELECT name FROM products WHERE name LIKE 'Widget%' ORDER BY name");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Widget Alpha', $rows[0]['name']);
        $this->assertSame('Widget Beta', $rows[1]['name']);
    }

    public function testLikeWithUnderscoreWildcard(): void
    {
        // _ matches exactly one character: "Mini Tool" = 9 chars, "Super Tool" = 10 chars
        $stmt = $this->pdo->query("SELECT name FROM products WHERE name LIKE '_ini Tool' ORDER BY name");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Mini Tool', $rows[0]['name']);
    }

    public function testNotLike(): void
    {
        $stmt = $this->pdo->query("SELECT name FROM products WHERE name NOT LIKE '%Tool%' ORDER BY name");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
    }

    public function testLikeAfterUpdate(): void
    {
        $this->pdo->exec("UPDATE products SET name = 'Widget Gamma' WHERE id = 3");

        $stmt = $this->pdo->query("SELECT name FROM products WHERE name LIKE 'Widget%' ORDER BY name");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('Widget Gamma', $rows[2]['name']);
    }

    public function testLikeAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO products VALUES (6, 'Widget Delta', 'electronics', 39.99, 1)");

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM products WHERE name LIKE 'Widget%'");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(3, (int) $row['cnt']);
    }

    public function testBetween(): void
    {
        $stmt = $this->pdo->query("SELECT name FROM products WHERE price BETWEEN 10 AND 50 ORDER BY price");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('Gadget Pro', $rows[0]['name']);
    }

    public function testNotBetween(): void
    {
        $stmt = $this->pdo->query("SELECT name FROM products WHERE price NOT BETWEEN 10 AND 50 ORDER BY price");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Mini Tool', $rows[0]['name']);
        $this->assertSame('Super Tool', $rows[1]['name']);
    }

    public function testBetweenAfterUpdate(): void
    {
        $this->pdo->exec("UPDATE products SET price = 25.00 WHERE id = 5"); // Mini Tool: 9.99 -> 25.00

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM products WHERE price BETWEEN 10 AND 50");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(4, (int) $row['cnt']); // Was 3, now Mini Tool also qualifies
    }

    public function testExistsSubquery(): void
    {
        $stmt = $this->pdo->query("
            SELECT p.name FROM products p
            WHERE EXISTS (SELECT 1 FROM orders o WHERE o.product_id = p.id)
            ORDER BY p.name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('Super Tool', $rows[0]['name']);
        $this->assertSame('Widget Alpha', $rows[1]['name']);
        $this->assertSame('Widget Beta', $rows[2]['name']);
    }

    public function testNotExistsSubquery(): void
    {
        $stmt = $this->pdo->query("
            SELECT p.name FROM products p
            WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.product_id = p.id)
            ORDER BY p.name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Gadget Pro', $rows[0]['name']);
        $this->assertSame('Mini Tool', $rows[1]['name']);
    }

    public function testExistsAfterInsertingOrder(): void
    {
        $this->pdo->exec("INSERT INTO orders VALUES (4, 3, 'Charlie', 1)"); // Order for Gadget Pro

        $stmt = $this->pdo->query("
            SELECT p.name FROM products p
            WHERE EXISTS (SELECT 1 FROM orders o WHERE o.product_id = p.id)
            ORDER BY p.name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(4, $rows); // Now includes Gadget Pro
    }

    public function testNotExistsAfterDeletingOrders(): void
    {
        $this->pdo->exec("DELETE FROM orders WHERE product_id = 1");

        $stmt = $this->pdo->query("
            SELECT p.name FROM products p
            WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.product_id = p.id)
            ORDER BY p.name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows); // Widget Alpha now has no orders
    }

    public function testInWithSubquery(): void
    {
        $stmt = $this->pdo->query("
            SELECT name FROM products
            WHERE id IN (SELECT product_id FROM orders WHERE customer = 'Alice')
            ORDER BY name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Super Tool', $rows[0]['name']);
        $this->assertSame('Widget Alpha', $rows[1]['name']);
    }

    public function testComparisonOperatorsAfterMutation(): void
    {
        $this->pdo->exec("UPDATE products SET price = 150.00 WHERE id = 1"); // Widget Alpha: 29.99 -> 150.00

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM products WHERE price > 100");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(1, (int) $row['cnt']); // Only Widget Alpha at 150

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM products WHERE price >= 99.99");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(2, (int) $row['cnt']); // Widget Alpha + Super Tool

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM products WHERE price < 10");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(1, (int) $row['cnt']); // Mini Tool

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM products WHERE price <> 49.99");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(4, (int) $row['cnt']);
    }

    public function testPreparedLikeWithParameter(): void
    {
        $stmt = $this->pdo->prepare("SELECT name FROM products WHERE name LIKE ? ORDER BY name");
        $stmt->execute(['%Tool%']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Mini Tool', $rows[0]['name']);
        $this->assertSame('Super Tool', $rows[1]['name']);
    }

    public function testPreparedBetweenWithParameters(): void
    {
        $stmt = $this->pdo->prepare("SELECT name FROM products WHERE price BETWEEN ? AND ? ORDER BY price");
        $stmt->execute([20, 60]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Widget Alpha', $rows[0]['name']);
        $this->assertSame('Widget Beta', $rows[1]['name']);
    }
}
