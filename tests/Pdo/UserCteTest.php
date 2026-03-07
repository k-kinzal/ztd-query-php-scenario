<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests user-written CTE (WITH) queries in ZTD mode.
 * Since ZTD uses CTE shadowing internally, user CTEs could
 * potentially conflict with the rewriting mechanism.
 */
class UserCteTest extends TestCase
{
    private PDO $raw;
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $this->raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $this->raw->exec('CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, category TEXT, price REAL)');

        $this->pdo = ZtdPdo::fromPdo($this->raw);

        $this->pdo->exec("INSERT INTO products (id, name, category, price) VALUES (1, 'Widget A', 'gadgets', 10.00)");
        $this->pdo->exec("INSERT INTO products (id, name, category, price) VALUES (2, 'Widget B', 'gadgets', 20.00)");
        $this->pdo->exec("INSERT INTO products (id, name, category, price) VALUES (3, 'Gizmo X', 'tools', 30.00)");
        $this->pdo->exec("INSERT INTO products (id, name, category, price) VALUES (4, 'Gizmo Y', 'tools', 40.00)");
    }

    public function testUserCteSelect(): void
    {
        $stmt = $this->pdo->query("
            WITH expensive AS (
                SELECT * FROM products WHERE price > 15
            )
            SELECT name, price FROM expensive ORDER BY price
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(3, $rows);
        $this->assertSame('Widget B', $rows[0]['name']);
        $this->assertSame('Gizmo X', $rows[1]['name']);
        $this->assertSame('Gizmo Y', $rows[2]['name']);
    }

    public function testUserCteWithAggregation(): void
    {
        $stmt = $this->pdo->query("
            WITH category_stats AS (
                SELECT category, COUNT(*) as cnt, SUM(price) as total
                FROM products
                GROUP BY category
            )
            SELECT category, cnt, total FROM category_stats ORDER BY category
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('gadgets', $rows[0]['category']);
        $this->assertSame(2, (int) $rows[0]['cnt']);
        $this->assertSame('tools', $rows[1]['category']);
        $this->assertSame(2, (int) $rows[1]['cnt']);
    }

    public function testInsertSelect(): void
    {
        $this->raw->exec('CREATE TABLE products_backup (id INTEGER PRIMARY KEY, name TEXT, category TEXT, price REAL)');
        // Re-wrap to pick up new table
        $this->pdo = ZtdPdo::fromPdo($this->raw);

        // Re-insert data since new session
        $this->pdo->exec("INSERT INTO products (id, name, category, price) VALUES (1, 'Widget A', 'gadgets', 10.00)");
        $this->pdo->exec("INSERT INTO products (id, name, category, price) VALUES (2, 'Widget B', 'gadgets', 20.00)");

        // INSERT ... SELECT from shadow data into another table
        $this->pdo->exec("INSERT INTO products_backup SELECT * FROM products");

        $stmt = $this->pdo->query('SELECT * FROM products_backup ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Widget A', $rows[0]['name']);
        $this->assertSame('Widget B', $rows[1]['name']);
    }

    public function testInsertSelectIsolation(): void
    {
        $this->raw->exec('CREATE TABLE products_copy (id INTEGER PRIMARY KEY, name TEXT, category TEXT, price REAL)');
        $this->pdo = ZtdPdo::fromPdo($this->raw);

        $this->pdo->exec("INSERT INTO products (id, name, category, price) VALUES (1, 'Widget A', 'gadgets', 10.00)");
        $this->pdo->exec("INSERT INTO products_copy SELECT * FROM products");

        // Physical products_copy should be empty
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT * FROM products_copy');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));
    }
}
