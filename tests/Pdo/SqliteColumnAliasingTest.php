<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests column aliasing patterns (AS in SELECT) and expression aliasing
 * through CTE rewriting.
 */
class SqliteColumnAliasingTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE ca_items (id INTEGER PRIMARY KEY, name TEXT, price REAL, qty INTEGER, category TEXT)');

        $this->pdo = ZtdPdo::fromPdo($raw);

        $this->pdo->exec("INSERT INTO ca_items (id, name, price, qty, category) VALUES (1, 'Widget', 10.50, 100, 'A')");
        $this->pdo->exec("INSERT INTO ca_items (id, name, price, qty, category) VALUES (2, 'Gadget', 25.00, 50, 'A')");
        $this->pdo->exec("INSERT INTO ca_items (id, name, price, qty, category) VALUES (3, 'Doohickey', 5.75, 200, 'B')");
    }

    public function testSimpleColumnAlias(): void
    {
        $stmt = $this->pdo->query("SELECT name AS item_name, price AS unit_price FROM ca_items WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Widget', $row['item_name']);
        $this->assertEqualsWithDelta(10.50, (float) $row['unit_price'], 0.01);
    }

    public function testExpressionAlias(): void
    {
        $stmt = $this->pdo->query("SELECT name, price * qty AS total_value FROM ca_items ORDER BY total_value DESC");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // Gadget: 25*50=1250, Doohickey: 5.75*200=1150, Widget: 10.50*100=1050
        $this->assertSame('Gadget', $rows[0]['name']);
        $this->assertEqualsWithDelta(1250.0, (float) $rows[0]['total_value'], 0.01);
    }

    public function testAggregateAlias(): void
    {
        $stmt = $this->pdo->query("
            SELECT category AS cat,
                   COUNT(*) AS item_count,
                   SUM(price * qty) AS total_value,
                   AVG(price) AS avg_price
            FROM ca_items
            GROUP BY category
            ORDER BY cat
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('A', $rows[0]['cat']);
        $this->assertSame(2, (int) $rows[0]['item_count']);
        $this->assertEqualsWithDelta(2300.0, (float) $rows[0]['total_value'], 0.01); // 1050+1250
    }

    public function testAliasInOrderBy(): void
    {
        $stmt = $this->pdo->query("SELECT name, price * qty AS revenue FROM ca_items ORDER BY revenue ASC");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('Widget', $rows[0]['name']);
        $this->assertSame('Doohickey', $rows[1]['name']);
        $this->assertSame('Gadget', $rows[2]['name']);
    }

    public function testAliasInHaving(): void
    {
        $stmt = $this->pdo->query("
            SELECT category, SUM(qty) AS total_qty
            FROM ca_items
            GROUP BY category
            HAVING total_qty > 100
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // A: 100+50=150, B: 200 → both > 100
        $this->assertCount(2, $rows);
    }

    public function testMultipleAliasesWithJoin(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE ca_orders (id INTEGER PRIMARY KEY, item_id INTEGER, customer TEXT)');
        $raw->exec('CREATE TABLE ca_products (id INTEGER PRIMARY KEY, name TEXT, price REAL)');

        $pdo = ZtdPdo::fromPdo($raw);
        $pdo->exec("INSERT INTO ca_products (id, name, price) VALUES (1, 'Widget', 10.00)");
        $pdo->exec("INSERT INTO ca_products (id, name, price) VALUES (2, 'Gadget', 20.00)");
        $pdo->exec("INSERT INTO ca_orders (id, item_id, customer) VALUES (1, 1, 'Alice')");
        $pdo->exec("INSERT INTO ca_orders (id, item_id, customer) VALUES (2, 1, 'Bob')");
        $pdo->exec("INSERT INTO ca_orders (id, item_id, customer) VALUES (3, 2, 'Alice')");

        $stmt = $pdo->query("
            SELECT o.customer AS buyer,
                   p.name AS product,
                   p.price AS cost
            FROM ca_orders o
            JOIN ca_products p ON p.id = o.item_id
            ORDER BY buyer, product
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['buyer']);
        $this->assertSame('Gadget', $rows[0]['product']);
        $this->assertSame('Alice', $rows[1]['buyer']);
        $this->assertSame('Widget', $rows[1]['product']);
        $this->assertSame('Bob', $rows[2]['buyer']);
    }

    public function testCaseExpressionAlias(): void
    {
        $stmt = $this->pdo->query("
            SELECT name,
                   CASE
                       WHEN price > 20 THEN 'expensive'
                       WHEN price > 8 THEN 'moderate'
                       ELSE 'cheap'
                   END AS tier
            FROM ca_items
            ORDER BY id
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('moderate', $rows[0]['tier']);  // Widget 10.50
        $this->assertSame('expensive', $rows[1]['tier']); // Gadget 25.00
        $this->assertSame('cheap', $rows[2]['tier']);      // Doohickey 5.75
    }

    public function testCoalesceAlias(): void
    {
        $this->pdo->exec("INSERT INTO ca_items (id, name, price, qty, category) VALUES (4, 'Unknown', 0, 0, NULL)");

        $stmt = $this->pdo->query("
            SELECT name, COALESCE(category, 'Uncategorized') AS display_cat
            FROM ca_items
            ORDER BY id
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('A', $rows[0]['display_cat']);
        $this->assertSame('Uncategorized', $rows[3]['display_cat']);
    }
}
