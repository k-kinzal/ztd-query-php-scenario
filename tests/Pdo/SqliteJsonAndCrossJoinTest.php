<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests JSON data handling and CROSS JOIN patterns on SQLite.
 * JSON is common in modern PHP apps; CROSS JOIN is an important untested SQL pattern.
 */
class SqliteJsonAndCrossJoinTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, metadata TEXT)');
        $raw->exec('CREATE TABLE colors (id INTEGER PRIMARY KEY, color TEXT)');
        $raw->exec('CREATE TABLE sizes (id INTEGER PRIMARY KEY, size TEXT)');

        $this->pdo = ZtdPdo::fromPdo($raw);
    }

    public function testInsertAndSelectJsonData(): void
    {
        $this->pdo->exec("INSERT INTO products (id, name, metadata) VALUES (1, 'Widget', '{\"color\":\"red\",\"weight\":1.5}')");
        $this->pdo->exec("INSERT INTO products (id, name, metadata) VALUES (2, 'Gadget', '{\"color\":\"blue\",\"weight\":2.0}')");

        $stmt = $this->pdo->query('SELECT name, metadata FROM products ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);

        $meta1 = json_decode($rows[0]['metadata'], true);
        $this->assertSame('red', $meta1['color']);
        $this->assertSame(1.5, $meta1['weight']);
    }

    public function testJsonExtractFunction(): void
    {
        $this->pdo->exec("INSERT INTO products (id, name, metadata) VALUES (1, 'Widget', '{\"color\":\"red\",\"weight\":1.5}')");
        $this->pdo->exec("INSERT INTO products (id, name, metadata) VALUES (2, 'Gadget', '{\"color\":\"blue\",\"weight\":2.0}')");

        // SQLite json_extract function
        $stmt = $this->pdo->query("SELECT name, json_extract(metadata, '$.color') AS color FROM products ORDER BY id");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('red', $rows[0]['color']);
        $this->assertSame('blue', $rows[1]['color']);
    }

    public function testJsonWhereClause(): void
    {
        $this->pdo->exec("INSERT INTO products (id, name, metadata) VALUES (1, 'Widget', '{\"color\":\"red\",\"weight\":1.5}')");
        $this->pdo->exec("INSERT INTO products (id, name, metadata) VALUES (2, 'Gadget', '{\"color\":\"blue\",\"weight\":2.0}')");
        $this->pdo->exec("INSERT INTO products (id, name, metadata) VALUES (3, 'Doohickey', '{\"color\":\"red\",\"weight\":0.5}')");

        $stmt = $this->pdo->query("SELECT name FROM products WHERE json_extract(metadata, '$.color') = 'red' ORDER BY name");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Doohickey', $rows[0]['name']);
        $this->assertSame('Widget', $rows[1]['name']);
    }

    public function testUpdateJsonData(): void
    {
        $this->pdo->exec("INSERT INTO products (id, name, metadata) VALUES (1, 'Widget', '{\"color\":\"red\",\"weight\":1.5}')");

        $this->pdo->exec("UPDATE products SET metadata = '{\"color\":\"green\",\"weight\":1.5}' WHERE id = 1");

        $stmt = $this->pdo->query("SELECT json_extract(metadata, '$.color') AS color FROM products WHERE id = 1");
        $this->assertSame('green', $stmt->fetch(PDO::FETCH_ASSOC)['color']);
    }

    public function testPreparedStatementWithJsonData(): void
    {
        $json = json_encode(['tags' => ['sale', 'new'], 'price' => 19.99]);
        $stmt = $this->pdo->prepare("INSERT INTO products (id, name, metadata) VALUES (?, ?, ?)");
        $stmt->execute([1, 'Widget', $json]);

        $stmt2 = $this->pdo->query('SELECT metadata FROM products WHERE id = 1');
        $decoded = json_decode($stmt2->fetch(PDO::FETCH_ASSOC)['metadata'], true);
        $this->assertSame(['sale', 'new'], $decoded['tags']);
        $this->assertSame(19.99, $decoded['price']);
    }

    public function testCrossJoin(): void
    {
        $this->pdo->exec("INSERT INTO colors (id, color) VALUES (1, 'Red')");
        $this->pdo->exec("INSERT INTO colors (id, color) VALUES (2, 'Blue')");
        $this->pdo->exec("INSERT INTO sizes (id, size) VALUES (1, 'Small')");
        $this->pdo->exec("INSERT INTO sizes (id, size) VALUES (2, 'Medium')");
        $this->pdo->exec("INSERT INTO sizes (id, size) VALUES (3, 'Large')");

        $stmt = $this->pdo->query("
            SELECT c.color, s.size
            FROM colors c
            CROSS JOIN sizes s
            ORDER BY c.color, s.size
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // 2 colors × 3 sizes = 6 combinations
        $this->assertCount(6, $rows);
        $this->assertSame('Blue', $rows[0]['color']);
        $this->assertSame('Large', $rows[0]['size']);
    }

    public function testCrossJoinWithFilter(): void
    {
        $this->pdo->exec("INSERT INTO colors (id, color) VALUES (1, 'Red')");
        $this->pdo->exec("INSERT INTO colors (id, color) VALUES (2, 'Blue')");
        $this->pdo->exec("INSERT INTO sizes (id, size) VALUES (1, 'Small')");
        $this->pdo->exec("INSERT INTO sizes (id, size) VALUES (2, 'Large')");

        $stmt = $this->pdo->query("
            SELECT c.color, s.size
            FROM colors c
            CROSS JOIN sizes s
            WHERE c.color = 'Red'
            ORDER BY s.size
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Red', $rows[0]['color']);
    }

    public function testImplicitCrossJoin(): void
    {
        $this->pdo->exec("INSERT INTO colors (id, color) VALUES (1, 'Red')");
        $this->pdo->exec("INSERT INTO colors (id, color) VALUES (2, 'Blue')");
        $this->pdo->exec("INSERT INTO sizes (id, size) VALUES (1, 'S')");
        $this->pdo->exec("INSERT INTO sizes (id, size) VALUES (2, 'M')");

        // Implicit cross join (comma-separated FROM)
        $stmt = $this->pdo->query("
            SELECT c.color, s.size
            FROM colors c, sizes s
            ORDER BY c.color, s.size
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(4, $rows);
    }

    public function testCrossJoinAfterMutations(): void
    {
        $this->pdo->exec("INSERT INTO colors (id, color) VALUES (1, 'Red')");
        $this->pdo->exec("INSERT INTO colors (id, color) VALUES (2, 'Blue')");
        $this->pdo->exec("INSERT INTO sizes (id, size) VALUES (1, 'S')");
        $this->pdo->exec("INSERT INTO sizes (id, size) VALUES (2, 'M')");

        // Delete one color
        $this->pdo->exec("DELETE FROM colors WHERE id = 2");

        $stmt = $this->pdo->query("
            SELECT c.color, s.size
            FROM colors c
            CROSS JOIN sizes s
            ORDER BY c.color, s.size
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // 1 color × 2 sizes = 2
        $this->assertCount(2, $rows);
        $this->assertSame('Red', $rows[0]['color']);
    }
}
