<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests parameterized LIMIT/OFFSET, expression-based GROUP BY,
 * INSERT...SELECT with filtering, and correlated HAVING on SQLite.
 * @spec SPEC-3.1
 */
class SqlitePaginationAndGroupingTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, category TEXT, price REAL, stock INTEGER)',
            'CREATE TABLE archive (id INTEGER PRIMARY KEY, name TEXT, category TEXT, price REAL, stock INTEGER)',
            'CREATE TABLE thresholds (category TEXT PRIMARY KEY, min_count INTEGER)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['products', 'archive', 'thresholds'];
    }



    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO products VALUES (1, 'Widget A', 'hardware', 9.99, 50)");
        $this->pdo->exec("INSERT INTO products VALUES (2, 'Widget B', 'hardware', 14.99, 30)");
        $this->pdo->exec("INSERT INTO products VALUES (3, 'Gadget X', 'electronics', 29.99, 10)");
        $this->pdo->exec("INSERT INTO products VALUES (4, 'Gadget Y', 'electronics', 49.99, 5)");
        $this->pdo->exec("INSERT INTO products VALUES (5, 'Gizmo', 'electronics', 19.99, 20)");
        $this->pdo->exec("INSERT INTO products VALUES (6, 'Tool', 'hardware', 7.99, 100)");
        $this->pdo->exec("INSERT INTO products VALUES (7, 'Sensor', 'electronics', 39.99, 15)");
    }
    // --- Parameterized LIMIT/OFFSET ---

    public function testPreparedLimitOffset(): void
    {
        $stmt = $this->pdo->prepare("SELECT name FROM products ORDER BY id LIMIT ? OFFSET ?");
        $stmt->execute([3, 0]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('Widget A', $rows[0]['name']);

        // Re-execute: page 2
        $stmt->execute([3, 3]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('Gadget Y', $rows[0]['name']);

        // Re-execute: page 3 (partial)
        $stmt->execute([3, 6]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Sensor', $rows[0]['name']);
    }

    public function testPreparedLimitOnly(): void
    {
        $stmt = $this->pdo->prepare("SELECT name FROM products ORDER BY price DESC LIMIT ?");
        $stmt->execute([2]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Gadget Y', $rows[0]['name']); // 49.99
    }

    public function testPreparedLimitWithWhereClause(): void
    {
        $stmt = $this->pdo->prepare("SELECT name FROM products WHERE category = ? ORDER BY price LIMIT ? OFFSET ?");
        $stmt->execute(['electronics', 2, 0]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Gizmo', $rows[0]['name']); // 19.99

        // Page 2
        $stmt->execute(['electronics', 2, 2]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
    }

    // --- Expression-based GROUP BY ---

    public function testGroupByCase(): void
    {
        $stmt = $this->pdo->query("
            SELECT
                CASE WHEN price < 20 THEN 'budget' WHEN price < 40 THEN 'mid' ELSE 'premium' END AS tier,
                COUNT(*) AS cnt,
                AVG(price) AS avg_price
            FROM products
            GROUP BY CASE WHEN price < 20 THEN 'budget' WHEN price < 40 THEN 'mid' ELSE 'premium' END
            ORDER BY tier
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('budget', $rows[0]['tier']);
        $this->assertSame('mid', $rows[1]['tier']);
        $this->assertSame('premium', $rows[2]['tier']);
    }

    public function testGroupByLength(): void
    {
        $stmt = $this->pdo->query("
            SELECT LENGTH(name) AS name_len, COUNT(*) AS cnt
            FROM products
            GROUP BY LENGTH(name)
            HAVING COUNT(*) > 1
            ORDER BY name_len
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // Multiple products with same name length
        $this->assertGreaterThanOrEqual(1, count($rows));
    }

    public function testGroupBySubstr(): void
    {
        $stmt = $this->pdo->query("
            SELECT SUBSTR(name, 1, 1) AS first_letter, COUNT(*) AS cnt
            FROM products
            GROUP BY SUBSTR(name, 1, 1)
            ORDER BY first_letter
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // G (Gadget X, Gadget Y, Gizmo) = 3, S (Sensor) = 1, T (Tool) = 1, W (Widget A, Widget B) = 2
        $gRow = array_values(array_filter($rows, fn($r) => $r['first_letter'] === 'G'));
        $this->assertSame(3, (int) $gRow[0]['cnt']);
    }

    // --- INSERT...SELECT with filtering ---

    public function testInsertSelectWithWhere(): void
    {
        $this->pdo->exec("INSERT INTO archive (id, name, category, price, stock) SELECT id, name, category, price, stock FROM products WHERE stock <= 10");

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM archive");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(2, (int) $row['cnt']); // Gadget X (10), Gadget Y (5)
    }

    public function testInsertSelectWithJoin(): void
    {
        $this->pdo->exec("INSERT INTO thresholds VALUES ('electronics', 3)");
        $this->pdo->exec("INSERT INTO thresholds VALUES ('hardware', 2)");

        // Insert products from categories that meet threshold
        $this->pdo->exec("
            INSERT INTO archive (id, name, category, price, stock)
            SELECT p.id, p.name, p.category, p.price, p.stock
            FROM products p
            JOIN thresholds t ON p.category = t.category
            WHERE p.stock >= 15
        ");

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM archive");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertGreaterThan(0, (int) $row['cnt']);
    }

    public function testInsertSelectWithAggregateSubquery(): void
    {
        // Insert products priced above average
        $this->pdo->exec("
            INSERT INTO archive (id, name, category, price, stock)
            SELECT id, name, category, price, stock
            FROM products
            WHERE price > (SELECT AVG(price) FROM products)
        ");

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM archive");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertGreaterThan(0, (int) $row['cnt']);

        // Verify all archived products are above average
        $avg_stmt = $this->pdo->query("SELECT AVG(price) AS avg FROM products");
        $avg = (float) $avg_stmt->fetch(PDO::FETCH_ASSOC)['avg'];

        $archive_stmt = $this->pdo->query("SELECT MIN(price) AS min_price FROM archive");
        $min_archived = (float) $archive_stmt->fetch(PDO::FETCH_ASSOC)['min_price'];
        $this->assertGreaterThan($avg, $min_archived);
    }

    // --- Correlated HAVING ---

    public function testHavingWithCorrelatedSubquery(): void
    {
        $this->pdo->exec("INSERT INTO thresholds VALUES ('electronics', 3)");
        $this->pdo->exec("INSERT INTO thresholds VALUES ('hardware', 2)");

        $stmt = $this->pdo->query("
            SELECT p.category, COUNT(*) AS cnt
            FROM products p
            GROUP BY p.category
            HAVING COUNT(*) >= (SELECT min_count FROM thresholds t WHERE t.category = p.category)
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // electronics has 4 products, threshold 3 → included
        // hardware has 3 products, threshold 2 → included
        $this->assertCount(2, $rows);
    }

    // --- Pagination after mutations ---

    public function testPaginationAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO products VALUES (8, 'New Item', 'hardware', 5.99, 200)");

        $stmt = $this->pdo->prepare("SELECT name FROM products ORDER BY id LIMIT ? OFFSET ?");
        $stmt->execute([3, 6]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Sensor', $rows[0]['name']);
        $this->assertSame('New Item', $rows[1]['name']);
    }

    public function testPaginationAfterDelete(): void
    {
        $this->pdo->exec("DELETE FROM products WHERE id IN (2, 4)");

        $stmt = $this->pdo->prepare("SELECT COUNT(*) AS cnt FROM products");
        $stmt->execute();
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(5, (int) $row['cnt']);

        $page = $this->pdo->prepare("SELECT name FROM products ORDER BY id LIMIT ? OFFSET ?");
        $page->execute([3, 0]);
        $rows = $page->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('Widget A', $rows[0]['name']); // id=1
        $this->assertSame('Gadget X', $rows[1]['name']); // id=3
        $this->assertSame('Gizmo', $rows[2]['name']);     // id=5
    }
}
