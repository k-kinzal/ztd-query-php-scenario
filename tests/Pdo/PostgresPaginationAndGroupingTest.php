<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests parameterized LIMIT/OFFSET, expression-based GROUP BY,
 * and INSERT...SELECT with filtering on PostgreSQL PDO.
 * @spec SPEC-3.1
 */
class PostgresPaginationAndGroupingTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_pg_products (id INT PRIMARY KEY, name VARCHAR(255), category VARCHAR(50), price NUMERIC(10,2), stock INT)',
            'CREATE TABLE pg_pg_archive (id INT PRIMARY KEY, name VARCHAR(255), category VARCHAR(50), price NUMERIC(10,2), stock INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_pg_archive', 'pg_pg_products'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_pg_products VALUES (1, 'Widget A', 'hardware', 9.99, 50)");
        $this->pdo->exec("INSERT INTO pg_pg_products VALUES (2, 'Widget B', 'hardware', 14.99, 30)");
        $this->pdo->exec("INSERT INTO pg_pg_products VALUES (3, 'Gadget X', 'electronics', 29.99, 10)");
        $this->pdo->exec("INSERT INTO pg_pg_products VALUES (4, 'Gadget Y', 'electronics', 49.99, 5)");
        $this->pdo->exec("INSERT INTO pg_pg_products VALUES (5, 'Gizmo', 'electronics', 19.99, 20)");
    }

    public function testPreparedLimitOffset(): void
    {
        $stmt = $this->pdo->prepare("SELECT name FROM pg_pg_products ORDER BY id LIMIT ? OFFSET ?");
        $stmt->execute([2, 0]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Widget A', $rows[0]['name']);

        $stmt->execute([2, 2]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Gadget X', $rows[0]['name']);
    }

    public function testGroupByCase(): void
    {
        $stmt = $this->pdo->query("
            SELECT
                CASE WHEN price < 20 THEN 'budget' WHEN price < 40 THEN 'mid' ELSE 'premium' END AS tier,
                COUNT(*) AS cnt
            FROM pg_pg_products
            GROUP BY CASE WHEN price < 20 THEN 'budget' WHEN price < 40 THEN 'mid' ELSE 'premium' END
            ORDER BY tier
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('budget', $rows[0]['tier']);
    }

    public function testInsertSelectWithWhere(): void
    {
        $this->pdo->exec("INSERT INTO pg_pg_archive (id, name, category, price, stock) SELECT id, name, category, price, stock FROM pg_pg_products WHERE stock <= 10");

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_pg_archive");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(2, (int) $row['cnt']);
    }

    public function testInsertSelectWithSubquery(): void
    {
        $this->pdo->exec("
            INSERT INTO pg_pg_archive (id, name, category, price, stock)
            SELECT id, name, category, price, stock
            FROM pg_pg_products
            WHERE price > (SELECT AVG(price) FROM pg_pg_products)
        ");

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_pg_archive");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertGreaterThan(0, (int) $row['cnt']);
    }

    public function testPaginationAfterMutations(): void
    {
        $this->pdo->exec("DELETE FROM pg_pg_products WHERE id = 2");

        $stmt = $this->pdo->prepare("SELECT name FROM pg_pg_products ORDER BY id LIMIT ? OFFSET ?");
        $stmt->execute([2, 0]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Widget A', $rows[0]['name']);
        $this->assertSame('Gadget X', $rows[1]['name']);
    }
}
