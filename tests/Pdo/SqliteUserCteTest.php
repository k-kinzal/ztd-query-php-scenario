<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests user-written CTE queries and INSERT ... SELECT on SQLite.
 * @spec SPEC-3.3
 */
class SqliteUserCteTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, category TEXT, price REAL)',
            'CREATE TABLE backup (id INTEGER PRIMARY KEY, name TEXT, category TEXT, price REAL)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['products', 'backup'];
    }



    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO products (id, name, category, price) VALUES (1, 'Widget A', 'gadgets', 10.00)");
        $this->pdo->exec("INSERT INTO products (id, name, category, price) VALUES (2, 'Widget B', 'gadgets', 20.00)");
        $this->pdo->exec("INSERT INTO products (id, name, category, price) VALUES (3, 'Gizmo X', 'tools', 30.00)");
    }
    public function testUserCteSelectReadsShadowData(): void
    {
        // On SQLite, user-written CTEs correctly read from the shadow store
        // (unlike PostgreSQL where inner CTE reads from physical table)
        $stmt = $this->pdo->query("
            WITH expensive AS (
                SELECT * FROM products WHERE price > 15
            )
            SELECT name, price FROM expensive ORDER BY price
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Widget B', $rows[0]['name']);
        $this->assertSame('Gizmo X', $rows[1]['name']);
    }

    public function testUserCteWithAggregation(): void
    {
        $stmt = $this->pdo->query("
            WITH category_summary AS (
                SELECT category, COUNT(*) as cnt, SUM(price) as total
                FROM products
                GROUP BY category
            )
            SELECT * FROM category_summary ORDER BY category
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('gadgets', $rows[0]['category']);
        $this->assertSame(2, (int) $rows[0]['cnt']);
        $this->assertSame(30.0, (float) $rows[0]['total']);
        $this->assertSame('tools', $rows[1]['category']);
        $this->assertSame(1, (int) $rows[1]['cnt']);
    }

    public function testInsertSelectExplicitColumns(): void
    {
        $this->pdo->exec("INSERT INTO backup (id, name, category, price) SELECT id, name, category, price FROM products");

        $stmt = $this->pdo->query('SELECT * FROM backup ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(3, $rows);
        $this->assertSame('Widget A', $rows[0]['name']);
    }

    public function testInsertSelectStarWorksOnSqlite(): void
    {
        // Unlike MySQL (which throws for SELECT *), SQLite handles it correctly
        $this->pdo->exec("INSERT INTO backup SELECT * FROM products");

        $stmt = $this->pdo->query('SELECT * FROM backup ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(3, $rows);
        $this->assertSame('Widget A', $rows[0]['name']);
    }

    public function testInsertSelectIsolation(): void
    {
        $this->pdo->exec("INSERT INTO backup (id, name, category, price) SELECT id, name, category, price FROM products");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT * FROM backup');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));
    }
}
