<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;
use ZtdQuery\Adapter\Pdo\ZtdPdoException;

/**
 * Tests UPDATE with subquery in SET clause on SQLite.
 *
 * Non-correlated scalar subqueries in SET work correctly.
 * Correlated subqueries (referencing the outer table being updated)
 * fail with a syntax error because the CTE rewriter generates invalid SQL.
 */
class SqliteUpdateWithSubqueryInSetTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE sl_updsub_products (id INTEGER PRIMARY KEY, name TEXT, price REAL, category_id INTEGER)');
        $raw->exec('CREATE TABLE sl_updsub_categories (id INTEGER PRIMARY KEY, name TEXT, avg_price REAL)');
        $this->pdo = ZtdPdo::fromPdo($raw);

        $this->pdo->exec("INSERT INTO sl_updsub_categories VALUES (1, 'Electronics', 0)");
        $this->pdo->exec("INSERT INTO sl_updsub_categories VALUES (2, 'Books', 0)");

        $this->pdo->exec("INSERT INTO sl_updsub_products VALUES (1, 'Laptop', 1000.00, 1)");
        $this->pdo->exec("INSERT INTO sl_updsub_products VALUES (2, 'Phone', 500.00, 1)");
        $this->pdo->exec("INSERT INTO sl_updsub_products VALUES (3, 'Novel', 15.00, 2)");
        $this->pdo->exec("INSERT INTO sl_updsub_products VALUES (4, 'Textbook', 85.00, 2)");
    }

    /**
     * UPDATE SET col = (non-correlated scalar subquery) works.
     */
    public function testUpdateSetNonCorrelatedSubqueryWorks(): void
    {
        $this->pdo->exec('
            UPDATE sl_updsub_products
            SET price = (SELECT MAX(price) FROM sl_updsub_products)
            WHERE id = 3
        ');

        $stmt = $this->pdo->query('SELECT price FROM sl_updsub_products WHERE id = 3');
        $this->assertEqualsWithDelta(1000.0, (float) $stmt->fetchColumn(), 0.01);
    }

    /**
     * UPDATE SET col = (AVG subquery) works for non-correlated.
     */
    public function testUpdateSetAvgSubqueryWorks(): void
    {
        $this->pdo->exec('
            UPDATE sl_updsub_categories
            SET avg_price = (SELECT AVG(price) FROM sl_updsub_products)
            WHERE id = 1
        ');

        $stmt = $this->pdo->query('SELECT avg_price FROM sl_updsub_categories WHERE id = 1');
        // AVG of all products: (1000+500+15+85)/4 = 400
        $this->assertEqualsWithDelta(400.0, (float) $stmt->fetchColumn(), 0.01);
    }

    /**
     * UPDATE SET col = (correlated subquery) fails with syntax error.
     *
     * The CTE rewriter cannot handle subqueries that reference the
     * outer table being updated.
     */
    public function testUpdateSetCorrelatedSubqueryFails(): void
    {
        $this->expectException(ZtdPdoException::class);
        $this->expectExceptionMessage('syntax error');

        $this->pdo->exec('
            UPDATE sl_updsub_categories
            SET avg_price = (SELECT AVG(price) FROM sl_updsub_products WHERE category_id = sl_updsub_categories.id)
            WHERE id = 1
        ');
    }

    /**
     * UPDATE SET col = (COUNT correlated subquery) also fails.
     */
    public function testUpdateSetCountCorrelatedSubqueryFails(): void
    {
        $this->expectException(ZtdPdoException::class);
        $this->expectExceptionMessage('syntax error');

        $this->pdo->exec('
            UPDATE sl_updsub_categories
            SET avg_price = (SELECT COUNT(*) FROM sl_updsub_products WHERE category_id = sl_updsub_categories.id)
            WHERE id = 1
        ');
    }

    /**
     * Simple UPDATE without subquery still works.
     */
    public function testSimpleUpdateStillWorks(): void
    {
        $this->pdo->exec('UPDATE sl_updsub_categories SET avg_price = 999 WHERE id = 1');

        $stmt = $this->pdo->query('SELECT avg_price FROM sl_updsub_categories WHERE id = 1');
        $this->assertEqualsWithDelta(999.0, (float) $stmt->fetchColumn(), 0.01);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec('UPDATE sl_updsub_categories SET avg_price = 999');

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_updsub_categories');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
