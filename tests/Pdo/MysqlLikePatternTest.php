<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests LIKE pattern matching through the CTE shadow store on MySQL (PDO).
 *
 * Covers % and _ wildcards, ESCAPE clause, NOT LIKE, LIKE in
 * UPDATE/DELETE WHERE clauses, prepared-statement parameters,
 * and MySQL's default case-insensitive LIKE behavior.
 */
class MysqlLikePatternTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE my_like_products (
            id INT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            sku VARCHAR(50) NOT NULL,
            description TEXT NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['my_like_products'];
    }

    private function seed(): void
    {
        $this->pdo->exec("INSERT INTO my_like_products (id, name, sku, description) VALUES (1, '100% Cotton T-Shirt', 'COT-100', 'Premium 100% organic cotton tee')");
        $this->pdo->exec("INSERT INTO my_like_products (id, name, sku, description) VALUES (2, '50% Off Sale', 'SALE-50', 'Half price clearance item')");
        $this->pdo->exec("INSERT INTO my_like_products (id, name, sku, description) VALUES (3, 'Under_score Brand', 'UND-001', 'Trendy underscore-branded product')");
        $this->pdo->exec("INSERT INTO my_like_products (id, name, sku, description) VALUES (4, 'Normal Product', 'NRM-001', 'A regular everyday product')");
        $this->pdo->exec("INSERT INTO my_like_products (id, name, sku, description) VALUES (5, 'UPPER case', 'UPR-001', 'Testing uppercase name')");
        $this->pdo->exec("INSERT INTO my_like_products (id, name, sku, description) VALUES (6, 'lower case', 'LWR-001', 'Testing lowercase name')");
    }

    /**
     * Basic LIKE with % wildcard — matches any sequence of characters.
     */
    public function testLikePercentWildcard(): void
    {
        $this->seed();

        $rows = $this->ztdQuery(
            "SELECT id, name FROM my_like_products WHERE name LIKE '%Cotton%' ORDER BY id"
        );

        $this->assertCount(1, $rows);
        $this->assertSame(1, (int) $rows[0]['id']);
        $this->assertSame('100% Cotton T-Shirt', $rows[0]['name']);
    }

    /**
     * LIKE with _ single-character wildcard — matches exactly one character.
     */
    public function testLikeUnderscoreWildcard(): void
    {
        $this->seed();

        $rows = $this->ztdQuery(
            "SELECT id, sku FROM my_like_products WHERE sku LIKE 'COT-1__' ORDER BY id"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('COT-100', $rows[0]['sku']);
    }

    /**
     * LIKE with ESCAPE clause — search for a literal % in data.
     */
    public function testLikeWithEscapeClause(): void
    {
        $this->seed();

        $rows = $this->ztdQuery(
            "SELECT id, name FROM my_like_products WHERE name LIKE '%!%%' ESCAPE '!' ORDER BY id"
        );

        $this->assertCount(2, $rows);
        $this->assertSame(1, (int) $rows[0]['id']); // '100% Cotton T-Shirt'
        $this->assertSame(2, (int) $rows[1]['id']); // '50% Off Sale'
    }

    /**
     * NOT LIKE — excludes matching rows.
     */
    public function testNotLike(): void
    {
        $this->seed();

        $rows = $this->ztdQuery(
            "SELECT id FROM my_like_products WHERE name NOT LIKE '%case%' ORDER BY id"
        );

        // MySQL LIKE is case-insensitive by default (utf8/utf8mb4 collation)
        // Should exclude id=5 'UPPER case' and id=6 'lower case'
        $this->assertCount(4, $rows);
        $ids = array_map(fn($r) => (int) $r['id'], $rows);
        $this->assertContains(1, $ids);
        $this->assertContains(2, $ids);
        $this->assertContains(3, $ids);
        $this->assertContains(4, $ids);
    }

    /**
     * LIKE in an UPDATE WHERE clause — shadow store reflects the change.
     */
    public function testLikeInUpdateWhere(): void
    {
        $this->seed();

        $this->pdo->exec(
            "UPDATE my_like_products SET description = 'UPDATED' WHERE name LIKE '%Sale%'"
        );

        $rows = $this->ztdQuery(
            "SELECT id, description FROM my_like_products WHERE id = 2"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('UPDATED', $rows[0]['description']);
    }

    /**
     * LIKE in a DELETE WHERE clause — shadow store reflects the deletion.
     */
    public function testLikeInDeleteWhere(): void
    {
        $this->seed();

        $this->pdo->exec(
            "DELETE FROM my_like_products WHERE name LIKE '%Off%'"
        );

        $rows = $this->ztdQuery(
            "SELECT id FROM my_like_products ORDER BY id"
        );

        $this->assertCount(5, $rows);
        $ids = array_map(fn($r) => (int) $r['id'], $rows);
        $this->assertNotContains(2, $ids);
    }

    /**
     * LIKE with prepared statement parameter.
     */
    public function testLikeWithPreparedParameter(): void
    {
        $this->seed();

        $rows = $this->ztdPrepareAndExecute(
            "SELECT id, name FROM my_like_products WHERE name LIKE ? ORDER BY id",
            ['%Product%']
        );

        $this->assertCount(1, $rows);
        $this->assertSame(4, (int) $rows[0]['id']);
        $this->assertSame('Normal Product', $rows[0]['name']);
    }

    /**
     * MySQL LIKE is case-insensitive by default with utf8/utf8mb4 collation.
     * Searching for 'upper' should match 'UPPER case'.
     */
    public function testLikeCaseInsensitivity(): void
    {
        $this->seed();

        $rows = $this->ztdQuery(
            "SELECT id, name FROM my_like_products WHERE name LIKE '%upper%' ORDER BY id"
        );

        $this->assertCount(1, $rows);
        $this->assertSame(5, (int) $rows[0]['id']);
    }

    /**
     * ESCAPE clause with _ wildcard — search for literal underscore in data.
     */
    public function testEscapeUnderscore(): void
    {
        $this->seed();

        $rows = $this->ztdQuery(
            "SELECT id, name FROM my_like_products WHERE name LIKE '%!_%' ESCAPE '!' ORDER BY id"
        );

        // Should match 'Under_score Brand' (contains a literal _)
        $this->assertCount(1, $rows);
        $this->assertSame(3, (int) $rows[0]['id']);
    }
}
