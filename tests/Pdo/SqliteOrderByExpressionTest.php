<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests ORDER BY with function calls and complex expressions on shadow data.
 *
 * The CTE rewriter must preserve ORDER BY expressions that use SQL functions,
 * arithmetic, CASE, and NULLS FIRST/LAST. These are common in real applications
 * but could trip up the CTE-based query rewriting.
 *
 * @spec SPEC-3.1
 */
class SqliteOrderByExpressionTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE oby (id INTEGER PRIMARY KEY, name TEXT, score INTEGER, category TEXT, updated_at TEXT)';
    }

    protected function getTableNames(): array
    {
        return ['oby'];
    }

    private function seedData(): void
    {
        $this->pdo->exec("INSERT INTO oby (id, name, score, category, updated_at) VALUES
            (1, 'alice', 85, 'A', '2026-01-15'),
            (2, 'BOB', 92, 'B', '2026-02-20'),
            (3, 'Charlie', 78, 'A', '2026-03-10'),
            (4, 'DIANA', 95, 'B', NULL),
            (5, 'eve', 85, 'C', '2026-01-01')");
    }

    /**
     * ORDER BY UPPER(column).
     */
    public function testOrderByUpper(): void
    {
        $this->seedData();
        $rows = $this->ztdQuery('SELECT name FROM oby ORDER BY UPPER(name)');
        $this->assertCount(5, $rows);
        $names = array_column($rows, 'name');
        $this->assertSame(['alice', 'BOB', 'Charlie', 'DIANA', 'eve'], $names);
    }

    /**
     * ORDER BY LENGTH(column).
     */
    public function testOrderByLength(): void
    {
        $this->seedData();
        $rows = $this->ztdQuery('SELECT name FROM oby ORDER BY LENGTH(name), name');
        $this->assertCount(5, $rows);
        // 3-char: BOB, eve; 5-char: alice, DIANA; 7-char: Charlie
        $this->assertEquals(3, strlen($rows[0]['name']));
        $this->assertEquals(7, strlen($rows[4]['name']));
    }

    /**
     * ORDER BY arithmetic expression (score * 2 + id).
     */
    public function testOrderByArithmetic(): void
    {
        $this->seedData();
        $rows = $this->ztdQuery('SELECT name, score FROM oby ORDER BY score * 2 + id');
        $this->assertCount(5, $rows);
        // 78*2+3=159, 85*2+1=171, 85*2+5=175, 92*2+2=186, 95*2+4=194
        $this->assertSame('Charlie', $rows[0]['name']);
        $this->assertSame('DIANA', $rows[4]['name']);
    }

    /**
     * ORDER BY CASE expression.
     */
    public function testOrderByCaseExpression(): void
    {
        $this->seedData();
        $rows = $this->ztdQuery("SELECT name, category FROM oby ORDER BY
            CASE category WHEN 'C' THEN 1 WHEN 'A' THEN 2 WHEN 'B' THEN 3 END, name");
        $this->assertCount(5, $rows);
        $this->assertSame('C', $rows[0]['category']);
    }

    /**
     * ORDER BY with COALESCE for NULL handling.
     */
    public function testOrderByCoalesceNullHandling(): void
    {
        $this->seedData();
        $rows = $this->ztdQuery("SELECT name, updated_at FROM oby ORDER BY COALESCE(updated_at, '9999-12-31')");
        $this->assertCount(5, $rows);
        // NULL updated_at (DIANA) should sort last due to COALESCE
        $this->assertSame('DIANA', $rows[4]['name']);
    }

    /**
     * ORDER BY ABS() for magnitude-based sorting.
     */
    public function testOrderByAbsFunction(): void
    {
        $this->pdo->exec("INSERT INTO oby (id, name, score) VALUES (1, 'a', -50), (2, 'b', 30), (3, 'c', -10), (4, 'd', 40)");
        $rows = $this->ztdQuery('SELECT name, score FROM oby ORDER BY ABS(score)');
        $this->assertCount(4, $rows);
        $this->assertSame('c', $rows[0]['name']); // ABS(-10) = 10
        $this->assertSame('b', $rows[1]['name']); // ABS(30) = 30
    }

    /**
     * ORDER BY with SUBSTR function.
     */
    public function testOrderBySubstr(): void
    {
        $this->pdo->exec("INSERT INTO oby (id, name, score) VALUES (1, 'z_first', 10), (2, 'a_second', 20), (3, 'z_third', 30)");
        $rows = $this->ztdQuery('SELECT name FROM oby ORDER BY SUBSTR(name, 3)');
        // 'first', 'second', 'third' alphabetical order
        $this->assertSame('z_first', $rows[0]['name']);
        $this->assertSame('a_second', $rows[1]['name']);
        $this->assertSame('z_third', $rows[2]['name']);
    }

    /**
     * ORDER BY expression after shadow mutation.
     */
    public function testOrderByExpressionAfterMutation(): void
    {
        $this->seedData();
        $this->pdo->exec("UPDATE oby SET score = 200 WHERE id = 3"); // Charlie 78→200

        $rows = $this->ztdQuery('SELECT name, score FROM oby ORDER BY score DESC');
        $this->assertCount(5, $rows);
        $this->assertSame('Charlie', $rows[0]['name']);
        $this->assertEquals(200, (int) $rows[0]['score']);
    }

    /**
     * ORDER BY IIF (SQLite-specific conditional).
     */
    public function testOrderByIif(): void
    {
        $this->seedData();
        $rows = $this->ztdQuery("SELECT name, score FROM oby ORDER BY IIF(score >= 90, 0, 1), score DESC");
        $this->assertCount(5, $rows);
        // 90+ first (DIANA 95, BOB 92), then rest by score desc
        $this->assertSame('DIANA', $rows[0]['name']);
        $this->assertSame('BOB', $rows[1]['name']);
    }
}
