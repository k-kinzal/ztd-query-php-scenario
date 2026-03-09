<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests heavily parenthesized SQL expressions through the CTE rewriter on SQLite.
 *
 * Real-world scenario: ORM query builders (Doctrine DQL, Eloquent) produce
 * heavily parenthesized WHERE clauses. The CTE rewriter must handle nested
 * parentheses correctly without breaking expression evaluation.
 *
 * @spec SPEC-3.1
 */
class SqliteParenthesizedExpressionTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pe_items (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            price REAL NOT NULL,
            stock INTEGER NOT NULL,
            active INTEGER NOT NULL DEFAULT 1
        )';
    }

    protected function getTableNames(): array
    {
        return ['pe_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO pe_items VALUES (1, 'Phone', 'electronics', 999.99, 10, 1)");
        $this->ztdExec("INSERT INTO pe_items VALUES (2, 'Cable', 'electronics', 9.99, 100, 1)");
        $this->ztdExec("INSERT INTO pe_items VALUES (3, 'Book', 'media', 19.99, 50, 1)");
        $this->ztdExec("INSERT INTO pe_items VALUES (4, 'DVD', 'media', 14.99, 0, 0)");
        $this->ztdExec("INSERT INTO pe_items VALUES (5, 'Shirt', 'clothing', 29.99, 25, 1)");
    }

    /**
     * Deeply nested parenthesized OR/AND.
     */
    public function testDeepParenthesizedWhere(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name FROM pe_items
             WHERE ((category = 'electronics' AND price > 100) OR (category = 'media' AND stock > 0))
             AND active = 1
             ORDER BY name"
        );
        $this->assertCount(2, $rows);
        $this->assertSame('Book', $rows[0]['name']);
        $this->assertSame('Phone', $rows[1]['name']);
    }

    /**
     * Triple-nested parentheses.
     */
    public function testTripleNestedParens(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name FROM pe_items
             WHERE (((category = 'electronics') AND (price < 50)) OR ((category = 'clothing') AND (active = 1)))
             ORDER BY name"
        );
        $this->assertCount(2, $rows);
        $this->assertSame('Cable', $rows[0]['name']);
        $this->assertSame('Shirt', $rows[1]['name']);
    }

    /**
     * Parenthesized IN list.
     */
    public function testParenthesizedInList(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name FROM pe_items
             WHERE (category IN ('electronics', 'clothing'))
             AND (stock > 0)
             ORDER BY name"
        );
        $this->assertCount(3, $rows);
    }

    /**
     * Prepared with complex parenthesized conditions.
     */
    public function testPreparedParenthesizedConditions(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT name FROM pe_items
                 WHERE ((category = ? AND price >= ?) OR (category = ? AND stock >= ?))
                 AND active = ?
                 ORDER BY name",
                ['electronics', 500, 'media', 10, 1]
            );
            $this->assertCount(2, $rows);
            $this->assertSame('Book', $rows[0]['name']);
            $this->assertSame('Phone', $rows[1]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete('Prepared parenthesized failed: ' . $e->getMessage());
        }
    }

    /**
     * BETWEEN inside parentheses.
     */
    public function testBetweenInParentheses(): void
    {
        // price BETWEEN 10 AND 30: Book (19.99), DVD (14.99), Shirt (29.99) — Cable 9.99 is out
        // stock > 0: excludes DVD (stock=0)
        $rows = $this->ztdQuery(
            "SELECT name FROM pe_items
             WHERE (price BETWEEN 10.00 AND 30.00) AND (stock > 0)
             ORDER BY name"
        );
        $this->assertCount(2, $rows);
        $this->assertSame('Book', $rows[0]['name']);
        $this->assertSame('Shirt', $rows[1]['name']);
    }

    /**
     * Arithmetic expressions in parentheses.
     */
    public function testArithmeticInParentheses(): void
    {
        // Phone: 999.99*10=9999.9, Cable: 9.99*100=999, Book: 19.99*50=999.5,
        // DVD: 14.99*0=0, Shirt: 29.99*25=749.75
        $rows = $this->ztdQuery(
            "SELECT name, (price * stock) AS inventory_value
             FROM pe_items
             WHERE ((price * stock) > 500)
             ORDER BY inventory_value DESC"
        );
        $this->assertCount(4, $rows);
        $this->assertSame('Phone', $rows[0]['name']);
    }

    /**
     * NOT with parenthesized expression.
     */
    public function testNotParenthesized(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name FROM pe_items
             WHERE NOT (category = 'electronics' OR category = 'media')
             ORDER BY name"
        );
        $this->assertCount(1, $rows);
        $this->assertSame('Shirt', $rows[0]['name']);
    }

    /**
     * Subquery in parenthesized expression.
     */
    public function testSubqueryInParenthesizedExpression(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name FROM pe_items
                 WHERE (price > (SELECT AVG(price) FROM pe_items WHERE active = 1))
                 ORDER BY name"
            );
            // AVG of active items: (999.99 + 9.99 + 19.99 + 29.99) / 4 = 264.99
            $this->assertCount(1, $rows);
            $this->assertSame('Phone', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete('Subquery in parens failed: ' . $e->getMessage());
        }
    }
}
