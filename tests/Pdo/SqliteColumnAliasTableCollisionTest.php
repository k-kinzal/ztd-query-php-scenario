<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests queries where column aliases match table names that have shadow data.
 *
 * The CTE rewriter uses pattern matching to find table references. A column
 * alias like "AS orders" in a SELECT list could be mistakenly identified as
 * a table reference, causing incorrect CTE rewriting.
 *
 * @spec SPEC-3.1
 */
class SqliteColumnAliasTableCollisionTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price REAL)',
            'CREATE TABLE orders (id INTEGER PRIMARY KEY, item_id INTEGER, qty INTEGER)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['items', 'orders'];
    }

    /**
     * Column alias same as another table name (JOIN query).
     */
    public function testColumnAliasSameAsTableName(): void
    {
        $this->pdo->exec("INSERT INTO items (id, name, price) VALUES (1, 'Widget', 10.00)");
        $this->pdo->exec("INSERT INTO orders (id, item_id, qty) VALUES (1, 1, 5)");

        $rows = $this->ztdQuery(
            'SELECT i.name, i.price * o.qty AS orders FROM items i JOIN orders o ON o.item_id = i.id'
        );
        $this->assertCount(1, $rows);
        $this->assertSame('Widget', $rows[0]['name']);
        $this->assertEquals(50.0, (float) $rows[0]['orders']);
    }

    /**
     * Column alias same as source table name (COUNT).
     */
    public function testColumnAliasSameAsSourceTable(): void
    {
        $this->pdo->exec("INSERT INTO items (id, name, price) VALUES (1, 'A', 5.00)");
        $this->pdo->exec("INSERT INTO items (id, name, price) VALUES (2, 'B', 15.00)");

        $rows = $this->ztdQuery('SELECT COUNT(*) AS items FROM items WHERE price > 3');
        $this->assertCount(1, $rows);
        $this->assertEquals(2, (int) $rows[0]['items']);
    }

    /**
     * Column alias same as table name with shadow mutations.
     */
    public function testColumnAliasAfterMutation(): void
    {
        $this->pdo->exec("INSERT INTO items (id, name, price) VALUES (1, 'X', 10.00)");
        $this->pdo->exec("INSERT INTO orders (id, item_id, qty) VALUES (1, 1, 3)");

        // Mutate shadow data
        $this->pdo->exec("UPDATE orders SET qty = 7 WHERE id = 1");

        $rows = $this->ztdQuery(
            'SELECT i.name, o.qty AS items FROM items i JOIN orders o ON o.item_id = i.id'
        );
        $this->assertCount(1, $rows);
        $this->assertEquals(7, (int) $rows[0]['items']);
    }

    /**
     * Subquery alias same as table name.
     * Related: SPEC-11.BARE-SUBQUERY-REWRITE [Issue #73] — bare subquery
     * table references in SELECT list are not rewritten by CTE rewriter.
     */
    public function testSubqueryAliasSameAsTableName(): void
    {
        $this->pdo->exec("INSERT INTO items (id, name, price) VALUES (1, 'A', 10.00), (2, 'B', 20.00)");

        $rows = $this->ztdQuery(
            'SELECT (SELECT SUM(price) FROM items) AS items'
        );
        $this->assertCount(1, $rows);
        // Known issue: scalar subquery in SELECT list not rewritten (#73)
        // CTE rewriter does not rewrite table references inside bare subqueries,
        // so SUM reads from the physically empty table and returns NULL/0.
        $this->markTestSkipped(
            'Scalar subquery in SELECT list returns NULL — bare subquery not rewritten (Issue #73)'
        );
    }

    /**
     * Multiple column aliases each matching a different table.
     */
    public function testMultipleColumnAliasesMatchingTables(): void
    {
        $this->pdo->exec("INSERT INTO items (id, name, price) VALUES (1, 'Widget', 10.00)");
        $this->pdo->exec("INSERT INTO orders (id, item_id, qty) VALUES (1, 1, 5)");

        $rows = $this->ztdQuery(
            "SELECT COUNT(i.id) AS orders, SUM(o.qty) AS items
             FROM items i JOIN orders o ON o.item_id = i.id"
        );
        $this->assertCount(1, $rows);
        $this->assertEquals(1, (int) $rows[0]['orders']);
        $this->assertEquals(5, (int) $rows[0]['items']);
    }

    /**
     * CASE expression result aliased as table name.
     */
    public function testCaseExpressionAliasedAsTableName(): void
    {
        $this->pdo->exec("INSERT INTO items (id, name, price) VALUES (1, 'Cheap', 5.00), (2, 'Pricey', 50.00)");

        $rows = $this->ztdQuery(
            "SELECT name, CASE WHEN price > 20 THEN 'expensive' ELSE 'cheap' END AS orders
             FROM items ORDER BY id"
        );
        $this->assertCount(2, $rows);
        $this->assertSame('cheap', $rows[0]['orders']);
        $this->assertSame('expensive', $rows[1]['orders']);
    }
}
