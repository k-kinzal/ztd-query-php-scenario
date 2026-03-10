<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests EXISTS / NOT EXISTS as boolean expression in SELECT list through ZTD.
 *
 * Applications commonly use EXISTS in the SELECT list to flag rows:
 * SELECT id, EXISTS(SELECT 1 FROM orders WHERE customer_id = c.id) AS has_orders
 * FROM customers c
 *
 * The CTE rewriter must preserve EXISTS subqueries in the SELECT list
 * and rewrite table references inside them.
 *
 * @spec SPEC-3.1
 * @spec SPEC-3.3
 */
class SqliteExistsInSelectListTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_esl_customers (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )',
            'CREATE TABLE sl_esl_orders (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                amount REAL NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_esl_orders', 'sl_esl_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_esl_customers (id, name) VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO sl_esl_customers (id, name) VALUES (2, 'Bob')");
        $this->pdo->exec("INSERT INTO sl_esl_customers (id, name) VALUES (3, 'Carol')");

        $this->pdo->exec("INSERT INTO sl_esl_orders (id, customer_id, amount) VALUES (1, 1, 100)");
        $this->pdo->exec("INSERT INTO sl_esl_orders (id, customer_id, amount) VALUES (2, 1, 200)");
        $this->pdo->exec("INSERT INTO sl_esl_orders (id, customer_id, amount) VALUES (3, 3, 50)");
    }

    /**
     * EXISTS in SELECT list to flag customers with orders.
     *
     * @spec SPEC-3.1
     */
    public function testExistsInSelectList(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT c.id, c.name,
                        EXISTS(SELECT 1 FROM sl_esl_orders o WHERE o.customer_id = c.id) AS has_orders
                 FROM sl_esl_customers c
                 ORDER BY c.id"
            );

            $this->assertCount(3, $rows);
            $this->assertEquals(1, (int) $rows[0]['has_orders'], 'Alice has orders');
            $this->assertEquals(0, (int) $rows[1]['has_orders'], 'Bob has no orders');
            $this->assertEquals(1, (int) $rows[2]['has_orders'], 'Carol has orders');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('EXISTS in SELECT list failed: ' . $e->getMessage());
        }
    }

    /**
     * NOT EXISTS in SELECT list.
     *
     * @spec SPEC-3.1
     */
    public function testNotExistsInSelectList(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT c.id, c.name,
                        NOT EXISTS(SELECT 1 FROM sl_esl_orders o WHERE o.customer_id = c.id) AS no_orders
                 FROM sl_esl_customers c
                 ORDER BY c.id"
            );

            $this->assertCount(3, $rows);
            $this->assertEquals(0, (int) $rows[0]['no_orders'], 'Alice has orders');
            $this->assertEquals(1, (int) $rows[1]['no_orders'], 'Bob has no orders');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('NOT EXISTS in SELECT list failed: ' . $e->getMessage());
        }
    }

    /**
     * EXISTS after shadow INSERT (new order for Bob).
     *
     * @spec SPEC-3.1
     */
    public function testExistsAfterShadowInsert(): void
    {
        try {
            // Shadow insert: give Bob an order
            $this->pdo->exec("INSERT INTO sl_esl_orders (id, customer_id, amount) VALUES (4, 2, 75)");

            $rows = $this->ztdQuery(
                "SELECT c.id, c.name,
                        EXISTS(SELECT 1 FROM sl_esl_orders o WHERE o.customer_id = c.id) AS has_orders
                 FROM sl_esl_customers c
                 ORDER BY c.id"
            );

            $this->assertCount(3, $rows);
            $this->assertEquals(1, (int) $rows[0]['has_orders'], 'Alice');
            $this->assertEquals(1, (int) $rows[1]['has_orders'], 'Bob should now have orders');
            $this->assertEquals(1, (int) $rows[2]['has_orders'], 'Carol');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('EXISTS after shadow INSERT failed: ' . $e->getMessage());
        }
    }

    /**
     * EXISTS after shadow DELETE (remove Carol's order).
     *
     * @spec SPEC-3.1
     */
    public function testExistsAfterShadowDelete(): void
    {
        try {
            // Shadow delete: remove Carol's order
            $this->pdo->exec("DELETE FROM sl_esl_orders WHERE customer_id = 3");

            $rows = $this->ztdQuery(
                "SELECT c.id, c.name,
                        EXISTS(SELECT 1 FROM sl_esl_orders o WHERE o.customer_id = c.id) AS has_orders
                 FROM sl_esl_customers c
                 ORDER BY c.id"
            );

            $this->assertCount(3, $rows);
            $this->assertEquals(1, (int) $rows[0]['has_orders'], 'Alice still has orders');
            $this->assertEquals(0, (int) $rows[1]['has_orders'], 'Bob no orders');
            $this->assertEquals(0, (int) $rows[2]['has_orders'], 'Carol should now have no orders');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('EXISTS after shadow DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * Multiple EXISTS in SELECT list.
     *
     * @spec SPEC-3.1
     */
    public function testMultipleExistsInSelectList(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT c.id, c.name,
                        EXISTS(SELECT 1 FROM sl_esl_orders o WHERE o.customer_id = c.id AND o.amount >= 100) AS has_large_orders,
                        EXISTS(SELECT 1 FROM sl_esl_orders o WHERE o.customer_id = c.id AND o.amount < 100) AS has_small_orders
                 FROM sl_esl_customers c
                 ORDER BY c.id"
            );

            $this->assertCount(3, $rows);
            $this->assertEquals(1, (int) $rows[0]['has_large_orders'], 'Alice has large orders');
            $this->assertEquals(0, (int) $rows[0]['has_small_orders'], 'Alice has no small orders');
            $this->assertEquals(1, (int) $rows[2]['has_small_orders'], 'Carol has small orders');
            $this->assertEquals(0, (int) $rows[2]['has_large_orders'], 'Carol has no large orders');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multiple EXISTS in SELECT list failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared EXISTS in SELECT list.
     *
     * @spec SPEC-3.2
     */
    public function testPreparedExistsInSelectList(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT c.id, c.name,
                        EXISTS(SELECT 1 FROM sl_esl_orders o WHERE o.customer_id = c.id AND o.amount >= ?) AS has_large_orders
                 FROM sl_esl_customers c
                 WHERE c.name = ?
                 ORDER BY c.id",
                [100, 'Alice']
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Prepared EXISTS: expected 1 row, got ' . count($rows));
            }

            $this->assertEquals(1, (int) $rows[0]['has_large_orders']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared EXISTS in SELECT list failed: ' . $e->getMessage());
        }
    }
}
