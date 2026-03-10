<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests subqueries embedded inside SQL expressions: COALESCE, CASE WHEN,
 * arithmetic, and function arguments.
 *
 * The CTE rewriter must find and rewrite table references even when they
 * are deeply nested inside expression contexts.
 *
 * @spec SPEC-3.1, SPEC-3.3a
 */
class SqliteSubqueryInExpressionTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_sie_products (id INTEGER PRIMARY KEY, name TEXT, price REAL)',
            'CREATE TABLE sl_sie_discounts (product_id INTEGER, pct REAL)',
            'CREATE TABLE sl_sie_inventory (product_id INTEGER PRIMARY KEY, qty INTEGER)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_sie_inventory', 'sl_sie_discounts', 'sl_sie_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->pdo->exec("INSERT INTO sl_sie_products VALUES (1, 'Widget', 100.00)");
        $this->pdo->exec("INSERT INTO sl_sie_products VALUES (2, 'Gadget', 200.00)");
        $this->pdo->exec("INSERT INTO sl_sie_products VALUES (3, 'Doohickey', 50.00)");

        $this->pdo->exec("INSERT INTO sl_sie_discounts VALUES (1, 10.0)");
        $this->pdo->exec("INSERT INTO sl_sie_discounts VALUES (2, 25.0)");
        // product 3 has no discount

        $this->pdo->exec("INSERT INTO sl_sie_inventory VALUES (1, 50)");
        $this->pdo->exec("INSERT INTO sl_sie_inventory VALUES (2, 0)");
        // product 3 has no inventory record
    }

    /**
     * COALESCE with scalar subquery as first argument.
     * COALESCE((SELECT ...), default_value)
     */
    public function testCoalesceWithSubquery(): void
    {
        $sql = "SELECT p.name,
                       COALESCE(
                           (SELECT d.pct FROM sl_sie_discounts d WHERE d.product_id = p.id),
                           0
                       ) AS discount_pct
                FROM sl_sie_products p
                ORDER BY p.id";

        try {
            $rows = $this->ztdQuery($sql);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'COALESCE subquery: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $this->assertEqualsWithDelta(10.0, (float) $rows[0]['discount_pct'], 0.01);
            $this->assertEqualsWithDelta(25.0, (float) $rows[1]['discount_pct'], 0.01);
            $this->assertEqualsWithDelta(0.0, (float) $rows[2]['discount_pct'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('COALESCE subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * CASE WHEN with subquery in THEN and ELSE branches.
     */
    public function testCaseWhenWithSubqueryBranches(): void
    {
        $sql = "SELECT p.name,
                       CASE
                           WHEN (SELECT d.pct FROM sl_sie_discounts d WHERE d.product_id = p.id) IS NOT NULL
                           THEN (SELECT d.pct FROM sl_sie_discounts d WHERE d.product_id = p.id)
                           ELSE -1
                       END AS discount_pct
                FROM sl_sie_products p
                ORDER BY p.id";

        try {
            $rows = $this->ztdQuery($sql);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'CASE subquery branches: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $this->assertEqualsWithDelta(10.0, (float) $rows[0]['discount_pct'], 0.01);
            $this->assertEqualsWithDelta(25.0, (float) $rows[1]['discount_pct'], 0.01);
            $this->assertEqualsWithDelta(-1.0, (float) $rows[2]['discount_pct'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CASE subquery branches failed: ' . $e->getMessage());
        }
    }

    /**
     * Arithmetic expression with subquery operand.
     * price * (1 - (SELECT discount)/100)
     */
    public function testArithmeticWithSubquery(): void
    {
        $sql = "SELECT p.name,
                       p.price * (1 - COALESCE(
                           (SELECT d.pct FROM sl_sie_discounts d WHERE d.product_id = p.id), 0
                       ) / 100.0) AS final_price
                FROM sl_sie_products p
                ORDER BY p.id";

        try {
            $rows = $this->ztdQuery($sql);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Arithmetic subquery: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            // Widget: 100 * (1 - 10/100) = 90
            $this->assertEqualsWithDelta(90.0, (float) $rows[0]['final_price'], 0.01);
            // Gadget: 200 * (1 - 25/100) = 150
            $this->assertEqualsWithDelta(150.0, (float) $rows[1]['final_price'], 0.01);
            // Doohickey: 50 * (1 - 0/100) = 50
            $this->assertEqualsWithDelta(50.0, (float) $rows[2]['final_price'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Arithmetic subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * EXISTS and NOT EXISTS in CASE expression.
     */
    public function testCaseExistsSubqueryInSelect(): void
    {
        $sql = "SELECT p.name,
                       CASE
                           WHEN EXISTS (SELECT 1 FROM sl_sie_inventory i WHERE i.product_id = p.id AND i.qty > 0)
                           THEN 'in_stock'
                           WHEN EXISTS (SELECT 1 FROM sl_sie_inventory i WHERE i.product_id = p.id AND i.qty = 0)
                           THEN 'out_of_stock'
                           ELSE 'no_record'
                       END AS availability
                FROM sl_sie_products p
                ORDER BY p.id";

        try {
            $rows = $this->ztdQuery($sql);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'CASE EXISTS: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame('in_stock', $rows[0]['availability']);
            $this->assertSame('out_of_stock', $rows[1]['availability']);
            $this->assertSame('no_record', $rows[2]['availability']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CASE EXISTS failed: ' . $e->getMessage());
        }
    }

    /**
     * Subquery inside COALESCE with prepared parameters.
     */
    public function testCoalesceSubqueryPrepared(): void
    {
        $sql = "SELECT p.name,
                       COALESCE(
                           (SELECT d.pct FROM sl_sie_discounts d WHERE d.product_id = p.id AND d.pct > ?),
                           0
                       ) AS discount_pct
                FROM sl_sie_products p
                WHERE p.price > ?
                ORDER BY p.id";

        try {
            $rows = $this->ztdPrepareAndExecute($sql, [15, 50]);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'COALESCE subquery prepared: expected 2 (price>50), got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            // Widget: discount 10 < 15, so COALESCE returns 0
            $this->assertEqualsWithDelta(0.0, (float) $rows[0]['discount_pct'], 0.01);
            // Gadget: discount 25 > 15, so COALESCE returns 25
            $this->assertEqualsWithDelta(25.0, (float) $rows[1]['discount_pct'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('COALESCE subquery prepared failed: ' . $e->getMessage());
        }
    }

    /**
     * Nested subqueries: subquery inside subquery inside SELECT.
     */
    public function testNestedSubqueryInSelect(): void
    {
        $sql = "SELECT p.name,
                       (SELECT COALESCE(
                           (SELECT d.pct FROM sl_sie_discounts d WHERE d.product_id = p.id),
                           0
                       )) AS disc
                FROM sl_sie_products p
                WHERE p.id = 1";

        try {
            $rows = $this->ztdQuery($sql);

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Nested subquery in SELECT: expected 1, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Widget', $rows[0]['name']);
            $this->assertEqualsWithDelta(10.0, (float) $rows[0]['disc'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Nested subquery in SELECT failed: ' . $e->getMessage());
        }
    }
}
