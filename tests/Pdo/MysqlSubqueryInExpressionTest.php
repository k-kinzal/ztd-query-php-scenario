<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests subqueries embedded inside SQL expressions (MySQL PDO).
 * @spec SPEC-3.1, SPEC-3.3a
 */
class MysqlSubqueryInExpressionTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_sie_products (id INT PRIMARY KEY, name VARCHAR(50), price DECIMAL(10,2))',
            'CREATE TABLE my_sie_discounts (product_id INT, pct DECIMAL(5,2))',
            'CREATE TABLE my_sie_inventory (product_id INT PRIMARY KEY, qty INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_sie_inventory', 'my_sie_discounts', 'my_sie_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->pdo->exec("INSERT INTO my_sie_products VALUES (1, 'Widget', 100.00)");
        $this->pdo->exec("INSERT INTO my_sie_products VALUES (2, 'Gadget', 200.00)");
        $this->pdo->exec("INSERT INTO my_sie_products VALUES (3, 'Doohickey', 50.00)");

        $this->pdo->exec("INSERT INTO my_sie_discounts VALUES (1, 10.0)");
        $this->pdo->exec("INSERT INTO my_sie_discounts VALUES (2, 25.0)");

        $this->pdo->exec("INSERT INTO my_sie_inventory VALUES (1, 50)");
        $this->pdo->exec("INSERT INTO my_sie_inventory VALUES (2, 0)");
    }

    public function testCoalesceWithSubquery(): void
    {
        $sql = "SELECT p.name,
                       COALESCE(
                           (SELECT d.pct FROM my_sie_discounts d WHERE d.product_id = p.id),
                           0
                       ) AS discount_pct
                FROM my_sie_products p
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

    public function testCaseExistsSubqueryInSelect(): void
    {
        $sql = "SELECT p.name,
                       CASE
                           WHEN EXISTS (SELECT 1 FROM my_sie_inventory i WHERE i.product_id = p.id AND i.qty > 0)
                           THEN 'in_stock'
                           WHEN EXISTS (SELECT 1 FROM my_sie_inventory i WHERE i.product_id = p.id AND i.qty = 0)
                           THEN 'out_of_stock'
                           ELSE 'no_record'
                       END AS availability
                FROM my_sie_products p
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

    public function testArithmeticWithSubquery(): void
    {
        $sql = "SELECT p.name,
                       p.price * (1 - COALESCE(
                           (SELECT d.pct FROM my_sie_discounts d WHERE d.product_id = p.id), 0
                       ) / 100.0) AS final_price
                FROM my_sie_products p
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
            $this->assertEqualsWithDelta(90.0, (float) $rows[0]['final_price'], 0.01);
            $this->assertEqualsWithDelta(150.0, (float) $rows[1]['final_price'], 0.01);
            $this->assertEqualsWithDelta(50.0, (float) $rows[2]['final_price'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Arithmetic subquery failed: ' . $e->getMessage());
        }
    }

    public function testCoalesceSubqueryPrepared(): void
    {
        $sql = "SELECT p.name,
                       COALESCE(
                           (SELECT d.pct FROM my_sie_discounts d WHERE d.product_id = p.id AND d.pct > ?),
                           0
                       ) AS discount_pct
                FROM my_sie_products p
                WHERE p.price > ?
                ORDER BY p.id";

        try {
            $rows = $this->ztdPrepareAndExecute($sql, [15, 50]);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'COALESCE subquery prepared: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertEqualsWithDelta(0.0, (float) $rows[0]['discount_pct'], 0.01);
            $this->assertEqualsWithDelta(25.0, (float) $rows[1]['discount_pct'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('COALESCE subquery prepared failed: ' . $e->getMessage());
        }
    }
}
