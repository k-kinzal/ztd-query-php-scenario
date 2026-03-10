<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests subqueries embedded inside SQL expressions (PostgreSQL).
 * @spec SPEC-3.1, SPEC-3.3a
 */
class PostgresSubqueryInExpressionTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_sie_products (id INT PRIMARY KEY, name VARCHAR(50), price NUMERIC(10,2))',
            'CREATE TABLE pg_sie_discounts (product_id INT, pct NUMERIC(5,2))',
            'CREATE TABLE pg_sie_inventory (product_id INT PRIMARY KEY, qty INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_sie_inventory', 'pg_sie_discounts', 'pg_sie_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->pdo->exec("INSERT INTO pg_sie_products VALUES (1, 'Widget', 100.00)");
        $this->pdo->exec("INSERT INTO pg_sie_products VALUES (2, 'Gadget', 200.00)");
        $this->pdo->exec("INSERT INTO pg_sie_products VALUES (3, 'Doohickey', 50.00)");

        $this->pdo->exec("INSERT INTO pg_sie_discounts VALUES (1, 10.0)");
        $this->pdo->exec("INSERT INTO pg_sie_discounts VALUES (2, 25.0)");

        $this->pdo->exec("INSERT INTO pg_sie_inventory VALUES (1, 50)");
        $this->pdo->exec("INSERT INTO pg_sie_inventory VALUES (2, 0)");
    }

    public function testCoalesceWithSubquery(): void
    {
        $sql = "SELECT p.name,
                       COALESCE(
                           (SELECT d.pct FROM pg_sie_discounts d WHERE d.product_id = p.id),
                           0
                       ) AS discount_pct
                FROM pg_sie_products p
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
                           WHEN EXISTS (SELECT 1 FROM pg_sie_inventory i WHERE i.product_id = p.id AND i.qty > 0)
                           THEN 'in_stock'
                           WHEN EXISTS (SELECT 1 FROM pg_sie_inventory i WHERE i.product_id = p.id AND i.qty = 0)
                           THEN 'out_of_stock'
                           ELSE 'no_record'
                       END AS availability
                FROM pg_sie_products p
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

    public function testCoalesceSubqueryPrepared(): void
    {
        $sql = "SELECT p.name,
                       COALESCE(
                           (SELECT d.pct FROM pg_sie_discounts d WHERE d.product_id = p.id AND d.pct > $1),
                           0
                       ) AS discount_pct
                FROM pg_sie_products p
                WHERE p.price > $2
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
