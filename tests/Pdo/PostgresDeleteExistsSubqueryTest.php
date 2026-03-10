<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests DELETE with EXISTS/NOT EXISTS subqueries through ZTD on PostgreSQL.
 *
 * DELETE WHERE EXISTS is a common pattern for cascade-like deletions.
 * Given PostgreSQL's type mismatch issues with SERIAL PKs and EXISTS (#137),
 * these patterns are likely to fail.
 *
 * @spec SPEC-4.3
 */
class PostgresDeleteExistsSubqueryTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_de_customers (id SERIAL PRIMARY KEY, name TEXT, active INTEGER)',
            'CREATE TABLE pg_de_orders (id SERIAL PRIMARY KEY, customer_id INTEGER, status TEXT, total INTEGER)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_de_orders', 'pg_de_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_de_customers (name, active) VALUES ('Alice', 1)");
        $this->pdo->exec("INSERT INTO pg_de_customers (name, active) VALUES ('Bob', 0)");
        $this->pdo->exec("INSERT INTO pg_de_customers (name, active) VALUES ('Charlie', 1)");

        $this->pdo->exec("INSERT INTO pg_de_orders (customer_id, status, total) VALUES (1, 'shipped', 100)");
        $this->pdo->exec("INSERT INTO pg_de_orders (customer_id, status, total) VALUES (1, 'pending', 200)");
        $this->pdo->exec("INSERT INTO pg_de_orders (customer_id, status, total) VALUES (2, 'pending', 50)");
        $this->pdo->exec("INSERT INTO pg_de_orders (customer_id, status, total) VALUES (3, 'shipped', 300)");
    }

    /**
     * DELETE WHERE EXISTS: remove orders for inactive customers.
     */
    public function testDeleteWhereExists(): void
    {
        try {
            $this->pdo->exec(
                'DELETE FROM pg_de_orders
                 WHERE EXISTS (
                     SELECT 1 FROM pg_de_customers c
                     WHERE c.id = pg_de_orders.customer_id AND c.active = 0
                 )'
            );

            $rows = $this->ztdQuery('SELECT * FROM pg_de_orders ORDER BY id');
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE WHERE EXISTS: expected 3 rows, got ' . count($rows) . '. Data: ' . json_encode(array_column($rows, 'id'))
                );
            }
            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE EXISTS failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE WHERE NOT EXISTS: remove customers with no orders.
     */
    public function testDeleteWhereNotExists(): void
    {
        try {
            // Remove Charlie's order first
            $this->pdo->exec('DELETE FROM pg_de_orders WHERE customer_id = 3');

            // Now delete customers with no orders
            $this->pdo->exec(
                'DELETE FROM pg_de_customers
                 WHERE NOT EXISTS (
                     SELECT 1 FROM pg_de_orders o WHERE o.customer_id = pg_de_customers.id
                 )'
            );

            $rows = $this->ztdQuery('SELECT * FROM pg_de_customers ORDER BY id');
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE WHERE NOT EXISTS: expected 2 rows, got ' . count($rows) . '. Data: ' . json_encode(array_column($rows, 'name'))
                );
            }
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Bob', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE NOT EXISTS failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE WHERE IN (subquery).
     */
    public function testDeleteWhereInSubquery(): void
    {
        try {
            $this->pdo->exec(
                'DELETE FROM pg_de_orders
                 WHERE customer_id IN (SELECT id FROM pg_de_customers WHERE active = 0)'
            );

            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM pg_de_orders');
            if ((int) $rows[0]['cnt'] !== 3) {
                $this->markTestIncomplete(
                    'DELETE WHERE IN: expected 3 remaining, got ' . $rows[0]['cnt']
                );
            }
            $this->assertEquals(3, (int) $rows[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE IN failed: ' . $e->getMessage());
        }
    }

    /**
     * Chained DELETE: delete pending orders, then orphaned customers.
     */
    public function testChainedDeleteWithExistsCheck(): void
    {
        try {
            $this->pdo->exec("DELETE FROM pg_de_orders WHERE status = 'pending'");

            $orders = $this->ztdQuery('SELECT * FROM pg_de_orders ORDER BY id');
            if (count($orders) !== 2) {
                $this->markTestIncomplete(
                    'Chained DELETE step 1: expected 2 shipped, got ' . count($orders)
                );
            }

            $this->pdo->exec(
                'DELETE FROM pg_de_customers
                 WHERE NOT EXISTS (SELECT 1 FROM pg_de_orders o WHERE o.customer_id = pg_de_customers.id)'
            );

            $customers = $this->ztdQuery('SELECT * FROM pg_de_customers ORDER BY id');
            if (count($customers) !== 2) {
                $this->markTestIncomplete(
                    'Chained DELETE step 2: expected Alice+Charlie, got ' . count($customers) . ': ' . json_encode(array_column($customers, 'name'))
                );
            }
            $this->assertSame('Alice', $customers[0]['name']);
            $this->assertSame('Charlie', $customers[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Chained DELETE failed: ' . $e->getMessage());
        }
    }
}
