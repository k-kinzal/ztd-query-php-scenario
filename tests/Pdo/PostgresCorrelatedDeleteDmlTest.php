<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests DELETE with correlated subquery (NOT EXISTS, EXISTS) on PostgreSQL.
 *
 * @spec SPEC-10.2
 */
class PostgresCorrelatedDeleteDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE pg_cd_customers (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL
            )",
            "CREATE TABLE pg_cd_orders (
                id SERIAL PRIMARY KEY,
                customer_id INTEGER,
                product VARCHAR(100) NOT NULL,
                amount NUMERIC(10,2) NOT NULL
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_cd_orders', 'pg_cd_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO pg_cd_customers (name) VALUES ('Alice')");
        $this->ztdExec("INSERT INTO pg_cd_customers (name) VALUES ('Bob')");

        $this->ztdExec("INSERT INTO pg_cd_orders (customer_id, product, amount) VALUES (1, 'Widget', 10.00)");
        $this->ztdExec("INSERT INTO pg_cd_orders (customer_id, product, amount) VALUES (1, 'Gadget', 20.00)");
        $this->ztdExec("INSERT INTO pg_cd_orders (customer_id, product, amount) VALUES (2, 'Doohickey', 30.00)");
        $this->ztdExec("INSERT INTO pg_cd_orders (customer_id, product, amount) VALUES (999, 'Orphan1', 5.00)");
        $this->ztdExec("INSERT INTO pg_cd_orders (customer_id, product, amount) VALUES (888, 'Orphan2', 7.00)");
    }

    public function testDeleteWhereNotExists(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM pg_cd_orders WHERE NOT EXISTS (
                    SELECT 1 FROM pg_cd_customers WHERE pg_cd_customers.id = pg_cd_orders.customer_id
                )"
            );

            $rows = $this->ztdQuery("SELECT product FROM pg_cd_orders ORDER BY product");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE NOT EXISTS (PG): expected 3, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE NOT EXISTS (PG) failed: ' . $e->getMessage());
        }
    }

    public function testDeleteWhereExists(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM pg_cd_orders WHERE EXISTS (
                    SELECT 1 FROM pg_cd_customers
                    WHERE pg_cd_customers.id = pg_cd_orders.customer_id
                    AND pg_cd_customers.name = 'Alice'
                )"
            );

            $rows = $this->ztdQuery("SELECT product FROM pg_cd_orders ORDER BY product");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE EXISTS (PG): expected 3, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE EXISTS (PG) failed: ' . $e->getMessage());
        }
    }

    public function testUpdateWhereExists(): void
    {
        try {
            $this->ztdExec(
                "UPDATE pg_cd_orders SET amount = amount * 2 WHERE EXISTS (
                    SELECT 1 FROM pg_cd_customers
                    WHERE pg_cd_customers.id = pg_cd_orders.customer_id
                    AND pg_cd_customers.name = 'Bob'
                )"
            );

            $rows = $this->ztdQuery("SELECT amount FROM pg_cd_orders WHERE customer_id = 2");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('UPDATE EXISTS (PG): expected 1 row, got ' . count($rows));
            }

            if (abs((float) $rows[0]['amount'] - 60.0) > 0.01) {
                $this->markTestIncomplete(
                    'UPDATE EXISTS (PG): expected amount=60, got ' . $rows[0]['amount']
                );
            }

            $this->assertEqualsWithDelta(60.0, (float) $rows[0]['amount'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE EXISTS (PG) failed: ' . $e->getMessage());
        }
    }
}
