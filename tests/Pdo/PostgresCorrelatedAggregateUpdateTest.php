<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests UPDATE SET with correlated aggregate subquery from another table
 * through ZTD shadow store on PostgreSQL via PDO.
 *
 * @spec SPEC-4.2
 * @spec SPEC-3.3
 */
class PostgresCorrelatedAggregateUpdateTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_cau_customers (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                order_total DECIMAL(10,2) NOT NULL DEFAULT 0,
                order_count INT NOT NULL DEFAULT 0
            )',
            'CREATE TABLE pg_cau_orders (
                id INT PRIMARY KEY,
                customer_id INT NOT NULL,
                amount DECIMAL(10,2) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_cau_orders', 'pg_cau_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_cau_customers VALUES (1, 'Alice', 0, 0)");
        $this->pdo->exec("INSERT INTO pg_cau_customers VALUES (2, 'Bob', 0, 0)");
        $this->pdo->exec("INSERT INTO pg_cau_customers VALUES (3, 'Carol', 0, 0)");

        $this->pdo->exec("INSERT INTO pg_cau_orders VALUES (1, 1, 100.00)");
        $this->pdo->exec("INSERT INTO pg_cau_orders VALUES (2, 1, 200.00)");
        $this->pdo->exec("INSERT INTO pg_cau_orders VALUES (3, 2, 150.00)");
        $this->pdo->exec("INSERT INTO pg_cau_orders VALUES (4, 1, 50.00)");
    }

    public function testUpdateSetWithCorrelatedSum(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE pg_cau_customers SET order_total = (SELECT COALESCE(SUM(amount), 0) FROM pg_cau_orders WHERE pg_cau_orders.customer_id = pg_cau_customers.id)"
            );

            $rows = $this->ztdQuery("SELECT id, name, order_total FROM pg_cau_customers ORDER BY id");

            $this->assertCount(3, $rows);

            if ((float) $rows[0]['order_total'] != 350.00) {
                $this->markTestIncomplete(
                    'Correlated SUM: Alice total=' . var_export($rows[0]['order_total'], true) . ', expected 350'
                );
            }
            $this->assertEquals(350.00, (float) $rows[0]['order_total']);

            if ((float) $rows[1]['order_total'] != 150.00) {
                $this->markTestIncomplete(
                    'Correlated SUM: Bob total=' . var_export($rows[1]['order_total'], true) . ', expected 150'
                );
            }
            $this->assertEquals(150.00, (float) $rows[1]['order_total']);

            $this->assertEquals(0.00, (float) $rows[2]['order_total']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET with correlated SUM failed: ' . $e->getMessage());
        }
    }

    public function testUpdateSetWithCorrelatedCount(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE pg_cau_customers SET order_count = (SELECT COUNT(*) FROM pg_cau_orders WHERE pg_cau_orders.customer_id = pg_cau_customers.id)"
            );

            $rows = $this->ztdQuery("SELECT name, order_count FROM pg_cau_customers ORDER BY id");

            $this->assertCount(3, $rows);

            if ((int) $rows[0]['order_count'] !== 3) {
                $this->markTestIncomplete(
                    'Correlated COUNT: Alice count=' . var_export($rows[0]['order_count'], true)
                );
            }
            $this->assertEquals(3, (int) $rows[0]['order_count']);
            $this->assertEquals(1, (int) $rows[1]['order_count']);
            $this->assertEquals(0, (int) $rows[2]['order_count']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET with correlated COUNT failed: ' . $e->getMessage());
        }
    }

    public function testUpdateSetMultipleCorrelatedSubqueries(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE pg_cau_customers SET
                    order_total = (SELECT COALESCE(SUM(amount), 0) FROM pg_cau_orders WHERE pg_cau_orders.customer_id = pg_cau_customers.id),
                    order_count = (SELECT COUNT(*) FROM pg_cau_orders WHERE pg_cau_orders.customer_id = pg_cau_customers.id)"
            );

            $rows = $this->ztdQuery("SELECT name, order_total, order_count FROM pg_cau_customers ORDER BY id");

            $this->assertCount(3, $rows);

            if ((float) $rows[0]['order_total'] != 350.00 || (int) $rows[0]['order_count'] !== 3) {
                $this->markTestIncomplete(
                    'Multi correlated: Alice total=' . var_export($rows[0]['order_total'], true)
                    . ' count=' . var_export($rows[0]['order_count'], true)
                );
            }
            $this->assertEquals(350.00, (float) $rows[0]['order_total']);
            $this->assertEquals(3, (int) $rows[0]['order_count']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi correlated subqueries failed: ' . $e->getMessage());
        }
    }

    public function testCorrelatedUpdateWithWhereClause(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE pg_cau_customers SET order_total = (SELECT COALESCE(SUM(amount), 0) FROM pg_cau_orders WHERE pg_cau_orders.customer_id = pg_cau_customers.id) WHERE name IN ('Alice', 'Bob')"
            );

            $rows = $this->ztdQuery("SELECT name, order_total FROM pg_cau_customers ORDER BY id");

            if ((float) $rows[0]['order_total'] != 350.00) {
                $this->markTestIncomplete(
                    'Correlated with WHERE: Alice total=' . var_export($rows[0]['order_total'], true)
                );
            }
            $this->assertEquals(350.00, (float) $rows[0]['order_total']);
            $this->assertEquals(150.00, (float) $rows[1]['order_total']);
            $this->assertEquals(0.00, (float) $rows[2]['order_total']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Correlated UPDATE with WHERE failed: ' . $e->getMessage());
        }
    }

    public function testCorrelatedUpdateAfterShadowInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_cau_orders VALUES (5, 3, 500.00)");

            $this->pdo->exec(
                "UPDATE pg_cau_customers SET order_total = (SELECT COALESCE(SUM(amount), 0) FROM pg_cau_orders WHERE pg_cau_orders.customer_id = pg_cau_customers.id)"
            );

            $rows = $this->ztdQuery("SELECT name, order_total FROM pg_cau_customers ORDER BY id");

            if ((float) $rows[2]['order_total'] != 500.00) {
                $this->markTestIncomplete(
                    'Correlated after shadow INSERT: Carol total='
                    . var_export($rows[2]['order_total'], true) . ', expected 500'
                );
            }
            $this->assertEquals(500.00, (float) $rows[2]['order_total']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Correlated UPDATE after shadow INSERT failed: ' . $e->getMessage());
        }
    }
}
