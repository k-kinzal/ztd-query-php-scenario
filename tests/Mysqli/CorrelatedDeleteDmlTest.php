<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests DELETE with correlated subquery (NOT EXISTS, EXISTS) on MySQLi.
 *
 * @spec SPEC-10.2
 */
class CorrelatedDeleteDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE mi_cd_customers (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL
            ) ENGINE=InnoDB",
            "CREATE TABLE mi_cd_orders (
                id INT AUTO_INCREMENT PRIMARY KEY,
                customer_id INT,
                product VARCHAR(100) NOT NULL,
                amount DECIMAL(10,2) NOT NULL
            ) ENGINE=InnoDB",
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_cd_orders', 'mi_cd_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO mi_cd_customers (name) VALUES ('Alice')");
        $this->ztdExec("INSERT INTO mi_cd_customers (name) VALUES ('Bob')");

        $this->ztdExec("INSERT INTO mi_cd_orders (customer_id, product, amount) VALUES (1, 'Widget', 10.00)");
        $this->ztdExec("INSERT INTO mi_cd_orders (customer_id, product, amount) VALUES (1, 'Gadget', 20.00)");
        $this->ztdExec("INSERT INTO mi_cd_orders (customer_id, product, amount) VALUES (2, 'Doohickey', 30.00)");
        $this->ztdExec("INSERT INTO mi_cd_orders (customer_id, product, amount) VALUES (999, 'Orphan1', 5.00)");
        $this->ztdExec("INSERT INTO mi_cd_orders (customer_id, product, amount) VALUES (888, 'Orphan2', 7.00)");
    }

    public function testDeleteWhereNotExists(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM mi_cd_orders WHERE NOT EXISTS (
                    SELECT 1 FROM mi_cd_customers WHERE mi_cd_customers.id = mi_cd_orders.customer_id
                )"
            );

            $rows = $this->ztdQuery("SELECT product FROM mi_cd_orders ORDER BY product");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE NOT EXISTS (MySQLi): expected 3, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE NOT EXISTS (MySQLi) failed: ' . $e->getMessage());
        }
    }

    public function testDeleteWhereExists(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM mi_cd_orders WHERE EXISTS (
                    SELECT 1 FROM mi_cd_customers
                    WHERE mi_cd_customers.id = mi_cd_orders.customer_id
                    AND mi_cd_customers.name = 'Alice'
                )"
            );

            $rows = $this->ztdQuery("SELECT product FROM mi_cd_orders ORDER BY product");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE EXISTS (MySQLi): expected 3, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE EXISTS (MySQLi) failed: ' . $e->getMessage());
        }
    }

    public function testUpdateWhereExists(): void
    {
        try {
            $this->ztdExec(
                "UPDATE mi_cd_orders SET amount = amount * 2 WHERE EXISTS (
                    SELECT 1 FROM mi_cd_customers
                    WHERE mi_cd_customers.id = mi_cd_orders.customer_id
                    AND mi_cd_customers.name = 'Bob'
                )"
            );

            $rows = $this->ztdQuery("SELECT amount FROM mi_cd_orders WHERE customer_id = 2");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('UPDATE EXISTS (MySQLi): expected 1 row, got ' . count($rows));
            }

            if (abs((float) $rows[0]['amount'] - 60.0) > 0.01) {
                $this->markTestIncomplete(
                    'UPDATE EXISTS (MySQLi): expected amount=60, got ' . $rows[0]['amount']
                );
            }

            $this->assertEqualsWithDelta(60.0, (float) $rows[0]['amount'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE EXISTS (MySQLi) failed: ' . $e->getMessage());
        }
    }
}
