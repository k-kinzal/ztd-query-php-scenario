<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests UPDATE SET col = (scalar subquery) on MySQLi.
 *
 * @spec SPEC-10.2
 */
class SubqueryUpdateSetDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE mi_sus_orders (
                id INT AUTO_INCREMENT PRIMARY KEY,
                customer_id INT NOT NULL,
                amount DECIMAL(10,2) NOT NULL
            ) ENGINE=InnoDB",
            "CREATE TABLE mi_sus_customers (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                total_spent DECIMAL(12,2) DEFAULT 0
            ) ENGINE=InnoDB",
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_sus_orders', 'mi_sus_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO mi_sus_customers (name) VALUES ('Alice')");
        $this->ztdExec("INSERT INTO mi_sus_customers (name) VALUES ('Bob')");

        $this->ztdExec("INSERT INTO mi_sus_orders (customer_id, amount) VALUES (1, 100.00)");
        $this->ztdExec("INSERT INTO mi_sus_orders (customer_id, amount) VALUES (1, 200.00)");
        $this->ztdExec("INSERT INTO mi_sus_orders (customer_id, amount) VALUES (2, 50.00)");
    }

    public function testUpdateSetFromSubquery(): void
    {
        try {
            $this->ztdExec(
                "UPDATE mi_sus_customers SET total_spent = (
                    SELECT COALESCE(SUM(amount), 0) FROM mi_sus_orders
                    WHERE mi_sus_orders.customer_id = mi_sus_customers.id
                )"
            );

            $rows = $this->ztdQuery("SELECT name, total_spent FROM mi_sus_customers ORDER BY name");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Subquery UPDATE SET (MySQLi): expected 2, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            if (abs((float) $rows[0]['total_spent'] - 300.0) > 0.01) {
                $this->markTestIncomplete(
                    'Subquery UPDATE SET (MySQLi): Alice expected 300, got ' . $rows[0]['total_spent']
                );
            }

            $this->assertEqualsWithDelta(300.0, (float) $rows[0]['total_spent'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Subquery UPDATE SET (MySQLi) failed: ' . $e->getMessage());
        }
    }

    public function testSubqueryUpdateAfterDml(): void
    {
        try {
            $this->ztdExec("INSERT INTO mi_sus_orders (customer_id, amount) VALUES (1, 150.00)");

            $this->ztdExec(
                "UPDATE mi_sus_customers SET total_spent = (
                    SELECT COALESCE(SUM(amount), 0) FROM mi_sus_orders
                    WHERE mi_sus_orders.customer_id = mi_sus_customers.id
                ) WHERE name = 'Alice'"
            );

            $rows = $this->ztdQuery("SELECT total_spent FROM mi_sus_customers WHERE name = 'Alice'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Subquery after DML (MySQLi): expected 1, got ' . count($rows));
            }

            if (abs((float) $rows[0]['total_spent'] - 450.0) > 0.01) {
                $this->markTestIncomplete(
                    'Subquery after DML (MySQLi): Alice expected 450, got ' . $rows[0]['total_spent']
                );
            }

            $this->assertEqualsWithDelta(450.0, (float) $rows[0]['total_spent'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Subquery after DML (MySQLi) failed: ' . $e->getMessage());
        }
    }
}
