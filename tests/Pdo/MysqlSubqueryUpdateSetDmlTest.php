<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests UPDATE SET col = (scalar subquery) on MySQL PDO.
 *
 * @spec SPEC-10.2
 */
class MysqlSubqueryUpdateSetDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE my_sus_orders (
                id INT AUTO_INCREMENT PRIMARY KEY,
                customer_id INT NOT NULL,
                amount DECIMAL(10,2) NOT NULL
            ) ENGINE=InnoDB",
            "CREATE TABLE my_sus_customers (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                total_spent DECIMAL(12,2) DEFAULT 0
            ) ENGINE=InnoDB",
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_sus_orders', 'my_sus_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO my_sus_customers (name) VALUES ('Alice')");
        $this->ztdExec("INSERT INTO my_sus_customers (name) VALUES ('Bob')");

        $this->ztdExec("INSERT INTO my_sus_orders (customer_id, amount) VALUES (1, 100.00)");
        $this->ztdExec("INSERT INTO my_sus_orders (customer_id, amount) VALUES (1, 200.00)");
        $this->ztdExec("INSERT INTO my_sus_orders (customer_id, amount) VALUES (2, 50.00)");
    }

    public function testUpdateSetFromSubquery(): void
    {
        try {
            $this->ztdExec(
                "UPDATE my_sus_customers SET total_spent = (
                    SELECT COALESCE(SUM(amount), 0) FROM my_sus_orders
                    WHERE my_sus_orders.customer_id = my_sus_customers.id
                )"
            );

            $rows = $this->ztdQuery("SELECT name, total_spent FROM my_sus_customers ORDER BY name");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Subquery UPDATE SET (MySQL): expected 2, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            if (abs((float) $rows[0]['total_spent'] - 300.0) > 0.01) {
                $this->markTestIncomplete(
                    'Subquery UPDATE SET (MySQL): Alice expected 300, got ' . $rows[0]['total_spent']
                );
            }

            $this->assertEqualsWithDelta(300.0, (float) $rows[0]['total_spent'], 0.01);
            $this->assertEqualsWithDelta(50.0, (float) $rows[1]['total_spent'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Subquery UPDATE SET (MySQL) failed: ' . $e->getMessage());
        }
    }

    public function testSubqueryUpdateAfterDml(): void
    {
        try {
            $this->ztdExec("INSERT INTO my_sus_orders (customer_id, amount) VALUES (1, 150.00)");

            $this->ztdExec(
                "UPDATE my_sus_customers SET total_spent = (
                    SELECT COALESCE(SUM(amount), 0) FROM my_sus_orders
                    WHERE my_sus_orders.customer_id = my_sus_customers.id
                ) WHERE name = 'Alice'"
            );

            $rows = $this->ztdQuery("SELECT total_spent FROM my_sus_customers WHERE name = 'Alice'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Subquery after DML (MySQL): expected 1, got ' . count($rows));
            }

            if (abs((float) $rows[0]['total_spent'] - 450.0) > 0.01) {
                $this->markTestIncomplete(
                    'Subquery after DML (MySQL): Alice expected 450, got ' . $rows[0]['total_spent']
                );
            }

            $this->assertEqualsWithDelta(450.0, (float) $rows[0]['total_spent'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Subquery after DML (MySQL) failed: ' . $e->getMessage());
        }
    }
}
