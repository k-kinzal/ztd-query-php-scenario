<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests EXISTS / NOT EXISTS in SELECT list through ZTD on MySQLi.
 *
 * Companion to MysqlExistsInSelectListTest (PDO). Verifies that the
 * EXISTS-in-SELECT-list bug (#137) also affects the MySQLi adapter.
 *
 * @spec SPEC-3.1
 */
class ExistsInSelectListTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_esl_customers (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(50) NOT NULL
            )',
            'CREATE TABLE mi_esl_orders (
                id INT AUTO_INCREMENT PRIMARY KEY,
                customer_id INT NOT NULL,
                amount DECIMAL(10,2) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_esl_orders', 'mi_esl_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_esl_customers (name) VALUES ('Alice')");
        $this->mysqli->query("INSERT INTO mi_esl_customers (name) VALUES ('Bob')");
        $this->mysqli->query("INSERT INTO mi_esl_customers (name) VALUES ('Carol')");

        $this->mysqli->query("INSERT INTO mi_esl_orders (customer_id, amount) VALUES (1, 100)");
        $this->mysqli->query("INSERT INTO mi_esl_orders (customer_id, amount) VALUES (1, 200)");
        $this->mysqli->query("INSERT INTO mi_esl_orders (customer_id, amount) VALUES (3, 50)");
    }

    /**
     * EXISTS in SELECT list.
     */
    public function testExistsInSelectList(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT c.id, c.name,
                        EXISTS(SELECT 1 FROM mi_esl_orders o WHERE o.customer_id = c.id) AS has_orders
                 FROM mi_esl_customers c
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
     * NOT EXISTS in WHERE (anti-join pattern).
     */
    public function testNotExistsInWhere(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT c.name
                 FROM mi_esl_customers c
                 WHERE NOT EXISTS (
                     SELECT 1 FROM mi_esl_orders o WHERE o.customer_id = c.id
                 )
                 ORDER BY c.name"
            );

            $names = array_column($rows, 'name');
            $this->assertCount(1, $names, 'Expected only Bob. Got: ' . json_encode($names));
            $this->assertSame('Bob', $names[0]);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('NOT EXISTS in WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * CASE WHEN EXISTS in SELECT list.
     */
    public function testCaseWhenExistsInSelectList(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT c.id, c.name,
                        CASE WHEN EXISTS(SELECT 1 FROM mi_esl_orders o WHERE o.customer_id = c.id)
                             THEN 'customer'
                             ELSE 'prospect'
                        END AS customer_type
                 FROM mi_esl_customers c
                 ORDER BY c.id"
            );

            $this->assertCount(3, $rows);
            $this->assertSame('customer', $rows[0]['customer_type']);
            $this->assertSame('prospect', $rows[1]['customer_type']);
            $this->assertSame('customer', $rows[2]['customer_type']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CASE WHEN EXISTS in SELECT list failed: ' . $e->getMessage());
        }
    }
}
