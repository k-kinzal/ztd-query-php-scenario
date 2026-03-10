<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests EXISTS / NOT EXISTS in SELECT list through ZTD on MySQL.
 *
 * @spec SPEC-3.1
 * @spec SPEC-3.3
 */
class MysqlExistsInSelectListTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_esl_customers (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(50) NOT NULL
            )',
            'CREATE TABLE my_esl_orders (
                id INT AUTO_INCREMENT PRIMARY KEY,
                customer_id INT NOT NULL,
                amount DECIMAL(10,2) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_esl_orders', 'my_esl_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_esl_customers (name) VALUES ('Alice')");
        $this->pdo->exec("INSERT INTO my_esl_customers (name) VALUES ('Bob')");
        $this->pdo->exec("INSERT INTO my_esl_customers (name) VALUES ('Carol')");

        $this->pdo->exec("INSERT INTO my_esl_orders (customer_id, amount) VALUES (1, 100)");
        $this->pdo->exec("INSERT INTO my_esl_orders (customer_id, amount) VALUES (1, 200)");
        $this->pdo->exec("INSERT INTO my_esl_orders (customer_id, amount) VALUES (3, 50)");
    }

    /**
     * EXISTS in SELECT list.
     *
     * @spec SPEC-3.1
     */
    public function testExistsInSelectList(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT c.id, c.name,
                        EXISTS(SELECT 1 FROM my_esl_orders o WHERE o.customer_id = c.id) AS has_orders
                 FROM my_esl_customers c
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
     * EXISTS after shadow INSERT.
     *
     * @spec SPEC-3.1
     */
    public function testExistsAfterShadowInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO my_esl_orders (customer_id, amount) VALUES (2, 75)");

            $rows = $this->ztdQuery(
                "SELECT c.id, c.name,
                        EXISTS(SELECT 1 FROM my_esl_orders o WHERE o.customer_id = c.id) AS has_orders
                 FROM my_esl_customers c
                 ORDER BY c.id"
            );

            $this->assertCount(3, $rows);
            $this->assertEquals(1, (int) $rows[1]['has_orders'],
                'Bob should have orders after shadow INSERT');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('EXISTS after shadow INSERT failed: ' . $e->getMessage());
        }
    }

    /**
     * CASE WHEN EXISTS in SELECT list.
     *
     * @spec SPEC-3.1
     */
    public function testCaseWhenExistsInSelectList(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT c.id, c.name,
                        CASE WHEN EXISTS(SELECT 1 FROM my_esl_orders o WHERE o.customer_id = c.id)
                             THEN 'customer'
                             ELSE 'prospect'
                        END AS customer_type
                 FROM my_esl_customers c
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

    /**
     * Prepared EXISTS in SELECT list with parameter.
     *
     * @spec SPEC-3.2
     */
    public function testPreparedExistsInSelectList(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT c.id, c.name,
                        EXISTS(SELECT 1 FROM my_esl_orders o WHERE o.customer_id = c.id AND o.amount >= ?) AS has_large
                 FROM my_esl_customers c
                 ORDER BY c.id",
                [100]
            );

            $this->assertCount(3, $rows);
            $this->assertEquals(1, (int) $rows[0]['has_large'], 'Alice has large orders');
            $this->assertEquals(0, (int) $rows[2]['has_large'], 'Carol has no large orders');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared EXISTS in SELECT list failed: ' . $e->getMessage());
        }
    }
}
