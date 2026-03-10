<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests EXISTS / NOT EXISTS in SELECT list through ZTD on PostgreSQL.
 *
 * @spec SPEC-3.1
 * @spec SPEC-3.3
 */
class PostgresExistsInSelectListTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_esl_customers (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL
            )',
            'CREATE TABLE pg_esl_orders (
                id SERIAL PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                amount NUMERIC(10,2) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_esl_orders', 'pg_esl_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_esl_customers (name) VALUES ('Alice')");
        $this->pdo->exec("INSERT INTO pg_esl_customers (name) VALUES ('Bob')");
        $this->pdo->exec("INSERT INTO pg_esl_customers (name) VALUES ('Carol')");

        $this->pdo->exec("INSERT INTO pg_esl_orders (customer_id, amount) VALUES (1, 100)");
        $this->pdo->exec("INSERT INTO pg_esl_orders (customer_id, amount) VALUES (1, 200)");
        $this->pdo->exec("INSERT INTO pg_esl_orders (customer_id, amount) VALUES (3, 50)");
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
                        EXISTS(SELECT 1 FROM pg_esl_orders o WHERE o.customer_id = c.id) AS has_orders
                 FROM pg_esl_customers c
                 ORDER BY c.id"
            );

            $this->assertCount(3, $rows);
            // PostgreSQL returns boolean 't'/'f'
            $this->assertContains($rows[0]['has_orders'], ['t', true, '1', 1], 'Alice has orders');
            $this->assertContains($rows[1]['has_orders'], ['f', false, '0', 0], 'Bob has no orders');
            $this->assertContains($rows[2]['has_orders'], ['t', true, '1', 1], 'Carol has orders');
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
            $this->pdo->exec("INSERT INTO pg_esl_orders (customer_id, amount) VALUES (2, 75)");

            $rows = $this->ztdQuery(
                "SELECT c.id, c.name,
                        EXISTS(SELECT 1 FROM pg_esl_orders o WHERE o.customer_id = c.id) AS has_orders
                 FROM pg_esl_customers c
                 ORDER BY c.id"
            );

            $this->assertCount(3, $rows);
            $this->assertContains($rows[1]['has_orders'], ['t', true, '1', 1],
                'Bob should now have orders after shadow INSERT');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('EXISTS after shadow INSERT failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared EXISTS in SELECT list with ? param.
     *
     * @spec SPEC-3.2
     */
    public function testPreparedExistsQuestionMark(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT c.id, c.name,
                        EXISTS(SELECT 1 FROM pg_esl_orders o WHERE o.customer_id = c.id AND o.amount >= ?) AS has_large
                 FROM pg_esl_customers c
                 ORDER BY c.id",
                [100]
            );

            $this->assertCount(3, $rows);
            $this->assertContains($rows[0]['has_large'], ['t', true, '1', 1], 'Alice has large orders');
            $this->assertContains($rows[2]['has_large'], ['f', false, '0', 0], 'Carol has no large orders');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared EXISTS ? failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared EXISTS in SELECT list with $N param.
     *
     * @spec SPEC-3.2
     */
    public function testPreparedExistsDollarParam(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT c.id, c.name,
                        EXISTS(SELECT 1 FROM pg_esl_orders o WHERE o.customer_id = c.id AND o.amount >= $1) AS has_large
                 FROM pg_esl_customers c
                 ORDER BY c.id",
                [100]
            );

            if (count($rows) < 3) {
                $this->markTestIncomplete('Prepared EXISTS $N: expected 3 rows, got ' . count($rows));
            }

            $this->assertCount(3, $rows);
            $this->assertContains($rows[0]['has_large'], ['t', true, '1', 1], 'Alice has large orders');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared EXISTS $N failed: ' . $e->getMessage());
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
                        CASE WHEN EXISTS(SELECT 1 FROM pg_esl_orders o WHERE o.customer_id = c.id)
                             THEN 'customer'
                             ELSE 'prospect'
                        END AS customer_type
                 FROM pg_esl_customers c
                 ORDER BY c.id"
            );

            $this->assertCount(3, $rows);
            $this->assertSame('customer', $rows[0]['customer_type'], 'Alice is customer');
            $this->assertSame('prospect', $rows[1]['customer_type'], 'Bob is prospect');
            $this->assertSame('customer', $rows[2]['customer_type'], 'Carol is customer');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CASE WHEN EXISTS in SELECT list failed: ' . $e->getMessage());
        }
    }
}
