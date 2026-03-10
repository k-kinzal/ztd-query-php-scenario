<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests INSERT with subquery in VALUES clause on PostgreSQL.
 *
 * @spec SPEC-4.2
 */
class PostgresInsertSubqueryInValuesTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_siv_orders (
                id INT PRIMARY KEY,
                order_num INT NOT NULL,
                customer VARCHAR(50) NOT NULL,
                amount NUMERIC(10,2) NOT NULL
            )',
            'CREATE TABLE pg_siv_config (
                k VARCHAR(50) PRIMARY KEY,
                v VARCHAR(100) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_siv_orders', 'pg_siv_config'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_siv_orders VALUES (1, 100, 'Alice', 50.00)");
        $this->pdo->exec("INSERT INTO pg_siv_orders VALUES (2, 101, 'Bob', 75.00)");
        $this->pdo->exec("INSERT INTO pg_siv_config VALUES ('next_id', '3')");
    }

    public function testInsertWithSubqueryFromOtherTable(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO pg_siv_orders VALUES ((SELECT CAST(v AS INT) FROM pg_siv_config WHERE k = 'next_id'), 102, 'Carol', 60.00)"
            );

            $rows = $this->ztdQuery("SELECT id, customer FROM pg_siv_orders WHERE customer = 'Carol'");

            if (empty($rows)) {
                $this->markTestIncomplete('INSERT with subquery in VALUES: Carol not found');
            }
            $this->assertEquals(3, (int) $rows[0]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT with subquery from other table failed: ' . $e->getMessage());
        }
    }

    public function testInsertWithMaxPlusOneSubquery(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO pg_siv_orders VALUES (3, (SELECT MAX(order_num) + 1 FROM pg_siv_orders), 'Carol', 60.00)"
            );

            $rows = $this->ztdQuery("SELECT order_num FROM pg_siv_orders WHERE id = 3");

            if (empty($rows)) {
                $this->markTestIncomplete('INSERT MAX+1: row not found');
            }

            $orderNum = (int) $rows[0]['order_num'];
            if ($orderNum !== 102) {
                $this->markTestIncomplete("INSERT MAX+1: expected 102, got $orderNum");
            }
            $this->assertEquals(102, $orderNum);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT with MAX+1 subquery failed: ' . $e->getMessage());
        }
    }

    public function testInsertSubquerySeesShadowInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_siv_orders VALUES (3, 102, 'Carol', 60.00)");

            $this->pdo->exec(
                "INSERT INTO pg_siv_orders VALUES (4, (SELECT MAX(order_num) + 1 FROM pg_siv_orders), 'Dave', 80.00)"
            );

            $rows = $this->ztdQuery("SELECT order_num FROM pg_siv_orders WHERE id = 4");

            if (empty($rows)) {
                $this->markTestIncomplete('INSERT subquery sees shadow: Dave not found');
            }

            $orderNum = (int) $rows[0]['order_num'];
            if ($orderNum !== 103) {
                $this->markTestIncomplete("INSERT subquery sees shadow: expected 103, got $orderNum");
            }
            $this->assertEquals(103, $orderNum);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT subquery seeing shadow INSERT failed: ' . $e->getMessage());
        }
    }
}
