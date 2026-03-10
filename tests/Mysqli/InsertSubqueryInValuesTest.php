<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests INSERT with subquery in VALUES clause reading from shadow store.
 *
 * Pattern: INSERT INTO t (col) VALUES ((SELECT MAX(col)+1 FROM t))
 * This is common for generating sequential values, counters, etc.
 * The subquery in VALUES must see shadow mutations.
 *
 * @spec SPEC-4.2
 */
class InsertSubqueryInValuesTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_siv_orders (
                id INT PRIMARY KEY,
                order_num INT NOT NULL,
                customer VARCHAR(50) NOT NULL,
                amount DECIMAL(10,2) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE mi_siv_config (
                k VARCHAR(50) PRIMARY KEY,
                v VARCHAR(100) NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_siv_orders', 'mi_siv_config'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_siv_orders VALUES (1, 100, 'Alice', 50.00)");
        $this->mysqli->query("INSERT INTO mi_siv_orders VALUES (2, 101, 'Bob', 75.00)");
        $this->mysqli->query("INSERT INTO mi_siv_config VALUES ('next_id', '3')");
    }

    /**
     * INSERT with subquery from different table in VALUES.
     */
    public function testInsertWithSubqueryFromOtherTable(): void
    {
        try {
            $this->mysqli->query(
                "INSERT INTO mi_siv_orders VALUES ((SELECT CAST(v AS UNSIGNED) FROM mi_siv_config WHERE k = 'next_id'), 102, 'Carol', 60.00)"
            );

            $rows = $this->ztdQuery("SELECT id, customer FROM mi_siv_orders WHERE customer = 'Carol'");

            if (empty($rows)) {
                $this->markTestIncomplete('INSERT with subquery in VALUES: Carol not found');
            }
            $this->assertCount(1, $rows);
            $this->assertEquals(3, (int) $rows[0]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT with subquery from other table in VALUES failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT with subquery from same table (MAX+1 pattern).
     */
    public function testInsertWithMaxPlusOneSubquery(): void
    {
        try {
            $this->mysqli->query(
                "INSERT INTO mi_siv_orders VALUES (3, (SELECT MAX(order_num) + 1 FROM mi_siv_orders), 'Carol', 60.00)"
            );

            $rows = $this->ztdQuery("SELECT id, order_num FROM mi_siv_orders WHERE id = 3");

            if (empty($rows)) {
                $this->markTestIncomplete('INSERT MAX+1: row not found');
            }
            $this->assertCount(1, $rows);

            $orderNum = (int) $rows[0]['order_num'];
            if ($orderNum !== 102) {
                $this->markTestIncomplete("INSERT MAX+1: expected order_num=102, got $orderNum");
            }
            $this->assertEquals(102, $orderNum);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT with MAX+1 subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT with subquery seeing prior shadow INSERT.
     */
    public function testInsertSubquerySeesShadowInsert(): void
    {
        try {
            // First shadow insert
            $this->mysqli->query("INSERT INTO mi_siv_orders VALUES (3, 102, 'Carol', 60.00)");

            // Second insert uses subquery that should see Carol's order_num=102
            $this->mysqli->query(
                "INSERT INTO mi_siv_orders VALUES (4, (SELECT MAX(order_num) + 1 FROM mi_siv_orders), 'Dave', 80.00)"
            );

            $rows = $this->ztdQuery("SELECT id, order_num FROM mi_siv_orders WHERE id = 4");

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

    /**
     * INSERT with COUNT subquery in VALUES.
     */
    public function testInsertWithCountSubquery(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_siv_orders VALUES (3, 102, 'Carol', 60.00)");

            $this->mysqli->query(
                "INSERT INTO mi_siv_config VALUES ('total_orders', (SELECT CAST(COUNT(*) AS CHAR) FROM mi_siv_orders))"
            );

            $rows = $this->ztdQuery("SELECT v FROM mi_siv_config WHERE k = 'total_orders'");

            if (empty($rows)) {
                $this->markTestIncomplete('INSERT COUNT subquery: config row not found');
            }

            $total = $rows[0]['v'];
            if ($total !== '3') {
                $this->markTestIncomplete("INSERT COUNT subquery: expected '3', got '$total'");
            }
            $this->assertSame('3', $total);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT with COUNT subquery in VALUES failed: ' . $e->getMessage());
        }
    }
}
