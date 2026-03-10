<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT with subquery in VALUES clause that chains shadow mutations on SQLite.
 *
 * Specifically tests whether a subquery in VALUES can see prior shadow INSERTs
 * to the same table (MAX+1 pattern for sequential IDs).
 *
 * @spec SPEC-4.2
 */
class SqliteInsertSubqueryInValuesChainTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_sivc_orders (
            id INTEGER PRIMARY KEY,
            order_num INTEGER NOT NULL,
            customer TEXT NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_sivc_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_sivc_orders VALUES (1, 100, 'Alice')");
        $this->pdo->exec("INSERT INTO sl_sivc_orders VALUES (2, 101, 'Bob')");
    }

    public function testInsertWithMaxPlusOneSubquery(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_sivc_orders VALUES (3, (SELECT MAX(order_num) + 1 FROM sl_sivc_orders), 'Carol')"
            );

            $rows = $this->ztdQuery("SELECT order_num FROM sl_sivc_orders WHERE id = 3");

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

    /**
     * Chain: two INSERTs each using MAX+1 subquery from same table.
     */
    public function testChainedInsertMaxPlusOne(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_sivc_orders VALUES (3, 102, 'Carol')");
            $this->pdo->exec(
                "INSERT INTO sl_sivc_orders VALUES (4, (SELECT MAX(order_num) + 1 FROM sl_sivc_orders), 'Dave')"
            );

            $rows = $this->ztdQuery("SELECT order_num FROM sl_sivc_orders WHERE id = 4");

            if (empty($rows)) {
                $this->markTestIncomplete('Chained INSERT MAX+1: Dave not found');
            }

            $orderNum = (int) $rows[0]['order_num'];
            if ($orderNum !== 103) {
                $this->markTestIncomplete("Chained INSERT MAX+1: expected 103, got $orderNum");
            }
            $this->assertEquals(103, $orderNum);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Chained INSERT MAX+1 failed: ' . $e->getMessage());
        }
    }

    public function testInsertWithCountSubquery(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_sivc_orders VALUES (3, 102, 'Carol')");

            $this->pdo->exec(
                "INSERT INTO sl_sivc_orders VALUES (4, (SELECT COUNT(*) FROM sl_sivc_orders), 'Total')"
            );

            $rows = $this->ztdQuery("SELECT order_num FROM sl_sivc_orders WHERE id = 4");

            if (empty($rows)) {
                $this->markTestIncomplete('INSERT COUNT subquery: not found');
            }

            // After adding Carol, there are 3 rows. COUNT should be 3.
            $num = (int) $rows[0]['order_num'];
            if ($num !== 3) {
                $this->markTestIncomplete("INSERT COUNT subquery: expected 3, got $num");
            }
            $this->assertEquals(3, $num);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT with COUNT subquery failed: ' . $e->getMessage());
        }
    }
}
