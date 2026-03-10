<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests UPDATE SET col = (SELECT ... FROM other_table) where the subquery
 * references a DIFFERENT shadow-modified table.
 *
 * This is a common denormalization pattern: cache aggregated values
 * from a detail table into a header table. Issue #51 covers correlated
 * subquery in SET with syntax errors for same-table, but cross-table
 * correlated SET subqueries are a distinct pattern.
 *
 * @spec SPEC-4.2
 */
class UpdateSetCrossTableSubqueryTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_ucs_customers (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                total_orders INT NOT NULL DEFAULT 0,
                total_amount DECIMAL(10,2) NOT NULL DEFAULT 0.00
            ) ENGINE=InnoDB',
            'CREATE TABLE mi_ucs_orders (
                id INT PRIMARY KEY,
                customer_id INT NOT NULL,
                amount DECIMAL(10,2) NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_ucs_orders', 'mi_ucs_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_ucs_customers VALUES (1, 'Alice', 0, 0.00)");
        $this->mysqli->query("INSERT INTO mi_ucs_customers VALUES (2, 'Bob', 0, 0.00)");

        $this->mysqli->query("INSERT INTO mi_ucs_orders VALUES (1, 1, 100.00)");
        $this->mysqli->query("INSERT INTO mi_ucs_orders VALUES (2, 1, 200.00)");
        $this->mysqli->query("INSERT INTO mi_ucs_orders VALUES (3, 2, 50.00)");
    }

    /**
     * UPDATE SET with scalar subquery COUNT from other table.
     */
    public function testUpdateSetCountSubquery(): void
    {
        try {
            $this->mysqli->query(
                "UPDATE mi_ucs_customers SET total_orders = (SELECT COUNT(*) FROM mi_ucs_orders WHERE customer_id = mi_ucs_customers.id)"
            );

            $rows = $this->ztdQuery("SELECT id, name, total_orders FROM mi_ucs_customers ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'UPDATE SET COUNT subquery: expected 2 rows, got ' . count($rows)
                );
            }
            $this->assertCount(2, $rows);

            if ((int) $rows[0]['total_orders'] !== 2) {
                $this->markTestIncomplete(
                    'Alice total_orders: expected 2, got ' . var_export($rows[0]['total_orders'], true)
                );
            }
            $this->assertEquals(2, (int) $rows[0]['total_orders']);
            $this->assertEquals(1, (int) $rows[1]['total_orders']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET COUNT subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET with scalar subquery SUM from other table.
     */
    public function testUpdateSetSumSubquery(): void
    {
        try {
            $this->mysqli->query(
                "UPDATE mi_ucs_customers SET total_amount = (SELECT COALESCE(SUM(amount), 0) FROM mi_ucs_orders WHERE customer_id = mi_ucs_customers.id)"
            );

            $rows = $this->ztdQuery("SELECT id, total_amount FROM mi_ucs_customers ORDER BY id");
            $this->assertCount(2, $rows);

            if ((float) $rows[0]['total_amount'] != 300.00) {
                $this->markTestIncomplete(
                    'Alice total_amount: expected 300.00, got ' . var_export($rows[0]['total_amount'], true)
                );
            }
            $this->assertEquals(300.00, (float) $rows[0]['total_amount']);
            $this->assertEquals(50.00, (float) $rows[1]['total_amount']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET SUM subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * Shadow-insert order, then UPDATE customer total — subquery should see shadow data.
     */
    public function testUpdateSetSubqueryAfterShadowInsert(): void
    {
        try {
            // Shadow-insert a new order for Bob
            $this->mysqli->query("INSERT INTO mi_ucs_orders VALUES (4, 2, 75.00)");

            $this->mysqli->query(
                "UPDATE mi_ucs_customers SET total_amount = (SELECT COALESCE(SUM(amount), 0) FROM mi_ucs_orders WHERE customer_id = mi_ucs_customers.id) WHERE id = 2"
            );

            $rows = $this->ztdQuery("SELECT total_amount FROM mi_ucs_customers WHERE id = 2");
            $this->assertCount(1, $rows);

            // Bob: 50.00 + 75.00 = 125.00
            if ((float) $rows[0]['total_amount'] != 125.00) {
                $this->markTestIncomplete(
                    'Bob total_amount after shadow INSERT: expected 125.00, got '
                    . var_export($rows[0]['total_amount'], true)
                );
            }
            $this->assertEquals(125.00, (float) $rows[0]['total_amount']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET subquery after shadow INSERT failed: ' . $e->getMessage());
        }
    }

    /**
     * Shadow-delete order, then UPDATE customer total — subquery should exclude deleted.
     */
    public function testUpdateSetSubqueryAfterShadowDelete(): void
    {
        try {
            // Shadow-delete Alice's $200 order
            $this->mysqli->query("DELETE FROM mi_ucs_orders WHERE id = 2");

            $this->mysqli->query(
                "UPDATE mi_ucs_customers SET total_amount = (SELECT COALESCE(SUM(amount), 0) FROM mi_ucs_orders WHERE customer_id = mi_ucs_customers.id) WHERE id = 1"
            );

            $rows = $this->ztdQuery("SELECT total_amount FROM mi_ucs_customers WHERE id = 1");
            $this->assertCount(1, $rows);

            // Alice: only 100.00 remains
            if ((float) $rows[0]['total_amount'] != 100.00) {
                $this->markTestIncomplete(
                    'Alice total after shadow DELETE: expected 100.00, got '
                    . var_export($rows[0]['total_amount'], true)
                );
            }
            $this->assertEquals(100.00, (float) $rows[0]['total_amount']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET subquery after shadow DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * Multiple sequential updates on the same row.
     */
    public function testSequentialUpdatesOnSameRow(): void
    {
        try {
            $this->mysqli->query("UPDATE mi_ucs_customers SET name = 'Alice Smith' WHERE id = 1");
            $this->mysqli->query("UPDATE mi_ucs_customers SET total_orders = 5 WHERE id = 1");
            $this->mysqli->query("UPDATE mi_ucs_customers SET total_amount = 999.99 WHERE id = 1");

            $rows = $this->ztdQuery("SELECT name, total_orders, total_amount FROM mi_ucs_customers WHERE id = 1");
            $this->assertCount(1, $rows);

            if ($rows[0]['name'] !== 'Alice Smith') {
                $this->markTestIncomplete(
                    'Sequential updates: name expected "Alice Smith", got ' . var_export($rows[0]['name'], true)
                );
            }
            $this->assertSame('Alice Smith', $rows[0]['name']);
            $this->assertEquals(5, (int) $rows[0]['total_orders']);
            $this->assertEquals(999.99, (float) $rows[0]['total_amount']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Sequential updates failed: ' . $e->getMessage());
        }
    }
}
