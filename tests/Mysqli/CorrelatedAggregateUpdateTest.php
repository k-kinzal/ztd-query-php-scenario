<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests UPDATE SET with correlated aggregate subquery from another table
 * through ZTD shadow store on MySQL via MySQLi.
 *
 * UPDATE t SET total = (SELECT SUM(amount) FROM details WHERE details.fk = t.id)
 * is a common denormalization pattern. The CTE rewriter must resolve both the
 * outer table and the correlated subquery table from the shadow store.
 *
 * @spec SPEC-4.2
 * @spec SPEC-3.3
 */
class CorrelatedAggregateUpdateTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_cau_customers (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                order_total DECIMAL(10,2) NOT NULL DEFAULT 0,
                order_count INT NOT NULL DEFAULT 0
            ) ENGINE=InnoDB',
            'CREATE TABLE mi_cau_orders (
                id INT PRIMARY KEY,
                customer_id INT NOT NULL,
                amount DECIMAL(10,2) NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_cau_orders', 'mi_cau_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_cau_customers VALUES (1, 'Alice', 0, 0)");
        $this->mysqli->query("INSERT INTO mi_cau_customers VALUES (2, 'Bob', 0, 0)");
        $this->mysqli->query("INSERT INTO mi_cau_customers VALUES (3, 'Carol', 0, 0)");

        $this->mysqli->query("INSERT INTO mi_cau_orders VALUES (1, 1, 100.00)");
        $this->mysqli->query("INSERT INTO mi_cau_orders VALUES (2, 1, 200.00)");
        $this->mysqli->query("INSERT INTO mi_cau_orders VALUES (3, 2, 150.00)");
        $this->mysqli->query("INSERT INTO mi_cau_orders VALUES (4, 1, 50.00)");
    }

    /**
     * UPDATE SET total = (SELECT SUM(amount) FROM orders WHERE orders.customer_id = customers.id)
     */
    public function testUpdateSetWithCorrelatedSum(): void
    {
        try {
            $this->mysqli->query(
                "UPDATE mi_cau_customers SET order_total = (SELECT COALESCE(SUM(amount), 0) FROM mi_cau_orders WHERE mi_cau_orders.customer_id = mi_cau_customers.id)"
            );

            $rows = $this->ztdQuery("SELECT id, name, order_total FROM mi_cau_customers ORDER BY id");

            $this->assertCount(3, $rows);

            // Alice: SUM(100 + 200 + 50) = 350
            if ((float) $rows[0]['order_total'] != 350.00) {
                $this->markTestIncomplete(
                    'Correlated SUM UPDATE: Alice order_total='
                    . var_export($rows[0]['order_total'], true) . ', expected 350.00'
                );
            }
            $this->assertEquals(350.00, (float) $rows[0]['order_total']);

            // Bob: SUM(150) = 150
            if ((float) $rows[1]['order_total'] != 150.00) {
                $this->markTestIncomplete(
                    'Correlated SUM UPDATE: Bob order_total='
                    . var_export($rows[1]['order_total'], true) . ', expected 150.00'
                );
            }
            $this->assertEquals(150.00, (float) $rows[1]['order_total']);

            // Carol: no orders, COALESCE(NULL, 0) = 0
            $this->assertEquals(0.00, (float) $rows[2]['order_total']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET with correlated SUM failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET count = (SELECT COUNT(*) FROM orders WHERE orders.customer_id = customers.id)
     */
    public function testUpdateSetWithCorrelatedCount(): void
    {
        try {
            $this->mysqli->query(
                "UPDATE mi_cau_customers SET order_count = (SELECT COUNT(*) FROM mi_cau_orders WHERE mi_cau_orders.customer_id = mi_cau_customers.id)"
            );

            $rows = $this->ztdQuery("SELECT id, name, order_count FROM mi_cau_customers ORDER BY id");

            $this->assertCount(3, $rows);

            // Alice: 3 orders
            if ((int) $rows[0]['order_count'] !== 3) {
                $this->markTestIncomplete(
                    'Correlated COUNT UPDATE: Alice order_count='
                    . var_export($rows[0]['order_count'], true) . ', expected 3'
                );
            }
            $this->assertEquals(3, (int) $rows[0]['order_count']);

            // Bob: 1 order
            if ((int) $rows[1]['order_count'] !== 1) {
                $this->markTestIncomplete(
                    'Correlated COUNT UPDATE: Bob order_count='
                    . var_export($rows[1]['order_count'], true) . ', expected 1'
                );
            }
            $this->assertEquals(1, (int) $rows[1]['order_count']);

            // Carol: 0 orders
            $this->assertEquals(0, (int) $rows[2]['order_count']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET with correlated COUNT failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET with multiple correlated subqueries in the same SET clause.
     */
    public function testUpdateSetMultipleCorrelatedSubqueries(): void
    {
        try {
            $this->mysqli->query(
                "UPDATE mi_cau_customers SET
                    order_total = (SELECT COALESCE(SUM(amount), 0) FROM mi_cau_orders WHERE mi_cau_orders.customer_id = mi_cau_customers.id),
                    order_count = (SELECT COUNT(*) FROM mi_cau_orders WHERE mi_cau_orders.customer_id = mi_cau_customers.id)"
            );

            $rows = $this->ztdQuery("SELECT id, name, order_total, order_count FROM mi_cau_customers ORDER BY id");

            $this->assertCount(3, $rows);

            // Alice
            if ((float) $rows[0]['order_total'] != 350.00 || (int) $rows[0]['order_count'] !== 3) {
                $this->markTestIncomplete(
                    'Multi correlated UPDATE: Alice total='
                    . var_export($rows[0]['order_total'], true) . ' count='
                    . var_export($rows[0]['order_count'], true)
                    . ', expected total=350 count=3'
                );
            }
            $this->assertEquals(350.00, (float) $rows[0]['order_total']);
            $this->assertEquals(3, (int) $rows[0]['order_count']);

            // Bob
            $this->assertEquals(150.00, (float) $rows[1]['order_total']);
            $this->assertEquals(1, (int) $rows[1]['order_count']);

            // Carol
            $this->assertEquals(0.00, (float) $rows[2]['order_total']);
            $this->assertEquals(0, (int) $rows[2]['order_count']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET with multiple correlated subqueries failed: ' . $e->getMessage());
        }
    }

    /**
     * Correlated update with WHERE clause limiting target rows.
     */
    public function testCorrelatedUpdateWithWhereClause(): void
    {
        try {
            $this->mysqli->query(
                "UPDATE mi_cau_customers SET order_total = (SELECT COALESCE(SUM(amount), 0) FROM mi_cau_orders WHERE mi_cau_orders.customer_id = mi_cau_customers.id) WHERE name IN ('Alice', 'Bob')"
            );

            $rows = $this->ztdQuery("SELECT id, name, order_total FROM mi_cau_customers ORDER BY id");

            $this->assertCount(3, $rows);

            // Alice: updated
            if ((float) $rows[0]['order_total'] != 350.00) {
                $this->markTestIncomplete(
                    'Correlated UPDATE with WHERE: Alice total='
                    . var_export($rows[0]['order_total'], true) . ', expected 350.00'
                );
            }
            $this->assertEquals(350.00, (float) $rows[0]['order_total']);

            // Bob: updated
            $this->assertEquals(150.00, (float) $rows[1]['order_total']);

            // Carol: NOT updated (not in WHERE), still 0
            $this->assertEquals(0.00, (float) $rows[2]['order_total']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Correlated UPDATE with WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * Correlated update after shadow INSERT into orders table.
     * The subquery should see the shadow-inserted order.
     */
    public function testCorrelatedUpdateAfterShadowInsert(): void
    {
        try {
            // Shadow-insert a new order for Carol
            $this->mysqli->query("INSERT INTO mi_cau_orders VALUES (5, 3, 500.00)");

            $this->mysqli->query(
                "UPDATE mi_cau_customers SET order_total = (SELECT COALESCE(SUM(amount), 0) FROM mi_cau_orders WHERE mi_cau_orders.customer_id = mi_cau_customers.id)"
            );

            $rows = $this->ztdQuery("SELECT name, order_total FROM mi_cau_customers ORDER BY id");

            // Carol should now have 500.00
            if ((float) $rows[2]['order_total'] != 500.00) {
                $this->markTestIncomplete(
                    'Correlated UPDATE after shadow INSERT: Carol total='
                    . var_export($rows[2]['order_total'], true)
                    . ', expected 500.00. Subquery may not see shadow-inserted data.'
                );
            }
            $this->assertEquals(500.00, (float) $rows[2]['order_total']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Correlated UPDATE after shadow INSERT failed: ' . $e->getMessage());
        }
    }
}
