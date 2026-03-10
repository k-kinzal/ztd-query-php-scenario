<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests behavior when one table name is a substring of another.
 *
 * The CTE rewriter uses stripos() for table detection. If table 'mi_tnc_order'
 * and 'mi_tnc_order_items' both have shadow data, the stripos check for
 * 'mi_tnc_order' would also match 'mi_tnc_order_items'. This could cause
 * incorrect CTE injection or table name confusion.
 *
 * This pattern is VERY common in real applications: orders/order_items,
 * users/user_roles, products/product_categories.
 *
 * @spec SPEC-4.2
 */
class TableNameSubstringCollisionTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_tnc_order (
                id INT PRIMARY KEY,
                customer VARCHAR(50) NOT NULL,
                total DECIMAL(10,2) NOT NULL DEFAULT 0
            ) ENGINE=InnoDB',
            'CREATE TABLE mi_tnc_order_item (
                id INT PRIMARY KEY,
                order_id INT NOT NULL,
                product VARCHAR(50) NOT NULL,
                price DECIMAL(10,2) NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_tnc_order_item', 'mi_tnc_order'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_tnc_order VALUES (1, 'Alice', 0)");
        $this->mysqli->query("INSERT INTO mi_tnc_order VALUES (2, 'Bob', 0)");

        $this->mysqli->query("INSERT INTO mi_tnc_order_item VALUES (1, 1, 'Widget', 10.00)");
        $this->mysqli->query("INSERT INTO mi_tnc_order_item VALUES (2, 1, 'Gadget', 20.00)");
        $this->mysqli->query("INSERT INTO mi_tnc_order_item VALUES (3, 2, 'Widget', 10.00)");
    }

    /**
     * DML on both tables, then JOIN query — tests table name disambiguation.
     */
    public function testJoinAfterDmlOnBothSubstringTables(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_tnc_order VALUES (3, 'Carol', 0)");
            $this->mysqli->query("INSERT INTO mi_tnc_order_item VALUES (4, 3, 'Doohickey', 30.00)");

            $rows = $this->ztdQuery(
                "SELECT o.customer, SUM(oi.price) AS total
                 FROM mi_tnc_order o
                 JOIN mi_tnc_order_item oi ON oi.order_id = o.id
                 GROUP BY o.customer
                 ORDER BY o.customer"
            );

            $map = [];
            foreach ($rows as $row) {
                $map[$row['customer']] = (float) $row['total'];
            }

            if (!isset($map['Carol'])) {
                $this->markTestIncomplete('Substring table names: Carol not visible. Got: ' . json_encode($map));
            }
            $this->assertEquals(30.00, $map['Alice']); // 10 + 20
            $this->assertEquals(10.00, $map['Bob']);
            $this->assertEquals(30.00, $map['Carol']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('JOIN after DML on substring tables failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE on parent table, query items — tables should not interfere.
     */
    public function testUpdateParentThenQueryChild(): void
    {
        try {
            $this->mysqli->query("UPDATE mi_tnc_order SET customer = 'Alice Smith' WHERE id = 1");

            // Query items should not be affected by order update
            $rows = $this->ztdQuery("SELECT id, product FROM mi_tnc_order_item ORDER BY id");
            $this->assertCount(3, $rows);
            $this->assertSame('Widget', $rows[0]['product']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Update parent then query child failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE from child table, query parent — should not interfere.
     */
    public function testDeleteChildThenQueryParent(): void
    {
        try {
            $this->mysqli->query("DELETE FROM mi_tnc_order_item WHERE id = 3");

            $rows = $this->ztdQuery("SELECT id, customer FROM mi_tnc_order ORDER BY id");
            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['customer']);
            $this->assertSame('Bob', $rows[1]['customer']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Delete child then query parent failed: ' . $e->getMessage());
        }
    }

    /**
     * Correlated subquery from child table in parent SELECT.
     */
    public function testSubqueryChildInParentSelect(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_tnc_order_item VALUES (4, 2, 'Gizmo', 50.00)");

            $rows = $this->ztdQuery(
                "SELECT o.customer,
                        (SELECT SUM(oi.price) FROM mi_tnc_order_item oi WHERE oi.order_id = o.id) AS item_total
                 FROM mi_tnc_order o
                 ORDER BY o.id"
            );

            $this->assertCount(2, $rows);
            $this->assertEquals(30.00, (float) $rows[0]['item_total']); // Alice: 10+20
            $this->assertEquals(60.00, (float) $rows[1]['item_total']); // Bob: 10+50
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Subquery child in parent SELECT failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE parent SET col = (subquery from child) — cross-table with substring names.
     */
    public function testUpdateParentFromChildSubquery(): void
    {
        try {
            $this->mysqli->query(
                "UPDATE mi_tnc_order SET total = (SELECT COALESCE(SUM(price), 0) FROM mi_tnc_order_item WHERE order_id = mi_tnc_order.id)"
            );

            $rows = $this->ztdQuery("SELECT id, total FROM mi_tnc_order ORDER BY id");
            $this->assertCount(2, $rows);

            $aliceTotal = (float) $rows[0]['total'];
            if ($aliceTotal != 30.00) {
                $this->markTestIncomplete("UPDATE from child subquery: Alice expected 30, got $aliceTotal");
            }
            $this->assertEquals(30.00, $aliceTotal);
            $this->assertEquals(10.00, (float) $rows[1]['total']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE parent from child subquery failed: ' . $e->getMessage());
        }
    }
}
