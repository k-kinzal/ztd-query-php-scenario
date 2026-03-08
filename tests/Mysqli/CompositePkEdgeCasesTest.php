<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Extended composite PK scenarios: subquery WHERE, IN subquery, cross-table
 * JOINs with composite keys, and multi-execute prepared statements.
 * @spec SPEC-3.6
 */
class CompositePkEdgeCasesTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_cpke_order_items (order_id INT, item_id INT, product VARCHAR(255), quantity INT, price DOUBLE, PRIMARY KEY (order_id, item_id))',
            'CREATE TABLE mi_cpke_orders (order_id INT PRIMARY KEY, customer VARCHAR(255), status VARCHAR(50))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_cpke_order_items', 'mi_cpke_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_cpke_orders VALUES (1, 'Alice', 'shipped')");
        $this->mysqli->query("INSERT INTO mi_cpke_orders VALUES (2, 'Bob', 'pending')");
        $this->mysqli->query("INSERT INTO mi_cpke_orders VALUES (3, 'Charlie', 'shipped')");

        $this->mysqli->query("INSERT INTO mi_cpke_order_items VALUES (1, 1, 'Widget', 3, 9.99)");
        $this->mysqli->query("INSERT INTO mi_cpke_order_items VALUES (1, 2, 'Gadget', 1, 29.99)");
        $this->mysqli->query("INSERT INTO mi_cpke_order_items VALUES (2, 1, 'Widget', 5, 9.99)");
        $this->mysqli->query("INSERT INTO mi_cpke_order_items VALUES (3, 1, 'Sprocket', 2, 14.99)");
        $this->mysqli->query("INSERT INTO mi_cpke_order_items VALUES (3, 2, 'Bolt', 10, 0.99)");
    }

    public function testUpdateWithSubqueryWhereOnCompositePk(): void
    {
        // Update items for shipped orders only via subquery
        $this->mysqli->query("
            UPDATE mi_cpke_order_items SET price = price * 0.9
            WHERE order_id IN (SELECT order_id FROM mi_cpke_orders WHERE status = 'shipped')
        ");

        $rows = $this->ztdQuery("SELECT order_id, item_id, price FROM mi_cpke_order_items ORDER BY order_id, item_id");
        // Orders 1 and 3 are shipped -> 10% discount
        $this->assertEqualsWithDelta(8.99, (float) $rows[0]['price'], 0.01); // 1,1
        $this->assertEqualsWithDelta(26.99, (float) $rows[1]['price'], 0.01); // 1,2
        // Order 2 is pending -> unchanged
        $this->assertEqualsWithDelta(9.99, (float) $rows[2]['price'], 0.01); // 2,1
        // Order 3 items discounted
        $this->assertEqualsWithDelta(13.49, (float) $rows[3]['price'], 0.01); // 3,1
    }

    public function testDeleteWithSubqueryOnCompositePk(): void
    {
        // Delete items for pending orders
        $this->mysqli->query("
            DELETE FROM mi_cpke_order_items
            WHERE order_id IN (SELECT order_id FROM mi_cpke_orders WHERE status = 'pending')
        ");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_cpke_order_items");
        $this->assertSame(4, (int) $rows[0]['cnt']); // 5 - 1 = 4
    }

    public function testCrossTableJoinWithCompositePk(): void
    {
        $rows = $this->ztdQuery("
            SELECT o.customer, oi.product, oi.quantity * oi.price AS line_total
            FROM mi_cpke_orders o
            JOIN mi_cpke_order_items oi ON o.order_id = oi.order_id
            ORDER BY o.customer, oi.product
        ");
        $this->assertCount(5, $rows);
        $this->assertSame('Alice', $rows[0]['customer']);
    }

    public function testAggregateAcrossCompositePkJoin(): void
    {
        $rows = $this->ztdQuery("
            SELECT o.customer,
                   COUNT(*) AS item_count,
                   SUM(oi.quantity * oi.price) AS total
            FROM mi_cpke_orders o
            JOIN mi_cpke_order_items oi ON o.order_id = oi.order_id
            GROUP BY o.customer
            ORDER BY o.customer
        ");
        $this->assertCount(3, $rows);
        $this->assertSame(2, (int) $rows[0]['item_count']); // Alice: 2 items
        $this->assertEqualsWithDelta(59.96, (float) $rows[0]['total'], 0.01);
    }

    public function testPreparedMultiExecuteCompositePk(): void
    {
        $stmt = $this->mysqli->prepare("SELECT product, quantity FROM mi_cpke_order_items WHERE order_id = ? AND item_id = ?");

        $orderId = 1;
        $itemId = 1;
        $stmt->bind_param('ii', $orderId, $itemId);
        $stmt->execute();
        $result = $stmt->get_result();
        $row = $result->fetch_assoc();
        $this->assertSame('Widget', $row['product']);

        $orderId = 3;
        $itemId = 2;
        $stmt->bind_param('ii', $orderId, $itemId);
        $stmt->execute();
        $result = $stmt->get_result();
        $row = $result->fetch_assoc();
        $this->assertSame('Bolt', $row['product']);
    }

    public function testDeleteThenReinsertCompositePk(): void
    {
        $this->mysqli->query("DELETE FROM mi_cpke_order_items WHERE order_id = 1 AND item_id = 2");
        $this->mysqli->query("INSERT INTO mi_cpke_order_items VALUES (1, 2, 'Replacement', 7, 19.99)");

        $rows = $this->ztdQuery("SELECT product, quantity FROM mi_cpke_order_items WHERE order_id = 1 AND item_id = 2");
        $this->assertSame('Replacement', $rows[0]['product']);
        $this->assertSame(7, (int) $rows[0]['quantity']);
    }

    public function testCorrelatedSubqueryWithCompositePk(): void
    {
        $rows = $this->ztdQuery("
            SELECT o.customer,
                   (SELECT SUM(oi.quantity * oi.price)
                    FROM mi_cpke_order_items oi
                    WHERE oi.order_id = o.order_id) AS order_total
            FROM mi_cpke_orders o
            ORDER BY o.customer
        ");
        $this->assertCount(3, $rows);
        $this->assertEqualsWithDelta(59.96, (float) $rows[0]['order_total'], 0.01); // Alice
        $this->assertEqualsWithDelta(49.95, (float) $rows[1]['order_total'], 0.01); // Bob
    }

    public function testUpdatePartialPkWithAggregateCheck(): void
    {
        // Double quantity for all items in order 1
        $this->mysqli->query("UPDATE mi_cpke_order_items SET quantity = quantity * 2 WHERE order_id = 1");

        $rows = $this->ztdQuery("
            SELECT SUM(quantity) AS total_qty FROM mi_cpke_order_items WHERE order_id = 1
        ");
        $this->assertSame(8, (int) $rows[0]['total_qty']); // (3+1)*2 = 8
    }
}
