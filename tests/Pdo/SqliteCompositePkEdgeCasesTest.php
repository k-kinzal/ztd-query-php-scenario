<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Extended composite PK scenarios: subquery WHERE, IN subquery, cross-table
 * JOINs with composite keys, and multi-execute prepared statements.
 * @spec SPEC-3.6
 */
class SqliteCompositePkEdgeCasesTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE order_items (order_id INTEGER, item_id INTEGER, product TEXT, quantity INTEGER, price REAL, PRIMARY KEY (order_id, item_id))',
            'CREATE TABLE orders (order_id INTEGER PRIMARY KEY, customer TEXT, status TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['order_items', 'orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO orders VALUES (1, 'Alice', 'shipped')");
        $this->pdo->exec("INSERT INTO orders VALUES (2, 'Bob', 'pending')");
        $this->pdo->exec("INSERT INTO orders VALUES (3, 'Charlie', 'shipped')");

        $this->pdo->exec("INSERT INTO order_items VALUES (1, 1, 'Widget', 3, 9.99)");
        $this->pdo->exec("INSERT INTO order_items VALUES (1, 2, 'Gadget', 1, 29.99)");
        $this->pdo->exec("INSERT INTO order_items VALUES (2, 1, 'Widget', 5, 9.99)");
        $this->pdo->exec("INSERT INTO order_items VALUES (3, 1, 'Sprocket', 2, 14.99)");
        $this->pdo->exec("INSERT INTO order_items VALUES (3, 2, 'Bolt', 10, 0.99)");
    }

    public function testUpdateWithSubqueryWhereOnCompositePk(): void
    {
        // Update items for shipped orders only via subquery
        $this->pdo->exec("
            UPDATE order_items SET price = price * 0.9
            WHERE order_id IN (SELECT order_id FROM orders WHERE status = 'shipped')
        ");

        $rows = $this->ztdQuery("SELECT order_id, item_id, price FROM order_items ORDER BY order_id, item_id");
        // Orders 1 and 3 are shipped → 10% discount
        $this->assertEqualsWithDelta(8.99, (float) $rows[0]['price'], 0.01); // 1,1
        $this->assertEqualsWithDelta(26.99, (float) $rows[1]['price'], 0.01); // 1,2
        // Order 2 is pending → unchanged
        $this->assertEqualsWithDelta(9.99, (float) $rows[2]['price'], 0.01); // 2,1
        // Order 3 items discounted
        $this->assertEqualsWithDelta(13.49, (float) $rows[3]['price'], 0.01); // 3,1
    }

    public function testDeleteWithSubqueryOnCompositePk(): void
    {
        // Delete items for pending orders
        $this->pdo->exec("
            DELETE FROM order_items
            WHERE order_id IN (SELECT order_id FROM orders WHERE status = 'pending')
        ");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM order_items");
        $this->assertSame(4, (int) $rows[0]['cnt']); // 5 - 1 = 4
    }

    public function testCrossTableJoinWithCompositePk(): void
    {
        $rows = $this->ztdQuery("
            SELECT o.customer, oi.product, oi.quantity * oi.price AS line_total
            FROM orders o
            JOIN order_items oi ON o.order_id = oi.order_id
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
            FROM orders o
            JOIN order_items oi ON o.order_id = oi.order_id
            GROUP BY o.customer
            ORDER BY o.customer
        ");
        $this->assertCount(3, $rows);
        $this->assertSame(2, (int) $rows[0]['item_count']); // Alice: 2 items
        $this->assertEqualsWithDelta(59.96, (float) $rows[0]['total'], 0.01);
    }

    public function testPreparedMultiExecuteCompositePk(): void
    {
        $stmt = $this->pdo->prepare("SELECT product, quantity FROM order_items WHERE order_id = ? AND item_id = ?");

        $stmt->execute([1, 1]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Widget', $row['product']);

        $stmt->execute([3, 2]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Bolt', $row['product']);
    }

    public function testDeleteThenReinsertCompositePk(): void
    {
        $this->pdo->exec("DELETE FROM order_items WHERE order_id = 1 AND item_id = 2");
        $this->pdo->exec("INSERT INTO order_items VALUES (1, 2, 'Replacement', 7, 19.99)");

        $rows = $this->ztdQuery("SELECT product, quantity FROM order_items WHERE order_id = 1 AND item_id = 2");
        $this->assertSame('Replacement', $rows[0]['product']);
        $this->assertSame(7, (int) $rows[0]['quantity']);
    }

    public function testCorrelatedSubqueryWithCompositePk(): void
    {
        $rows = $this->ztdQuery("
            SELECT o.customer,
                   (SELECT SUM(oi.quantity * oi.price)
                    FROM order_items oi
                    WHERE oi.order_id = o.order_id) AS order_total
            FROM orders o
            ORDER BY o.customer
        ");
        $this->assertCount(3, $rows);
        $this->assertEqualsWithDelta(59.96, (float) $rows[0]['order_total'], 0.01); // Alice
        $this->assertEqualsWithDelta(49.95, (float) $rows[1]['order_total'], 0.01); // Bob
    }

    public function testUpdatePartialPkWithAggregateCheck(): void
    {
        // Double quantity for all items in order 1
        $this->pdo->exec("UPDATE order_items SET quantity = quantity * 2 WHERE order_id = 1");

        $rows = $this->ztdQuery("
            SELECT SUM(quantity) AS total_qty FROM order_items WHERE order_id = 1
        ");
        $this->assertSame(8, (int) $rows[0]['total_qty']); // (3+1)*2 = 8
    }
}
