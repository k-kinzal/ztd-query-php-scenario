<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests JOINs across three shadow tables on MySQL.
 *
 * Verifies the CTE rewriter handles multiple simultaneous CTE rewrites
 * for all three tables referenced in a single query on MySQL.
 *
 * @spec SPEC-3.1
 * @spec SPEC-3.3
 */
class MysqlThreeTableShadowJoinTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_3tj_users (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE my_3tj_orders (
                id INT PRIMARY KEY,
                user_id INT NOT NULL,
                total DECIMAL(10,2) NOT NULL,
                status VARCHAR(20) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE my_3tj_order_items (
                id INT PRIMARY KEY,
                order_id INT NOT NULL,
                product VARCHAR(100) NOT NULL,
                quantity INT NOT NULL,
                price DECIMAL(10,2) NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_3tj_order_items', 'my_3tj_orders', 'my_3tj_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO my_3tj_users VALUES (1, 'Alice')");
        $this->ztdExec("INSERT INTO my_3tj_users VALUES (2, 'Bob')");

        $this->ztdExec("INSERT INTO my_3tj_orders VALUES (1, 1, 100.00, 'completed')");
        $this->ztdExec("INSERT INTO my_3tj_orders VALUES (2, 2, 200.00, 'completed')");

        $this->ztdExec("INSERT INTO my_3tj_order_items VALUES (1, 1, 'Widget', 2, 25.00)");
        $this->ztdExec("INSERT INTO my_3tj_order_items VALUES (2, 1, 'Gadget', 1, 50.00)");
        $this->ztdExec("INSERT INTO my_3tj_order_items VALUES (3, 2, 'Bolt', 4, 50.00)");
    }

    /**
     * Three-table JOIN on all shadow data.
     */
    public function testThreeTableJoin(): void
    {
        $rows = $this->ztdQuery(
            "SELECT u.name, o.total, oi.product, oi.quantity
             FROM my_3tj_users u
             JOIN my_3tj_orders o ON o.user_id = u.id
             JOIN my_3tj_order_items oi ON oi.order_id = o.id
             ORDER BY u.name, oi.product"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Gadget', $rows[0]['product']);
        $this->assertSame('Alice', $rows[1]['name']);
        $this->assertSame('Widget', $rows[1]['product']);
        $this->assertSame('Bob', $rows[2]['name']);
        $this->assertSame('Bolt', $rows[2]['product']);
    }

    /**
     * Three-table JOIN with aggregate.
     */
    public function testThreeTableJoinWithAggregate(): void
    {
        $rows = $this->ztdQuery(
            "SELECT u.name,
                    COUNT(oi.id) AS item_count,
                    SUM(oi.quantity * oi.price) AS item_total
             FROM my_3tj_users u
             JOIN my_3tj_orders o ON o.user_id = u.id
             JOIN my_3tj_order_items oi ON oi.order_id = o.id
             GROUP BY u.id, u.name
             ORDER BY u.name"
        );

        $this->assertCount(2, $rows);
        // Alice: Widget(2*25=50) + Gadget(1*50=50) = 100
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEquals(2, (int) $rows[0]['item_count']);
        $this->assertEqualsWithDelta(100.00, (float) $rows[0]['item_total'], 0.01);
        // Bob: Bolt(4*50=200)
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertEquals(1, (int) $rows[1]['item_count']);
        $this->assertEqualsWithDelta(200.00, (float) $rows[1]['item_total'], 0.01);
    }

    /**
     * Three-table JOIN after mutation to middle table.
     */
    public function testThreeTableJoinAfterMutation(): void
    {
        $this->ztdExec("INSERT INTO my_3tj_orders VALUES (3, 1, 50.00, 'completed')");
        $this->ztdExec("INSERT INTO my_3tj_order_items VALUES (4, 3, 'Nut', 10, 5.00)");

        $rows = $this->ztdQuery(
            "SELECT u.name, COUNT(DISTINCT o.id) AS orders, SUM(oi.quantity) AS total_qty
             FROM my_3tj_users u
             JOIN my_3tj_orders o ON o.user_id = u.id
             JOIN my_3tj_order_items oi ON oi.order_id = o.id
             WHERE u.name = 'Alice'
             GROUP BY u.id, u.name"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(2, (int) $rows[0]['orders']); // orders 1 and 3
        $this->assertEquals(13, (int) $rows[0]['total_qty']); // 2+1+10
    }

    /**
     * Scalar subquery across two shadow tables in SELECT over third.
     */
    public function testScalarSubqueryAcrossShadowTables(): void
    {
        $rows = $this->ztdQuery(
            "SELECT u.name,
                (SELECT SUM(oi.quantity * oi.price)
                 FROM my_3tj_orders o
                 JOIN my_3tj_order_items oi ON oi.order_id = o.id
                 WHERE o.user_id = u.id) AS total_value
             FROM my_3tj_users u
             ORDER BY u.name"
        );

        $this->assertCount(2, $rows);
        $this->assertEqualsWithDelta(100.00, (float) $rows[0]['total_value'], 0.01);
        $this->assertEqualsWithDelta(200.00, (float) $rows[1]['total_value'], 0.01);
    }

    /**
     * Physical isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $this->disableZtd();
        $count = (int) $this->pdo->query('SELECT COUNT(*) FROM my_3tj_users')->fetchColumn();
        $this->assertSame(0, $count);
    }
}
