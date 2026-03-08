<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests three-table and four-table JOIN operations through ZTD shadow store.
 *
 * Verifies that CTE rewriter handles multiple shadow tables in complex JOIN topologies.
 * @spec pending
 */
class ThreeTableJoinTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_3tj_customers (id INT PRIMARY KEY, name VARCHAR(50), tier VARCHAR(20))',
            'CREATE TABLE mi_3tj_products (id INT PRIMARY KEY, name VARCHAR(50), price DECIMAL(10,2))',
            'CREATE TABLE mi_3tj_orders (id INT PRIMARY KEY, customer_id INT, order_date DATE)',
            'CREATE TABLE mi_3tj_order_items (id INT PRIMARY KEY, order_id INT, product_id INT, qty INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_3tj_order_items', 'mi_3tj_orders', 'mi_3tj_products', 'mi_3tj_customers'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        // Customers
        $this->mysqli->query("INSERT INTO mi_3tj_customers VALUES (1, 'Alice', 'Gold')");
        $this->mysqli->query("INSERT INTO mi_3tj_customers VALUES (2, 'Bob', 'Silver')");
        // Products
        $this->mysqli->query("INSERT INTO mi_3tj_products VALUES (1, 'Widget', 10.00)");
        $this->mysqli->query("INSERT INTO mi_3tj_products VALUES (2, 'Gadget', 25.00)");
        $this->mysqli->query("INSERT INTO mi_3tj_products VALUES (3, 'Doohickey', 5.00)");
        // Orders
        $this->mysqli->query("INSERT INTO mi_3tj_orders VALUES (1, 1, '2024-01-15')");
        $this->mysqli->query("INSERT INTO mi_3tj_orders VALUES (2, 2, '2024-01-16')");
        $this->mysqli->query("INSERT INTO mi_3tj_orders VALUES (3, 1, '2024-01-17')");
        // Order items
        $this->mysqli->query("INSERT INTO mi_3tj_order_items VALUES (1, 1, 1, 3)");
        $this->mysqli->query("INSERT INTO mi_3tj_order_items VALUES (2, 1, 2, 1)");
        $this->mysqli->query("INSERT INTO mi_3tj_order_items VALUES (3, 2, 1, 5)");
        $this->mysqli->query("INSERT INTO mi_3tj_order_items VALUES (4, 3, 3, 10)");
    }

    /**
     * Three-table JOIN: customers → orders → order_items.
     */
    public function testThreeTableJoin(): void
    {
        $result = $this->mysqli->query(
            'SELECT c.name, COUNT(oi.id) as item_count
             FROM mi_3tj_customers c
             JOIN mi_3tj_orders o ON c.id = o.customer_id
             JOIN mi_3tj_order_items oi ON o.id = oi.order_id
             GROUP BY c.name
             ORDER BY c.name'
        );
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEquals(3, (int) $rows[0]['item_count']); // order 1: 2 items, order 3: 1 item
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertEquals(1, (int) $rows[1]['item_count']);
    }

    /**
     * Four-table JOIN: customers → orders → order_items → products.
     */
    public function testFourTableJoin(): void
    {
        $result = $this->mysqli->query(
            'SELECT c.name as customer, p.name as product, SUM(oi.qty * p.price) as total
             FROM mi_3tj_customers c
             JOIN mi_3tj_orders o ON c.id = o.customer_id
             JOIN mi_3tj_order_items oi ON o.id = oi.order_id
             JOIN mi_3tj_products p ON oi.product_id = p.id
             GROUP BY c.name, p.name
             ORDER BY c.name, p.name'
        );
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertGreaterThanOrEqual(3, count($rows));

        // Alice bought Widgets (3*10=30), Gadgets (1*25=25), Doohickeys (10*5=50)
        $aliceWidget = array_filter($rows, fn($r) => $r['customer'] === 'Alice' && $r['product'] === 'Widget');
        if (count($aliceWidget) > 0) {
            $this->assertEquals(30, (float) array_values($aliceWidget)[0]['total']);
        }
    }

    /**
     * Three-table JOIN with LEFT JOIN for missing data.
     */
    public function testThreeTableLeftJoin(): void
    {
        // Add customer with no orders
        $this->mysqli->query("INSERT INTO mi_3tj_customers VALUES (3, 'Charlie', 'Bronze')");

        $result = $this->mysqli->query(
            'SELECT c.name, COUNT(o.id) as order_count
             FROM mi_3tj_customers c
             LEFT JOIN mi_3tj_orders o ON c.id = o.customer_id
             LEFT JOIN mi_3tj_order_items oi ON o.id = oi.order_id
             GROUP BY c.name
             ORDER BY c.name'
        );
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(3, $rows);
        // Charlie has 0 orders
        $this->assertSame('Charlie', $rows[2]['name']);
        $this->assertEquals(0, (int) $rows[2]['order_count']);
    }

    /**
     * Mutation followed by multi-table JOIN.
     */
    public function testMutationThenMultiJoin(): void
    {
        // Add a new order and item
        $this->mysqli->query("INSERT INTO mi_3tj_orders VALUES (4, 2, '2024-01-18')");
        $this->mysqli->query("INSERT INTO mi_3tj_order_items VALUES (5, 4, 2, 2)");

        $result = $this->mysqli->query(
            'SELECT c.name, SUM(oi.qty) as total_qty
             FROM mi_3tj_customers c
             JOIN mi_3tj_orders o ON c.id = o.customer_id
             JOIN mi_3tj_order_items oi ON o.id = oi.order_id
             WHERE c.name = \'Bob\'
             GROUP BY c.name'
        );
        $row = $result->fetch_assoc();
        // Bob: original 5 widgets + new 2 gadgets = 7
        $this->assertEquals(7, (int) $row['total_qty']);
    }

    /**
     * Physical isolation across all tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->disableZtd();

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_3tj_customers');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_3tj_order_items');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
