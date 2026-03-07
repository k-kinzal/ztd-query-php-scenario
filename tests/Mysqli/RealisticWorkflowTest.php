<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use mysqli;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests realistic user workflow scenarios on MySQL via MySQLi:
 * e-commerce order processing, user registration, inventory management.
 */
class RealisticWorkflowTest extends TestCase
{
    private ZtdMysqli $mysqli;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS mi_wf_order_items');
        $raw->query('DROP TABLE IF EXISTS mi_wf_orders');
        $raw->query('DROP TABLE IF EXISTS mi_wf_products');
        $raw->query('DROP TABLE IF EXISTS mi_wf_customers');
        $raw->query("CREATE TABLE mi_wf_customers (id INT PRIMARY KEY, name VARCHAR(255), email VARCHAR(255), tier VARCHAR(50) DEFAULT 'standard')");
        $raw->query('CREATE TABLE mi_wf_products (id INT PRIMARY KEY, name VARCHAR(255), price DECIMAL(10,2), stock INT)');
        $raw->query('CREATE TABLE mi_wf_orders (id INT PRIMARY KEY, customer_id INT, total DECIMAL(10,2), status VARCHAR(50), created_at DATE)');
        $raw->query('CREATE TABLE mi_wf_order_items (id INT PRIMARY KEY, order_id INT, product_id INT, qty INT, unit_price DECIMAL(10,2))');
        $raw->close();
    }

    protected function setUp(): void
    {
        $this->mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
    }

    public function testEcommerceOrderWorkflow(): void
    {
        // Create customer and products
        $this->mysqli->query("INSERT INTO mi_wf_customers (id, name, email) VALUES (1, 'Alice Smith', 'alice@example.com')");
        $this->mysqli->query("INSERT INTO mi_wf_products (id, name, price, stock) VALUES (1, 'Widget', 29.99, 100)");
        $this->mysqli->query("INSERT INTO mi_wf_products (id, name, price, stock) VALUES (2, 'Gadget', 49.99, 50)");
        $this->mysqli->query("INSERT INTO mi_wf_products (id, name, price, stock) VALUES (3, 'Doohickey', 9.99, 200)");

        // Create order and items
        $this->mysqli->query("INSERT INTO mi_wf_orders (id, customer_id, total, status, created_at) VALUES (1, 1, 0, 'pending', '2026-03-07')");
        $this->mysqli->query("INSERT INTO mi_wf_order_items (id, order_id, product_id, qty, unit_price) VALUES (1, 1, 1, 2, 29.99)");
        $this->mysqli->query("INSERT INTO mi_wf_order_items (id, order_id, product_id, qty, unit_price) VALUES (2, 1, 3, 5, 9.99)");

        // Calculate total
        $result = $this->mysqli->query('SELECT SUM(qty * unit_price) AS total FROM mi_wf_order_items WHERE order_id = 1');
        $total = (float) $result->fetch_assoc()['total'];
        $this->assertSame(109.93, round($total, 2));

        // Update order, reduce stock, complete
        $this->mysqli->query("UPDATE mi_wf_orders SET total = 109.93 WHERE id = 1");
        $this->mysqli->query("UPDATE mi_wf_products SET stock = stock - 2 WHERE id = 1");
        $this->mysqli->query("UPDATE mi_wf_products SET stock = stock - 5 WHERE id = 3");
        $this->mysqli->query("UPDATE mi_wf_orders SET status = 'completed' WHERE id = 1");

        // Verify
        $result = $this->mysqli->query('SELECT status, total FROM mi_wf_orders WHERE id = 1');
        $order = $result->fetch_assoc();
        $this->assertSame('completed', $order['status']);
        $this->assertSame(109.93, (float) $order['total']);

        $result = $this->mysqli->query('SELECT stock FROM mi_wf_products WHERE id = 1');
        $this->assertSame(98, (int) $result->fetch_assoc()['stock']);

        $result = $this->mysqli->query('SELECT stock FROM mi_wf_products WHERE id = 3');
        $this->assertSame(195, (int) $result->fetch_assoc()['stock']);

        // Verify nothing leaked
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) as c FROM mi_wf_orders');
        $this->assertSame(0, (int) $result->fetch_assoc()['c']);
    }

    public function testUserRegistrationWithTierUpgrade(): void
    {
        $this->mysqli->query("INSERT INTO mi_wf_customers (id, name, email, tier) VALUES (1, 'Alice', 'alice@example.com', 'standard')");
        $this->mysqli->query("INSERT INTO mi_wf_customers (id, name, email, tier) VALUES (2, 'Bob', 'bob@example.com', 'standard')");
        $this->mysqli->query("INSERT INTO mi_wf_customers (id, name, email, tier) VALUES (3, 'Charlie', 'charlie@example.com', 'standard')");

        $this->mysqli->query("INSERT INTO mi_wf_orders (id, customer_id, total, status, created_at) VALUES (1, 1, 500.00, 'completed', '2026-01-01')");
        $this->mysqli->query("INSERT INTO mi_wf_orders (id, customer_id, total, status, created_at) VALUES (2, 1, 300.00, 'completed', '2026-02-01')");
        $this->mysqli->query("INSERT INTO mi_wf_orders (id, customer_id, total, status, created_at) VALUES (3, 2, 100.00, 'completed', '2026-01-15')");

        // Identify high-spenders
        $result = $this->mysqli->query("SELECT customer_id, SUM(total) AS spent FROM mi_wf_orders WHERE status = 'completed' GROUP BY customer_id HAVING SUM(total) > 400");
        $highSpenders = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $highSpenders);
        $this->assertSame(1, (int) $highSpenders[0]['customer_id']);

        // Upgrade using UPDATE with IN subquery (works on MySQL)
        $this->mysqli->query("UPDATE mi_wf_customers SET tier = 'premium' WHERE id IN (SELECT customer_id FROM mi_wf_orders WHERE status = 'completed' GROUP BY customer_id HAVING SUM(total) > 400)");

        // Verify
        $result = $this->mysqli->query('SELECT name, tier FROM mi_wf_customers ORDER BY id');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertSame('premium', $rows[0]['tier']);
        $this->assertSame('standard', $rows[1]['tier']);
        $this->assertSame('standard', $rows[2]['tier']);
    }

    public function testInventoryReportWithJoins(): void
    {
        $this->mysqli->query("INSERT INTO mi_wf_products (id, name, price, stock) VALUES (1, 'Widget', 29.99, 100)");
        $this->mysqli->query("INSERT INTO mi_wf_products (id, name, price, stock) VALUES (2, 'Gadget', 49.99, 0)");
        $this->mysqli->query("INSERT INTO mi_wf_products (id, name, price, stock) VALUES (3, 'Doohickey', 9.99, 200)");

        $this->mysqli->query("INSERT INTO mi_wf_customers (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");
        $this->mysqli->query("INSERT INTO mi_wf_orders (id, customer_id, total, status, created_at) VALUES (1, 1, 89.97, 'completed', '2026-03-07')");
        $this->mysqli->query("INSERT INTO mi_wf_order_items (id, order_id, product_id, qty, unit_price) VALUES (1, 1, 1, 3, 29.99)");

        $result = $this->mysqli->query("
            SELECT p.name, p.stock, COALESCE(SUM(oi.qty), 0) AS total_sold
            FROM mi_wf_products p
            LEFT JOIN mi_wf_order_items oi ON p.id = oi.product_id
            GROUP BY p.id, p.name, p.stock
            ORDER BY p.id
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(3, $rows);
        $this->assertSame('Widget', $rows[0]['name']);
        $this->assertSame(3, (int) $rows[0]['total_sold']);
        $this->assertSame('Gadget', $rows[1]['name']);
        $this->assertSame(0, (int) $rows[1]['total_sold']);

        $result = $this->mysqli->query('SELECT name FROM mi_wf_products WHERE stock = 0');
        $outOfStock = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $outOfStock);
        $this->assertSame('Gadget', $outOfStock[0]['name']);
    }

    public function testOrderCancellationWorkflow(): void
    {
        $this->mysqli->query("INSERT INTO mi_wf_customers (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");
        $this->mysqli->query("INSERT INTO mi_wf_products (id, name, price, stock) VALUES (1, 'Widget', 29.99, 100)");
        $this->mysqli->query("INSERT INTO mi_wf_orders (id, customer_id, total, status, created_at) VALUES (1, 1, 59.98, 'completed', '2026-03-07')");
        $this->mysqli->query("INSERT INTO mi_wf_order_items (id, order_id, product_id, qty, unit_price) VALUES (1, 1, 1, 2, 29.99)");
        $this->mysqli->query("UPDATE mi_wf_products SET stock = stock - 2 WHERE id = 1");

        // Cancel: restore stock
        $result = $this->mysqli->query('SELECT product_id, qty FROM mi_wf_order_items WHERE order_id = 1');
        $items = $result->fetch_all(MYSQLI_ASSOC);
        foreach ($items as $item) {
            $this->mysqli->query("UPDATE mi_wf_products SET stock = stock + {$item['qty']} WHERE id = {$item['product_id']}");
        }
        $this->mysqli->query("UPDATE mi_wf_orders SET status = 'cancelled' WHERE id = 1");
        $this->mysqli->query("DELETE FROM mi_wf_order_items WHERE order_id = 1");

        // Verify
        $result = $this->mysqli->query('SELECT status FROM mi_wf_orders WHERE id = 1');
        $this->assertSame('cancelled', $result->fetch_assoc()['status']);

        $result = $this->mysqli->query('SELECT stock FROM mi_wf_products WHERE id = 1');
        $this->assertSame(100, (int) $result->fetch_assoc()['stock']);

        $result = $this->mysqli->query('SELECT COUNT(*) as c FROM mi_wf_order_items WHERE order_id = 1');
        $this->assertSame(0, (int) $result->fetch_assoc()['c']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS mi_wf_order_items');
        $raw->query('DROP TABLE IF EXISTS mi_wf_orders');
        $raw->query('DROP TABLE IF EXISTS mi_wf_products');
        $raw->query('DROP TABLE IF EXISTS mi_wf_customers');
        $raw->close();
    }
}
