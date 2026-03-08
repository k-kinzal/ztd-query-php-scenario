<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests realistic user workflow scenarios on SQLite:
 * e-commerce order processing, user registration, inventory management.
 * These simulate how a real application would use ztd-query for testing.
 * @spec SPEC-2.1
 */
class SqliteRealisticWorkflowTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, email TEXT, tier TEXT DEFAULT \'standard\')',
            'CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL, stock INTEGER)',
            'CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, total REAL, status TEXT, created_at TEXT)',
            'CREATE TABLE order_items (id INTEGER PRIMARY KEY, order_id INTEGER, product_id INTEGER, qty INTEGER, unit_price REAL)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['customers', 'products', 'orders', 'order_items'];
    }

    private PDO $raw;

    protected function setUp(): void
    {
        parent::setUp();

        $this->raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $this->raw->exec("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, email TEXT, tier TEXT DEFAULT 'standard')");
        $this->raw->exec('CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL, stock INTEGER)');
        $this->raw->exec('CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, total REAL, status TEXT, created_at TEXT)');
        $this->raw->exec('CREATE TABLE order_items (id INTEGER PRIMARY KEY, order_id INTEGER, product_id INTEGER, qty INTEGER, unit_price REAL)');

        }

    public function testEcommerceOrderWorkflow(): void
    {
        // Step 1: Create customer
        $this->pdo->exec("INSERT INTO customers (id, name, email) VALUES (1, 'Alice Smith', 'alice@example.com')");

        // Step 2: Add products to catalog
        $this->pdo->exec("INSERT INTO products (id, name, price, stock) VALUES (1, 'Widget', 29.99, 100)");
        $this->pdo->exec("INSERT INTO products (id, name, price, stock) VALUES (2, 'Gadget', 49.99, 50)");
        $this->pdo->exec("INSERT INTO products (id, name, price, stock) VALUES (3, 'Doohickey', 9.99, 200)");

        // Step 3: Create an order
        $this->pdo->exec("INSERT INTO orders (id, customer_id, total, status, created_at) VALUES (1, 1, 0, 'pending', '2026-03-07')");

        // Step 4: Add items to order
        $this->pdo->exec("INSERT INTO order_items (id, order_id, product_id, qty, unit_price) VALUES (1, 1, 1, 2, 29.99)");
        $this->pdo->exec("INSERT INTO order_items (id, order_id, product_id, qty, unit_price) VALUES (2, 1, 3, 5, 9.99)");

        // Step 5: Calculate order total
        $stmt = $this->pdo->query('SELECT SUM(qty * unit_price) AS total FROM order_items WHERE order_id = 1');
        $total = (float) $stmt->fetch(PDO::FETCH_ASSOC)['total'];
        $this->assertSame(109.93, round($total, 2));

        // Step 6: Update order total
        $this->pdo->exec("UPDATE orders SET total = 109.93 WHERE id = 1");

        // Step 7: Reduce stock
        $this->pdo->exec("UPDATE products SET stock = stock - 2 WHERE id = 1");
        $this->pdo->exec("UPDATE products SET stock = stock - 5 WHERE id = 3");

        // Step 8: Complete order
        $this->pdo->exec("UPDATE orders SET status = 'completed' WHERE id = 1");

        // Verify final state
        $stmt = $this->pdo->query('SELECT status, total FROM orders WHERE id = 1');
        $order = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('completed', $order['status']);
        $this->assertSame(109.93, (float) $order['total']);

        $stmt = $this->pdo->query('SELECT stock FROM products WHERE id = 1');
        $this->assertSame(98, (int) $stmt->fetch(PDO::FETCH_ASSOC)['stock']);

        $stmt = $this->pdo->query('SELECT stock FROM products WHERE id = 3');
        $this->assertSame(195, (int) $stmt->fetch(PDO::FETCH_ASSOC)['stock']);

        // Verify nothing leaked to physical DB
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) as c FROM orders');
        $this->assertSame(0, (int) $stmt->fetch(PDO::FETCH_ASSOC)['c']);
    }

    public function testUserRegistrationWithTierUpgrade(): void
    {
        // Register users via exec
        $this->pdo->exec("INSERT INTO customers (id, name, email, tier) VALUES (1, 'Alice', 'alice@example.com', 'standard')");
        $this->pdo->exec("INSERT INTO customers (id, name, email, tier) VALUES (2, 'Bob', 'bob@example.com', 'standard')");
        $this->pdo->exec("INSERT INTO customers (id, name, email, tier) VALUES (3, 'Charlie', 'charlie@example.com', 'standard')");

        // Add orders with varying totals
        $this->pdo->exec("INSERT INTO orders (id, customer_id, total, status, created_at) VALUES (1, 1, 500.00, 'completed', '2026-01-01')");
        $this->pdo->exec("INSERT INTO orders (id, customer_id, total, status, created_at) VALUES (2, 1, 300.00, 'completed', '2026-02-01')");
        $this->pdo->exec("INSERT INTO orders (id, customer_id, total, status, created_at) VALUES (3, 2, 100.00, 'completed', '2026-01-15')");

        // First check: identify high-spending customers via SELECT
        $stmt = $this->pdo->query("SELECT customer_id, SUM(total) AS spent FROM orders WHERE status = 'completed' GROUP BY customer_id HAVING SUM(total) > 400");
        $highSpenders = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $highSpenders);
        $this->assertSame(1, (int) $highSpenders[0]['customer_id']);

        // Upgrade tier for high-spending customer
        $this->pdo->exec("UPDATE customers SET tier = 'premium' WHERE id = 1");

        // Verify tier assignments
        $stmt = $this->pdo->query('SELECT name, tier FROM customers ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertSame('premium', $rows[0]['tier']); // Alice
        $this->assertSame('standard', $rows[1]['tier']); // Bob
        $this->assertSame('standard', $rows[2]['tier']); // Charlie
    }

    public function testInventoryReportWithJoins(): void
    {
        // Set up inventory
        $this->pdo->exec("INSERT INTO products (id, name, price, stock) VALUES (1, 'Widget', 29.99, 100)");
        $this->pdo->exec("INSERT INTO products (id, name, price, stock) VALUES (2, 'Gadget', 49.99, 0)");
        $this->pdo->exec("INSERT INTO products (id, name, price, stock) VALUES (3, 'Doohickey', 9.99, 200)");

        // Set up customers and orders
        $this->pdo->exec("INSERT INTO customers (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");
        $this->pdo->exec("INSERT INTO orders (id, customer_id, total, status, created_at) VALUES (1, 1, 89.97, 'completed', '2026-03-07')");
        $this->pdo->exec("INSERT INTO order_items (id, order_id, product_id, qty, unit_price) VALUES (1, 1, 1, 3, 29.99)");

        // Report: products with total quantity sold
        $stmt = $this->pdo->query("
            SELECT p.name, p.stock, COALESCE(SUM(oi.qty), 0) AS total_sold
            FROM products p
            LEFT JOIN order_items oi ON p.id = oi.product_id
            GROUP BY p.id, p.name, p.stock
            ORDER BY p.id
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(3, $rows);
        $this->assertSame('Widget', $rows[0]['name']);
        $this->assertSame(3, (int) $rows[0]['total_sold']);
        $this->assertSame('Gadget', $rows[1]['name']);
        $this->assertSame(0, (int) $rows[1]['total_sold']);

        // Report: out of stock products
        $stmt = $this->pdo->query('SELECT name FROM products WHERE stock = 0');
        $outOfStock = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $outOfStock);
        $this->assertSame('Gadget', $outOfStock[0]['name']);
    }

    public function testOrderCancellationWorkflow(): void
    {
        // Set up
        $this->pdo->exec("INSERT INTO customers (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");
        $this->pdo->exec("INSERT INTO products (id, name, price, stock) VALUES (1, 'Widget', 29.99, 100)");
        $this->pdo->exec("INSERT INTO orders (id, customer_id, total, status, created_at) VALUES (1, 1, 59.98, 'completed', '2026-03-07')");
        $this->pdo->exec("INSERT INTO order_items (id, order_id, product_id, qty, unit_price) VALUES (1, 1, 1, 2, 29.99)");
        $this->pdo->exec("UPDATE products SET stock = stock - 2 WHERE id = 1");

        // Cancel: restore stock and update status
        $stmt = $this->pdo->query('SELECT product_id, qty FROM order_items WHERE order_id = 1');
        $items = $stmt->fetchAll(PDO::FETCH_ASSOC);
        foreach ($items as $item) {
            $this->pdo->exec("UPDATE products SET stock = stock + {$item['qty']} WHERE id = {$item['product_id']}");
        }
        $this->pdo->exec("UPDATE orders SET status = 'cancelled' WHERE id = 1");
        $this->pdo->exec("DELETE FROM order_items WHERE order_id = 1");

        // Verify cancellation
        $stmt = $this->pdo->query('SELECT status FROM orders WHERE id = 1');
        $this->assertSame('cancelled', $stmt->fetch(PDO::FETCH_ASSOC)['status']);

        $stmt = $this->pdo->query('SELECT stock FROM products WHERE id = 1');
        $this->assertSame(100, (int) $stmt->fetch(PDO::FETCH_ASSOC)['stock']);

        $stmt = $this->pdo->query('SELECT COUNT(*) as c FROM order_items WHERE order_id = 1');
        $this->assertSame(0, (int) $stmt->fetch(PDO::FETCH_ASSOC)['c']);
    }
}
