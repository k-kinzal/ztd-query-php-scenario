<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests realistic user workflow scenarios on PostgreSQL PDO:
 * e-commerce order processing, user registration, inventory management.
 * @spec pending
 */
class PostgresRealisticWorkflowTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_wf_customers (id INT PRIMARY KEY, name VARCHAR(255), email VARCHAR(255), tier VARCHAR(50) DEFAULT \'standard\')',
            'CREATE TABLE pg_wf_products (id INT PRIMARY KEY, name VARCHAR(255), price DECIMAL(10,2), stock INT)',
            'CREATE TABLE pg_wf_orders (id INT PRIMARY KEY, customer_id INT, total DECIMAL(10,2), status VARCHAR(50), created_at DATE)',
            'CREATE TABLE pg_wf_order_items (id INT PRIMARY KEY, order_id INT, product_id INT, qty INT, unit_price DECIMAL(10,2))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_wf_order_items', 'pg_wf_orders', 'pg_wf_products', 'pg_wf_customers'];
    }


    public function testEcommerceOrderWorkflow(): void
    {
        // Step 1: Create customer
        $this->pdo->exec("INSERT INTO pg_wf_customers (id, name, email) VALUES (1, 'Alice Smith', 'alice@example.com')");

        // Step 2: Add products
        $this->pdo->exec("INSERT INTO pg_wf_products (id, name, price, stock) VALUES (1, 'Widget', 29.99, 100)");
        $this->pdo->exec("INSERT INTO pg_wf_products (id, name, price, stock) VALUES (2, 'Gadget', 49.99, 50)");
        $this->pdo->exec("INSERT INTO pg_wf_products (id, name, price, stock) VALUES (3, 'Doohickey', 9.99, 200)");

        // Step 3: Create order
        $this->pdo->exec("INSERT INTO pg_wf_orders (id, customer_id, total, status, created_at) VALUES (1, 1, 0, 'pending', '2026-03-07')");

        // Step 4: Add items
        $this->pdo->exec("INSERT INTO pg_wf_order_items (id, order_id, product_id, qty, unit_price) VALUES (1, 1, 1, 2, 29.99)");
        $this->pdo->exec("INSERT INTO pg_wf_order_items (id, order_id, product_id, qty, unit_price) VALUES (2, 1, 3, 5, 9.99)");

        // Step 5: Calculate total
        $stmt = $this->pdo->query('SELECT SUM(qty * unit_price) AS total FROM pg_wf_order_items WHERE order_id = 1');
        $total = (float) $stmt->fetch(PDO::FETCH_ASSOC)['total'];
        $this->assertSame(109.93, round($total, 2));

        // Step 6: Update order total and reduce stock
        $this->pdo->exec("UPDATE pg_wf_orders SET total = 109.93 WHERE id = 1");
        $this->pdo->exec("UPDATE pg_wf_products SET stock = stock - 2 WHERE id = 1");
        $this->pdo->exec("UPDATE pg_wf_products SET stock = stock - 5 WHERE id = 3");

        // Step 7: Complete order
        $this->pdo->exec("UPDATE pg_wf_orders SET status = 'completed' WHERE id = 1");

        // Verify
        $stmt = $this->pdo->query('SELECT status, total FROM pg_wf_orders WHERE id = 1');
        $order = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('completed', $order['status']);
        $this->assertSame(109.93, round((float) $order['total'], 2));

        $stmt = $this->pdo->query('SELECT stock FROM pg_wf_products WHERE id = 1');
        $this->assertSame(98, (int) $stmt->fetch(PDO::FETCH_ASSOC)['stock']);

        $stmt = $this->pdo->query('SELECT stock FROM pg_wf_products WHERE id = 3');
        $this->assertSame(195, (int) $stmt->fetch(PDO::FETCH_ASSOC)['stock']);

        // Verify nothing leaked
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) as c FROM pg_wf_orders');
        $this->assertSame(0, (int) $stmt->fetch(PDO::FETCH_ASSOC)['c']);
    }

    public function testUserRegistrationWithTierUpgrade(): void
    {
        $this->pdo->exec("INSERT INTO pg_wf_customers (id, name, email, tier) VALUES (1, 'Alice', 'alice@example.com', 'standard')");
        $this->pdo->exec("INSERT INTO pg_wf_customers (id, name, email, tier) VALUES (2, 'Bob', 'bob@example.com', 'standard')");
        $this->pdo->exec("INSERT INTO pg_wf_customers (id, name, email, tier) VALUES (3, 'Charlie', 'charlie@example.com', 'standard')");

        $this->pdo->exec("INSERT INTO pg_wf_orders (id, customer_id, total, status, created_at) VALUES (1, 1, 500.00, 'completed', '2026-01-01')");
        $this->pdo->exec("INSERT INTO pg_wf_orders (id, customer_id, total, status, created_at) VALUES (2, 1, 300.00, 'completed', '2026-02-01')");
        $this->pdo->exec("INSERT INTO pg_wf_orders (id, customer_id, total, status, created_at) VALUES (3, 2, 100.00, 'completed', '2026-01-15')");

        // Identify high-spending customers
        $stmt = $this->pdo->query("SELECT customer_id, SUM(total) AS spent FROM pg_wf_orders WHERE status = 'completed' GROUP BY customer_id HAVING SUM(total) > 400");
        $highSpenders = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $highSpenders);
        $this->assertSame(1, (int) $highSpenders[0]['customer_id']);

        // Upgrade tier directly (avoid GROUP BY HAVING subquery to stay safe)
        $this->pdo->exec("UPDATE pg_wf_customers SET tier = 'premium' WHERE id = 1");

        // Verify
        $stmt = $this->pdo->query('SELECT name, tier FROM pg_wf_customers ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('premium', $rows[0]['tier']);
        $this->assertSame('standard', $rows[1]['tier']);
        $this->assertSame('standard', $rows[2]['tier']);
    }

    public function testInventoryReportWithJoins(): void
    {
        $this->pdo->exec("INSERT INTO pg_wf_products (id, name, price, stock) VALUES (1, 'Widget', 29.99, 100)");
        $this->pdo->exec("INSERT INTO pg_wf_products (id, name, price, stock) VALUES (2, 'Gadget', 49.99, 0)");
        $this->pdo->exec("INSERT INTO pg_wf_products (id, name, price, stock) VALUES (3, 'Doohickey', 9.99, 200)");

        $this->pdo->exec("INSERT INTO pg_wf_customers (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");
        $this->pdo->exec("INSERT INTO pg_wf_orders (id, customer_id, total, status, created_at) VALUES (1, 1, 89.97, 'completed', '2026-03-07')");
        $this->pdo->exec("INSERT INTO pg_wf_order_items (id, order_id, product_id, qty, unit_price) VALUES (1, 1, 1, 3, 29.99)");

        // Report: products with total sold
        $stmt = $this->pdo->query("
            SELECT p.name, p.stock, COALESCE(SUM(oi.qty), 0) AS total_sold
            FROM pg_wf_products p
            LEFT JOIN pg_wf_order_items oi ON p.id = oi.product_id
            GROUP BY p.id, p.name, p.stock
            ORDER BY p.id
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(3, $rows);
        $this->assertSame('Widget', $rows[0]['name']);
        $this->assertSame(3, (int) $rows[0]['total_sold']);
        $this->assertSame('Gadget', $rows[1]['name']);
        $this->assertSame(0, (int) $rows[1]['total_sold']);

        // Out of stock
        $stmt = $this->pdo->query('SELECT name FROM pg_wf_products WHERE stock = 0');
        $outOfStock = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $outOfStock);
        $this->assertSame('Gadget', $outOfStock[0]['name']);
    }

    public function testOrderCancellationWorkflow(): void
    {
        $this->pdo->exec("INSERT INTO pg_wf_customers (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");
        $this->pdo->exec("INSERT INTO pg_wf_products (id, name, price, stock) VALUES (1, 'Widget', 29.99, 100)");
        $this->pdo->exec("INSERT INTO pg_wf_orders (id, customer_id, total, status, created_at) VALUES (1, 1, 59.98, 'completed', '2026-03-07')");
        $this->pdo->exec("INSERT INTO pg_wf_order_items (id, order_id, product_id, qty, unit_price) VALUES (1, 1, 1, 2, 29.99)");
        $this->pdo->exec("UPDATE pg_wf_products SET stock = stock - 2 WHERE id = 1");

        // Cancel: restore stock
        $stmt = $this->pdo->query('SELECT product_id, qty FROM pg_wf_order_items WHERE order_id = 1');
        $items = $stmt->fetchAll(PDO::FETCH_ASSOC);
        foreach ($items as $item) {
            $this->pdo->exec("UPDATE pg_wf_products SET stock = stock + {$item['qty']} WHERE id = {$item['product_id']}");
        }
        $this->pdo->exec("UPDATE pg_wf_orders SET status = 'cancelled' WHERE id = 1");
        $this->pdo->exec("DELETE FROM pg_wf_order_items WHERE order_id = 1");

        // Verify
        $stmt = $this->pdo->query('SELECT status FROM pg_wf_orders WHERE id = 1');
        $this->assertSame('cancelled', $stmt->fetch(PDO::FETCH_ASSOC)['status']);

        $stmt = $this->pdo->query('SELECT stock FROM pg_wf_products WHERE id = 1');
        $this->assertSame(100, (int) $stmt->fetch(PDO::FETCH_ASSOC)['stock']);

        $stmt = $this->pdo->query('SELECT COUNT(*) as c FROM pg_wf_order_items WHERE order_id = 1');
        $this->assertSame(0, (int) $stmt->fetch(PDO::FETCH_ASSOC)['c']);
    }
}
