<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests a shopping cart and checkout workflow through ZTD shadow store (SQLite PDO).
 * Covers cart aggregation, multi-step checkout, stock management,
 * category reporting, and physical isolation.
 * @spec SPEC-10.2.61
 */
class SqliteShoppingCartCheckoutTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_sc_products (
                id INTEGER PRIMARY KEY,
                name TEXT,
                price REAL,
                stock INTEGER,
                category TEXT
            )',
            'CREATE TABLE sl_sc_cart_items (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                product_id INTEGER,
                quantity INTEGER,
                added_at TEXT
            )',
            'CREATE TABLE sl_sc_orders (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                status TEXT,
                total REAL,
                created_at TEXT
            )',
            'CREATE TABLE sl_sc_order_items (
                id INTEGER PRIMARY KEY,
                order_id INTEGER,
                product_id INTEGER,
                quantity INTEGER,
                unit_price REAL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_sc_order_items', 'sl_sc_orders', 'sl_sc_cart_items', 'sl_sc_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Products
        $this->pdo->exec("INSERT INTO sl_sc_products VALUES (1, 'Laptop', 999.99, 10, 'electronics')");
        $this->pdo->exec("INSERT INTO sl_sc_products VALUES (2, 'Mouse', 29.99, 50, 'electronics')");
        $this->pdo->exec("INSERT INTO sl_sc_products VALUES (3, 'Notebook', 4.99, 200, 'stationery')");
        $this->pdo->exec("INSERT INTO sl_sc_products VALUES (4, 'Pen', 1.99, 500, 'stationery')");
        $this->pdo->exec("INSERT INTO sl_sc_products VALUES (5, 'Headphones', 149.99, 25, 'electronics')");

        // User 1's cart: Laptop x1, Mouse x2, Notebook x5
        $this->pdo->exec("INSERT INTO sl_sc_cart_items VALUES (1, 1, 1, 1, '2026-03-09 10:00:00')");
        $this->pdo->exec("INSERT INTO sl_sc_cart_items VALUES (2, 1, 2, 2, '2026-03-09 10:05:00')");
        $this->pdo->exec("INSERT INTO sl_sc_cart_items VALUES (3, 1, 3, 5, '2026-03-09 10:10:00')");

        // User 2's cart: Headphones x1, Pen x10
        $this->pdo->exec("INSERT INTO sl_sc_cart_items VALUES (4, 2, 5, 1, '2026-03-09 11:00:00')");
        $this->pdo->exec("INSERT INTO sl_sc_cart_items VALUES (5, 2, 4, 10, '2026-03-09 11:05:00')");
    }

    public function testCartContents(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.name, p.price, ci.quantity,
                    ROUND(p.price * ci.quantity, 2) AS line_total
             FROM sl_sc_cart_items ci
             JOIN sl_sc_products p ON p.id = ci.product_id
             WHERE ci.user_id = 1
             ORDER BY ci.added_at"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Laptop', $rows[0]['name']);
        $this->assertEquals(999.99, round((float) $rows[0]['line_total'], 2));
        $this->assertSame('Mouse', $rows[1]['name']);
        $this->assertEquals(59.98, round((float) $rows[1]['line_total'], 2));
        $this->assertSame('Notebook', $rows[2]['name']);
        $this->assertEquals(24.95, round((float) $rows[2]['line_total'], 2));
    }

    public function testCartTotalWithPreparedStatement(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT ROUND(SUM(p.price * ci.quantity), 2) AS cart_total
             FROM sl_sc_cart_items ci
             JOIN sl_sc_products p ON p.id = ci.product_id
             WHERE ci.user_id = ?",
            [1]
        );

        $this->assertEquals(1084.92, round((float) $rows[0]['cart_total'], 2));

        $rows = $this->ztdPrepareAndExecute(
            "SELECT ROUND(SUM(p.price * ci.quantity), 2) AS cart_total
             FROM sl_sc_cart_items ci
             JOIN sl_sc_products p ON p.id = ci.product_id
             WHERE ci.user_id = ?",
            [2]
        );

        $this->assertEquals(169.89, round((float) $rows[0]['cart_total'], 2));
    }

    public function testAddToCartAndVerifyTotal(): void
    {
        $this->pdo->exec("INSERT INTO sl_sc_cart_items VALUES (6, 1, 5, 1, '2026-03-09 10:15:00')");

        $rows = $this->ztdQuery(
            "SELECT ROUND(SUM(p.price * ci.quantity), 2) AS cart_total
             FROM sl_sc_cart_items ci
             JOIN sl_sc_products p ON p.id = ci.product_id
             WHERE ci.user_id = 1"
        );

        $this->assertEquals(1234.91, round((float) $rows[0]['cart_total'], 2));
    }

    public function testCheckoutCreatesOrder(): void
    {
        // Calculate cart total
        $rows = $this->ztdQuery(
            "SELECT ROUND(SUM(p.price * ci.quantity), 2) AS cart_total
             FROM sl_sc_cart_items ci
             JOIN sl_sc_products p ON p.id = ci.product_id
             WHERE ci.user_id = 1"
        );
        $total = $rows[0]['cart_total'];

        // Create order
        $this->pdo->exec("INSERT INTO sl_sc_orders VALUES (1, 1, 'pending', {$total}, '2026-03-09 12:00:00')");

        // Create order items
        $this->pdo->exec("INSERT INTO sl_sc_order_items VALUES (1, 1, 1, 1, 999.99)");
        $this->pdo->exec("INSERT INTO sl_sc_order_items VALUES (2, 1, 2, 2, 29.99)");
        $this->pdo->exec("INSERT INTO sl_sc_order_items VALUES (3, 1, 3, 5, 4.99)");

        // Clear cart
        $this->pdo->exec("DELETE FROM sl_sc_cart_items WHERE user_id = 1");

        // Verify order
        $rows = $this->ztdQuery("SELECT status, total FROM sl_sc_orders WHERE id = 1");
        $this->assertSame('pending', $rows[0]['status']);
        $this->assertEquals(1084.92, round((float) $rows[0]['total'], 2));

        // Verify order items
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_sc_order_items WHERE order_id = 1");
        $this->assertEquals(3, (int) $rows[0]['cnt']);

        // Verify cart is empty for user 1
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_sc_cart_items WHERE user_id = 1");
        $this->assertEquals(0, (int) $rows[0]['cnt']);

        // User 2's cart is untouched
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_sc_cart_items WHERE user_id = 2");
        $this->assertEquals(2, (int) $rows[0]['cnt']);
    }

    public function testStockDecrementAfterOrder(): void
    {
        $this->pdo->exec("INSERT INTO sl_sc_orders VALUES (1, 2, 'confirmed', 169.89, '2026-03-09 13:00:00')");
        $this->pdo->exec("INSERT INTO sl_sc_order_items VALUES (1, 1, 5, 1, 149.99)");
        $this->pdo->exec("INSERT INTO sl_sc_order_items VALUES (2, 1, 4, 10, 1.99)");

        $this->pdo->exec("UPDATE sl_sc_products SET stock = stock - 1 WHERE id = 5");
        $this->pdo->exec("UPDATE sl_sc_products SET stock = stock - 10 WHERE id = 4");

        $rows = $this->ztdQuery("SELECT stock FROM sl_sc_products WHERE id = 5");
        $this->assertEquals(24, (int) $rows[0]['stock']);

        $rows = $this->ztdQuery("SELECT stock FROM sl_sc_products WHERE id = 4");
        $this->assertEquals(490, (int) $rows[0]['stock']);
    }

    public function testCategoryRevenueReport(): void
    {
        $this->pdo->exec("INSERT INTO sl_sc_orders VALUES (1, 1, 'completed', 1084.92, '2026-03-09 12:00:00')");
        $this->pdo->exec("INSERT INTO sl_sc_order_items VALUES (1, 1, 1, 1, 999.99)");
        $this->pdo->exec("INSERT INTO sl_sc_order_items VALUES (2, 1, 2, 2, 29.99)");
        $this->pdo->exec("INSERT INTO sl_sc_order_items VALUES (3, 1, 3, 5, 4.99)");

        $rows = $this->ztdQuery(
            "SELECT p.category,
                    SUM(oi.quantity) AS items_sold,
                    ROUND(SUM(oi.quantity * oi.unit_price), 2) AS revenue
             FROM sl_sc_order_items oi
             JOIN sl_sc_products p ON p.id = oi.product_id
             GROUP BY p.category
             ORDER BY revenue DESC"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('electronics', $rows[0]['category']);
        $this->assertEquals(1059.97, round((float) $rows[0]['revenue'], 2));
        $this->assertSame('stationery', $rows[1]['category']);
        $this->assertEquals(24.95, round((float) $rows[1]['revenue'], 2));
    }

    public function testOrderStatusTransition(): void
    {
        $this->pdo->exec("INSERT INTO sl_sc_orders VALUES (1, 1, 'pending', 1084.92, '2026-03-09 12:00:00')");

        $affected = $this->pdo->exec("UPDATE sl_sc_orders SET status = 'confirmed' WHERE id = 1 AND status = 'pending'");
        $this->assertSame(1, $affected);

        $rows = $this->ztdQuery("SELECT status FROM sl_sc_orders WHERE id = 1");
        $this->assertSame('confirmed', $rows[0]['status']);

        $affected = $this->pdo->exec("UPDATE sl_sc_orders SET status = 'shipped' WHERE id = 1 AND status = 'confirmed'");
        $this->assertSame(1, $affected);

        $rows = $this->ztdQuery("SELECT status FROM sl_sc_orders WHERE id = 1");
        $this->assertSame('shipped', $rows[0]['status']);

        // Guard: can't transition from pending when status is shipped
        $affected = $this->pdo->exec("UPDATE sl_sc_orders SET status = 'confirmed' WHERE id = 1 AND status = 'pending'");
        $this->assertSame(0, $affected);
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO sl_sc_products VALUES (6, 'Tablet', 499.99, 15, 'electronics')");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_sc_products");
        $this->assertSame(6, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_sc_products")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
