<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests common REST API / controller-layer query patterns.
 * These simulate typical CRUD operations in web frameworks like Laravel/Symfony.
 */
class SqliteRestApiPatternTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);

        $this->pdo->exec('CREATE TABLE api_products (id INT PRIMARY KEY, name VARCHAR(100), price DECIMAL(10,2), stock INT, active INT DEFAULT 1)');
        $this->pdo->exec('CREATE TABLE api_categories (id INT PRIMARY KEY, name VARCHAR(50))');
        $this->pdo->exec('CREATE TABLE api_product_categories (product_id INT, category_id INT, PRIMARY KEY (product_id, category_id))');

        // Seed categories
        $this->pdo->exec("INSERT INTO api_categories VALUES (1, 'Electronics')");
        $this->pdo->exec("INSERT INTO api_categories VALUES (2, 'Books')");
        $this->pdo->exec("INSERT INTO api_categories VALUES (3, 'Clothing')");

        // Seed products
        $this->pdo->exec("INSERT INTO api_products VALUES (1, 'Laptop', 999.99, 50, 1)");
        $this->pdo->exec("INSERT INTO api_products VALUES (2, 'PHP Book', 29.99, 100, 1)");
        $this->pdo->exec("INSERT INTO api_products VALUES (3, 'T-Shirt', 19.99, 200, 1)");
        $this->pdo->exec("INSERT INTO api_products VALUES (4, 'Headphones', 79.99, 75, 1)");
        $this->pdo->exec("INSERT INTO api_products VALUES (5, 'Old Widget', 5.00, 0, 0)");

        // Seed mappings
        $this->pdo->exec('INSERT INTO api_product_categories VALUES (1, 1)');
        $this->pdo->exec('INSERT INTO api_product_categories VALUES (2, 2)');
        $this->pdo->exec('INSERT INTO api_product_categories VALUES (3, 3)');
        $this->pdo->exec('INSERT INTO api_product_categories VALUES (4, 1)');
    }

    /**
     * GET /api/products - List active products with pagination
     */
    public function testListActiveProductsPaginated(): void
    {
        $stmt = $this->pdo->prepare(
            'SELECT id, name, price, stock FROM api_products WHERE active = 1 ORDER BY name LIMIT ? OFFSET ?'
        );
        $stmt->execute([2, 0]);
        $page1 = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $page1);
        $this->assertSame('Headphones', $page1[0]['name']);

        $stmt->execute([2, 2]);
        $page2 = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $page2);
    }

    /**
     * GET /api/products/1 - Get single product
     */
    public function testGetSingleProduct(): void
    {
        $stmt = $this->pdo->prepare('SELECT * FROM api_products WHERE id = ?');
        $stmt->execute([1]);
        $product = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->assertNotFalse($product);
        $this->assertSame('Laptop', $product['name']);
        $this->assertEquals(999.99, (float) $product['price']);
    }

    /**
     * GET /api/products/999 - Product not found
     */
    public function testGetNonExistentProduct(): void
    {
        $stmt = $this->pdo->prepare('SELECT * FROM api_products WHERE id = ?');
        $stmt->execute([999]);
        $product = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->assertFalse($product);
    }

    /**
     * POST /api/products - Create new product
     */
    public function testCreateProduct(): void
    {
        $this->pdo->exec("INSERT INTO api_products VALUES (6, 'New Phone', 599.99, 30, 1)");

        $stmt = $this->pdo->query('SELECT * FROM api_products WHERE id = 6');
        $product = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('New Phone', $product['name']);

        // Verify count increased
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM api_products WHERE active = 1');
        $this->assertSame(5, (int) $stmt->fetchColumn());
    }

    /**
     * PUT /api/products/1 - Update product
     */
    public function testUpdateProduct(): void
    {
        $stmt = $this->pdo->prepare('UPDATE api_products SET name = ?, price = ?, stock = ? WHERE id = ?');
        $stmt->execute(['Laptop Pro', 1299.99, 25, 1]);

        $query = $this->pdo->query('SELECT name, price, stock FROM api_products WHERE id = 1');
        $product = $query->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Laptop Pro', $product['name']);
        $this->assertEquals(1299.99, (float) $product['price']);
        $this->assertEquals(25, (int) $product['stock']);
    }

    /**
     * PATCH /api/products/1 - Partial update (soft-delete)
     */
    public function testSoftDeleteProduct(): void
    {
        $this->pdo->exec('UPDATE api_products SET active = 0 WHERE id = 4');

        // Active products count should decrease
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM api_products WHERE active = 1');
        $this->assertSame(3, (int) $stmt->fetchColumn());

        // Product still exists
        $stmt = $this->pdo->query('SELECT active FROM api_products WHERE id = 4');
        $this->assertEquals(0, (int) $stmt->fetchColumn());
    }

    /**
     * DELETE /api/products/5 - Hard delete
     */
    public function testDeleteProduct(): void
    {
        $result = $this->pdo->exec('DELETE FROM api_products WHERE id = 5');
        $this->assertSame(1, $result);

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM api_products');
        $this->assertSame(4, (int) $stmt->fetchColumn());
    }

    /**
     * GET /api/products?category=Electronics - Filter by category (JOIN)
     */
    public function testFilterProductsByCategory(): void
    {
        $stmt = $this->pdo->prepare(
            'SELECT p.name, p.price
             FROM api_products p
             JOIN api_product_categories pc ON p.id = pc.product_id
             JOIN api_categories c ON pc.category_id = c.id
             WHERE c.name = ? AND p.active = 1
             ORDER BY p.name'
        );
        $stmt->execute(['Electronics']);
        $products = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $products);
        $this->assertSame('Headphones', $products[0]['name']);
        $this->assertSame('Laptop', $products[1]['name']);
    }

    /**
     * GET /api/products/stats - Aggregate statistics
     */
    public function testProductStatistics(): void
    {
        $stmt = $this->pdo->query(
            'SELECT
                COUNT(*) as total,
                COUNT(CASE WHEN active = 1 THEN 1 END) as active_count,
                SUM(CASE WHEN active = 1 THEN stock ELSE 0 END) as total_stock,
                ROUND(AVG(CASE WHEN active = 1 THEN price END), 2) as avg_price
             FROM api_products'
        );
        $stats = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->assertEquals(5, (int) $stats['total']);
        $this->assertEquals(4, (int) $stats['active_count']);
    }

    /**
     * PUT /api/products/bulk-deactivate - Bulk update
     */
    public function testBulkDeactivate(): void
    {
        // Deactivate all low-stock items (stock < 60)
        $result = $this->pdo->exec('UPDATE api_products SET active = 0 WHERE stock < 60');
        // Laptop(50), Old Widget(0) → 2 rows matched
        $this->assertSame(2, $result);

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM api_products WHERE active = 0');
        // Old Widget already inactive + Laptop now deactivated = 2
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * GET /api/products/search?q=book - Search by name
     */
    public function testSearchByName(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM api_products WHERE name LIKE ? AND active = 1');
        $stmt->execute(['%Book%']);
        $results = $stmt->fetchAll(PDO::FETCH_COLUMN);

        $this->assertCount(1, $results);
        $this->assertSame('PHP Book', $results[0]);
    }

    /**
     * GET /api/products?min_price=50&max_price=500 - Price range filter
     */
    public function testPriceRangeFilter(): void
    {
        $stmt = $this->pdo->prepare(
            'SELECT name, price FROM api_products WHERE price BETWEEN ? AND ? AND active = 1 ORDER BY price'
        );
        $stmt->execute([50, 500]);
        $products = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $products);
        $this->assertSame('Headphones', $products[0]['name']);
    }

    /**
     * Full CRUD lifecycle test
     */
    public function testFullCrudLifecycle(): void
    {
        // Create
        $this->pdo->exec("INSERT INTO api_products VALUES (10, 'Test Item', 10.00, 5, 1)");

        // Read
        $stmt = $this->pdo->query('SELECT name FROM api_products WHERE id = 10');
        $this->assertSame('Test Item', $stmt->fetchColumn());

        // Update
        $this->pdo->exec("UPDATE api_products SET price = 15.00, stock = 3 WHERE id = 10");
        $stmt = $this->pdo->query('SELECT price, stock FROM api_products WHERE id = 10');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertEquals(15.00, (float) $row['price']);
        $this->assertEquals(3, (int) $row['stock']);

        // Delete
        $this->pdo->exec('DELETE FROM api_products WHERE id = 10');
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM api_products WHERE id = 10');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
