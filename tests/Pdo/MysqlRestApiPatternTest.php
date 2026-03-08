<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests common REST API / controller-layer query patterns on MySQL.
 * @spec SPEC-4.1
 */
class MysqlRestApiPatternTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE api_products_m (id INT PRIMARY KEY, name VARCHAR(100), price DECIMAL(10,2), stock INT, active INT DEFAULT 1)',
            'CREATE TABLE api_categories_m (id INT PRIMARY KEY, name VARCHAR(50))',
            'CREATE TABLE api_product_categories_m (product_id INT, category_id INT, PRIMARY KEY (product_id, category_id))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['api_product_categories_m', 'api_products_m', 'api_categories_m'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO api_categories_m VALUES (1, 'Electronics')");
        $this->pdo->exec("INSERT INTO api_categories_m VALUES (2, 'Books')");
        $this->pdo->exec("INSERT INTO api_products_m VALUES (1, 'Laptop', 999.99, 50, 1)");
        $this->pdo->exec("INSERT INTO api_products_m VALUES (2, 'PHP Book', 29.99, 100, 1)");
        $this->pdo->exec("INSERT INTO api_products_m VALUES (3, 'Headphones', 79.99, 75, 1)");
        $this->pdo->exec("INSERT INTO api_products_m VALUES (4, 'Old Widget', 5.00, 0, 0)");
        $this->pdo->exec('INSERT INTO api_product_categories_m VALUES (1, 1)');
        $this->pdo->exec('INSERT INTO api_product_categories_m VALUES (2, 2)');
        $this->pdo->exec('INSERT INTO api_product_categories_m VALUES (3, 1)');
    }

    public function testListActiveProductsPaginated(): void
    {
        $stmt = $this->pdo->prepare(
            'SELECT id, name, price FROM api_products_m WHERE active = 1 ORDER BY name LIMIT ? OFFSET ?'
        );
        $stmt->bindValue(1, 2, PDO::PARAM_INT);
        $stmt->bindValue(2, 0, PDO::PARAM_INT);
        $stmt->execute();
        $page1 = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $page1);
    }

    public function testGetSingleProduct(): void
    {
        $stmt = $this->pdo->prepare('SELECT * FROM api_products_m WHERE id = ?');
        $stmt->execute([1]);
        $product = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Laptop', $product['name']);
    }

    public function testGetNonExistentProduct(): void
    {
        $stmt = $this->pdo->prepare('SELECT * FROM api_products_m WHERE id = ?');
        $stmt->execute([999]);
        $this->assertFalse($stmt->fetch(PDO::FETCH_ASSOC));
    }

    public function testCreateAndReadProduct(): void
    {
        $this->pdo->exec("INSERT INTO api_products_m VALUES (5, 'New Phone', 599.99, 30, 1)");
        $stmt = $this->pdo->query('SELECT name FROM api_products_m WHERE id = 5');
        $this->assertSame('New Phone', $stmt->fetchColumn());
    }

    public function testFilterProductsByCategory(): void
    {
        $stmt = $this->pdo->prepare(
            'SELECT p.name FROM api_products_m p
             JOIN api_product_categories_m pc ON p.id = pc.product_id
             JOIN api_categories_m c ON pc.category_id = c.id
             WHERE c.name = ? AND p.active = 1 ORDER BY p.name'
        );
        $stmt->execute(['Electronics']);
        $products = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Headphones', 'Laptop'], $products);
    }

    public function testSearchByName(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM api_products_m WHERE name LIKE ? AND active = 1');
        $stmt->execute(['%Book%']);
        $this->assertSame(['PHP Book'], $stmt->fetchAll(PDO::FETCH_COLUMN));
    }

    public function testFullCrudLifecycle(): void
    {
        $this->pdo->exec("INSERT INTO api_products_m VALUES (10, 'Test', 10.00, 5, 1)");
        $this->pdo->exec("UPDATE api_products_m SET price = 15.00 WHERE id = 10");

        $stmt = $this->pdo->query('SELECT price FROM api_products_m WHERE id = 10');
        $this->assertEquals(15.00, (float) $stmt->fetchColumn());

        $this->pdo->exec('DELETE FROM api_products_m WHERE id = 10');
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM api_products_m WHERE id = 10');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
