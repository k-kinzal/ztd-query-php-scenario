<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests common REST API / controller-layer query patterns via MySQLi.
 *
 * Cross-platform parity with MysqlRestApiPatternTest (PDO).
 * @spec SPEC-4.1
 */
class RestApiPatternTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_api_products (id INT PRIMARY KEY, name VARCHAR(100), price DECIMAL(10,2), stock INT, active INT DEFAULT 1)',
            'CREATE TABLE mi_api_categories (id INT PRIMARY KEY, name VARCHAR(50))',
            'CREATE TABLE mi_api_pc (product_id INT, category_id INT, PRIMARY KEY (product_id, category_id))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_api_pc', 'mi_api_products', 'mi_api_categories'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_api_categories VALUES (1, 'Electronics')");
        $this->mysqli->query("INSERT INTO mi_api_categories VALUES (2, 'Books')");
        $this->mysqli->query("INSERT INTO mi_api_products VALUES (1, 'Laptop', 999.99, 50, 1)");
        $this->mysqli->query("INSERT INTO mi_api_products VALUES (2, 'PHP Book', 29.99, 100, 1)");
        $this->mysqli->query("INSERT INTO mi_api_products VALUES (3, 'Headphones', 79.99, 75, 1)");
        $this->mysqli->query("INSERT INTO mi_api_products VALUES (4, 'Old Widget', 5.00, 0, 0)");
        $this->mysqli->query('INSERT INTO mi_api_pc VALUES (1, 1)');
        $this->mysqli->query('INSERT INTO mi_api_pc VALUES (2, 2)');
        $this->mysqli->query('INSERT INTO mi_api_pc VALUES (3, 1)');
    }

    public function testGetSingleProduct(): void
    {
        $stmt = $this->mysqli->prepare('SELECT name, price FROM mi_api_products WHERE id = ?');
        $id = 1;
        $stmt->bind_param('i', $id);
        $stmt->execute();
        $result = $stmt->get_result();
        $row = $result->fetch_assoc();
        $this->assertSame('Laptop', $row['name']);
    }

    public function testGetNonExistentProduct(): void
    {
        $stmt = $this->mysqli->prepare('SELECT * FROM mi_api_products WHERE id = ?');
        $id = 999;
        $stmt->bind_param('i', $id);
        $stmt->execute();
        $result = $stmt->get_result();
        $this->assertNull($result->fetch_assoc());
    }

    public function testFilterProductsByCategory(): void
    {
        $stmt = $this->mysqli->prepare(
            'SELECT p.name FROM mi_api_products p
             JOIN mi_api_pc pc ON p.id = pc.product_id
             JOIN mi_api_categories c ON pc.category_id = c.id
             WHERE c.name = ? AND p.active = 1 ORDER BY p.name'
        );
        $cat = 'Electronics';
        $stmt->bind_param('s', $cat);
        $stmt->execute();
        $result = $stmt->get_result();
        $names = [];
        while ($row = $result->fetch_assoc()) {
            $names[] = $row['name'];
        }
        $this->assertSame(['Headphones', 'Laptop'], $names);
    }

    public function testSearchByName(): void
    {
        $stmt = $this->mysqli->prepare('SELECT name FROM mi_api_products WHERE name LIKE ? AND active = 1');
        $like = '%Book%';
        $stmt->bind_param('s', $like);
        $stmt->execute();
        $result = $stmt->get_result();
        $row = $result->fetch_assoc();
        $this->assertSame('PHP Book', $row['name']);
    }

    public function testFullCrudLifecycle(): void
    {
        $this->mysqli->query("INSERT INTO mi_api_products VALUES (10, 'Test', 10.00, 5, 1)");
        $this->mysqli->query('UPDATE mi_api_products SET price = 15.00 WHERE id = 10');

        $result = $this->mysqli->query('SELECT price FROM mi_api_products WHERE id = 10');
        $this->assertEquals(15.00, (float) $result->fetch_assoc()['price']);

        $this->mysqli->query('DELETE FROM mi_api_products WHERE id = 10');
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_api_products WHERE id = 10');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
