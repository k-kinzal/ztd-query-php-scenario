<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests DELETE with subqueries that JOIN other tables through MySQL PDO CTE shadow store.
 *
 * @spec SPEC-4.3
 */
class MysqlDeleteWithSubqueryJoinTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_dj_categories (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL, active TINYINT NOT NULL DEFAULT 1)',
            'CREATE TABLE my_dj_products (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL, category_id INT NOT NULL, price DECIMAL(10,2) NOT NULL, discontinued TINYINT NOT NULL DEFAULT 0)',
            'CREATE TABLE my_dj_order_items (id INT PRIMARY KEY, product_id INT NOT NULL, quantity INT NOT NULL)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_dj_order_items', 'my_dj_products', 'my_dj_categories'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_dj_categories VALUES (1, 'Electronics', 1)");
        $this->pdo->exec("INSERT INTO my_dj_categories VALUES (2, 'Clothing', 1)");
        $this->pdo->exec("INSERT INTO my_dj_categories VALUES (3, 'Discontinued', 0)");

        $this->pdo->exec("INSERT INTO my_dj_products VALUES (1, 'Laptop', 1, 999.99, 0)");
        $this->pdo->exec("INSERT INTO my_dj_products VALUES (2, 'Phone', 1, 599.99, 0)");
        $this->pdo->exec("INSERT INTO my_dj_products VALUES (3, 'T-shirt', 2, 29.99, 0)");
        $this->pdo->exec("INSERT INTO my_dj_products VALUES (4, 'Old Gadget', 3, 49.99, 1)");
        $this->pdo->exec("INSERT INTO my_dj_products VALUES (5, 'Old Widget', 3, 19.99, 1)");

        $this->pdo->exec("INSERT INTO my_dj_order_items VALUES (1, 1, 2)");
        $this->pdo->exec("INSERT INTO my_dj_order_items VALUES (2, 2, 1)");
        $this->pdo->exec("INSERT INTO my_dj_order_items VALUES (3, 3, 5)");
    }

    public function testDeleteWithSubqueryJoin(): void
    {
        $this->pdo->exec(
            "DELETE FROM my_dj_products WHERE category_id IN (SELECT c.id FROM my_dj_categories c WHERE c.active = 0)"
        );

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM my_dj_products");
        $this->assertEquals(3, (int) $rows[0]['cnt']);
    }

    public function testDeleteOrphansNotExists(): void
    {
        $this->pdo->exec(
            "DELETE FROM my_dj_products WHERE NOT EXISTS (SELECT 1 FROM my_dj_order_items oi WHERE oi.product_id = my_dj_products.id)"
        );

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM my_dj_products");
        $this->assertEquals(3, (int) $rows[0]['cnt']);
    }

    public function testDeleteWithMultiTableSubquery(): void
    {
        $this->pdo->exec(
            "DELETE FROM my_dj_order_items WHERE product_id IN (SELECT p.id FROM my_dj_products p JOIN my_dj_categories c ON c.id = p.category_id WHERE c.name = 'Clothing')"
        );

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM my_dj_order_items");
        $this->assertEquals(2, (int) $rows[0]['cnt']);
    }

    public function testPreparedDeleteWithSubquery(): void
    {
        $stmt = $this->pdo->prepare(
            "DELETE FROM my_dj_products WHERE category_id IN (SELECT id FROM my_dj_categories WHERE name = ?)"
        );
        $stmt->execute(['Discontinued']);

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM my_dj_products");
        $this->assertEquals(3, (int) $rows[0]['cnt']);
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM my_dj_products")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
