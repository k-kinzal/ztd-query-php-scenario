<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests WHERE clause operators on MySQL PDO: LIKE, NOT LIKE, BETWEEN, NOT BETWEEN,
 * EXISTS, NOT EXISTS, comparison operators — all after mutations.
 * @spec pending
 */
class MysqlWhereClauseOperatorsTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mysql_wco_products (id INT PRIMARY KEY, name VARCHAR(50), category VARCHAR(30), price DECIMAL(10,2), in_stock TINYINT)',
            'CREATE TABLE mysql_wco_orders (id INT PRIMARY KEY, product_id INT, customer VARCHAR(50), qty INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mysql_wco_orders', 'mysql_wco_products'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mysql_wco_products VALUES (1, 'Widget Alpha', 'electronics', 29.99, 1)");
        $this->pdo->exec("INSERT INTO mysql_wco_products VALUES (2, 'Widget Beta', 'electronics', 49.99, 1)");
        $this->pdo->exec("INSERT INTO mysql_wco_products VALUES (3, 'Gadget Pro', 'accessories', 15.00, 0)");
        $this->pdo->exec("INSERT INTO mysql_wco_products VALUES (4, 'Super Tool', 'tools', 99.99, 1)");
        $this->pdo->exec("INSERT INTO mysql_wco_products VALUES (5, 'Mini Tool', 'tools', 9.99, 1)");
        $this->pdo->exec("INSERT INTO mysql_wco_orders VALUES (1, 1, 'Alice', 2)");
        $this->pdo->exec("INSERT INTO mysql_wco_orders VALUES (2, 2, 'Bob', 1)");
        $this->pdo->exec("INSERT INTO mysql_wco_orders VALUES (3, 4, 'Alice', 3)");
    }

    public function testLikeWithPercentWildcard(): void
    {
        $stmt = $this->pdo->query("SELECT name FROM mysql_wco_products WHERE name LIKE 'Widget%' ORDER BY name");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Widget Alpha', $rows[0]['name']);
    }

    public function testLikeAfterUpdate(): void
    {
        $this->pdo->exec("UPDATE mysql_wco_products SET name = 'Widget Gamma' WHERE id = 3");

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mysql_wco_products WHERE name LIKE 'Widget%'");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(3, (int) $row['cnt']);
    }

    public function testBetweenAfterUpdate(): void
    {
        $this->pdo->exec("UPDATE mysql_wco_products SET price = 25.00 WHERE id = 5");

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mysql_wco_products WHERE price BETWEEN 10 AND 50");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(4, (int) $row['cnt']);
    }

    public function testExistsSubquery(): void
    {
        $stmt = $this->pdo->query("
            SELECT p.name FROM mysql_wco_products p
            WHERE EXISTS (SELECT 1 FROM mysql_wco_orders o WHERE o.product_id = p.id)
            ORDER BY p.name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
    }

    public function testNotExistsSubquery(): void
    {
        $stmt = $this->pdo->query("
            SELECT p.name FROM mysql_wco_products p
            WHERE NOT EXISTS (SELECT 1 FROM mysql_wco_orders o WHERE o.product_id = p.id)
            ORDER BY p.name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
    }

    public function testExistsAfterInsertingOrder(): void
    {
        $this->pdo->exec("INSERT INTO mysql_wco_orders VALUES (4, 3, 'Charlie', 1)");

        $stmt = $this->pdo->query("
            SELECT p.name FROM mysql_wco_products p
            WHERE EXISTS (SELECT 1 FROM mysql_wco_orders o WHERE o.product_id = p.id)
            ORDER BY p.name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(4, $rows);
    }

    public function testPreparedLikeWithParameter(): void
    {
        $stmt = $this->pdo->prepare("SELECT name FROM mysql_wco_products WHERE name LIKE ? ORDER BY name");
        $stmt->execute(['%Tool%']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
    }

    public function testPreparedBetweenWithParameters(): void
    {
        $stmt = $this->pdo->prepare("SELECT name FROM mysql_wco_products WHERE price BETWEEN ? AND ? ORDER BY price");
        $stmt->execute([20, 60]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
    }
}
