<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests WHERE clause operators on MySQLi: LIKE, BETWEEN, EXISTS, NOT EXISTS,
 * comparison operators — all after mutations.
 * @spec SPEC-3.1
 */
class WhereClauseOperatorsTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_wco_products (id INT PRIMARY KEY, name VARCHAR(50), category VARCHAR(30), price DECIMAL(10,2), in_stock TINYINT)',
            'CREATE TABLE mi_wco_orders (id INT PRIMARY KEY, product_id INT, customer VARCHAR(50), qty INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_wco_orders', 'mi_wco_products'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_wco_products VALUES (1, 'Widget Alpha', 'electronics', 29.99, 1)");
        $this->mysqli->query("INSERT INTO mi_wco_products VALUES (2, 'Widget Beta', 'electronics', 49.99, 1)");
        $this->mysqli->query("INSERT INTO mi_wco_products VALUES (3, 'Gadget Pro', 'accessories', 15.00, 0)");
        $this->mysqli->query("INSERT INTO mi_wco_products VALUES (4, 'Super Tool', 'tools', 99.99, 1)");
        $this->mysqli->query("INSERT INTO mi_wco_products VALUES (5, 'Mini Tool', 'tools', 9.99, 1)");
        $this->mysqli->query("INSERT INTO mi_wco_orders VALUES (1, 1, 'Alice', 2)");
        $this->mysqli->query("INSERT INTO mi_wco_orders VALUES (2, 2, 'Bob', 1)");
        $this->mysqli->query("INSERT INTO mi_wco_orders VALUES (3, 4, 'Alice', 3)");
    }

    public function testLikeWithPercentWildcard(): void
    {
        $result = $this->mysqli->query("SELECT name FROM mi_wco_products WHERE name LIKE 'Widget%' ORDER BY name");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Widget Alpha', $rows[0]['name']);
    }

    public function testLikeAfterUpdate(): void
    {
        $this->mysqli->query("UPDATE mi_wco_products SET name = 'Widget Gamma' WHERE id = 3");

        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_wco_products WHERE name LIKE 'Widget%'");
        $row = $result->fetch_assoc();
        $this->assertSame(3, (int) $row['cnt']);
    }

    public function testBetweenAfterUpdate(): void
    {
        $this->mysqli->query("UPDATE mi_wco_products SET price = 25.00 WHERE id = 5");

        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_wco_products WHERE price BETWEEN 10 AND 50");
        $row = $result->fetch_assoc();
        $this->assertSame(4, (int) $row['cnt']);
    }

    public function testExistsSubquery(): void
    {
        $result = $this->mysqli->query("
            SELECT p.name FROM mi_wco_products p
            WHERE EXISTS (SELECT 1 FROM mi_wco_orders o WHERE o.product_id = p.id)
            ORDER BY p.name
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(3, $rows);
    }

    public function testNotExistsAfterDeletingOrders(): void
    {
        $this->mysqli->query("DELETE FROM mi_wco_orders WHERE product_id = 1");

        $result = $this->mysqli->query("
            SELECT p.name FROM mi_wco_products p
            WHERE NOT EXISTS (SELECT 1 FROM mi_wco_orders o WHERE o.product_id = p.id)
            ORDER BY p.name
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(3, $rows);
    }

    public function testPreparedLikeWithParameter(): void
    {
        $stmt = $this->mysqli->prepare("SELECT name FROM mi_wco_products WHERE name LIKE ? ORDER BY name");
        $pattern = '%Tool%';
        $stmt->bind_param('s', $pattern);
        $stmt->execute();
        $result = $stmt->get_result();
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
    }
}
