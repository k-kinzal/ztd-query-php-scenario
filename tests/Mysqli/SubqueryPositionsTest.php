<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests subqueries in various SQL positions on MySQLi.
 * @spec pending
 */
class SubqueryPositionsTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_sp_products (id INT PRIMARY KEY, name VARCHAR(50), category VARCHAR(10), price DECIMAL(10,2))',
            'CREATE TABLE mi_sp_orders (id INT PRIMARY KEY, product_id INT, qty INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_sp_orders', 'mi_sp_products'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_sp_products (id, name, category, price) VALUES (1, 'Widget', 'A', 10.00)");
        $this->mysqli->query("INSERT INTO mi_sp_products (id, name, category, price) VALUES (2, 'Gadget', 'A', 25.00)");
        $this->mysqli->query("INSERT INTO mi_sp_products (id, name, category, price) VALUES (3, 'Doohickey', 'B', 5.00)");
        $this->mysqli->query("INSERT INTO mi_sp_products (id, name, category, price) VALUES (4, 'Thingamajig', 'B', 50.00)");
        $this->mysqli->query("INSERT INTO mi_sp_orders (id, product_id, qty) VALUES (1, 1, 3)");
        $this->mysqli->query("INSERT INTO mi_sp_orders (id, product_id, qty) VALUES (2, 2, 1)");
        $this->mysqli->query("INSERT INTO mi_sp_orders (id, product_id, qty) VALUES (3, 1, 2)");
        $this->mysqli->query("INSERT INTO mi_sp_orders (id, product_id, qty) VALUES (4, 3, 5)");
    }

    public function testScalarSubqueryInSelectList(): void
    {
        $result = $this->mysqli->query("
            SELECT p.name,
                   (SELECT SUM(o.qty) FROM mi_sp_orders o WHERE o.product_id = p.id) AS total_qty
            FROM mi_sp_products p
            ORDER BY p.id
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertSame(5, (int) $rows[0]['total_qty']);
        $this->assertNull($rows[3]['total_qty']);
    }

    public function testNestedWhereWithMultipleOperators(): void
    {
        $result = $this->mysqli->query("
            SELECT p.name FROM mi_sp_products p
            WHERE (p.price > 8 AND p.category = 'A')
               OR (p.price < 10 AND p.id IN (SELECT o.product_id FROM mi_sp_orders o WHERE o.qty >= 5))
            ORDER BY p.id
        ");
        $names = array_column($result->fetch_all(MYSQLI_ASSOC), 'name');
        $this->assertSame(['Widget', 'Gadget', 'Doohickey'], $names);
    }

    public function testExistsNotExistsCombined(): void
    {
        $result = $this->mysqli->query("
            SELECT p.name FROM mi_sp_products p
            WHERE EXISTS (SELECT 1 FROM mi_sp_orders o WHERE o.product_id = p.id)
              AND NOT EXISTS (SELECT 1 FROM mi_sp_orders o WHERE o.product_id = p.id AND o.qty > 4)
            ORDER BY p.id
        ");
        $names = array_column($result->fetch_all(MYSQLI_ASSOC), 'name');
        $this->assertSame(['Widget', 'Gadget'], $names);
    }
}
