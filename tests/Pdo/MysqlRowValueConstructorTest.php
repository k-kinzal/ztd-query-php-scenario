<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests row value constructors (tuple comparisons) through MySQL PDO CTE shadow store.
 *
 * @spec SPEC-3.1
 */
class MysqlRowValueConstructorTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_rv_order_items (
                order_id INT NOT NULL,
                product_id INT NOT NULL,
                quantity INT NOT NULL,
                unit_price DECIMAL(10,2) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT \'pending\',
                PRIMARY KEY (order_id, product_id)
            )',
            'CREATE TABLE my_rv_orders (
                id INT PRIMARY KEY,
                customer VARCHAR(100) NOT NULL,
                order_date DATE NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_rv_order_items', 'my_rv_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_rv_orders VALUES (1, 'Alice', '2025-01-10')");
        $this->pdo->exec("INSERT INTO my_rv_orders VALUES (2, 'Bob', '2025-01-11')");
        $this->pdo->exec("INSERT INTO my_rv_orders VALUES (3, 'Carol', '2025-01-12')");

        $this->pdo->exec("INSERT INTO my_rv_order_items VALUES (1, 100, 2, 25.00, 'shipped')");
        $this->pdo->exec("INSERT INTO my_rv_order_items VALUES (1, 200, 1, 50.00, 'pending')");
        $this->pdo->exec("INSERT INTO my_rv_order_items VALUES (2, 100, 3, 25.00, 'shipped')");
        $this->pdo->exec("INSERT INTO my_rv_order_items VALUES (2, 300, 1, 75.00, 'shipped')");
        $this->pdo->exec("INSERT INTO my_rv_order_items VALUES (3, 200, 2, 50.00, 'pending')");
        $this->pdo->exec("INSERT INTO my_rv_order_items VALUES (3, 300, 1, 75.00, 'cancelled')");
    }

    public function testRowValueIn(): void
    {
        $rows = $this->ztdQuery(
            "SELECT order_id, product_id, quantity
             FROM my_rv_order_items
             WHERE (order_id, product_id) IN ((1, 100), (2, 300), (3, 200))
             ORDER BY order_id"
        );

        $this->assertCount(3, $rows);
        $this->assertEquals(1, (int) $rows[0]['order_id']);
        $this->assertEquals(100, (int) $rows[0]['product_id']);
        $this->assertEquals(2, (int) $rows[1]['order_id']);
        $this->assertEquals(300, (int) $rows[1]['product_id']);
    }

    public function testRowValueNotIn(): void
    {
        $rows = $this->ztdQuery(
            "SELECT order_id, product_id
             FROM my_rv_order_items
             WHERE (order_id, product_id) NOT IN ((1, 100), (2, 300), (3, 200))
             ORDER BY order_id, product_id"
        );

        $this->assertCount(3, $rows);
    }

    public function testRowValueEquality(): void
    {
        $rows = $this->ztdQuery(
            "SELECT quantity FROM my_rv_order_items
             WHERE (order_id, product_id) = (2, 100)"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(3, (int) $rows[0]['quantity']);
    }

    public function testRowValueInWithJoin(): void
    {
        $rows = $this->ztdQuery(
            "SELECT o.customer, oi.product_id, oi.quantity
             FROM my_rv_order_items oi
             JOIN my_rv_orders o ON o.id = oi.order_id
             WHERE (oi.order_id, oi.product_id) IN ((1, 100), (3, 300))
             ORDER BY o.customer"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['customer']);
        $this->assertSame('Carol', $rows[1]['customer']);
    }

    public function testRowValueInSubquery(): void
    {
        $rows = $this->ztdQuery(
            "SELECT order_id, product_id, quantity
             FROM my_rv_order_items
             WHERE (order_id, product_id) IN (
                 SELECT order_id, product_id
                 FROM my_rv_order_items
                 WHERE status = 'shipped'
             )
             ORDER BY order_id, product_id"
        );

        $this->assertCount(3, $rows);
    }

    public function testUpdateWithRowValue(): void
    {
        $this->pdo->exec(
            "UPDATE my_rv_order_items SET status = 'shipped'
             WHERE (order_id, product_id) IN ((1, 200), (3, 200))"
        );

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM my_rv_order_items WHERE status = 'shipped'");
        $this->assertEquals(5, (int) $rows[0]['cnt']);
    }

    public function testDeleteWithRowValue(): void
    {
        $this->pdo->exec("DELETE FROM my_rv_order_items WHERE (order_id, product_id) IN ((3, 300))");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM my_rv_order_items");
        $this->assertEquals(5, (int) $rows[0]['cnt']);
    }

    public function testPreparedRowValueWhere(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT quantity FROM my_rv_order_items WHERE (order_id, product_id) = (?, ?)",
            [2, 300]
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(1, (int) $rows[0]['quantity']);
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM my_rv_order_items")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
