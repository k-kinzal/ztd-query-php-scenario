<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests row value constructors (tuple comparisons) through MySQLi CTE shadow store.
 *
 * @spec SPEC-3.1
 */
class RowValueConstructorTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_rv_order_items (
                order_id INT NOT NULL,
                product_id INT NOT NULL,
                quantity INT NOT NULL,
                unit_price DECIMAL(10,2) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT \'pending\',
                PRIMARY KEY (order_id, product_id)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_rv_order_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_rv_order_items VALUES (1, 100, 2, 25.00, 'shipped')");
        $this->mysqli->query("INSERT INTO mi_rv_order_items VALUES (1, 200, 1, 50.00, 'pending')");
        $this->mysqli->query("INSERT INTO mi_rv_order_items VALUES (2, 100, 3, 25.00, 'shipped')");
        $this->mysqli->query("INSERT INTO mi_rv_order_items VALUES (2, 300, 1, 75.00, 'shipped')");
        $this->mysqli->query("INSERT INTO mi_rv_order_items VALUES (3, 200, 2, 50.00, 'pending')");
        $this->mysqli->query("INSERT INTO mi_rv_order_items VALUES (3, 300, 1, 75.00, 'cancelled')");
    }

    public function testRowValueIn(): void
    {
        $rows = $this->ztdQuery(
            "SELECT order_id, product_id, quantity
             FROM mi_rv_order_items
             WHERE (order_id, product_id) IN ((1, 100), (2, 300), (3, 200))
             ORDER BY order_id"
        );

        $this->assertCount(3, $rows);
        $this->assertEquals(1, (int) $rows[0]['order_id']);
        $this->assertEquals(100, (int) $rows[0]['product_id']);
    }

    public function testRowValueEquality(): void
    {
        $rows = $this->ztdQuery(
            "SELECT quantity FROM mi_rv_order_items WHERE (order_id, product_id) = (2, 100)"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(3, (int) $rows[0]['quantity']);
    }

    public function testUpdateWithRowValue(): void
    {
        $this->mysqli->query(
            "UPDATE mi_rv_order_items SET status = 'shipped'
             WHERE (order_id, product_id) IN ((1, 200), (3, 200))"
        );

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_rv_order_items WHERE status = 'shipped'");
        $this->assertEquals(5, (int) $rows[0]['cnt']);
    }

    public function testDeleteWithRowValue(): void
    {
        $this->mysqli->query("DELETE FROM mi_rv_order_items WHERE (order_id, product_id) IN ((3, 300))");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_rv_order_items");
        $this->assertEquals(5, (int) $rows[0]['cnt']);
    }

    public function testPreparedRowValueWhere(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT quantity FROM mi_rv_order_items WHERE (order_id, product_id) = (?, ?)",
            [2, 300]
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(1, (int) $rows[0]['quantity']);
    }

    public function testPhysicalIsolation(): void
    {
        $this->disableZtd();
        $raw = new \mysqli(
            \Tests\Support\MySQLContainer::getHost(),
            'root', 'root', 'test',
            \Tests\Support\MySQLContainer::getPort(),
        );
        $result = $raw->query("SELECT COUNT(*) AS cnt FROM mi_rv_order_items");
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
        $raw->close();
    }
}
