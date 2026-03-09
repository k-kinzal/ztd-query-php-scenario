<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests row value comparisons (tuple comparisons) through the ZTD CTE rewriter on MySQLi.
 * Covers IN-list, equality, prepared params, greater-than, and post-mutation behavior.
 * @spec SPEC-10.2.97
 */
class RowValueComparisonTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_rvc_orders (
            order_id INT,
            line_num INT,
            product VARCHAR(255),
            quantity INT,
            price DECIMAL(10,2),
            PRIMARY KEY (order_id, line_num)
        )';
    }

    protected function getTableNames(): array
    {
        return ['mi_rvc_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_rvc_orders VALUES (1, 1, 'Widget',  5,  10.00)");
        $this->mysqli->query("INSERT INTO mi_rvc_orders VALUES (1, 2, 'Gadget',  3,  25.00)");
        $this->mysqli->query("INSERT INTO mi_rvc_orders VALUES (1, 3, 'Gizmo',   1,  50.00)");
        $this->mysqli->query("INSERT INTO mi_rvc_orders VALUES (2, 1, 'Widget', 10,   9.50)");
        $this->mysqli->query("INSERT INTO mi_rvc_orders VALUES (2, 2, 'Bolt',    20,  1.25)");
        $this->mysqli->query("INSERT INTO mi_rvc_orders VALUES (3, 1, 'Gadget',  2,  30.00)");
        $this->mysqli->query("INSERT INTO mi_rvc_orders VALUES (3, 2, 'Gizmo',   4,  45.00)");
        $this->mysqli->query("INSERT INTO mi_rvc_orders VALUES (3, 3, 'Nut',    50,   0.50)");
    }

    /**
     * Row value IN list to select specific composite key pairs.
     * @spec SPEC-10.2.97
     */
    public function testRowValueInList(): void
    {
        $rows = $this->ztdQuery("
            SELECT order_id, line_num, product
            FROM mi_rvc_orders
            WHERE (order_id, line_num) IN ((1, 2), (2, 1), (3, 3))
            ORDER BY order_id, line_num
        ");

        $this->assertCount(3, $rows);
        $this->assertSame('Gadget', $rows[0]['product']);
        $this->assertSame('Widget', $rows[1]['product']);
        $this->assertSame('Nut', $rows[2]['product']);
    }

    /**
     * Row value equality comparison.
     * @spec SPEC-10.2.97
     */
    public function testRowValueEquality(): void
    {
        $rows = $this->ztdQuery("
            SELECT product, quantity, price
            FROM mi_rvc_orders
            WHERE (order_id, line_num) = (3, 2)
        ");

        $this->assertCount(1, $rows);
        $this->assertSame('Gizmo', $rows[0]['product']);
        $this->assertEquals(4, (int) $rows[0]['quantity']);
        $this->assertEqualsWithDelta(45.00, (float) $rows[0]['price'], 0.01);
    }

    /**
     * Row value comparison with prepared statement parameters.
     * @spec SPEC-10.2.97
     */
    public function testRowValueWithPreparedParams(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT product, quantity FROM mi_rvc_orders WHERE (order_id, line_num) = (?, ?)",
            [1, 3]
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Gizmo', $rows[0]['product']);
        $this->assertEquals(1, (int) $rows[0]['quantity']);
    }

    /**
     * Row value greater-than for cursor-style pagination on composite keys.
     * @spec SPEC-10.2.97
     */
    public function testRowValueGreaterThan(): void
    {
        $rows = $this->ztdQuery("
            SELECT order_id, line_num, product
            FROM mi_rvc_orders
            WHERE (order_id, line_num) > (2, 1)
            ORDER BY order_id, line_num
        ");

        // (2,2), (3,1), (3,2), (3,3) — 4 rows after (2,1)
        $this->assertCount(4, $rows);
        $this->assertEquals(2, (int) $rows[0]['order_id']);
        $this->assertEquals(2, (int) $rows[0]['line_num']);
        $this->assertSame('Bolt', $rows[0]['product']);
        $this->assertEquals(3, (int) $rows[3]['order_id']);
        $this->assertEquals(3, (int) $rows[3]['line_num']);
        $this->assertSame('Nut', $rows[3]['product']);
    }

    /**
     * Row value comparison correctly reflects data after an UPDATE.
     * @spec SPEC-10.2.97
     */
    public function testRowValueAfterUpdate(): void
    {
        $this->mysqli->query("UPDATE mi_rvc_orders SET product = 'SuperGadget', price = 35.00 WHERE order_id = 1 AND line_num = 2");

        $rows = $this->ztdQuery("
            SELECT product, price
            FROM mi_rvc_orders
            WHERE (order_id, line_num) = (1, 2)
        ");

        $this->assertCount(1, $rows);
        $this->assertSame('SuperGadget', $rows[0]['product']);
        $this->assertEqualsWithDelta(35.00, (float) $rows[0]['price'], 0.01);
    }

    /**
     * Physical table remains empty — all mutations are in ZTD shadow store.
     * @spec SPEC-10.2.97
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("UPDATE mi_rvc_orders SET quantity = 999 WHERE order_id = 1 AND line_num = 1");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT quantity FROM mi_rvc_orders WHERE (order_id, line_num) = (1, 1)");
        $this->assertEquals(999, (int) $rows[0]['quantity']);

        // Physical table untouched
        $this->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_rvc_orders');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
