<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests row-value comparisons through the ZTD CTE rewriter on MySQL via PDO.
 * Covers IN with row tuples, equality, prepared params, greater-than,
 * and row-value queries after UPDATE.
 * @spec SPEC-10.2.97
 */
class MysqlRowValueComparisonTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mp_rvc_orders (
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
        return ['mp_rvc_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_rvc_orders VALUES (1, 1, 'Widget',   3, 10.00)");
        $this->pdo->exec("INSERT INTO mp_rvc_orders VALUES (1, 2, 'Gadget',   1, 25.00)");
        $this->pdo->exec("INSERT INTO mp_rvc_orders VALUES (2, 1, 'Widget',   5, 10.00)");
        $this->pdo->exec("INSERT INTO mp_rvc_orders VALUES (2, 2, 'Gizmo',    2, 30.00)");
        $this->pdo->exec("INSERT INTO mp_rvc_orders VALUES (3, 1, 'Gadget',   4, 25.00)");
    }

    /**
     * @spec SPEC-10.2.97
     */
    public function testRowValueInList(): void
    {
        $rows = $this->ztdQuery("
            SELECT product, quantity
            FROM mp_rvc_orders
            WHERE (order_id, line_num) IN ((1, 1), (2, 2), (3, 1))
            ORDER BY order_id, line_num
        ");

        $this->assertCount(3, $rows);
        $this->assertSame('Widget', $rows[0]['product']);
        $this->assertSame(3, (int) $rows[0]['quantity']);
        $this->assertSame('Gizmo', $rows[1]['product']);
        $this->assertSame(2, (int) $rows[1]['quantity']);
        $this->assertSame('Gadget', $rows[2]['product']);
        $this->assertSame(4, (int) $rows[2]['quantity']);
    }

    /**
     * @spec SPEC-10.2.97
     */
    public function testRowValueEquality(): void
    {
        $rows = $this->ztdQuery("
            SELECT product, price
            FROM mp_rvc_orders
            WHERE (order_id, line_num) = (2, 1)
        ");

        $this->assertCount(1, $rows);
        $this->assertSame('Widget', $rows[0]['product']);
        $this->assertEqualsWithDelta(10.00, (float) $rows[0]['price'], 0.01);
    }

    /**
     * @spec SPEC-10.2.97
     */
    public function testRowValueWithPreparedParams(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT product, quantity FROM mp_rvc_orders WHERE (order_id, line_num) = (?, ?)",
            [1, 2]
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Gadget', $rows[0]['product']);
        $this->assertSame(1, (int) $rows[0]['quantity']);
    }

    /**
     * @spec SPEC-10.2.97
     */
    public function testRowValueGreaterThan(): void
    {
        $rows = $this->ztdQuery("
            SELECT order_id, line_num, product
            FROM mp_rvc_orders
            WHERE (order_id, line_num) > (2, 1)
            ORDER BY order_id, line_num
        ");

        $this->assertCount(2, $rows);
        $this->assertSame(2, (int) $rows[0]['order_id']);
        $this->assertSame(2, (int) $rows[0]['line_num']);
        $this->assertSame('Gizmo', $rows[0]['product']);
        $this->assertSame(3, (int) $rows[1]['order_id']);
        $this->assertSame(1, (int) $rows[1]['line_num']);
        $this->assertSame('Gadget', $rows[1]['product']);
    }

    /**
     * @spec SPEC-10.2.97
     */
    public function testRowValueAfterUpdate(): void
    {
        $this->ztdExec("UPDATE mp_rvc_orders SET quantity = 99 WHERE order_id = 1 AND line_num = 1");

        $rows = $this->ztdQuery("
            SELECT product, quantity
            FROM mp_rvc_orders
            WHERE (order_id, line_num) IN ((1, 1), (1, 2))
            ORDER BY line_num
        ");

        $this->assertCount(2, $rows);
        $this->assertSame('Widget', $rows[0]['product']);
        $this->assertSame(99, (int) $rows[0]['quantity']);
        $this->assertSame('Gadget', $rows[1]['product']);
        $this->assertSame(1, (int) $rows[1]['quantity']);
    }

    /**
     * @spec SPEC-10.2.97
     */
    public function testPhysicalIsolation(): void
    {
        $this->ztdExec("INSERT INTO mp_rvc_orders VALUES (4, 1, 'Thingamajig', 1, 5.00)");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_rvc_orders");
        $this->assertSame(6, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM mp_rvc_orders')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
