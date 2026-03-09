<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests row value / tuple comparisons like WHERE (a, b) IN ((1, 'x'), (2, 'y')).
 * Common for composite key lookups. Parser may struggle with parenthesized tuples.
 * @spec SPEC-10.2.97
 */
class SqliteRowValueComparisonTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_rvc_orders (
            order_id INTEGER,
            line_num INTEGER,
            product TEXT,
            quantity INTEGER,
            price REAL,
            PRIMARY KEY (order_id, line_num)
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_rvc_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Order 1: 3 line items
        $this->pdo->exec("INSERT INTO sl_rvc_orders VALUES (1, 1, 'Widget A', 2, 10.00)");
        $this->pdo->exec("INSERT INTO sl_rvc_orders VALUES (1, 2, 'Widget B', 1, 20.00)");
        $this->pdo->exec("INSERT INTO sl_rvc_orders VALUES (1, 3, 'Gadget C', 3, 15.00)");
        // Order 2: 3 line items
        $this->pdo->exec("INSERT INTO sl_rvc_orders VALUES (2, 1, 'Gizmo X', 5, 8.00)");
        $this->pdo->exec("INSERT INTO sl_rvc_orders VALUES (2, 2, 'Widget A', 1, 10.00)");
        $this->pdo->exec("INSERT INTO sl_rvc_orders VALUES (2, 3, 'Gadget C', 2, 15.00)");
        // Order 3: 2 line items
        $this->pdo->exec("INSERT INTO sl_rvc_orders VALUES (3, 1, 'Widget B', 4, 20.00)");
        $this->pdo->exec("INSERT INTO sl_rvc_orders VALUES (3, 2, 'Gizmo X', 2, 8.00)");
    }

    /**
     * Row value IN list: match specific composite key pairs.
     */
    public function testRowValueInList(): void
    {
        $rows = $this->ztdQuery(
            "SELECT order_id, line_num, product
             FROM sl_rvc_orders
             WHERE (order_id, line_num) IN ((1, 1), (2, 2))
             ORDER BY order_id, line_num"
        );

        $this->assertCount(2, $rows);
        $this->assertEquals(1, (int) $rows[0]['order_id']);
        $this->assertEquals(1, (int) $rows[0]['line_num']);
        $this->assertSame('Widget A', $rows[0]['product']);
        $this->assertEquals(2, (int) $rows[1]['order_id']);
        $this->assertEquals(2, (int) $rows[1]['line_num']);
        $this->assertSame('Widget A', $rows[1]['product']);
    }

    /**
     * Row value equality: single tuple comparison.
     */
    public function testRowValueEquality(): void
    {
        $rows = $this->ztdQuery(
            "SELECT order_id, line_num, product
             FROM sl_rvc_orders
             WHERE (order_id, line_num) = (1, 2)"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(1, (int) $rows[0]['order_id']);
        $this->assertEquals(2, (int) $rows[0]['line_num']);
        $this->assertSame('Widget B', $rows[0]['product']);
    }

    /**
     * Row value with prepared parameters.
     */
    public function testRowValueWithPreparedParams(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT order_id, line_num, product
             FROM sl_rvc_orders
             WHERE (order_id, line_num) = (?, ?)",
            [1, 2]
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Widget B', $rows[0]['product']);
    }

    /**
     * Row value greater-than comparison: tuple ordering.
     */
    public function testRowValueGreaterThan(): void
    {
        $rows = $this->ztdQuery(
            "SELECT order_id, line_num, product
             FROM sl_rvc_orders
             WHERE (order_id, line_num) > (1, 2)
             ORDER BY order_id, line_num"
        );

        // Rows after (1,2): (1,3), (2,1), (2,2), (2,3), (3,1), (3,2)
        $this->assertCount(6, $rows);
        $this->assertEquals(1, (int) $rows[0]['order_id']);
        $this->assertEquals(3, (int) $rows[0]['line_num']);
        $this->assertEquals(2, (int) $rows[1]['order_id']);
        $this->assertEquals(1, (int) $rows[1]['line_num']);
    }

    /**
     * Update a row, then verify tuple lookup returns updated data.
     */
    public function testRowValueAfterUpdate(): void
    {
        $this->pdo->exec(
            "UPDATE sl_rvc_orders SET quantity = 99 WHERE order_id = 2 AND line_num = 2"
        );

        $rows = $this->ztdQuery(
            "SELECT order_id, line_num, product, quantity
             FROM sl_rvc_orders
             WHERE (order_id, line_num) = (2, 2)"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Widget A', $rows[0]['product']);
        $this->assertEquals(99, (int) $rows[0]['quantity']);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec(
            "UPDATE sl_rvc_orders SET quantity = 99 WHERE order_id = 1 AND line_num = 1"
        );

        $rows = $this->ztdQuery(
            "SELECT quantity FROM sl_rvc_orders WHERE (order_id, line_num) = (1, 1)"
        );
        $this->assertEquals(99, (int) $rows[0]['quantity']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query(
            'SELECT COUNT(*) AS cnt FROM sl_rvc_orders'
        )->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
