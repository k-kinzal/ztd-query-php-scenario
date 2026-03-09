<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests row-value comparisons (composite tuple syntax) on PostgreSQL via PDO.
 * Covers (col1, col2) IN ((v1, v2), ...), equality, ordering, and prepared params.
 * @spec SPEC-10.2.97
 */
class PostgresRowValueComparisonTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_rvc_orders (
            order_id INTEGER,
            line_num INTEGER,
            product VARCHAR(255),
            quantity INTEGER,
            price NUMERIC(10,2),
            PRIMARY KEY (order_id, line_num)
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_rvc_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_rvc_orders VALUES (1, 1, 'Widget', 5, 10.00)");
        $this->pdo->exec("INSERT INTO pg_rvc_orders VALUES (1, 2, 'Gadget', 3, 25.00)");
        $this->pdo->exec("INSERT INTO pg_rvc_orders VALUES (1, 3, 'Gizmo', 1, 50.00)");
        $this->pdo->exec("INSERT INTO pg_rvc_orders VALUES (2, 1, 'Widget', 10, 9.50)");
        $this->pdo->exec("INSERT INTO pg_rvc_orders VALUES (2, 2, 'Doohickey', 2, 35.00)");
        $this->pdo->exec("INSERT INTO pg_rvc_orders VALUES (3, 1, 'Thingamajig', 7, 15.00)");
    }

    /**
     * Row-value IN list: (order_id, line_num) IN ((1,2), (2,1), (3,1)).
     */
    public function testRowValueInList(): void
    {
        $rows = $this->ztdQuery(
            "SELECT product, quantity
             FROM pg_rvc_orders
             WHERE (order_id, line_num) IN ((1, 2), (2, 1), (3, 1))
             ORDER BY order_id, line_num"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Gadget', $rows[0]['product']);
        $this->assertEquals(3, (int) $rows[0]['quantity']);
        $this->assertSame('Widget', $rows[1]['product']);
        $this->assertEquals(10, (int) $rows[1]['quantity']);
        $this->assertSame('Thingamajig', $rows[2]['product']);
        $this->assertEquals(7, (int) $rows[2]['quantity']);
    }

    /**
     * Row-value equality: (order_id, line_num) = (1, 3).
     */
    public function testRowValueEquality(): void
    {
        $rows = $this->ztdQuery(
            "SELECT product, price
             FROM pg_rvc_orders
             WHERE (order_id, line_num) = (1, 3)"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Gizmo', $rows[0]['product']);
        $this->assertEquals(50.00, (float) $rows[0]['price']);
    }

    /**
     * Row-value comparison with prepared statement using positional params.
     */
    public function testRowValueWithPreparedParams(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT product, quantity
             FROM pg_rvc_orders
             WHERE order_id = ? AND line_num = ?",
            [2, 2]
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Doohickey', $rows[0]['product']);
        $this->assertEquals(2, (int) $rows[0]['quantity']);
    }

    /**
     * Row-value greater-than: lexicographic comparison (order_id, line_num) > (1, 2).
     */
    public function testRowValueGreaterThan(): void
    {
        $rows = $this->ztdQuery(
            "SELECT order_id, line_num, product
             FROM pg_rvc_orders
             WHERE (order_id, line_num) > (1, 2)
             ORDER BY order_id, line_num"
        );

        // (1,3), (2,1), (2,2), (3,1) are all > (1,2)
        $this->assertCount(4, $rows);
        $this->assertEquals(1, (int) $rows[0]['order_id']);
        $this->assertEquals(3, (int) $rows[0]['line_num']);
        $this->assertSame('Gizmo', $rows[0]['product']);
        $this->assertEquals(2, (int) $rows[1]['order_id']);
        $this->assertEquals(1, (int) $rows[1]['line_num']);
        $this->assertEquals(3, (int) $rows[3]['order_id']);
        $this->assertEquals(1, (int) $rows[3]['line_num']);
    }

    /**
     * Row-value comparison after an UPDATE mutation.
     */
    public function testRowValueAfterUpdate(): void
    {
        $this->pdo->exec("UPDATE pg_rvc_orders SET quantity = 99 WHERE order_id = 2 AND line_num = 1");

        $rows = $this->ztdQuery(
            "SELECT product, quantity
             FROM pg_rvc_orders
             WHERE (order_id, line_num) IN ((2, 1), (2, 2))
             ORDER BY line_num"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Widget', $rows[0]['product']);
        $this->assertEquals(99, (int) $rows[0]['quantity']);
        $this->assertSame('Doohickey', $rows[1]['product']);
        $this->assertEquals(2, (int) $rows[1]['quantity']);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_rvc_orders VALUES (4, 1, 'NewItem', 1, 5.00)");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_rvc_orders");
        $this->assertEquals(7, (int) $rows[0]['cnt']);

        $this->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM pg_rvc_orders')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
