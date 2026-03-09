<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests row value constructors (tuple comparisons) through CTE shadow store.
 *
 * Row value constructors — WHERE (a, b) IN ((1, 'x'), (2, 'y')) — are a common
 * SQL pattern for composite key lookups. The CTE rewriter must preserve the
 * tuple syntax in WHERE clauses for correct filtering.
 *
 * Also tests multi-column comparisons: (a, b) = (val1, val2), (a, b) > (val1, val2).
 *
 * @spec SPEC-3.1
 */
class SqliteRowValueConstructorTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_rv_order_items (
                order_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                unit_price REAL NOT NULL,
                status TEXT NOT NULL DEFAULT \'pending\',
                PRIMARY KEY (order_id, product_id)
            )',
            'CREATE TABLE sl_rv_orders (
                id INTEGER PRIMARY KEY,
                customer TEXT NOT NULL,
                order_date TEXT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_rv_order_items', 'sl_rv_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_rv_orders VALUES (1, 'Alice', '2025-01-10')");
        $this->pdo->exec("INSERT INTO sl_rv_orders VALUES (2, 'Bob', '2025-01-11')");
        $this->pdo->exec("INSERT INTO sl_rv_orders VALUES (3, 'Carol', '2025-01-12')");

        $this->pdo->exec("INSERT INTO sl_rv_order_items VALUES (1, 100, 2, 25.00, 'shipped')");
        $this->pdo->exec("INSERT INTO sl_rv_order_items VALUES (1, 200, 1, 50.00, 'pending')");
        $this->pdo->exec("INSERT INTO sl_rv_order_items VALUES (2, 100, 3, 25.00, 'shipped')");
        $this->pdo->exec("INSERT INTO sl_rv_order_items VALUES (2, 300, 1, 75.00, 'shipped')");
        $this->pdo->exec("INSERT INTO sl_rv_order_items VALUES (3, 200, 2, 50.00, 'pending')");
        $this->pdo->exec("INSERT INTO sl_rv_order_items VALUES (3, 300, 1, 75.00, 'cancelled')");
    }

    /**
     * Multi-column IN with row value constructor.
     */
    public function testRowValueIn(): void
    {
        $rows = $this->ztdQuery(
            "SELECT order_id, product_id, quantity
             FROM sl_rv_order_items
             WHERE (order_id, product_id) IN ((1, 100), (2, 300), (3, 200))
             ORDER BY order_id"
        );

        $this->assertCount(3, $rows);
        $this->assertEquals(1, (int) $rows[0]['order_id']);
        $this->assertEquals(100, (int) $rows[0]['product_id']);
        $this->assertEquals(2, (int) $rows[0]['quantity']);

        $this->assertEquals(2, (int) $rows[1]['order_id']);
        $this->assertEquals(300, (int) $rows[1]['product_id']);
        $this->assertEquals(1, (int) $rows[1]['quantity']);

        $this->assertEquals(3, (int) $rows[2]['order_id']);
        $this->assertEquals(200, (int) $rows[2]['product_id']);
        $this->assertEquals(2, (int) $rows[2]['quantity']);
    }

    /**
     * Multi-column NOT IN with row value constructor.
     */
    public function testRowValueNotIn(): void
    {
        $rows = $this->ztdQuery(
            "SELECT order_id, product_id
             FROM sl_rv_order_items
             WHERE (order_id, product_id) NOT IN ((1, 100), (2, 300), (3, 200))
             ORDER BY order_id, product_id"
        );

        $this->assertCount(3, $rows);
        $this->assertEquals(1, (int) $rows[0]['order_id']);
        $this->assertEquals(200, (int) $rows[0]['product_id']);
    }

    /**
     * Row value equality comparison.
     */
    public function testRowValueEquality(): void
    {
        $rows = $this->ztdQuery(
            "SELECT quantity, unit_price
             FROM sl_rv_order_items
             WHERE (order_id, product_id) = (2, 100)"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(3, (int) $rows[0]['quantity']);
        $this->assertEquals(25.00, (float) $rows[0]['unit_price']);
    }

    /**
     * Row value constructor with JOIN.
     */
    public function testRowValueInWithJoin(): void
    {
        $rows = $this->ztdQuery(
            "SELECT o.customer, oi.product_id, oi.quantity
             FROM sl_rv_order_items oi
             JOIN sl_rv_orders o ON o.id = oi.order_id
             WHERE (oi.order_id, oi.product_id) IN ((1, 100), (3, 300))
             ORDER BY o.customer"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['customer']);
        $this->assertEquals(100, (int) $rows[0]['product_id']);
        $this->assertSame('Carol', $rows[1]['customer']);
        $this->assertEquals(300, (int) $rows[1]['product_id']);
    }

    /**
     * Row value comparison for ordering (greater-than).
     */
    public function testRowValueGreaterThan(): void
    {
        $rows = $this->ztdQuery(
            "SELECT order_id, product_id
             FROM sl_rv_order_items
             WHERE (order_id, product_id) > (2, 100)
             ORDER BY order_id, product_id"
        );

        // (2,300), (3,200), (3,300) are > (2,100)
        $this->assertCount(3, $rows);
        $this->assertEquals(2, (int) $rows[0]['order_id']);
        $this->assertEquals(300, (int) $rows[0]['product_id']);
    }

    /**
     * Row value constructor with subquery.
     */
    public function testRowValueInSubquery(): void
    {
        $rows = $this->ztdQuery(
            "SELECT order_id, product_id, quantity
             FROM sl_rv_order_items
             WHERE (order_id, product_id) IN (
                 SELECT order_id, product_id
                 FROM sl_rv_order_items
                 WHERE status = 'shipped'
             )
             ORDER BY order_id, product_id"
        );

        $this->assertCount(3, $rows);
        $this->assertEquals(1, (int) $rows[0]['order_id']);
        $this->assertEquals(100, (int) $rows[0]['product_id']);
        $this->assertEquals(2, (int) $rows[1]['order_id']);
        $this->assertEquals(100, (int) $rows[1]['product_id']);
        $this->assertEquals(2, (int) $rows[2]['order_id']);
        $this->assertEquals(300, (int) $rows[2]['product_id']);
    }

    /**
     * UPDATE using row value constructor in WHERE.
     */
    public function testUpdateWithRowValue(): void
    {
        $this->pdo->exec(
            "UPDATE sl_rv_order_items SET status = 'shipped'
             WHERE (order_id, product_id) IN ((1, 200), (3, 200))"
        );

        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt FROM sl_rv_order_items WHERE status = 'shipped'"
        );
        $this->assertEquals(5, (int) $rows[0]['cnt']);
    }

    /**
     * DELETE using row value constructor in WHERE.
     */
    public function testDeleteWithRowValue(): void
    {
        $this->pdo->exec(
            "DELETE FROM sl_rv_order_items
             WHERE (order_id, product_id) IN ((3, 300))"
        );

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_rv_order_items");
        $this->assertEquals(5, (int) $rows[0]['cnt']);
    }

    /**
     * Prepared statement with row value constructor in WHERE.
     */
    public function testPreparedRowValueWhere(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT quantity FROM sl_rv_order_items WHERE (order_id, product_id) = (?, ?)",
            [2, 300]
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(1, (int) $rows[0]['quantity']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_rv_order_items")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
