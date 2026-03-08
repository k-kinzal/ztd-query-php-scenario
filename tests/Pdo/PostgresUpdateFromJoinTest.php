<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests UPDATE ... FROM syntax on PostgreSQL through ZTD shadow store.
 *
 * PostgreSQL natively supports UPDATE ... FROM join syntax:
 *   UPDATE t1 SET col = t2.col FROM t2 WHERE t1.id = t2.fk
 *
 * Finding: UPDATE ... FROM works through the ZTD layer.
 * @spec SPEC-10.2.56
 */
class PostgresUpdateFromJoinTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_ufj_customers (
                id INTEGER PRIMARY KEY,
                name TEXT,
                last_total DOUBLE PRECISION DEFAULT 0
            )',
            'CREATE TABLE pg_ufj_orders (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                total DOUBLE PRECISION
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_ufj_orders', 'pg_ufj_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 3 customers
        $this->pdo->exec("INSERT INTO pg_ufj_customers VALUES (1, 'Alice', 0)");
        $this->pdo->exec("INSERT INTO pg_ufj_customers VALUES (2, 'Bob', 0)");
        $this->pdo->exec("INSERT INTO pg_ufj_customers VALUES (3, 'Charlie', 0)");

        // 4 orders
        $this->pdo->exec("INSERT INTO pg_ufj_orders VALUES (1, 1, 100.00)");
        $this->pdo->exec("INSERT INTO pg_ufj_orders VALUES (2, 1, 250.00)");
        $this->pdo->exec("INSERT INTO pg_ufj_orders VALUES (3, 2, 75.00)");
        $this->pdo->exec("INSERT INTO pg_ufj_orders VALUES (4, 2, 300.00)");
    }

    /**
     * UPDATE FROM works through ZTD — updates rows using join data.
     */
    public function testUpdateFromJoinUpdatesRows(): void
    {
        $affected = $this->pdo->exec(
            "UPDATE pg_ufj_customers c
             SET last_total = o.total
             FROM pg_ufj_orders o
             WHERE c.id = o.customer_id"
        );

        // At least 2 customers should be updated (Alice and Bob have orders)
        $this->assertGreaterThanOrEqual(2, $affected);
    }

    /**
     * Verify UPDATE FROM results are visible through ZTD shadow store reads.
     */
    public function testUpdateFromResultsVisibleInShadowStore(): void
    {
        $this->pdo->exec(
            "UPDATE pg_ufj_customers c
             SET last_total = o.total
             FROM pg_ufj_orders o
             WHERE c.id = o.customer_id"
        );

        $rows = $this->ztdQuery(
            "SELECT name, last_total FROM pg_ufj_customers ORDER BY id"
        );

        $this->assertCount(3, $rows);
        // Alice has orders 100 and 250 — PostgreSQL picks one non-deterministically
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertGreaterThan(0, (float) $rows[0]['last_total']);
        // Bob has orders 75 and 300 — PostgreSQL picks one non-deterministically
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertGreaterThan(0, (float) $rows[1]['last_total']);
        // Charlie has no orders — should remain 0
        $this->assertSame('Charlie', $rows[2]['name']);
        $this->assertEqualsWithDelta(0.0, (float) $rows[2]['last_total'], 0.01);
    }

    /**
     * Correlated subquery workaround in SET clause fails with CTE rewriter syntax error.
     *
     * The table-qualified column reference (pg_ufj_customers.id) in the correlated
     * subquery confuses the CTE rewriter, producing a syntax error.
     * @spec SPEC-11.UPDATE-SUBQUERY-SET
     */
    public function testCorrelatedSubqueryInSetClauseFails(): void
    {
        $this->expectException(\ZtdQuery\Adapter\Pdo\ZtdPdoException::class);

        $this->pdo->exec(
            "UPDATE pg_ufj_customers
             SET last_total = (
                 SELECT o.total
                 FROM pg_ufj_orders o
                 WHERE o.customer_id = pg_ufj_customers.id
                 ORDER BY o.id DESC
                 LIMIT 1
             )
             WHERE id IN (SELECT customer_id FROM pg_ufj_orders)"
        );
    }
}
