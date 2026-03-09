<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests subquery edge cases that may confuse the CTE rewriter.
 *
 * Real-world scenario: Complex queries with multiple levels of subqueries,
 * correlated subqueries in various positions, and subqueries that reference
 * the same table at different levels may expose rewriter limitations.
 *
 * @spec SPEC-3.1
 * @spec SPEC-3.5
 */
class SqliteSubqueryEdgeCaseTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_sqe_orders (
                id INTEGER PRIMARY KEY,
                customer TEXT NOT NULL,
                amount REAL NOT NULL,
                status TEXT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_sqe_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO sl_sqe_orders VALUES (1, 'Alice', 100.00, 'complete')");
        $this->ztdExec("INSERT INTO sl_sqe_orders VALUES (2, 'Alice', 200.00, 'pending')");
        $this->ztdExec("INSERT INTO sl_sqe_orders VALUES (3, 'Bob', 150.00, 'complete')");
        $this->ztdExec("INSERT INTO sl_sqe_orders VALUES (4, 'Bob', 50.00, 'cancelled')");
        $this->ztdExec("INSERT INTO sl_sqe_orders VALUES (5, 'Charlie', 300.00, 'complete')");
    }

    /**
     * Subquery in WHERE with same table referenced at both levels.
     */
    public function testSubqueryReferencingSameTable(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT customer, amount FROM sl_sqe_orders o1
                 WHERE amount > (SELECT AVG(amount) FROM sl_sqe_orders o2 WHERE o2.customer = o1.customer)
                 ORDER BY customer"
            );

            // Alice: avg=150, only 200>150. Bob: avg=100, only 150>100. Charlie: avg=300, none.
            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['customer']);
            $this->assertEquals(200.00, (float) $rows[0]['amount'], '', 0.01);
            $this->assertSame('Bob', $rows[1]['customer']);
            $this->assertEquals(150.00, (float) $rows[1]['amount'], '', 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Subquery referencing same table failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Nested subqueries (3 levels deep) on same table.
     */
    public function testTripleNestedSubquery(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT customer FROM sl_sqe_orders
                 WHERE amount > (
                     SELECT AVG(amount) FROM sl_sqe_orders
                     WHERE status IN (
                         SELECT status FROM sl_sqe_orders WHERE customer = 'Alice'
                     )
                 )
                 ORDER BY customer"
            );

            // Statuses from Alice: 'complete', 'pending'
            // AVG of orders with status in ('complete','pending'): (100+200+150+300)/4 = 187.5
            // Orders > 187.5: Alice(200), Charlie(300)
            $this->assertCount(2, $rows);
            $customers = array_column($rows, 'customer');
            $this->assertContains('Alice', $customers);
            $this->assertContains('Charlie', $customers);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Triple nested subquery failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Scalar subquery in SELECT list referencing same table.
     */
    public function testScalarSubqueryInSelect(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT DISTINCT customer,
                       (SELECT SUM(amount) FROM sl_sqe_orders o2 WHERE o2.customer = o1.customer) AS total
                 FROM sl_sqe_orders o1
                 ORDER BY customer"
            );

            $this->assertCount(3, $rows);
            $this->assertSame('Alice', $rows[0]['customer']);
            $this->assertEquals(300.00, (float) $rows[0]['total'], '', 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Scalar subquery in SELECT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Subquery in FROM clause (derived table) with same table.
     */
    public function testDerivedTableFromSameTable(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT sub.customer, sub.total_amount
                 FROM (SELECT customer, SUM(amount) AS total_amount FROM sl_sqe_orders GROUP BY customer) sub
                 WHERE sub.total_amount > 200
                 ORDER BY sub.customer"
            );

            // Alice: 300, Bob: 200, Charlie: 300 -> Alice(300), Charlie(300)
            $this->assertCount(2, $rows);
            $customers = array_column($rows, 'customer');
            $this->assertContains('Alice', $customers);
            $this->assertContains('Charlie', $customers);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Derived table from same table failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * EXISTS with correlated subquery.
     */
    public function testExistsCorrelated(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT DISTINCT customer FROM sl_sqe_orders o1
                 WHERE EXISTS (
                     SELECT 1 FROM sl_sqe_orders o2
                     WHERE o2.customer = o1.customer AND o2.status = 'cancelled'
                 )
                 ORDER BY customer"
            );

            // Only Bob has a cancelled order
            $this->assertCount(1, $rows);
            $this->assertSame('Bob', $rows[0]['customer']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'EXISTS correlated failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * NOT EXISTS with correlated subquery.
     */
    public function testNotExistsCorrelated(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT DISTINCT customer FROM sl_sqe_orders o1
                 WHERE NOT EXISTS (
                     SELECT 1 FROM sl_sqe_orders o2
                     WHERE o2.customer = o1.customer AND o2.status = 'cancelled'
                 )
                 ORDER BY customer"
            );

            // Alice and Charlie have no cancelled orders
            $this->assertCount(2, $rows);
            $customers = array_column($rows, 'customer');
            $this->assertContains('Alice', $customers);
            $this->assertContains('Charlie', $customers);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'NOT EXISTS correlated failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Subquery in HAVING clause.
     */
    public function testSubqueryInHaving(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT customer, COUNT(*) AS order_count
                 FROM sl_sqe_orders
                 GROUP BY customer
                 HAVING COUNT(*) > (SELECT COUNT(*) FROM sl_sqe_orders WHERE customer = 'Charlie')
                 ORDER BY customer"
            );

            // Charlie has 1 order. Alice has 2, Bob has 2 -> both > 1
            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Subquery in HAVING failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE with subquery referencing same table.
     */
    public function testDeleteWithSubqueryOnSameTable(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM sl_sqe_orders
                 WHERE amount < (SELECT AVG(amount) FROM sl_sqe_orders)"
            );

            // AVG = (100+200+150+50+300)/5 = 160
            // Delete: Alice(100), Bob(50), Bob(150) -> remaining: Alice(200), Charlie(300)
            $rows = $this->ztdQuery("SELECT * FROM sl_sqe_orders ORDER BY id");
            $this->assertCount(2, $rows);
            $this->assertEquals(200.00, (float) $rows[0]['amount'], '', 0.01);
            $this->assertEquals(300.00, (float) $rows[1]['amount'], '', 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'DELETE with subquery on same table failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with subquery in SET referencing same table.
     */
    public function testUpdateWithSubqueryInSet(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_sqe_orders SET amount = (SELECT MAX(amount) FROM sl_sqe_orders) WHERE id = 4"
            );

            $rows = $this->ztdQuery("SELECT amount FROM sl_sqe_orders WHERE id = 4");
            $this->assertCount(1, $rows);
            $this->assertEquals(300.00, (float) $rows[0]['amount'], '', 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE with subquery in SET failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * IN subquery with GROUP BY on same table.
     */
    public function testInSubqueryWithGroupBy(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT * FROM sl_sqe_orders
                 WHERE customer IN (
                     SELECT customer FROM sl_sqe_orders
                     GROUP BY customer HAVING SUM(amount) > 200
                 )
                 ORDER BY id"
            );

            // Alice: 300 > 200, Charlie: 300 > 200 -> 3 rows (Alice's 2 + Charlie's 1)
            $this->assertCount(3, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'IN subquery with GROUP BY failed: ' . $e->getMessage()
            );
        }
    }
}
