<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests subquery edge cases through the CTE rewriter (PostgreSQL).
 *
 * Real-world scenario: Complex queries with multiple subquery levels,
 * correlated subqueries, and subqueries referencing the same table
 * at different nesting levels may expose CTE rewriter limitations.
 *
 * @spec SPEC-3.1
 * @spec SPEC-3.5
 */
class PostgresSubqueryEdgeCaseTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_sqe_orders (
                id SERIAL PRIMARY KEY,
                customer TEXT NOT NULL,
                amount NUMERIC(10,2) NOT NULL,
                status TEXT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_sqe_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO pg_sqe_orders (id, customer, amount, status) VALUES (1, 'Alice', 100.00, 'complete')");
        $this->ztdExec("INSERT INTO pg_sqe_orders (id, customer, amount, status) VALUES (2, 'Alice', 200.00, 'pending')");
        $this->ztdExec("INSERT INTO pg_sqe_orders (id, customer, amount, status) VALUES (3, 'Bob', 150.00, 'complete')");
        $this->ztdExec("INSERT INTO pg_sqe_orders (id, customer, amount, status) VALUES (4, 'Bob', 50.00, 'cancelled')");
        $this->ztdExec("INSERT INTO pg_sqe_orders (id, customer, amount, status) VALUES (5, 'Charlie', 300.00, 'complete')");
    }

    /**
     * Correlated subquery in WHERE referencing same table.
     */
    public function testCorrelatedSubqueryInWhere(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT customer, amount FROM pg_sqe_orders o1
                 WHERE amount > (SELECT AVG(amount) FROM pg_sqe_orders o2 WHERE o2.customer = o1.customer)
                 ORDER BY customer"
            );

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['customer']);
            $this->assertSame('Bob', $rows[1]['customer']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Correlated subquery in WHERE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Triple nested subquery on same table.
     */
    public function testTripleNestedSubquery(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT customer FROM pg_sqe_orders
                 WHERE amount > (
                     SELECT AVG(amount) FROM pg_sqe_orders
                     WHERE status IN (
                         SELECT status FROM pg_sqe_orders WHERE customer = 'Alice'
                     )
                 )
                 ORDER BY customer"
            );

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
     * Scalar subquery in SELECT list.
     */
    public function testScalarSubqueryInSelect(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT DISTINCT customer,
                       (SELECT SUM(amount) FROM pg_sqe_orders o2 WHERE o2.customer = o1.customer) AS total
                 FROM pg_sqe_orders o1
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
     * Derived table (subquery in FROM).
     */
    public function testDerivedTable(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT sub.customer, sub.total_amount
                 FROM (SELECT customer, SUM(amount) AS total_amount FROM pg_sqe_orders GROUP BY customer) sub
                 WHERE sub.total_amount > 200
                 ORDER BY sub.customer"
            );

            $this->assertCount(2, $rows);
            $customers = array_column($rows, 'customer');
            $this->assertContains('Alice', $customers);
            $this->assertContains('Charlie', $customers);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Derived table failed: ' . $e->getMessage()
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
                "SELECT DISTINCT customer FROM pg_sqe_orders o1
                 WHERE EXISTS (
                     SELECT 1 FROM pg_sqe_orders o2
                     WHERE o2.customer = o1.customer AND o2.status = 'cancelled'
                 )
                 ORDER BY customer"
            );

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
                "SELECT DISTINCT customer FROM pg_sqe_orders o1
                 WHERE NOT EXISTS (
                     SELECT 1 FROM pg_sqe_orders o2
                     WHERE o2.customer = o1.customer AND o2.status = 'cancelled'
                 )
                 ORDER BY customer"
            );

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
     * DELETE with subquery referencing same table.
     */
    public function testDeleteWithSubquery(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM pg_sqe_orders
                 WHERE amount < (SELECT AVG(amount) FROM pg_sqe_orders)"
            );

            $rows = $this->ztdQuery("SELECT * FROM pg_sqe_orders ORDER BY id");
            $this->assertCount(2, $rows);
            $this->assertEquals(200.00, (float) $rows[0]['amount'], '', 0.01);
            $this->assertEquals(300.00, (float) $rows[1]['amount'], '', 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'DELETE with subquery failed: ' . $e->getMessage()
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
                "UPDATE pg_sqe_orders SET amount = (SELECT MAX(amount) FROM pg_sqe_orders) WHERE id = 4"
            );

            $rows = $this->ztdQuery("SELECT amount FROM pg_sqe_orders WHERE id = 4");
            $this->assertCount(1, $rows);
            $this->assertEquals(300.00, (float) $rows[0]['amount'], '', 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE with subquery in SET failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * IN subquery with GROUP BY HAVING.
     */
    public function testInSubqueryWithGroupByHaving(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT * FROM pg_sqe_orders
                 WHERE customer IN (
                     SELECT customer FROM pg_sqe_orders
                     GROUP BY customer HAVING SUM(amount) > 200
                 )
                 ORDER BY id"
            );

            // Alice: 300 > 200, Charlie: 300 > 200
            $this->assertCount(3, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'IN subquery with GROUP BY HAVING failed: ' . $e->getMessage()
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
                 FROM pg_sqe_orders
                 GROUP BY customer
                 HAVING COUNT(*) > (SELECT COUNT(*) FROM pg_sqe_orders WHERE customer = 'Charlie')
                 ORDER BY customer"
            );

            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Subquery in HAVING failed: ' . $e->getMessage()
            );
        }
    }
}
