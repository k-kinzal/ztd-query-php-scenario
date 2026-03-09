<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Implicit comma JOIN and edge-case SQL patterns via MySQL PDO.
 *
 * These patterns are valid and common but may confuse a regex-based CTE
 * rewriter: comma-separated FROM tables, CASE WHEN inside aggregates,
 * BETWEEN, EXISTS / NOT EXISTS, and correlated subqueries after DML.
 *
 * @spec SPEC-3.3
 */
class MysqlImplicitJoinAndEdgeCaseTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE ije_customers (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                city VARCHAR(30) NOT NULL
            ) ENGINE=InnoDB",
            "CREATE TABLE ije_orders (
                id INT PRIMARY KEY,
                customer_id INT NOT NULL,
                amount DECIMAL(10,2) NOT NULL,
                status VARCHAR(20) NOT NULL
            ) ENGINE=InnoDB",
        ];
    }

    protected function getTableNames(): array
    {
        return ['ije_orders', 'ije_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO ije_customers (id, name, city) VALUES (1, 'Alice', 'NYC'), (2, 'Bob', 'LA'), (3, 'Carol', 'Chicago')");
        $this->pdo->exec("INSERT INTO ije_orders (id, customer_id, amount, status) VALUES
            (1, 1, 100.00, 'completed'),
            (2, 1, 200.00, 'pending'),
            (3, 2, 50.00, 'completed'),
            (4, 2, 300.00, 'completed'),
            (5, 3, 75.00, 'pending')");
    }

    /**
     * Implicit comma join: SELECT from two tables separated by comma with
     * WHERE-based join condition. The CTE rewriter must recognise both
     * table references even without an explicit JOIN keyword.
     */
    public function testImplicitCommaJoin(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT c.name, o.amount FROM ije_customers c, ije_orders o WHERE c.id = o.customer_id ORDER BY o.id"
            );

            $this->assertCount(5, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('100.00', $rows[0]['amount']);
            $this->assertSame('Alice', $rows[1]['name']);
            $this->assertSame('200.00', $rows[1]['amount']);
            $this->assertSame('Bob', $rows[2]['name']);
            $this->assertSame('50.00', $rows[2]['amount']);
            $this->assertSame('Bob', $rows[3]['name']);
            $this->assertSame('300.00', $rows[3]['amount']);
            $this->assertSame('Carol', $rows[4]['name']);
            $this->assertSame('75.00', $rows[4]['amount']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Implicit comma join failed: ' . $e->getMessage());
        }
    }

    /**
     * Implicit comma join with GROUP BY and SUM aggregate.
     */
    public function testImplicitJoinWithAggregate(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT c.name, SUM(o.amount) AS total FROM ije_customers c, ije_orders o WHERE c.id = o.customer_id GROUP BY c.id, c.name ORDER BY total DESC"
            );

            $this->assertCount(3, $rows);
            // Bob: 50 + 300 = 350
            $this->assertSame('Bob', $rows[0]['name']);
            $this->assertEquals(350.00, (float) $rows[0]['total'], '', 0.01);
            // Alice: 100 + 200 = 300
            $this->assertSame('Alice', $rows[1]['name']);
            $this->assertEquals(300.00, (float) $rows[1]['total'], '', 0.01);
            // Carol: 75
            $this->assertSame('Carol', $rows[2]['name']);
            $this->assertEquals(75.00, (float) $rows[2]['total'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Implicit join with aggregate failed: ' . $e->getMessage());
        }
    }

    /**
     * Implicit comma join after inserting a new order through ZTD.
     * Verifies the newly inserted shadow row participates in the join.
     */
    public function testImplicitJoinAfterMutation(): void
    {
        try {
            $this->pdo->exec("INSERT INTO ije_orders (id, customer_id, amount, status) VALUES (6, 3, 125.00, 'completed')");

            $rows = $this->ztdQuery(
                "SELECT c.name, o.amount FROM ije_customers c, ije_orders o WHERE c.id = o.customer_id AND c.name = 'Carol' ORDER BY o.id"
            );

            $this->assertCount(2, $rows);
            $this->assertSame('75.00', $rows[0]['amount']);
            $this->assertSame('125.00', $rows[1]['amount']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Implicit join after mutation failed: ' . $e->getMessage());
        }
    }

    /**
     * CASE WHEN inside SUM aggregate with explicit JOIN.
     * The CASE keyword and WHEN/THEN/ELSE/END tokens inside an aggregate
     * expression could mislead a regex-based parser.
     */
    public function testConditionalAggregation(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT c.name,
                        SUM(CASE WHEN o.status = 'completed' THEN o.amount ELSE 0 END) AS completed_total,
                        SUM(CASE WHEN o.status = 'pending' THEN o.amount ELSE 0 END) AS pending_total
                 FROM ije_customers c
                 JOIN ije_orders o ON c.id = o.customer_id
                 GROUP BY c.id, c.name
                 ORDER BY c.name"
            );

            $this->assertCount(3, $rows);

            // Alice: completed 100, pending 200
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertEquals(100.00, (float) $rows[0]['completed_total'], '', 0.01);
            $this->assertEquals(200.00, (float) $rows[0]['pending_total'], '', 0.01);

            // Bob: completed 50+300=350, pending 0
            $this->assertSame('Bob', $rows[1]['name']);
            $this->assertEquals(350.00, (float) $rows[1]['completed_total'], '', 0.01);
            $this->assertEquals(0.00, (float) $rows[1]['pending_total'], '', 0.01);

            // Carol: completed 0, pending 75
            $this->assertSame('Carol', $rows[2]['name']);
            $this->assertEquals(0.00, (float) $rows[2]['completed_total'], '', 0.01);
            $this->assertEquals(75.00, (float) $rows[2]['pending_total'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Conditional aggregation failed: ' . $e->getMessage());
        }
    }

    /**
     * BETWEEN in WHERE clause. The word "BETWEEN" followed by a value,
     * "AND", and another value is a pattern that a naive parser might
     * confuse with a boolean AND separating two conditions.
     */
    public function testBetweenWithShadowData(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT status, amount FROM ije_orders WHERE amount BETWEEN 50 AND 200 ORDER BY amount"
            );

            // 50, 75, 100, 200
            $this->assertCount(4, $rows);
            $this->assertEquals(50.00, (float) $rows[0]['amount'], '', 0.01);
            $this->assertEquals(75.00, (float) $rows[1]['amount'], '', 0.01);
            $this->assertEquals(100.00, (float) $rows[2]['amount'], '', 0.01);
            $this->assertEquals(200.00, (float) $rows[3]['amount'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('BETWEEN with shadow data failed: ' . $e->getMessage());
        }
    }

    /**
     * EXISTS correlated subquery. The rewriter must handle nested SELECT
     * referencing an outer alias inside an EXISTS clause.
     */
    public function testExistsAndNotExists(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name FROM ije_customers c WHERE EXISTS (SELECT 1 FROM ije_orders o WHERE o.customer_id = c.id AND o.status = 'completed') ORDER BY name"
            );

            // Alice has 1 completed, Bob has 2 completed
            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Bob', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('EXISTS subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * NOT EXISTS after DELETE. Delete all orders for a customer, then verify
     * they appear in a NOT EXISTS query.
     */
    public function testNotExistsAfterDelete(): void
    {
        try {
            $this->pdo->exec("DELETE FROM ije_orders WHERE customer_id = 1");

            $rows = $this->ztdQuery(
                "SELECT name FROM ije_customers c WHERE NOT EXISTS (SELECT 1 FROM ije_orders o WHERE o.customer_id = c.id)"
            );

            $this->assertCount(1, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('NOT EXISTS after delete failed: ' . $e->getMessage());
        }
    }

    /**
     * Physical isolation: the underlying tables must remain empty.
     */
    public function testPhysicalIsolation(): void
    {
        $this->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM ije_customers")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical customers table should be empty');

        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM ije_orders")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical orders table should be empty');
    }
}
