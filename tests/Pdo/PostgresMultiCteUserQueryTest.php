<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests multiple user-defined CTEs in a single query through ZTD shadow store on PostgreSQL.
 *
 * Pattern: WITH a AS (...), b AS (...) SELECT ... FROM a JOIN b ...
 * Known issue #52 covers single-CTE case. This tests whether multiple
 * user-defined CTEs interact correctly with the shadow store CTE rewriting.
 * @spec SPEC-3.3
 */
class PostgresMultiCteUserQueryTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mcu_orders (id INT PRIMARY KEY, customer_id INT, amount DECIMAL(10,2), region VARCHAR(20))',
            'CREATE TABLE mcu_customers (id INT PRIMARY KEY, name VARCHAR(50))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mcu_orders', 'mcu_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mcu_customers VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO mcu_customers VALUES (2, 'Bob')");
        $this->pdo->exec("INSERT INTO mcu_customers VALUES (3, 'Charlie')");

        $this->pdo->exec("INSERT INTO mcu_orders VALUES (1, 1, 100.00, 'east')");
        $this->pdo->exec("INSERT INTO mcu_orders VALUES (2, 1, 200.00, 'west')");
        $this->pdo->exec("INSERT INTO mcu_orders VALUES (3, 2, 150.00, 'east')");
        $this->pdo->exec("INSERT INTO mcu_orders VALUES (4, 3, 50.00, 'east')");
    }

    /**
     * Single user CTE reading from shadow table.
     * (Baseline for comparison - related to Issue #52)
     */
    public function testSingleUserCte(): void
    {
        $rows = $this->ztdQuery(
            'WITH top_customers AS (
                 SELECT customer_id, SUM(amount) AS total
                 FROM mcu_orders
                 GROUP BY customer_id
                 HAVING SUM(amount) >= 100
             )
             SELECT c.name, tc.total
             FROM top_customers tc
             JOIN mcu_customers c ON c.id = tc.customer_id
             ORDER BY tc.total DESC'
        );

        // Alice: 300, Bob: 150 (Charlie: 50 excluded)
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEquals(300.00, (float) $rows[0]['total']);
        $this->assertSame('Bob', $rows[1]['name']);
    }

    /**
     * Two independent user CTEs joined in final SELECT.
     */
    public function testTwoIndependentCtes(): void
    {
        $rows = $this->ztdQuery(
            'WITH
                 customer_totals AS (
                     SELECT customer_id, SUM(amount) AS total
                     FROM mcu_orders
                     GROUP BY customer_id
                 ),
                 region_totals AS (
                     SELECT region, SUM(amount) AS region_total
                     FROM mcu_orders
                     GROUP BY region
                 )
             SELECT c.name, ct.total
             FROM customer_totals ct
             JOIN mcu_customers c ON c.id = ct.customer_id
             ORDER BY ct.total DESC'
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    /**
     * Two user CTEs where one references the other.
     */
    public function testChainedCtes(): void
    {
        $rows = $this->ztdQuery(
            'WITH
                 order_summary AS (
                     SELECT customer_id, SUM(amount) AS total, COUNT(*) AS cnt
                     FROM mcu_orders
                     GROUP BY customer_id
                 ),
                 vip_customers AS (
                     SELECT customer_id, total
                     FROM order_summary
                     WHERE total > 100
                 )
             SELECT c.name, v.total
             FROM vip_customers v
             JOIN mcu_customers c ON c.id = v.customer_id
             ORDER BY v.total DESC'
        );

        // Alice: 300, Bob: 150
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    /**
     * User CTE after shadow INSERT mutation.
     */
    public function testUserCteAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO mcu_orders VALUES (5, 3, 500.00, 'west')");

        $rows = $this->ztdQuery(
            'WITH big_spenders AS (
                 SELECT customer_id, SUM(amount) AS total
                 FROM mcu_orders
                 GROUP BY customer_id
                 HAVING SUM(amount) > 200
             )
             SELECT c.name, bs.total
             FROM big_spenders bs
             JOIN mcu_customers c ON c.id = bs.customer_id
             ORDER BY bs.total DESC'
        );

        // Charlie now has 550, Alice has 300
        $this->assertCount(2, $rows);
        $this->assertSame('Charlie', $rows[0]['name']);
        $this->assertEquals(550.00, (float) $rows[0]['total']);
    }

    /**
     * User CTE after DELETE mutation.
     */
    public function testUserCteAfterDelete(): void
    {
        $this->pdo->exec('DELETE FROM mcu_orders WHERE id = 1');
        $this->pdo->exec('DELETE FROM mcu_orders WHERE id = 2');

        $rows = $this->ztdQuery(
            'WITH remaining AS (
                 SELECT customer_id, SUM(amount) AS total
                 FROM mcu_orders
                 GROUP BY customer_id
             )
             SELECT c.name, r.total
             FROM remaining r
             JOIN mcu_customers c ON c.id = r.customer_id
             ORDER BY c.name'
        );

        // Alice's orders deleted, only Bob (150) and Charlie (50)
        $this->assertCount(2, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
        $this->assertSame('Charlie', $rows[1]['name']);
    }

    /**
     * Two user CTEs JOINed together in the final SELECT.
     * This is the specific failing pattern from Issue #52:
     * returns 0 rows when two user-defined CTEs are JOINed.
     */
    public function testTwoCtesJoinedInFinalSelect(): void
    {
        $rows = $this->ztdQuery(
            'WITH
                 customer_totals AS (
                     SELECT customer_id, SUM(amount) AS total
                     FROM mcu_orders
                     GROUP BY customer_id
                 ),
                 customer_counts AS (
                     SELECT customer_id, COUNT(*) AS cnt
                     FROM mcu_orders
                     GROUP BY customer_id
                 )
             SELECT t.customer_id, t.total, c.cnt
             FROM customer_totals t
             JOIN customer_counts c ON t.customer_id = c.customer_id
             ORDER BY t.customer_id'
        );

        // Alice: total=300 cnt=2, Bob: total=150 cnt=1, Charlie: total=50 cnt=1
        $this->assertCount(3, $rows);
        $this->assertEquals(300.00, (float) $rows[0]['total']);
        $this->assertSame('2', (string) $rows[0]['cnt']);
        $this->assertEquals(150.00, (float) $rows[1]['total']);
        $this->assertSame('1', (string) $rows[1]['cnt']);
    }

    /**
     * Two CTEs JOINed with a base table.
     * Tests whether combining user CTEs with base table references works.
     */
    public function testTwoCtesJoinedWithBaseTable(): void
    {
        $rows = $this->ztdQuery(
            'WITH
                 totals AS (
                     SELECT customer_id, SUM(amount) AS total
                     FROM mcu_orders
                     GROUP BY customer_id
                 ),
                 counts AS (
                     SELECT customer_id, COUNT(*) AS cnt
                     FROM mcu_orders
                     GROUP BY customer_id
                 )
             SELECT cust.name, t.total, c.cnt
             FROM mcu_customers cust
             JOIN totals t ON t.customer_id = cust.id
             JOIN counts c ON c.customer_id = cust.id
             ORDER BY cust.name'
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEquals(300.00, (float) $rows[0]['total']);
        $this->assertSame('2', (string) $rows[0]['cnt']);
    }

    /**
     * Physical table isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM mcu_orders');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
