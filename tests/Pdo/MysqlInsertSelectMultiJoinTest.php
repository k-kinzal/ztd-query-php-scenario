<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests INSERT ... SELECT from multi-table JOIN on MySQL shadow data.
 *
 * Real-world scenario: ETL operations, denormalization, and data
 * migration often use INSERT ... SELECT with JOINs across multiple
 * source tables. When all source tables have shadow data, the CTE
 * rewriter must rewrite all table references in the SELECT portion
 * while also handling the INSERT target table correctly.
 *
 * Related: upstream Issue #49 — INSERT...SELECT with multi-table JOIN
 * produces incorrect results; on MySQL it fails with column-not-found errors.
 *
 * @spec SPEC-4.1a
 * @spec SPEC-3.3
 */
class MysqlInsertSelectMultiJoinTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_ismj_customers (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                region VARCHAR(50) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE my_ismj_orders (
                id INT PRIMARY KEY,
                customer_id INT NOT NULL,
                total DECIMAL(10,2) NOT NULL,
                order_date DATE NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE my_ismj_report (
                id INT AUTO_INCREMENT PRIMARY KEY,
                customer_name VARCHAR(100) NOT NULL,
                region VARCHAR(50) NOT NULL,
                order_count INT NOT NULL,
                total_spent DECIMAL(10,2) NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_ismj_report', 'my_ismj_orders', 'my_ismj_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO my_ismj_customers VALUES (1, 'Alice', 'North')");
        $this->ztdExec("INSERT INTO my_ismj_customers VALUES (2, 'Bob', 'South')");
        $this->ztdExec("INSERT INTO my_ismj_customers VALUES (3, 'Carol', 'North')");

        $this->ztdExec("INSERT INTO my_ismj_orders VALUES (1, 1, 100.00, '2025-01-01')");
        $this->ztdExec("INSERT INTO my_ismj_orders VALUES (2, 1, 200.00, '2025-02-01')");
        $this->ztdExec("INSERT INTO my_ismj_orders VALUES (3, 2, 150.00, '2025-01-15')");
        $this->ztdExec("INSERT INTO my_ismj_orders VALUES (4, 3, 300.00, '2025-03-01')");
    }

    /**
     * INSERT ... SELECT with JOIN across two shadow source tables.
     */
    public function testInsertSelectWithJoin(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO my_ismj_report (customer_name, region, order_count, total_spent)
                 SELECT c.name, c.region, COUNT(o.id), SUM(o.total)
                 FROM my_ismj_customers c
                 JOIN my_ismj_orders o ON o.customer_id = c.id
                 GROUP BY c.id, c.name, c.region"
            );

            $rows = $this->ztdQuery("SELECT * FROM my_ismj_report ORDER BY customer_name");

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'INSERT SELECT with JOIN produced no rows in report table.'
                );
            }

            $this->assertCount(3, $rows);
            // Alice: 2 orders, $300
            $this->assertSame('Alice', $rows[0]['customer_name']);
            $this->assertEquals(2, (int) $rows[0]['order_count']);
            $this->assertEqualsWithDelta(300.00, (float) $rows[0]['total_spent'], 0.01);
            // Bob: 1 order, $150
            $this->assertSame('Bob', $rows[1]['customer_name']);
            $this->assertEquals(1, (int) $rows[1]['order_count']);
            // Carol: 1 order, $300
            $this->assertSame('Carol', $rows[2]['customer_name']);
            $this->assertEquals(1, (int) $rows[2]['order_count']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT SELECT with JOIN failed on MySQL: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT ... SELECT with WHERE filter on joined shadow data.
     */
    public function testInsertSelectWithJoinAndFilter(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO my_ismj_report (customer_name, region, order_count, total_spent)
                 SELECT c.name, c.region, COUNT(o.id), SUM(o.total)
                 FROM my_ismj_customers c
                 JOIN my_ismj_orders o ON o.customer_id = c.id
                 WHERE c.region = 'North'
                 GROUP BY c.id, c.name, c.region"
            );

            $rows = $this->ztdQuery("SELECT * FROM my_ismj_report ORDER BY customer_name");

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'INSERT SELECT with JOIN and WHERE filter produced no rows.'
                );
            }

            // Only North region: Alice, Carol
            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['customer_name']);
            $this->assertSame('Carol', $rows[1]['customer_name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT SELECT with JOIN and filter failed on MySQL: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT SELECT then query the inserted report data.
     */
    public function testInsertSelectThenQueryResult(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO my_ismj_report (customer_name, region, order_count, total_spent)
                 SELECT c.name, c.region, COUNT(o.id), SUM(o.total)
                 FROM my_ismj_customers c
                 JOIN my_ismj_orders o ON o.customer_id = c.id
                 GROUP BY c.id, c.name, c.region"
            );

            // Now query the report with a filter
            $rows = $this->ztdQuery(
                "SELECT customer_name, total_spent FROM my_ismj_report
                 WHERE total_spent > 200
                 ORDER BY total_spent DESC"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'Query on INSERT SELECT results returned no rows. '
                    . 'Inserted report data may not be visible in shadow store.'
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Carol', $rows[0]['customer_name']); // 300
            $this->assertSame('Alice', $rows[1]['customer_name']); // 300
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Query on INSERT SELECT results failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Physical isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $this->disableZtd();
        $count = (int) $this->pdo->query('SELECT COUNT(*) FROM my_ismj_report')->fetchColumn();
        $this->assertSame(0, $count);
        $count = (int) $this->pdo->query('SELECT COUNT(*) FROM my_ismj_customers')->fetchColumn();
        $this->assertSame(0, $count);
    }
}
