<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL generate_series() with shadow data through CTE.
 *
 * generate_series() is a set-returning function commonly used in reporting
 * queries (filling date gaps, creating number sequences). When JOINed with
 * shadow-stored tables, the CTE rewriter must handle the combination.
 *
 * Known Issue: When generate_series() is LEFT JOINed with a shadow-stored table,
 * the table reference in the JOIN is not rewritten by the CTE rewriter. The
 * physical table (empty) is read instead, so all amounts are 0/NULL. Similarly,
 * NOT EXISTS subqueries against shadow tables in generate_series context cannot
 * see shadow data.
 *
 * @spec SPEC-3.1
 * @see SPEC-11.PG-GENERATE-SERIES
 */
class PostgresGenerateSeriesTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_gs_sales (id INT PRIMARY KEY, sale_date DATE, amount DECIMAL(10,2))';
    }

    protected function getTableNames(): array
    {
        return ['pg_gs_sales'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_gs_sales VALUES (1, '2024-01-01', 100.00)");
        $this->pdo->exec("INSERT INTO pg_gs_sales VALUES (2, '2024-01-01', 50.00)");
        $this->pdo->exec("INSERT INTO pg_gs_sales VALUES (3, '2024-01-03', 200.00)");
        $this->pdo->exec("INSERT INTO pg_gs_sales VALUES (4, '2024-01-05', 75.00)");
    }

    /**
     * generate_series standalone (no table join) works normally.
     */
    public function testGenerateSeriesStandalone(): void
    {
        $rows = $this->ztdQuery(
            "SELECT generate_series(1, 5) AS n"
        );

        $this->assertCount(5, $rows);
    }

    /**
     * generate_series LEFT JOIN returns all zeros (Known Issue).
     *
     * The CTE rewriter does not rewrite table references in the ON clause
     * of a LEFT JOIN with generate_series(). The physical table (empty) is
     * read, so COALESCE(SUM(...), 0) returns 0 for all days.
     */
    public function testGenerateSeriesLeftJoinReturnsZeros(): void
    {
        $rows = $this->ztdQuery(
            "SELECT d.day::date AS report_date,
                    COALESCE(SUM(s.amount), 0) AS daily_total
             FROM generate_series('2024-01-01'::date, '2024-01-05'::date, '1 day') AS d(day)
             LEFT JOIN pg_gs_sales s ON s.sale_date = d.day::date
             GROUP BY d.day
             ORDER BY d.day"
        );

        $this->assertCount(5, $rows, 'Should have 5 days in range');
        // Known Issue: All amounts are 0 because shadow data is not visible
        foreach ($rows as $row) {
            $this->assertEquals(0, (float) $row['daily_total'],
                'Known Issue: generate_series LEFT JOIN reads physical table (empty)');
        }
    }

    /**
     * generate_series NOT EXISTS returns all days (Known Issue).
     *
     * NOT EXISTS subquery reads the physical table (empty), so it thinks
     * no sales exist for any day, returning all 5 days as "missing".
     */
    public function testGenerateSeriesNotExistsReturnsAllDays(): void
    {
        $rows = $this->ztdQuery(
            "SELECT d.day::date AS missing_date
             FROM generate_series('2024-01-01'::date, '2024-01-05'::date, '1 day') AS d(day)
             WHERE NOT EXISTS (
                 SELECT 1 FROM pg_gs_sales WHERE sale_date = d.day::date
             )
             ORDER BY d.day"
        );

        // Known Issue: Returns all 5 days instead of 2 (shadow data not visible)
        $this->assertCount(5, $rows, 'Known Issue: NOT EXISTS reads physical table, all days appear missing');
    }

    /**
     * Integer generate_series in derived table LEFT JOIN works.
     *
     * Unlike the date-based generate_series AS d(day) form, the derived table
     * with integer generate_series does get table references rewritten.
     */
    public function testIntegerGenerateSeriesInDerivedTable(): void
    {
        $rows = $this->ztdQuery(
            "SELECT gs.n, s.amount
             FROM (SELECT generate_series(1, 5) AS n) AS gs
             LEFT JOIN pg_gs_sales s ON s.id = gs.n
             ORDER BY gs.n"
        );

        $this->assertCount(5, $rows);
        $this->assertEquals(100.00, (float) $rows[0]['amount']); // id=1
        $this->assertNull($rows[4]['amount']); // id=5 doesn't exist
    }

    /**
     * Workaround: pre-select shadow data into subquery, then join.
     */
    public function testWorkaroundSubqueryFirst(): void
    {
        // Without generate_series, direct query on shadow table works
        $rows = $this->ztdQuery(
            "SELECT sale_date, SUM(amount) AS daily_total
             FROM pg_gs_sales
             GROUP BY sale_date
             ORDER BY sale_date"
        );

        $this->assertCount(3, $rows, 'Direct query sees shadow data');
        $this->assertEquals(150.00, (float) $rows[0]['daily_total']); // Jan 1
        $this->assertEquals(200.00, (float) $rows[1]['daily_total']); // Jan 3
        $this->assertEquals(75.00, (float) $rows[2]['daily_total']);  // Jan 5
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_gs_sales');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
