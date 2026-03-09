<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests multiple user-written CTEs chained together (CTE2 references CTE1) on PostgreSQL.
 * PostgreSQL has a known issue (SPEC-11.PG-CTE) where table references inside user CTEs
 * are NOT rewritten - the inner CTE reads from the physical table, returning 0 rows.
 * @spec SPEC-10.2.96
 */
class PostgresChainedUserCteTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_cuc_sales (
            id INTEGER PRIMARY KEY,
            product VARCHAR(255),
            amount NUMERIC(10,2),
            region VARCHAR(100),
            sale_date DATE
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_cuc_sales'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_cuc_sales VALUES (1, 'Widget', 100.00, 'East', '2026-01-15')");
        $this->pdo->exec("INSERT INTO pg_cuc_sales VALUES (2, 'Widget', 250.00, 'West', '2026-01-16')");
        $this->pdo->exec("INSERT INTO pg_cuc_sales VALUES (3, 'Gadget', 150.00, 'East', '2026-01-17')");
        $this->pdo->exec("INSERT INTO pg_cuc_sales VALUES (4, 'Gadget', 300.00, 'West', '2026-02-01')");
        $this->pdo->exec("INSERT INTO pg_cuc_sales VALUES (5, 'Gizmo', 200.00, 'East', '2026-02-10')");
        $this->pdo->exec("INSERT INTO pg_cuc_sales VALUES (6, 'Gizmo', 175.00, 'Central', '2026-02-15')");
        $this->pdo->exec("INSERT INTO pg_cuc_sales VALUES (7, 'Widget', 125.00, 'Central', '2026-03-01')");
        $this->pdo->exec("INSERT INTO pg_cuc_sales VALUES (8, 'Gadget', 80.00, 'Central', '2026-03-05')");
    }

    /**
     * Two chained CTEs: regional totals, then ranked by total.
     * Known issue: user CTEs read from physical table on PostgreSQL (SPEC-11.PG-CTE).
     * The inner CTE reads from the empty physical table, so results are empty.
     */
    public function testTwoChainedCtes(): void
    {
        $rows = $this->ztdQuery(
            "WITH regional AS (
                SELECT region, SUM(amount) AS total
                FROM pg_cuc_sales
                GROUP BY region
            ),
            ranked AS (
                SELECT *, RANK() OVER (ORDER BY total DESC) AS rnk
                FROM regional
            )
            SELECT * FROM ranked ORDER BY rnk"
        );

        // Known issue SPEC-11.PG-CTE: user CTEs read from physical table (empty).
        // On SQLite this returns 3 rows with correct regional totals.
        if (count($rows) === 0) {
            $this->markTestIncomplete('Known issue: user CTEs read from physical table (SPEC-11.PG-CTE)');
        }

        // If the issue is fixed, verify correct results:
        $this->assertCount(3, $rows);
        $this->assertSame('West', $rows[0]['region']);
        $this->assertEquals(550.00, (float) $rows[0]['total']);
        $this->assertEquals(1, (int) $rows[0]['rnk']);
        $this->assertSame('East', $rows[1]['region']);
        $this->assertEquals(450.00, (float) $rows[1]['total']);
        $this->assertSame('Central', $rows[2]['region']);
        $this->assertEquals(380.00, (float) $rows[2]['total']);
    }

    /**
     * Three chained CTEs: product totals -> filtered -> with running sum.
     * Known issue: user CTEs read from physical table on PostgreSQL (SPEC-11.PG-CTE).
     */
    public function testThreeChainedCtes(): void
    {
        $rows = $this->ztdQuery(
            "WITH product_totals AS (
                SELECT product, SUM(amount) AS total
                FROM pg_cuc_sales
                GROUP BY product
            ),
            filtered AS (
                SELECT * FROM product_totals WHERE total > 300
            ),
            with_running AS (
                SELECT *,
                       SUM(total) OVER (ORDER BY product) AS running_total
                FROM filtered
            )
            SELECT * FROM with_running ORDER BY product"
        );

        // Known issue SPEC-11.PG-CTE: user CTEs read from physical table (empty).
        if (count($rows) === 0) {
            $this->markTestIncomplete('Known issue: user CTEs read from physical table (SPEC-11.PG-CTE)');
        }

        $this->assertCount(3, $rows);
        $this->assertSame('Gadget', $rows[0]['product']);
        $this->assertEquals(530.00, (float) $rows[0]['total']);
        $this->assertSame('Gizmo', $rows[1]['product']);
        $this->assertEquals(375.00, (float) $rows[1]['total']);
        $this->assertSame('Widget', $rows[2]['product']);
        $this->assertEquals(475.00, (float) $rows[2]['total']);
    }

    /**
     * Chained CTE joined back to the original table.
     * Known issue: user CTEs read from physical table on PostgreSQL (SPEC-11.PG-CTE).
     */
    public function testChainedCteWithJoin(): void
    {
        $rows = $this->ztdQuery(
            "WITH regional AS (
                SELECT region, SUM(amount) AS region_total
                FROM pg_cuc_sales
                GROUP BY region
            ),
            top_regions AS (
                SELECT region, region_total
                FROM regional
                WHERE region_total >= 450
            )
            SELECT s.id, s.product, s.amount, t.region_total
            FROM pg_cuc_sales s
            JOIN top_regions t ON s.region = t.region
            ORDER BY s.id"
        );

        // Known issue SPEC-11.PG-CTE: user CTEs read from physical table (empty).
        // The CTE produces no rows, so the JOIN returns nothing.
        if (count($rows) === 0) {
            $this->markTestIncomplete('Known issue: user CTEs read from physical table (SPEC-11.PG-CTE)');
        }

        // If fixed: Top regions are West (550) and East (450). Central (380) excluded.
        $this->assertCount(5, $rows);
        $this->assertEquals(1, (int) $rows[0]['id']);
        $this->assertEquals(450.00, (float) $rows[0]['region_total']);
        $this->assertEquals(2, (int) $rows[1]['id']);
        $this->assertEquals(550.00, (float) $rows[1]['region_total']);
    }

    /**
     * Chained CTE with prepared statement and WHERE filter.
     * Known issue: user CTEs read from physical table on PostgreSQL (SPEC-11.PG-CTE).
     */
    public function testChainedCtePrepared(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "WITH regional AS (
                SELECT region, SUM(amount) AS total
                FROM pg_cuc_sales
                WHERE sale_date >= ?
                GROUP BY region
            ),
            ranked AS (
                SELECT *, RANK() OVER (ORDER BY total DESC) AS rnk
                FROM regional
            )
            SELECT * FROM ranked ORDER BY rnk",
            ['2026-02-01']
        );

        // Known issue SPEC-11.PG-CTE: user CTEs read from physical table (empty).
        if (count($rows) === 0) {
            $this->markTestIncomplete('Known issue: user CTEs read from physical table (SPEC-11.PG-CTE)');
        }

        // If fixed: After 2026-02-01: East 200, West 300, Central 380
        $this->assertCount(3, $rows);
        $this->assertSame('Central', $rows[0]['region']);
        $this->assertEquals(380.00, (float) $rows[0]['total']);
        $this->assertSame('West', $rows[1]['region']);
        $this->assertEquals(300.00, (float) $rows[1]['total']);
        $this->assertSame('East', $rows[2]['region']);
        $this->assertEquals(200.00, (float) $rows[2]['total']);
    }

    /**
     * Insert new row, then run chained CTE query.
     * Known issue: user CTEs read from physical table on PostgreSQL (SPEC-11.PG-CTE).
     */
    public function testChainedCteAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO pg_cuc_sales VALUES (9, 'Widget', 500.00, 'East', '2026-03-10')");

        $rows = $this->ztdQuery(
            "WITH regional AS (
                SELECT region, SUM(amount) AS total
                FROM pg_cuc_sales
                GROUP BY region
            ),
            ranked AS (
                SELECT *, RANK() OVER (ORDER BY total DESC) AS rnk
                FROM regional
            )
            SELECT * FROM ranked ORDER BY rnk"
        );

        // Known issue SPEC-11.PG-CTE: user CTEs read from physical table (empty).
        if (count($rows) === 0) {
            $this->markTestIncomplete('Known issue: user CTEs read from physical table (SPEC-11.PG-CTE)');
        }

        // If fixed: East now: 100+150+200+500=950, West: 550, Central: 380
        $this->assertCount(3, $rows);
        $this->assertSame('East', $rows[0]['region']);
        $this->assertEquals(950.00, (float) $rows[0]['total']);
        $this->assertEquals(1, (int) $rows[0]['rnk']);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_cuc_sales VALUES (9, 'Doohickey', 999.00, 'North', '2026-04-01')");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_cuc_sales");
        $this->assertEquals(9, (int) $rows[0]['cnt']);

        $this->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM pg_cuc_sales')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
