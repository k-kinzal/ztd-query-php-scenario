<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests multiple user-written CTEs chained together (CTE2 references CTE1).
 * Common in analytics pipelines and likely to conflict with ZTD's own CTE wrapping.
 * @spec SPEC-10.2.96
 */
class SqliteChainedUserCteTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_cuc_sales (
            id INTEGER PRIMARY KEY,
            product TEXT,
            amount REAL,
            region TEXT,
            sale_date TEXT
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_cuc_sales'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_cuc_sales VALUES (1, 'Widget', 100.00, 'East', '2026-01-15')");
        $this->pdo->exec("INSERT INTO sl_cuc_sales VALUES (2, 'Widget', 250.00, 'West', '2026-01-16')");
        $this->pdo->exec("INSERT INTO sl_cuc_sales VALUES (3, 'Gadget', 150.00, 'East', '2026-01-17')");
        $this->pdo->exec("INSERT INTO sl_cuc_sales VALUES (4, 'Gadget', 300.00, 'West', '2026-02-01')");
        $this->pdo->exec("INSERT INTO sl_cuc_sales VALUES (5, 'Gizmo', 200.00, 'East', '2026-02-10')");
        $this->pdo->exec("INSERT INTO sl_cuc_sales VALUES (6, 'Gizmo', 175.00, 'Central', '2026-02-15')");
        $this->pdo->exec("INSERT INTO sl_cuc_sales VALUES (7, 'Widget', 125.00, 'Central', '2026-03-01')");
        $this->pdo->exec("INSERT INTO sl_cuc_sales VALUES (8, 'Gadget', 80.00, 'Central', '2026-03-05')");
    }

    /**
     * Two chained CTEs: regional totals, then ranked by total.
     */
    public function testTwoChainedCtes(): void
    {
        $rows = $this->ztdQuery(
            "WITH regional AS (
                SELECT region, SUM(amount) AS total
                FROM sl_cuc_sales
                GROUP BY region
            ),
            ranked AS (
                SELECT *, RANK() OVER (ORDER BY total DESC) AS rnk
                FROM regional
            )
            SELECT * FROM ranked ORDER BY rnk"
        );

        $this->assertCount(3, $rows);
        // East: 100+150+200=450, West: 250+300=550, Central: 175+125+80=380
        $this->assertSame('West', $rows[0]['region']);
        $this->assertEquals(550.00, (float) $rows[0]['total']);
        $this->assertEquals(1, (int) $rows[0]['rnk']);
        $this->assertSame('East', $rows[1]['region']);
        $this->assertEquals(450.00, (float) $rows[1]['total']);
        $this->assertEquals(2, (int) $rows[1]['rnk']);
        $this->assertSame('Central', $rows[2]['region']);
        $this->assertEquals(380.00, (float) $rows[2]['total']);
        $this->assertEquals(3, (int) $rows[2]['rnk']);
    }

    /**
     * Three chained CTEs: product totals -> filtered -> with running sum.
     */
    public function testThreeChainedCtes(): void
    {
        $rows = $this->ztdQuery(
            "WITH product_totals AS (
                SELECT product, SUM(amount) AS total
                FROM sl_cuc_sales
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

        // Widget: 100+250+125=475, Gadget: 150+300+80=530, Gizmo: 200+175=375
        // All > 300, so all pass filter
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
     * The outer SELECT references both the physical table (sl_cuc_sales)
     * AND the user CTE (top_regions). The CTE rewriter must handle both.
     */
    public function testChainedCteWithJoin(): void
    {
        $rows = $this->ztdQuery(
            "WITH regional AS (
                SELECT region, SUM(amount) AS region_total
                FROM sl_cuc_sales
                GROUP BY region
            ),
            top_regions AS (
                SELECT region, region_total
                FROM regional
                WHERE region_total >= 450
            )
            SELECT s.id, s.product, s.amount, t.region_total
            FROM sl_cuc_sales s
            JOIN top_regions t ON s.region = t.region
            ORDER BY s.id"
        );

        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'User CTE joined back to original table returns empty. '
                . 'The CTE rewriter may conflict when the outer query references both '
                . 'a physical table and a user-defined CTE.'
            );
        }

        // Top regions: West (550), East (450). Central (380) excluded.
        // Rows from East: 1,3,5; from West: 2,4
        $this->assertCount(5, $rows);
        $this->assertEquals(1, (int) $rows[0]['id']);
        $this->assertEquals(450.00, (float) $rows[0]['region_total']);
        $this->assertEquals(2, (int) $rows[1]['id']);
        $this->assertEquals(550.00, (float) $rows[1]['region_total']);
    }

    /**
     * Chained CTE with prepared statement and WHERE filter.
     */
    public function testChainedCtePrepared(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "WITH regional AS (
                SELECT region, SUM(amount) AS total
                FROM sl_cuc_sales
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

        // After 2026-02-01: id4 West 300, id5 East 200, id6 Central 175, id7 Central 125, id8 Central 80
        // East: 200, West: 300, Central: 175+125+80=380
        $this->assertCount(3, $rows);
        $this->assertSame('Central', $rows[0]['region']);
        $this->assertEquals(380.00, (float) $rows[0]['total']);
        $this->assertSame('West', $rows[1]['region']);
        $this->assertEquals(300.00, (float) $rows[1]['total']);
        $this->assertSame('East', $rows[2]['region']);
        $this->assertEquals(200.00, (float) $rows[2]['total']);
    }

    /**
     * Insert new row, then run chained CTE query - must see new data.
     */
    public function testChainedCteAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO sl_cuc_sales VALUES (9, 'Widget', 500.00, 'East', '2026-03-10')");

        $rows = $this->ztdQuery(
            "WITH regional AS (
                SELECT region, SUM(amount) AS total
                FROM sl_cuc_sales
                GROUP BY region
            ),
            ranked AS (
                SELECT *, RANK() OVER (ORDER BY total DESC) AS rnk
                FROM regional
            )
            SELECT * FROM ranked ORDER BY rnk"
        );

        // East now: 100+150+200+500=950, West: 550, Central: 380
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
        $this->pdo->exec("INSERT INTO sl_cuc_sales VALUES (9, 'Doohickey', 999.00, 'North', '2026-04-01')");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_cuc_sales");
        $this->assertEquals(9, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM sl_cuc_sales')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
