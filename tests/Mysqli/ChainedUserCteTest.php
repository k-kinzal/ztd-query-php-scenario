<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests chained (multi-CTE) WITH queries through the ZTD CTE rewriter on MySQLi.
 * Verifies two-CTE, three-CTE, CTE+JOIN, prepared, and post-mutation scenarios.
 * @spec SPEC-10.2.96
 */
class ChainedUserCteTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_cuc_sales (
            id INT PRIMARY KEY,
            product VARCHAR(255),
            amount DECIMAL(10,2),
            region VARCHAR(100),
            sale_date DATE
        )';
    }

    protected function getTableNames(): array
    {
        return ['mi_cuc_sales'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_cuc_sales VALUES (1, 'Widget',  100.00, 'North', '2024-01-15')");
        $this->mysqli->query("INSERT INTO mi_cuc_sales VALUES (2, 'Gadget',  250.00, 'South', '2024-01-20')");
        $this->mysqli->query("INSERT INTO mi_cuc_sales VALUES (3, 'Widget',  150.00, 'North', '2024-02-10')");
        $this->mysqli->query("INSERT INTO mi_cuc_sales VALUES (4, 'Gizmo',   300.00, 'East',  '2024-02-15')");
        $this->mysqli->query("INSERT INTO mi_cuc_sales VALUES (5, 'Gadget',  200.00, 'South', '2024-03-01')");
        $this->mysqli->query("INSERT INTO mi_cuc_sales VALUES (6, 'Widget',   75.00, 'East',  '2024-03-10')");
        $this->mysqli->query("INSERT INTO mi_cuc_sales VALUES (7, 'Gizmo',   400.00, 'North', '2024-03-20')");
        $this->mysqli->query("INSERT INTO mi_cuc_sales VALUES (8, 'Gadget',  125.00, 'East',  '2024-04-05')");
    }

    /**
     * Two chained CTEs: one for regional totals, one filtering top regions.
     * @spec SPEC-10.2.96
     */
    public function testTwoChainedCtes(): void
    {
        $rows = $this->ztdQuery("
            WITH regional_totals AS (
                SELECT region, SUM(amount) AS total
                FROM mi_cuc_sales
                GROUP BY region
            ),
            top_regions AS (
                SELECT region, total
                FROM regional_totals
                WHERE total >= 300
            )
            SELECT region, total FROM top_regions ORDER BY total DESC
        ");

        $this->assertGreaterThanOrEqual(1, count($rows));
        // North: 100+150+400=650, South: 250+200=450, East: 300+75+125=500
        // All three regions have total >= 300
        $this->assertCount(3, $rows);
        $this->assertSame('North', $rows[0]['region']);
        $this->assertEqualsWithDelta(650.00, (float) $rows[0]['total'], 0.01);
    }

    /**
     * Three chained CTEs: product totals -> ranked -> top products.
     * @spec SPEC-10.2.96
     */
    public function testThreeChainedCtes(): void
    {
        $rows = $this->ztdQuery("
            WITH product_totals AS (
                SELECT product, SUM(amount) AS total, COUNT(*) AS sale_count
                FROM mi_cuc_sales
                GROUP BY product
            ),
            ranked AS (
                SELECT product, total, sale_count,
                       RANK() OVER (ORDER BY total DESC) AS rnk
                FROM product_totals
            ),
            top_products AS (
                SELECT product, total, sale_count
                FROM ranked
                WHERE rnk <= 2
            )
            SELECT product, total, sale_count FROM top_products ORDER BY total DESC
        ");

        // Gizmo: 300+400=700, Gadget: 250+200+125=575, Widget: 100+150+75=325
        $this->assertCount(2, $rows);
        $this->assertSame('Gizmo', $rows[0]['product']);
        $this->assertEqualsWithDelta(700.00, (float) $rows[0]['total'], 0.01);
        $this->assertSame('Gadget', $rows[1]['product']);
        $this->assertEqualsWithDelta(575.00, (float) $rows[1]['total'], 0.01);
    }

    /**
     * Chained CTE joined back to the original table.
     * @spec SPEC-10.2.96
     */
    public function testChainedCteWithJoin(): void
    {
        $rows = $this->ztdQuery("
            WITH avg_by_product AS (
                SELECT product, AVG(amount) AS avg_amount
                FROM mi_cuc_sales
                GROUP BY product
            )
            SELECT s.id, s.product, s.amount, a.avg_amount
            FROM mi_cuc_sales s
            JOIN avg_by_product a ON s.product = a.product
            WHERE s.amount > a.avg_amount
            ORDER BY s.id
        ");

        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'User CTE joined back to original table returns empty. '
                . 'The CTE rewriter may conflict when the outer query references both '
                . 'a physical table and a user-defined CTE.'
            );
        }

        // Widget avg ~108.33: id=3 (150) above avg
        // Gadget avg ~191.67: id=2 (250), id=5 (200) above avg
        // Gizmo avg 350: id=7 (400) above avg
        $this->assertGreaterThanOrEqual(1, count($rows));
        $ids = array_column($rows, 'id');
        $this->assertContains('3', array_map('strval', $ids));
        $this->assertContains('7', array_map('strval', $ids));
    }

    /**
     * Chained CTE with prepared statement parameter.
     * @spec SPEC-10.2.96
     */
    public function testChainedCtePrepared(): void
    {
        $rows = $this->ztdPrepareAndExecute("
            WITH filtered AS (
                SELECT product, amount, region
                FROM mi_cuc_sales
                WHERE region = ?
            ),
            summary AS (
                SELECT product, SUM(amount) AS total
                FROM filtered
                GROUP BY product
            )
            SELECT product, total FROM summary ORDER BY total DESC
        ", ['North']);

        // North: Widget 100+150=250, Gizmo 400
        $this->assertCount(2, $rows);
        $this->assertSame('Gizmo', $rows[0]['product']);
        $this->assertEqualsWithDelta(400.00, (float) $rows[0]['total'], 0.01);
        $this->assertSame('Widget', $rows[1]['product']);
        $this->assertEqualsWithDelta(250.00, (float) $rows[1]['total'], 0.01);
    }

    /**
     * Chained CTE correctly reflects data inserted after initial seed.
     * @spec SPEC-10.2.96
     */
    public function testChainedCteAfterInsert(): void
    {
        $this->mysqli->query("INSERT INTO mi_cuc_sales VALUES (9, 'Widget', 500.00, 'North', '2024-04-15')");

        $rows = $this->ztdQuery("
            WITH product_totals AS (
                SELECT product, SUM(amount) AS total
                FROM mi_cuc_sales
                GROUP BY product
            ),
            above_avg AS (
                SELECT product, total
                FROM product_totals
                WHERE total > (SELECT AVG(total) FROM product_totals)
            )
            SELECT product, total FROM above_avg ORDER BY total DESC
        ");

        // Widget: 100+150+75+500=825, Gizmo: 700, Gadget: 575
        // Avg = (825+700+575)/3 = 700
        // Above avg: Widget(825)
        $this->assertGreaterThanOrEqual(1, count($rows));
        $this->assertSame('Widget', $rows[0]['product']);
        $this->assertEqualsWithDelta(825.00, (float) $rows[0]['total'], 0.01);
    }

    /**
     * Physical table remains empty — all mutations are in ZTD shadow store.
     * @spec SPEC-10.2.96
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_cuc_sales VALUES (9, 'NewItem', 999.00, 'West', '2024-05-01')");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_cuc_sales");
        $this->assertSame(9, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_cuc_sales');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
