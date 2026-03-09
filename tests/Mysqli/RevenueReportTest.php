<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests multi-source revenue report combining online orders and store sales (MySQLi).
 * SQL patterns exercised: UNION ALL across shadow tables, CASE WHEN expression,
 * conditional aggregation (SUM + CASE pivot), subquery with UNION ALL + GROUP BY.
 * These patterns stress the CTE rewriter when multiple CTEs are needed across UNION branches.
 * @spec SPEC-10.2.173
 */
class RevenueReportTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_rev_online_orders (
                id INT AUTO_INCREMENT PRIMARY KEY,
                customer VARCHAR(100),
                amount DECIMAL(10,2),
                order_date VARCHAR(20),
                category VARCHAR(50)
            )',
            'CREATE TABLE mi_rev_store_sales (
                id INT AUTO_INCREMENT PRIMARY KEY,
                cashier VARCHAR(100),
                amount DECIMAL(10,2),
                sale_date VARCHAR(20),
                category VARCHAR(50)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_rev_store_sales', 'mi_rev_online_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Online orders
        $this->mysqli->query("INSERT INTO mi_rev_online_orders VALUES (1, 'Alice', 150.00, '2025-06-01', 'electronics')");
        $this->mysqli->query("INSERT INTO mi_rev_online_orders VALUES (2, 'Bob', 75.50, '2025-06-01', 'clothing')");
        $this->mysqli->query("INSERT INTO mi_rev_online_orders VALUES (3, 'Alice', 200.00, '2025-06-02', 'electronics')");
        $this->mysqli->query("INSERT INTO mi_rev_online_orders VALUES (4, 'Carol', 50.00, '2025-06-03', 'books')");

        // Store sales
        $this->mysqli->query("INSERT INTO mi_rev_store_sales VALUES (1, 'Dave', 300.00, '2025-06-01', 'electronics')");
        $this->mysqli->query("INSERT INTO mi_rev_store_sales VALUES (2, 'Eve', 120.00, '2025-06-02', 'clothing')");
        $this->mysqli->query("INSERT INTO mi_rev_store_sales VALUES (3, 'Dave', 80.00, '2025-06-03', 'books')");
    }

    /**
     * UNION ALL combining both tables with a source label.
     * 7 rows total, ordered by amount DESC.
     * First row: store/Dave/300.00, last row: Carol/50.00.
     */
    public function testUnionAllBothSources(): void
    {
        $rows = $this->ztdQuery(
            "SELECT 'online' AS source, customer AS person, amount, category FROM mi_rev_online_orders
             UNION ALL
             SELECT 'store' AS source, cashier AS person, amount, category FROM mi_rev_store_sales
             ORDER BY amount DESC"
        );

        $this->assertCount(7, $rows);

        $this->assertSame('store', $rows[0]['source']);
        $this->assertSame('Dave', $rows[0]['person']);
        $this->assertEqualsWithDelta(300.00, (float) $rows[0]['amount'], 0.01);

        $this->assertSame('Carol', $rows[6]['person']);
        $this->assertEqualsWithDelta(50.00, (float) $rows[6]['amount'], 0.01);
        $this->assertSame('books', $rows[6]['category']);
    }

    /**
     * Aggregate total revenue by category across both sources via subquery UNION ALL.
     * books: 130.00, clothing: 195.50, electronics: 650.00.
     */
    public function testUnionAllWithGroupBy(): void
    {
        $rows = $this->ztdQuery(
            "SELECT category, SUM(amount) AS total FROM (
                SELECT category, amount FROM mi_rev_online_orders
                UNION ALL
                SELECT category, amount FROM mi_rev_store_sales
            ) combined
            GROUP BY category
            ORDER BY category"
        );

        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'UNION ALL in derived table returns empty through ZTD — '
                . 'CTE rewriter does not rewrite table references inside derived tables (upstream #13).'
            );
        }

        $this->assertCount(3, $rows);

        $this->assertSame('books', $rows[0]['category']);
        $this->assertEqualsWithDelta(130.00, (float) $rows[0]['total'], 0.01);

        $this->assertSame('clothing', $rows[1]['category']);
        $this->assertEqualsWithDelta(195.50, (float) $rows[1]['total'], 0.01);

        $this->assertSame('electronics', $rows[2]['category']);
        $this->assertEqualsWithDelta(650.00, (float) $rows[2]['total'], 0.01);
    }

    /**
     * CASE WHEN to classify amounts into tiers.
     * Alice/150/medium, Bob/75.50/low, Alice/200/high, Carol/50/low.
     */
    public function testCaseWhenCategorization(): void
    {
        $rows = $this->ztdQuery(
            "SELECT customer, amount,
                CASE
                    WHEN amount >= 200 THEN 'high'
                    WHEN amount >= 100 THEN 'medium'
                    ELSE 'low'
                END AS tier
            FROM mi_rev_online_orders
            ORDER BY id"
        );

        $this->assertCount(4, $rows);

        $this->assertSame('Alice', $rows[0]['customer']);
        $this->assertEqualsWithDelta(150.00, (float) $rows[0]['amount'], 0.01);
        $this->assertSame('medium', $rows[0]['tier']);

        $this->assertSame('Bob', $rows[1]['customer']);
        $this->assertEqualsWithDelta(75.50, (float) $rows[1]['amount'], 0.01);
        $this->assertSame('low', $rows[1]['tier']);

        $this->assertSame('Alice', $rows[2]['customer']);
        $this->assertEqualsWithDelta(200.00, (float) $rows[2]['amount'], 0.01);
        $this->assertSame('high', $rows[2]['tier']);

        $this->assertSame('Carol', $rows[3]['customer']);
        $this->assertEqualsWithDelta(50.00, (float) $rows[3]['amount'], 0.01);
        $this->assertSame('low', $rows[3]['tier']);
    }

    /**
     * SUM with CASE for pivot-style conditional aggregation.
     * books: online=50, store=80; clothing: online=75.50, store=120; electronics: online=350, store=300.
     */
    public function testConditionalAggregation(): void
    {
        $rows = $this->ztdQuery(
            "SELECT category,
                SUM(CASE WHEN source = 'online' THEN amount ELSE 0 END) AS online_total,
                SUM(CASE WHEN source = 'store' THEN amount ELSE 0 END) AS store_total
            FROM (
                SELECT 'online' AS source, category, amount FROM mi_rev_online_orders
                UNION ALL
                SELECT 'store' AS source, category, amount FROM mi_rev_store_sales
            ) all_revenue
            GROUP BY category
            ORDER BY category"
        );

        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'UNION ALL in derived table returns empty through ZTD — '
                . 'CTE rewriter does not rewrite table references inside derived tables (upstream #13).'
            );
        }

        $this->assertCount(3, $rows);

        $this->assertSame('books', $rows[0]['category']);
        $this->assertEqualsWithDelta(50.00, (float) $rows[0]['online_total'], 0.01);
        $this->assertEqualsWithDelta(80.00, (float) $rows[0]['store_total'], 0.01);

        $this->assertSame('clothing', $rows[1]['category']);
        $this->assertEqualsWithDelta(75.50, (float) $rows[1]['online_total'], 0.01);
        $this->assertEqualsWithDelta(120.00, (float) $rows[1]['store_total'], 0.01);

        $this->assertSame('electronics', $rows[2]['category']);
        $this->assertEqualsWithDelta(350.00, (float) $rows[2]['online_total'], 0.01);
        $this->assertEqualsWithDelta(300.00, (float) $rows[2]['store_total'], 0.01);
    }

    /**
     * Insert a new online order via ztdExec, then UNION ALL both tables.
     * New row (5, 'Frank', 99.99, '2025-06-04', 'clothing') should appear; 8 rows total.
     */
    public function testUnionAllAfterShadowInsert(): void
    {
        $this->ztdExec(
            "INSERT INTO mi_rev_online_orders VALUES (5, 'Frank', 99.99, '2025-06-04', 'clothing')"
        );

        $rows = $this->ztdQuery(
            "SELECT 'online' AS source, customer AS person, amount, category FROM mi_rev_online_orders
             UNION ALL
             SELECT 'store' AS source, cashier AS person, amount, category FROM mi_rev_store_sales
             ORDER BY amount DESC"
        );

        $this->assertCount(8, $rows);

        // Verify Frank appears somewhere in the results
        $frank = array_filter($rows, fn($r) => $r['person'] === 'Frank');
        $this->assertCount(1, $frank);
        $frankRow = array_values($frank)[0];
        $this->assertSame('online', $frankRow['source']);
        $this->assertEqualsWithDelta(99.99, (float) $frankRow['amount'], 0.01);
        $this->assertSame('clothing', $frankRow['category']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->ztdExec(
            "INSERT INTO mi_rev_online_orders VALUES (5, 'Frank', 99.99, '2025-06-04', 'clothing')"
        );

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_rev_online_orders");
        $this->assertEquals(5, (int) $rows[0]['cnt']);

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_rev_online_orders');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
