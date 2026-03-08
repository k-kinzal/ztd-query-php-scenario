<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests subquery rewriting gaps in the CTE rewriter through ZTD shadow store.
 *
 * Discovered issue: the CTE rewriter does not rewrite table references inside
 * subqueries (both scalar subqueries in SELECT and user CTEs) when the subquery
 * contains a bare SELECT FROM table without a WHERE or GROUP BY clause.
 * Adding WHERE 1=1 forces the rewriter to recognize and rewrite the reference.
 *
 * @spec SPEC-11.BARE-SUBQUERY-REWRITE
 */
class SqliteScalarSubqueryInSelectTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_sss_items (id INTEGER PRIMARY KEY, category TEXT, value REAL)',
            'CREATE TABLE sl_sss_totals (id INTEGER PRIMARY KEY, label TEXT, total REAL)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_sss_items', 'sl_sss_totals'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_sss_items VALUES (1, 'A', 100.0)");
        $this->pdo->exec("INSERT INTO sl_sss_items VALUES (2, 'A', 200.0)");
        $this->pdo->exec("INSERT INTO sl_sss_items VALUES (3, 'B', 300.0)");
        $this->pdo->exec("INSERT INTO sl_sss_items VALUES (4, 'B', 400.0)");
        $this->pdo->exec("INSERT INTO sl_sss_totals VALUES (1, 'grand_total', 1000.0)");
    }

    // ---------------------------------------------------------------
    // Scalar subquery in SELECT list
    // ---------------------------------------------------------------

    /**
     * Scalar subquery referencing same shadow table (bare SELECT) returns empty.
     * Root cause: the CTE rewriter does not rewrite table references in
     * bare "SELECT ... FROM table" subqueries (no WHERE/GROUP BY).
     */
    public function testScalarSubqueryBareSelectReturnsEmpty(): void
    {
        $rows = $this->ztdQuery(
            "SELECT category, SUM(value) AS cat_total,
                    SUM(value) / (SELECT SUM(value) FROM sl_sss_items) AS ratio
             FROM sl_sss_items
             GROUP BY category"
        );

        // Known issue: entire query returns empty
        $this->assertCount(0, $rows);
    }

    /**
     * Scalar subquery with WHERE 1=1 workaround works.
     */
    public function testScalarSubqueryWithWhereTrueWorks(): void
    {
        $rows = $this->ztdQuery(
            "SELECT category, SUM(value) AS cat_total,
                    ROUND(SUM(value) * 1.0 / (SELECT SUM(value) FROM sl_sss_items WHERE 1=1), 2) AS ratio
             FROM sl_sss_items
             GROUP BY category
             ORDER BY category"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('A', $rows[0]['category']);
        $this->assertEqualsWithDelta(0.3, (float) $rows[0]['ratio'], 0.01);
        $this->assertSame('B', $rows[1]['category']);
        $this->assertEqualsWithDelta(0.7, (float) $rows[1]['ratio'], 0.01);
    }

    /**
     * Scalar subquery referencing a DIFFERENT shadow table works
     * (because the different table's subquery has WHERE clause).
     */
    public function testScalarSubqueryDifferentTableWorks(): void
    {
        $rows = $this->ztdQuery(
            "SELECT category, SUM(value) AS cat_total,
                    SUM(value) / (SELECT total FROM sl_sss_totals WHERE label = 'grand_total') AS ratio
             FROM sl_sss_items
             GROUP BY category
             ORDER BY category"
        );

        $this->assertCount(2, $rows);
        $this->assertEqualsWithDelta(0.3, (float) $rows[0]['ratio'], 0.01);
    }

    /**
     * Scalar subquery in WHERE referencing same table works
     * (WHERE subqueries always have the outer WHERE context).
     */
    public function testScalarSubqueryInWhereWorks(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, value FROM sl_sss_items
             WHERE value > (SELECT AVG(value) FROM sl_sss_items)
             ORDER BY id"
        );

        // AVG = 250. Values > 250: 300, 400
        $this->assertCount(2, $rows);
        $this->assertEquals(3, (int) $rows[0]['id']);
        $this->assertEquals(4, (int) $rows[1]['id']);
    }

    // ---------------------------------------------------------------
    // User CTE bare SELECT issue
    // ---------------------------------------------------------------

    /**
     * User CTE with bare SELECT FROM table returns 0 rows.
     */
    public function testUserCteBareSelectReturnsEmpty(): void
    {
        $rows = $this->ztdQuery(
            "WITH t AS (SELECT * FROM sl_sss_items)
             SELECT COUNT(*) AS c FROM t"
        );

        // Known issue: CTE reads from physical table (empty)
        $this->assertEquals(0, (int) $rows[0]['c']);
    }

    /**
     * User CTE with WHERE 1=1 returns correct data.
     */
    public function testUserCteWithWhereTrueWorks(): void
    {
        $rows = $this->ztdQuery(
            "WITH t AS (SELECT * FROM sl_sss_items WHERE 1=1)
             SELECT COUNT(*) AS c FROM t"
        );

        $this->assertEquals(4, (int) $rows[0]['c']);
    }

    /**
     * User CTE with GROUP BY returns correct data (existing behavior).
     */
    public function testUserCteWithGroupByWorks(): void
    {
        $rows = $this->ztdQuery(
            "WITH t AS (SELECT category, SUM(value) AS total FROM sl_sss_items GROUP BY category)
             SELECT * FROM t ORDER BY category"
        );

        $this->assertCount(2, $rows);
        $this->assertEqualsWithDelta(300.0, (float) $rows[0]['total'], 0.01);
        $this->assertEqualsWithDelta(700.0, (float) $rows[1]['total'], 0.01);
    }

    /**
     * User CTE aggregate without GROUP BY returns NULL (bare SELECT variant).
     */
    public function testUserCteAggregateNoGroupByReturnsNull(): void
    {
        $rows = $this->ztdQuery(
            "WITH t AS (SELECT SUM(value) AS total FROM sl_sss_items)
             SELECT total FROM t"
        );

        // Returns NULL because CTE reads from physical table (empty)
        $this->assertCount(1, $rows);
        $this->assertNull($rows[0]['total']);
    }

    /**
     * User CTE aggregate with WHERE 1=1 returns correct total.
     */
    public function testUserCteAggregateWithWhereTrueWorks(): void
    {
        $rows = $this->ztdQuery(
            "WITH t AS (SELECT SUM(value) AS total FROM sl_sss_items WHERE 1=1)
             SELECT total FROM t"
        );

        $this->assertCount(1, $rows);
        $this->assertEqualsWithDelta(1000.0, (float) $rows[0]['total'], 0.01);
    }

    // ---------------------------------------------------------------
    // Workarounds
    // ---------------------------------------------------------------

    /**
     * Workaround: CROSS JOIN with derived table aggregate.
     * On SQLite, derived tables in JOINs ARE rewritten (SPEC-3.3a).
     */
    public function testWorkaroundCrossJoin(): void
    {
        $rows = $this->ztdQuery(
            "SELECT i.category, SUM(i.value) AS cat_total,
                    ROUND(SUM(i.value) / t.grand_total, 2) AS ratio
             FROM sl_sss_items i
             CROSS JOIN (SELECT SUM(value) AS grand_total FROM sl_sss_items) t
             GROUP BY i.category, t.grand_total
             ORDER BY i.category"
        );

        $this->assertCount(2, $rows);
        $this->assertEqualsWithDelta(0.3, (float) $rows[0]['ratio'], 0.01);
        $this->assertEqualsWithDelta(0.7, (float) $rows[1]['ratio'], 0.01);
    }

    /**
     * User CTE with CROSS JOIN to outer shadow table also fails.
     * Even with WHERE 1=1, combining user CTE + CROSS JOIN + outer shadow table
     * produces empty results on SQLite.
     */
    public function testUserCteCrossJoinOuterShadowTableFails(): void
    {
        $rows = $this->ztdQuery(
            "WITH totals AS (SELECT SUM(value) AS grand_total FROM sl_sss_items WHERE 1=1)
             SELECT i.category, SUM(i.value) AS cat_total, t.grand_total
             FROM sl_sss_items i
             CROSS JOIN totals t
             GROUP BY i.category, t.grand_total
             ORDER BY i.category"
        );

        // Known issue: CTE + CROSS JOIN + outer shadow table = empty
        $this->assertCount(0, $rows);
    }

    /**
     * Scalar subquery with literal (no table) works normally.
     */
    public function testScalarSubqueryLiteralWorks(): void
    {
        $rows = $this->ztdQuery(
            "SELECT category, SUM(value) AS cat_total,
                    SUM(value) / (SELECT 1000.0) AS ratio
             FROM sl_sss_items
             GROUP BY category
             ORDER BY category"
        );

        $this->assertCount(2, $rows);
        $this->assertEqualsWithDelta(0.3, (float) $rows[0]['ratio'], 0.01);
    }

    /**
     * Issue persists after INSERT mutation.
     */
    public function testBareSubqueryIssueAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO sl_sss_items VALUES (5, 'C', 500.0)");

        $rows = $this->ztdQuery(
            "SELECT category, SUM(value) / (SELECT SUM(value) FROM sl_sss_items) AS ratio
             FROM sl_sss_items
             GROUP BY category"
        );

        $this->assertCount(0, $rows);
    }
}
