<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests aggregate FILTER (WHERE ...) clause through SQLite CTE shadow store.
 *
 * SQLite supports FILTER since 3.30.0 (2019-10-04).
 * FILTER is a standard SQL feature that restricts which rows are included
 * in an aggregate computation: COUNT(*) FILTER (WHERE condition).
 * The CTE rewriter must preserve FILTER clauses during rewriting.
 */
class SqliteAggregateFilterClauseTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_afc_sales (
                id INTEGER PRIMARY KEY,
                rep_name TEXT NOT NULL,
                amount REAL NOT NULL,
                status TEXT NOT NULL,
                quarter TEXT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_afc_sales'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_afc_sales VALUES (1, 'Alice', 100, 'closed', 'Q1')");
        $this->pdo->exec("INSERT INTO sl_afc_sales VALUES (2, 'Alice', 200, 'closed', 'Q1')");
        $this->pdo->exec("INSERT INTO sl_afc_sales VALUES (3, 'Alice', 50, 'lost', 'Q1')");
        $this->pdo->exec("INSERT INTO sl_afc_sales VALUES (4, 'Alice', 300, 'closed', 'Q2')");
        $this->pdo->exec("INSERT INTO sl_afc_sales VALUES (5, 'Bob', 150, 'closed', 'Q1')");
        $this->pdo->exec("INSERT INTO sl_afc_sales VALUES (6, 'Bob', 75, 'lost', 'Q1')");
        $this->pdo->exec("INSERT INTO sl_afc_sales VALUES (7, 'Bob', 250, 'pending', 'Q2')");
    }

    /**
     * COUNT with FILTER clause — count only closed deals per rep.
     */
    public function testCountFilter(): void
    {
        $rows = $this->ztdQuery(
            "SELECT rep_name,
                    COUNT(*) AS total_deals,
                    COUNT(*) FILTER (WHERE status = 'closed') AS closed_deals
             FROM sl_afc_sales
             GROUP BY rep_name
             ORDER BY rep_name"
        );

        $this->assertCount(2, $rows);

        // Alice: 4 total, 3 closed
        $this->assertEquals('Alice', $rows[0]['rep_name']);
        $this->assertEquals(4, $rows[0]['total_deals']);
        $this->assertEquals(3, $rows[0]['closed_deals']);

        // Bob: 3 total, 1 closed
        $this->assertEquals('Bob', $rows[1]['rep_name']);
        $this->assertEquals(3, $rows[1]['total_deals']);
        $this->assertEquals(1, $rows[1]['closed_deals']);
    }

    /**
     * SUM with FILTER clause — sum only specific statuses.
     */
    public function testSumFilter(): void
    {
        $rows = $this->ztdQuery(
            "SELECT rep_name,
                    SUM(amount) AS total_amount,
                    SUM(amount) FILTER (WHERE status = 'closed') AS closed_amount,
                    SUM(amount) FILTER (WHERE status = 'lost') AS lost_amount
             FROM sl_afc_sales
             GROUP BY rep_name
             ORDER BY rep_name"
        );

        $this->assertCount(2, $rows);

        // Alice: total=650, closed=600, lost=50
        $this->assertEquals(650, (float) $rows[0]['total_amount']);
        $this->assertEquals(600, (float) $rows[0]['closed_amount']);
        $this->assertEquals(50, (float) $rows[0]['lost_amount']);

        // Bob: total=475, closed=150, lost=75
        $this->assertEquals(475, (float) $rows[1]['total_amount']);
        $this->assertEquals(150, (float) $rows[1]['closed_amount']);
        $this->assertEquals(75, (float) $rows[1]['lost_amount']);
    }

    /**
     * Multiple FILTER clauses with different conditions in same query.
     */
    public function testMultipleFilterClauses(): void
    {
        $rows = $this->ztdQuery(
            "SELECT rep_name,
                    COUNT(*) FILTER (WHERE quarter = 'Q1') AS q1_count,
                    COUNT(*) FILTER (WHERE quarter = 'Q2') AS q2_count,
                    SUM(amount) FILTER (WHERE quarter = 'Q1' AND status = 'closed') AS q1_closed_amount
             FROM sl_afc_sales
             GROUP BY rep_name
             ORDER BY rep_name"
        );

        $this->assertCount(2, $rows);

        // Alice: Q1=3, Q2=1, Q1 closed amount=300
        $this->assertEquals(3, $rows[0]['q1_count']);
        $this->assertEquals(1, $rows[0]['q2_count']);
        $this->assertEquals(300, (float) $rows[0]['q1_closed_amount']);
    }

    /**
     * FILTER clause without GROUP BY (whole-table aggregate).
     */
    public function testFilterWithoutGroupBy(): void
    {
        $rows = $this->ztdQuery(
            "SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE status = 'closed') AS closed,
                AVG(amount) FILTER (WHERE status = 'closed') AS avg_closed
             FROM sl_afc_sales"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(7, $rows[0]['total']);
        $this->assertEquals(4, $rows[0]['closed']);
    }

    /**
     * FILTER clause with HAVING.
     */
    public function testFilterWithHaving(): void
    {
        $rows = $this->ztdQuery(
            "SELECT rep_name,
                    COUNT(*) FILTER (WHERE status = 'closed') AS closed_deals
             FROM sl_afc_sales
             GROUP BY rep_name
             HAVING COUNT(*) FILTER (WHERE status = 'closed') >= 2
             ORDER BY rep_name"
        );

        // Only Alice has >= 2 closed deals
        $this->assertCount(1, $rows);
        $this->assertEquals('Alice', $rows[0]['rep_name']);
    }

    /**
     * FILTER clause with prepared statement.
     */
    public function testFilterWithPrepared(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT rep_name,
                    SUM(amount) FILTER (WHERE status = ?) AS target_amount
             FROM sl_afc_sales
             GROUP BY rep_name
             ORDER BY rep_name",
            ['closed']
        );

        $this->assertCount(2, $rows);
        $this->assertEquals(600, (float) $rows[0]['target_amount']); // Alice
        $this->assertEquals(150, (float) $rows[1]['target_amount']); // Bob
    }

    /**
     * Physical isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $count = (int) $this->pdo->query('SELECT COUNT(*) FROM sl_afc_sales')->fetchColumn();
        $this->assertSame(0, $count);
    }
}
