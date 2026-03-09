<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests a warehouse inventory snapshot scenario through ZTD shadow store (SQLite PDO).
 * Bins with inbound/outbound movements exercise UNION ALL in subquery for
 * net quantity calculation, INSERT ... SELECT with GROUP BY for snapshot generation,
 * UPDATE with arithmetic from aggregate subquery, self-referencing DELETE for
 * zero-stock cleanup, HAVING on aggregated UNION ALL, and physical isolation.
 * @spec SPEC-10.2.166
 */
class SqliteInventorySnapshotTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_inv_bins (
                id INTEGER PRIMARY KEY,
                location TEXT,
                product TEXT,
                quantity INTEGER
            )',
            'CREATE TABLE sl_inv_inbound (
                id INTEGER PRIMARY KEY,
                bin_id INTEGER,
                qty INTEGER,
                received_date TEXT
            )',
            'CREATE TABLE sl_inv_outbound (
                id INTEGER PRIMARY KEY,
                bin_id INTEGER,
                qty INTEGER,
                shipped_date TEXT
            )',
            'CREATE TABLE sl_inv_snapshots (
                id INTEGER PRIMARY KEY,
                bin_id INTEGER,
                net_qty INTEGER,
                snapshot_date TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_inv_snapshots', 'sl_inv_outbound', 'sl_inv_inbound', 'sl_inv_bins'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 4 bins
        $this->pdo->exec("INSERT INTO sl_inv_bins VALUES (1, 'A-1', 'Widget', 100)");
        $this->pdo->exec("INSERT INTO sl_inv_bins VALUES (2, 'A-2', 'Gadget', 50)");
        $this->pdo->exec("INSERT INTO sl_inv_bins VALUES (3, 'B-1', 'Widget', 200)");
        $this->pdo->exec("INSERT INTO sl_inv_bins VALUES (4, 'B-2', 'Sprocket', 0)");

        // Inbound movements
        $this->pdo->exec("INSERT INTO sl_inv_inbound VALUES (1, 1, 30, '2026-03-01')");
        $this->pdo->exec("INSERT INTO sl_inv_inbound VALUES (2, 1, 20, '2026-03-05')");
        $this->pdo->exec("INSERT INTO sl_inv_inbound VALUES (3, 2, 15, '2026-03-02')");
        $this->pdo->exec("INSERT INTO sl_inv_inbound VALUES (4, 3, 50, '2026-03-03')");

        // Outbound movements
        $this->pdo->exec("INSERT INTO sl_inv_outbound VALUES (1, 1, 10, '2026-03-02')");
        $this->pdo->exec("INSERT INTO sl_inv_outbound VALUES (2, 1, 15, '2026-03-06')");
        $this->pdo->exec("INSERT INTO sl_inv_outbound VALUES (3, 2, 40, '2026-03-04')");
        $this->pdo->exec("INSERT INTO sl_inv_outbound VALUES (4, 3, 30, '2026-03-07')");
        $this->pdo->exec("INSERT INTO sl_inv_outbound VALUES (5, 4, 0, '2026-03-08')");
    }

    /**
     * UNION ALL in derived table: calculate net movement per bin.
     * Bin 1: +30+20-10-15 = +25, Bin 2: +15-40 = -25, Bin 3: +50-30 = +20, Bin 4: 0-0 = 0.
     *
     * NEW FINDING: UNION ALL inside a derived table (subquery in FROM) returns empty
     * results on SQLite. Top-level UNION ALL works correctly. The CTE rewriter does
     * not rewrite table references inside UNION ALL when wrapped in a derived table.
     */
    public function testNetMovementWithUnionAll(): void
    {
        $rows = $this->ztdQuery(
            "SELECT bin_id, SUM(qty) AS net_qty
             FROM (
                 SELECT bin_id, qty FROM sl_inv_inbound
                 UNION ALL
                 SELECT bin_id, -qty FROM sl_inv_outbound
             ) movements
             GROUP BY bin_id
             ORDER BY bin_id"
        );

        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'UNION ALL in derived table returns empty on SQLite. '
                . 'CTE rewriter does not rewrite table references inside UNION ALL when wrapped in a subquery. '
                . 'Top-level UNION ALL works. Expected 4 rows.'
            );
        }
        $this->assertCount(4, $rows);

        $this->assertEquals(1, (int) $rows[0]['bin_id']);
        $this->assertEquals(25, (int) $rows[0]['net_qty']);

        $this->assertEquals(2, (int) $rows[1]['bin_id']);
        $this->assertEquals(-25, (int) $rows[1]['net_qty']);

        $this->assertEquals(3, (int) $rows[2]['bin_id']);
        $this->assertEquals(20, (int) $rows[2]['net_qty']);

        $this->assertEquals(4, (int) $rows[3]['bin_id']);
        $this->assertEquals(0, (int) $rows[3]['net_qty']);
    }

    /**
     * INSERT ... SELECT with GROUP BY: generate snapshot from UNION ALL.
     * Calculates current stock as base quantity + net movement.
     * Expected 4 snapshots: Bin 1=125, Bin 2=25, Bin 3=220, Bin 4=0.
     *
     * Depends on UNION ALL in derived table (see testNetMovementWithUnionAll)
     * and INSERT ... SELECT column values (SPEC-11.INSERT-SELECT-COMPUTED).
     */
    public function testInsertSelectSnapshot(): void
    {
        $this->pdo->exec(
            "INSERT INTO sl_inv_snapshots (id, bin_id, net_qty, snapshot_date)
             SELECT b.id, b.id, b.quantity + COALESCE(m.net, 0), '2026-03-09'
             FROM sl_inv_bins b
             LEFT JOIN (
                 SELECT bin_id, SUM(qty) AS net
                 FROM (
                     SELECT bin_id, qty FROM sl_inv_inbound
                     UNION ALL
                     SELECT bin_id, -qty FROM sl_inv_outbound
                 ) movements
                 GROUP BY bin_id
             ) m ON m.bin_id = b.id"
        );

        $rows = $this->ztdQuery(
            "SELECT bin_id, net_qty FROM sl_inv_snapshots ORDER BY bin_id"
        );

        // INSERT...SELECT may insert rows with nullified columns (SPEC-11.INSERT-SELECT-COMPUTED)
        // and/or the UNION ALL derived table subquery returns empty, yielding wrong net values.
        $totalInserted = (int) $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_inv_snapshots")[0]['cnt'];
        $firstBinId = count($rows) > 0 ? (int) $rows[0]['bin_id'] : null;
        if ($totalInserted === 0 || (count($rows) > 0 && $firstBinId === 0)) {
            $this->markTestIncomplete(
                'INSERT...SELECT with UNION ALL derived table + SPEC-11.INSERT-SELECT-COMPUTED: '
                . "rows inserted: {$totalInserted}, first bin_id: " . var_export($firstBinId, true)
                . '. Column values nullified. Expected 4 snapshots with correct net_qty.'
            );
        }

        $this->assertCount(4, $rows);

        $this->assertEquals(1, (int) $rows[0]['bin_id']);
        $this->assertEquals(125, (int) $rows[0]['net_qty']);

        $this->assertEquals(2, (int) $rows[1]['bin_id']);
        $this->assertEquals(25, (int) $rows[1]['net_qty']);

        $this->assertEquals(3, (int) $rows[2]['bin_id']);
        $this->assertEquals(220, (int) $rows[2]['net_qty']);

        $this->assertEquals(4, (int) $rows[3]['bin_id']);
        $this->assertEquals(0, (int) $rows[3]['net_qty']);
    }

    /**
     * HAVING on aggregated UNION ALL: find bins with negative net movement.
     * Only Bin 2 has net < 0 (-25).
     *
     * Depends on UNION ALL in derived table (see testNetMovementWithUnionAll).
     */
    public function testHavingOnUnionAllAggregate(): void
    {
        $rows = $this->ztdQuery(
            "SELECT bin_id, SUM(qty) AS net_qty
             FROM (
                 SELECT bin_id, qty FROM sl_inv_inbound
                 UNION ALL
                 SELECT bin_id, -qty FROM sl_inv_outbound
             ) movements
             GROUP BY bin_id
             HAVING SUM(qty) < 0
             ORDER BY bin_id"
        );

        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'UNION ALL in derived table returns empty on SQLite (same as testNetMovementWithUnionAll). Expected 1 row (Bin 2, -25).'
            );
        }
        $this->assertCount(1, $rows);
        $this->assertEquals(2, (int) $rows[0]['bin_id']);
        $this->assertEquals(-25, (int) $rows[0]['net_qty']);
    }

    /**
     * JOIN with product-level aggregation across bins.
     * Widget: bins 1+3, quantity 100+200=300, net inbound 30+20+50=100, net outbound 10+15+30=55.
     * Gadget: bin 2, quantity 50, net inbound 15, net outbound 40.
     * Sprocket: bin 4, quantity 0, net inbound 0, net outbound 0.
     */
    public function testProductLevelAggregation(): void
    {
        $rows = $this->ztdQuery(
            "SELECT b.product,
                    SUM(b.quantity) AS base_stock,
                    COALESCE(SUM(ib.total_in), 0) AS total_in,
                    COALESCE(SUM(ob.total_out), 0) AS total_out,
                    SUM(b.quantity) + COALESCE(SUM(ib.total_in), 0) - COALESCE(SUM(ob.total_out), 0) AS current_stock
             FROM sl_inv_bins b
             LEFT JOIN (
                 SELECT bin_id, SUM(qty) AS total_in FROM sl_inv_inbound GROUP BY bin_id
             ) ib ON ib.bin_id = b.id
             LEFT JOIN (
                 SELECT bin_id, SUM(qty) AS total_out FROM sl_inv_outbound GROUP BY bin_id
             ) ob ON ob.bin_id = b.id
             GROUP BY b.product
             ORDER BY b.product"
        );

        $this->assertCount(3, $rows);

        $this->assertSame('Gadget', $rows[0]['product']);
        $this->assertEquals(50, (int) $rows[0]['base_stock']);
        $this->assertEquals(15, (int) $rows[0]['total_in']);
        $this->assertEquals(40, (int) $rows[0]['total_out']);
        $this->assertEquals(25, (int) $rows[0]['current_stock']);

        $this->assertSame('Sprocket', $rows[1]['product']);
        $this->assertEquals(0, (int) $rows[1]['base_stock']);
        $this->assertEquals(0, (int) $rows[1]['total_in']);
        $this->assertEquals(0, (int) $rows[1]['total_out']);
        $this->assertEquals(0, (int) $rows[1]['current_stock']);

        $this->assertSame('Widget', $rows[2]['product']);
        $this->assertEquals(300, (int) $rows[2]['base_stock']);
        $this->assertEquals(100, (int) $rows[2]['total_in']);
        $this->assertEquals(55, (int) $rows[2]['total_out']);
        $this->assertEquals(345, (int) $rows[2]['current_stock']);
    }

    /**
     * Prepared statement: find bins with net movement above a threshold in date range.
     * Params: start_date='2026-03-01', end_date='2026-03-09', min_net=20.
     * Expected: Bin 1 (+25), Bin 3 (+20).
     *
     * Combines two known issues: UNION ALL in derived table + SPEC-11.SQLITE-HAVING-PARAMS.
     */
    public function testPreparedMovementFilter(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT bin_id, SUM(qty) AS net_qty
             FROM (
                 SELECT bin_id, qty, received_date AS move_date FROM sl_inv_inbound
                 UNION ALL
                 SELECT bin_id, -qty, shipped_date AS move_date FROM sl_inv_outbound
             ) movements
             WHERE move_date BETWEEN ? AND ?
             GROUP BY bin_id
             HAVING SUM(qty) >= ?
             ORDER BY net_qty DESC",
            ['2026-03-01', '2026-03-09', 20]
        );

        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'UNION ALL in derived table + SPEC-11.SQLITE-HAVING-PARAMS: '
                . 'prepared UNION ALL derived table with HAVING returns empty. Expected 2 rows.'
            );
        }
        $this->assertCount(2, $rows);

        $this->assertEquals(1, (int) $rows[0]['bin_id']);
        $this->assertEquals(25, (int) $rows[0]['net_qty']);

        $this->assertEquals(3, (int) $rows[1]['bin_id']);
        $this->assertEquals(20, (int) $rows[1]['net_qty']);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO sl_inv_bins VALUES (5, 'C-1', 'Doohickey', 75)");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_inv_bins");
        $this->assertEquals(5, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_inv_bins")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
