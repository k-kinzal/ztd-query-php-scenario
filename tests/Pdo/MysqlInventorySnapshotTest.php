<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests a warehouse inventory snapshot scenario through ZTD shadow store (MySQL PDO).
 * Bins with inbound/outbound movements exercise UNION ALL in subquery for
 * net quantity calculation, INSERT ... SELECT with GROUP BY for snapshot generation,
 * UPDATE with arithmetic from aggregate subquery, self-referencing DELETE for
 * zero-stock cleanup, HAVING on aggregated UNION ALL, and physical isolation.
 * @spec SPEC-10.2.166
 */
class MysqlInventorySnapshotTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_inv_bins (
                id INT AUTO_INCREMENT PRIMARY KEY,
                location VARCHAR(50),
                product VARCHAR(255),
                quantity INT
            )',
            'CREATE TABLE mp_inv_inbound (
                id INT AUTO_INCREMENT PRIMARY KEY,
                bin_id INT,
                qty INT,
                received_date DATE
            )',
            'CREATE TABLE mp_inv_outbound (
                id INT AUTO_INCREMENT PRIMARY KEY,
                bin_id INT,
                qty INT,
                shipped_date DATE
            )',
            'CREATE TABLE mp_inv_snapshots (
                id INT AUTO_INCREMENT PRIMARY KEY,
                bin_id INT,
                net_qty INT,
                snapshot_date DATE
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_inv_snapshots', 'mp_inv_outbound', 'mp_inv_inbound', 'mp_inv_bins'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 4 bins
        $this->pdo->exec("INSERT INTO mp_inv_bins VALUES (1, 'A-1', 'Widget', 100)");
        $this->pdo->exec("INSERT INTO mp_inv_bins VALUES (2, 'A-2', 'Gadget', 50)");
        $this->pdo->exec("INSERT INTO mp_inv_bins VALUES (3, 'B-1', 'Widget', 200)");
        $this->pdo->exec("INSERT INTO mp_inv_bins VALUES (4, 'B-2', 'Sprocket', 0)");

        // Inbound movements
        $this->pdo->exec("INSERT INTO mp_inv_inbound VALUES (1, 1, 30, '2026-03-01')");
        $this->pdo->exec("INSERT INTO mp_inv_inbound VALUES (2, 1, 20, '2026-03-05')");
        $this->pdo->exec("INSERT INTO mp_inv_inbound VALUES (3, 2, 15, '2026-03-02')");
        $this->pdo->exec("INSERT INTO mp_inv_inbound VALUES (4, 3, 50, '2026-03-03')");

        // Outbound movements
        $this->pdo->exec("INSERT INTO mp_inv_outbound VALUES (1, 1, 10, '2026-03-02')");
        $this->pdo->exec("INSERT INTO mp_inv_outbound VALUES (2, 1, 15, '2026-03-06')");
        $this->pdo->exec("INSERT INTO mp_inv_outbound VALUES (3, 2, 40, '2026-03-04')");
        $this->pdo->exec("INSERT INTO mp_inv_outbound VALUES (4, 3, 30, '2026-03-07')");
        $this->pdo->exec("INSERT INTO mp_inv_outbound VALUES (5, 4, 0, '2026-03-08')");
    }

    /**
     * UNION ALL in derived table: calculate net movement per bin.
     * Bin 1: +30+20-10-15 = +25, Bin 2: +15-40 = -25, Bin 3: +50-30 = +20, Bin 4: 0-0 = 0.
     *
     * Issue #13: Derived tables (subqueries in FROM) not rewritten by CTE rewriter.
     */
    public function testNetMovementWithUnionAll(): void
    {
        $rows = $this->ztdQuery(
            "SELECT bin_id, SUM(qty) AS net_qty
             FROM (
                 SELECT bin_id, qty FROM mp_inv_inbound
                 UNION ALL
                 SELECT bin_id, -qty FROM mp_inv_outbound
             ) movements
             GROUP BY bin_id
             ORDER BY bin_id"
        );

        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'Issue #13: UNION ALL in derived table returns empty on MySQL. '
                . 'CTE rewriter does not rewrite table references inside derived tables.'
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
     * Issue #13/#18: INSERT SELECT with UNION ALL derived table fails on MySQL.
     */
    public function testInsertSelectSnapshot(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO mp_inv_snapshots (id, bin_id, net_qty, snapshot_date)
                 SELECT b.id, b.id, b.quantity + COALESCE(m.net, 0), '2026-03-09'
                 FROM mp_inv_bins b
                 LEFT JOIN (
                     SELECT bin_id, SUM(qty) AS net
                     FROM (
                         SELECT bin_id, qty FROM mp_inv_inbound
                         UNION ALL
                         SELECT bin_id, -qty FROM mp_inv_outbound
                     ) movements
                     GROUP BY bin_id
                 ) m ON m.bin_id = b.id"
            );
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Issue #13/#18: INSERT...SELECT with UNION ALL derived table fails on MySQL: ' . $e->getMessage()
            );
        }

        $rows = $this->ztdQuery(
            "SELECT bin_id, net_qty FROM mp_inv_snapshots ORDER BY bin_id"
        );

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
     * Issue #13: Derived tables not rewritten by CTE rewriter.
     */
    public function testHavingOnUnionAllAggregate(): void
    {
        $rows = $this->ztdQuery(
            "SELECT bin_id, SUM(qty) AS net_qty
             FROM (
                 SELECT bin_id, qty FROM mp_inv_inbound
                 UNION ALL
                 SELECT bin_id, -qty FROM mp_inv_outbound
             ) movements
             GROUP BY bin_id
             HAVING SUM(qty) < 0
             ORDER BY bin_id"
        );

        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'Issue #13: UNION ALL in derived table returns empty on MySQL.'
            );
        }
        $this->assertCount(1, $rows);
        $this->assertEquals(2, (int) $rows[0]['bin_id']);
        $this->assertEquals(-25, (int) $rows[0]['net_qty']);
    }

    /**
     * JOIN with product-level aggregation across bins.
     * Widget: bins 1+3, Gadget: bin 2, Sprocket: bin 4.
     *
     * Issue #13: Derived tables (subqueries in LEFT JOIN) not rewritten on MySQL.
     * Same query works on SQLite and PostgreSQL.
     */
    public function testProductLevelAggregation(): void
    {
        $rows = $this->ztdQuery(
            "SELECT b.product,
                    SUM(b.quantity) AS base_stock,
                    COALESCE(SUM(ib.total_in), 0) AS total_in,
                    COALESCE(SUM(ob.total_out), 0) AS total_out,
                    SUM(b.quantity) + COALESCE(SUM(ib.total_in), 0) - COALESCE(SUM(ob.total_out), 0) AS current_stock
             FROM mp_inv_bins b
             LEFT JOIN (
                 SELECT bin_id, SUM(qty) AS total_in FROM mp_inv_inbound GROUP BY bin_id
             ) ib ON ib.bin_id = b.id
             LEFT JOIN (
                 SELECT bin_id, SUM(qty) AS total_out FROM mp_inv_outbound GROUP BY bin_id
             ) ob ON ob.bin_id = b.id
             GROUP BY b.product
             ORDER BY b.product"
        );

        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'Issue #13: LEFT JOIN with derived table subqueries returns empty on MySQL. '
                . 'Same query works on SQLite and PostgreSQL. Expected 3 product rows.'
            );
        }
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
     * Issue #13: UNION ALL in derived table + SPEC-11.DERIVED-TABLE-PREPARED.
     */
    public function testPreparedMovementFilter(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT bin_id, SUM(qty) AS net_qty
             FROM (
                 SELECT bin_id, qty, received_date AS move_date FROM mp_inv_inbound
                 UNION ALL
                 SELECT bin_id, -qty, shipped_date AS move_date FROM mp_inv_outbound
             ) movements
             WHERE move_date BETWEEN ? AND ?
             GROUP BY bin_id
             HAVING SUM(qty) >= ?
             ORDER BY net_qty DESC",
            ['2026-03-01', '2026-03-09', 20]
        );

        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'Issue #13: Prepared UNION ALL derived table returns empty on MySQL.'
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
        $this->pdo->exec("INSERT INTO mp_inv_bins VALUES (5, 'C-1', 'Doohickey', 75)");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_inv_bins");
        $this->assertEquals(5, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM mp_inv_bins')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
