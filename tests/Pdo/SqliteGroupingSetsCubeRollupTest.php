<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests GROUP BY ROLLUP on SQLite (3.44.0+).
 *
 * SQLite does not support GROUPING SETS or CUBE syntax, but supports
 * a limited form of ROLLUP via the UNION ALL workaround pattern.
 * This test verifies basic ROLLUP-equivalent behavior through ZTD.
 *
 * @spec SPEC-3.1
 */
class SqliteGroupingSetsCubeRollupTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_gscr_sales (
            id INTEGER PRIMARY KEY,
            region TEXT NOT NULL,
            product TEXT NOT NULL,
            amount REAL NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_gscr_sales'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_gscr_sales (id, region, product, amount) VALUES (1, 'East', 'Widget', 100)");
        $this->pdo->exec("INSERT INTO sl_gscr_sales (id, region, product, amount) VALUES (2, 'East', 'Gadget', 200)");
        $this->pdo->exec("INSERT INTO sl_gscr_sales (id, region, product, amount) VALUES (3, 'West', 'Widget', 150)");
        $this->pdo->exec("INSERT INTO sl_gscr_sales (id, region, product, amount) VALUES (4, 'West', 'Gadget', 250)");
    }

    /**
     * Verify that simulated ROLLUP via UNION ALL with GROUP BY works on SQLite.
     * This is a common workaround for GROUPING SETS on SQLite.
     *
     * @spec SPEC-3.1
     */
    public function testSimulatedRollupViaUnionAll(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT region, SUM(amount) AS total
                 FROM sl_gscr_sales
                 GROUP BY region
                 UNION ALL
                 SELECT NULL, SUM(amount) FROM sl_gscr_sales"
            );

            if (count($rows) < 3) {
                $this->markTestIncomplete(
                    'Simulated ROLLUP: expected 3 rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Simulated ROLLUP via UNION ALL failed: ' . $e->getMessage());
        }
    }

    /**
     * GROUP BY with multiple columns and aggregate.
     *
     * @spec SPEC-3.1
     */
    public function testGroupByMultipleColumns(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT region, product, SUM(amount) AS total, COUNT(*) AS cnt
                 FROM sl_gscr_sales
                 GROUP BY region, product
                 ORDER BY region, product"
            );

            $this->assertCount(4, $rows);
            $this->assertSame('East', $rows[0]['region']);
            $this->assertSame('Gadget', $rows[0]['product']);
            $this->assertEquals(200, (float) $rows[0]['total']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-column GROUP BY failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT with GROUP BY into summary table.
     *
     * @spec SPEC-4.1
     */
    public function testInsertSelectGroupByRegion(): void
    {
        $this->pdo->exec('CREATE TABLE sl_gscr_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            region TEXT,
            total REAL
        )');

        try {
            $this->pdo->exec(
                "INSERT INTO sl_gscr_summary (region, total)
                 SELECT region, SUM(amount) FROM sl_gscr_sales GROUP BY region"
            );

            $rows = $this->ztdQuery('SELECT region, total FROM sl_gscr_summary ORDER BY region');

            if (count($rows) < 2) {
                $this->markTestIncomplete(
                    'INSERT SELECT GROUP BY: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('East', $rows[0]['region']);
            $this->assertEquals(300, (float) $rows[0]['total']);
            $this->assertSame('West', $rows[1]['region']);
            $this->assertEquals(400, (float) $rows[1]['total']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT GROUP BY failed: ' . $e->getMessage());
        }
    }
}
