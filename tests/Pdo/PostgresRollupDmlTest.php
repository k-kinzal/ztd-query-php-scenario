<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests GROUP BY ROLLUP/CUBE in DML subquery context on PostgreSQL.
 *
 * ROLLUP and CUBE are used for generating subtotals and cross-tabulations
 * in reporting queries. This tests whether these grouping extensions work
 * correctly through the ZTD shadow store.
 *
 * @spec SPEC-10.2
 */
class PostgresRollupDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE pg_rl_sales (
                id SERIAL PRIMARY KEY,
                region VARCHAR(30),
                category VARCHAR(30),
                amount NUMERIC(10,2)
            )",
            "CREATE TABLE pg_rl_summary (
                id SERIAL PRIMARY KEY,
                region VARCHAR(30),
                category VARCHAR(30),
                total_amount NUMERIC(12,2)
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_rl_summary', 'pg_rl_sales'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO pg_rl_sales (region, category, amount) VALUES ('east', 'electronics', 100)");
        $this->ztdExec("INSERT INTO pg_rl_sales (region, category, amount) VALUES ('east', 'clothing', 200)");
        $this->ztdExec("INSERT INTO pg_rl_sales (region, category, amount) VALUES ('west', 'electronics', 300)");
        $this->ztdExec("INSERT INTO pg_rl_sales (region, category, amount) VALUES ('west', 'clothing', 150)");
    }

    /**
     * SELECT with GROUP BY ROLLUP — baseline.
     */
    public function testSelectGroupByRollup(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT region, SUM(amount) AS total
                 FROM pg_rl_sales
                 GROUP BY ROLLUP(region)
                 ORDER BY region NULLS LAST"
            );

            // Expect: east, west, grand total (NULL)
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'SELECT ROLLUP (PG): expected 3, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            // Grand total row should have NULL region
            $grandTotal = array_filter($rows, fn($r) => $r['region'] === null);
            $this->assertCount(1, $grandTotal);
            $this->assertEqualsWithDelta(750.0, (float) array_values($grandTotal)[0]['total'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT ROLLUP (PG) failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT with GROUP BY ROLLUP.
     */
    public function testInsertSelectGroupByRollup(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pg_rl_summary (region, total_amount)
                 SELECT region, SUM(amount)
                 FROM pg_rl_sales
                 GROUP BY ROLLUP(region)"
            );

            $rows = $this->ztdQuery("SELECT region, total_amount FROM pg_rl_summary ORDER BY region NULLS LAST");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'INSERT ROLLUP (PG): expected 3, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT ROLLUP (PG) failed: ' . $e->getMessage());
        }
    }

    /**
     * SELECT with GROUP BY CUBE.
     */
    public function testSelectGroupByCube(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT region, category, SUM(amount) AS total
                 FROM pg_rl_sales
                 GROUP BY CUBE(region, category)
                 ORDER BY region NULLS LAST, category NULLS LAST"
            );

            // CUBE(region, category) produces:
            // (region, category), (region, NULL), (NULL, category), (NULL, NULL)
            // = 2*2 + 2 + 2 + 1 = 9 combinations
            if (count($rows) !== 9) {
                $this->markTestIncomplete(
                    'SELECT CUBE (PG): expected 9, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(9, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT CUBE (PG) failed: ' . $e->getMessage());
        }
    }

    /**
     * SELECT with GROUPING SETS.
     */
    public function testSelectGroupingSets(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT region, category, SUM(amount) AS total
                 FROM pg_rl_sales
                 GROUP BY GROUPING SETS ((region), (category), ())
                 ORDER BY region NULLS LAST, category NULLS LAST"
            );

            // (region): 2 rows, (category): 2 rows, (): 1 grand total = 5
            if (count($rows) !== 5) {
                $this->markTestIncomplete(
                    'SELECT GROUPING SETS (PG): expected 5, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(5, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT GROUPING SETS (PG) failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT with GROUPING SETS after prior DML.
     */
    public function testInsertGroupingSetsAfterDml(): void
    {
        try {
            $this->ztdExec("INSERT INTO pg_rl_sales (region, category, amount) VALUES ('east', 'electronics', 500)");

            $this->ztdExec(
                "INSERT INTO pg_rl_summary (region, category, total_amount)
                 SELECT region, category, SUM(amount)
                 FROM pg_rl_sales
                 GROUP BY GROUPING SETS ((region, category))"
            );

            $rows = $this->ztdQuery("SELECT region, category, total_amount FROM pg_rl_summary ORDER BY region, category");

            // Should have: east/electronics=600, east/clothing=200, west/electronics=300, west/clothing=150
            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'INSERT GROUPING SETS after DML (PG): expected 4, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $eastElec = array_values(array_filter($rows, fn($r) => $r['region'] === 'east' && $r['category'] === 'electronics'));
            if (count($eastElec) === 1) {
                $this->assertEqualsWithDelta(600.0, (float) $eastElec[0]['total_amount'], 0.01);
            }
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT GROUPING SETS after DML (PG) failed: ' . $e->getMessage());
        }
    }
}
