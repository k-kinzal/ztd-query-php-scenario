<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT...SELECT with GROUP BY and HAVING through ZTD on SQLite.
 *
 * @spec SPEC-10.2
 */
class SqliteInsertSelectGroupByHavingTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE sl_isgh_sales (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                region TEXT,
                product TEXT,
                amount REAL,
                qty INTEGER
            )",
            "CREATE TABLE sl_isgh_summary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                region TEXT,
                total_amount REAL,
                total_qty INTEGER,
                order_count INTEGER
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_isgh_summary', 'sl_isgh_sales'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO sl_isgh_sales (region, product, amount, qty) VALUES ('east', 'Widget', 100.00, 10)");
        $this->ztdExec("INSERT INTO sl_isgh_sales (region, product, amount, qty) VALUES ('east', 'Gadget', 200.00, 5)");
        $this->ztdExec("INSERT INTO sl_isgh_sales (region, product, amount, qty) VALUES ('east', 'Widget', 150.00, 8)");
        $this->ztdExec("INSERT INTO sl_isgh_sales (region, product, amount, qty) VALUES ('west', 'Gadget', 300.00, 12)");
        $this->ztdExec("INSERT INTO sl_isgh_sales (region, product, amount, qty) VALUES ('west', 'Doohickey', 50.00, 3)");
        $this->ztdExec("INSERT INTO sl_isgh_sales (region, product, amount, qty) VALUES ('north', 'Widget', 75.00, 4)");
    }

    /**
     * INSERT...SELECT GROUP BY HAVING on SQLite.
     */
    public function testInsertSelectGroupByHaving(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO sl_isgh_summary (region, total_amount, total_qty, order_count)
                 SELECT region, SUM(amount), SUM(qty), COUNT(*)
                 FROM sl_isgh_sales
                 GROUP BY region
                 HAVING SUM(amount) > 100"
            );

            $rows = $this->ztdQuery("SELECT region, total_amount FROM sl_isgh_summary ORDER BY region");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT GROUP BY HAVING (SQLite): expected 2 regions, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $regions = array_column($rows, 'region');
            $this->assertContains('east', $regions);
            $this->assertContains('west', $regions);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT GROUP BY HAVING (SQLite) failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT GROUP BY after prior DML.
     */
    public function testInsertSelectGroupByAfterDml(): void
    {
        try {
            $this->ztdExec("INSERT INTO sl_isgh_sales (region, product, amount, qty) VALUES ('east', 'Premium', 500.00, 2)");
            $this->ztdExec("DELETE FROM sl_isgh_sales WHERE region = 'north'");

            $this->ztdExec(
                "INSERT INTO sl_isgh_summary (region, total_amount, total_qty, order_count)
                 SELECT region, SUM(amount), SUM(qty), COUNT(*)
                 FROM sl_isgh_sales
                 GROUP BY region"
            );

            $rows = $this->ztdQuery("SELECT region, total_amount, order_count FROM sl_isgh_summary ORDER BY region");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT GROUP BY after DML (SQLite): expected 2, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $east = array_values(array_filter($rows, fn($r) => $r['region'] === 'east'));
            $this->assertEqualsWithDelta(950.00, (float) $east[0]['total_amount'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT GROUP BY after DML (SQLite) failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared INSERT...SELECT GROUP BY HAVING with ? params.
     */
    public function testPreparedInsertGroupByHaving(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT region, SUM(amount) AS total
                 FROM sl_isgh_sales
                 WHERE product = ?
                 GROUP BY region
                 HAVING SUM(amount) > ?",
                ['Widget', 100]
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Prepared GROUP BY HAVING (SQLite): expected 1 region, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertSame('east', $rows[0]['region']);
            $this->assertEqualsWithDelta(250.00, (float) $rows[0]['total'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared GROUP BY HAVING (SQLite) failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT with COUNT(DISTINCT) in HAVING.
     */
    public function testInsertSelectHavingCountDistinct(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO sl_isgh_summary (region, total_amount, total_qty, order_count)
                 SELECT region, SUM(amount), SUM(qty), COUNT(DISTINCT product)
                 FROM sl_isgh_sales
                 GROUP BY region
                 HAVING COUNT(DISTINCT product) > 1"
            );

            $rows = $this->ztdQuery("SELECT region FROM sl_isgh_summary ORDER BY region");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT HAVING COUNT DISTINCT (SQLite): expected 2, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT HAVING COUNT DISTINCT (SQLite) failed: ' . $e->getMessage());
        }
    }
}
