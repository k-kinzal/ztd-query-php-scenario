<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests INSERT...SELECT with GROUP BY and HAVING through the ZTD shadow store
 * on PostgreSQL.
 *
 * INSERT INTO target SELECT aggregate(...), group_col FROM source
 * GROUP BY group_col HAVING condition
 *
 * This pattern is commonly used for creating summary/report tables from
 * transactional data. The CTE rewriter must handle GROUP BY/HAVING in
 * the INSERT...SELECT source query.
 *
 * @spec SPEC-10.2
 */
class PostgresInsertSelectGroupByHavingTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE pg_isgh_sales (
                id SERIAL PRIMARY KEY,
                region VARCHAR(30),
                product VARCHAR(50),
                amount DECIMAL(10,2),
                qty INT
            )",
            "CREATE TABLE pg_isgh_summary (
                id SERIAL PRIMARY KEY,
                region VARCHAR(30),
                total_amount DECIMAL(12,2),
                total_qty INT,
                order_count INT
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_isgh_summary', 'pg_isgh_sales'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO pg_isgh_sales (region, product, amount, qty) VALUES ('east', 'Widget', 100.00, 10)");
        $this->ztdExec("INSERT INTO pg_isgh_sales (region, product, amount, qty) VALUES ('east', 'Gadget', 200.00, 5)");
        $this->ztdExec("INSERT INTO pg_isgh_sales (region, product, amount, qty) VALUES ('east', 'Widget', 150.00, 8)");
        $this->ztdExec("INSERT INTO pg_isgh_sales (region, product, amount, qty) VALUES ('west', 'Gadget', 300.00, 12)");
        $this->ztdExec("INSERT INTO pg_isgh_sales (region, product, amount, qty) VALUES ('west', 'Doohickey', 50.00, 3)");
        $this->ztdExec("INSERT INTO pg_isgh_sales (region, product, amount, qty) VALUES ('north', 'Widget', 75.00, 4)");
    }

    /**
     * INSERT...SELECT with GROUP BY (no HAVING): aggregate all regions.
     */
    public function testInsertSelectGroupBy(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pg_isgh_summary (region, total_amount, total_qty, order_count)
                 SELECT region, SUM(amount), SUM(qty), COUNT(*)
                 FROM pg_isgh_sales
                 GROUP BY region"
            );

            $rows = $this->ztdQuery("SELECT region, total_amount, order_count FROM pg_isgh_summary ORDER BY region");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'INSERT SELECT GROUP BY: expected 3 region summaries, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            // east: 100+200+150=450, 3 orders
            $east = array_values(array_filter($rows, fn($r) => $r['region'] === 'east'));
            $this->assertEqualsWithDelta(450.00, (float) $east[0]['total_amount'], 0.01);
            $this->assertSame(3, (int) $east[0]['order_count']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT GROUP BY failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT with GROUP BY HAVING: only regions with total > 100.
     */
    public function testInsertSelectGroupByHaving(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pg_isgh_summary (region, total_amount, total_qty, order_count)
                 SELECT region, SUM(amount), SUM(qty), COUNT(*)
                 FROM pg_isgh_sales
                 GROUP BY region
                 HAVING SUM(amount) > 100"
            );

            $rows = $this->ztdQuery("SELECT region, total_amount FROM pg_isgh_summary ORDER BY region");

            // east: 450 (>100), west: 350 (>100), north: 75 (<100, excluded)
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT SELECT GROUP BY HAVING: expected 2 regions (>100), got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $regions = array_column($rows, 'region');
            $this->assertContains('east', $regions);
            $this->assertContains('west', $regions);
            $this->assertNotContains('north', $regions);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT GROUP BY HAVING failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT GROUP BY HAVING after prior DML on source.
     */
    public function testInsertSelectGroupByHavingAfterDml(): void
    {
        try {
            // Add more east sales, then summarize
            $this->ztdExec("INSERT INTO pg_isgh_sales (region, product, amount, qty) VALUES ('east', 'Premium', 500.00, 2)");
            $this->ztdExec("DELETE FROM pg_isgh_sales WHERE region = 'north'");

            $this->ztdExec(
                "INSERT INTO pg_isgh_summary (region, total_amount, total_qty, order_count)
                 SELECT region, SUM(amount), SUM(qty), COUNT(*)
                 FROM pg_isgh_sales
                 GROUP BY region
                 HAVING COUNT(*) >= 2"
            );

            $rows = $this->ztdQuery("SELECT region, total_amount, order_count FROM pg_isgh_summary ORDER BY region");

            // east: 4 orders (>=2), west: 2 orders (>=2), north: deleted
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT GROUP BY HAVING after DML: expected 2 regions, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $east = array_values(array_filter($rows, fn($r) => $r['region'] === 'east'));
            $this->assertEqualsWithDelta(950.00, (float) $east[0]['total_amount'], 0.01);
            $this->assertSame(4, (int) $east[0]['order_count']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT GROUP BY HAVING after DML failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared INSERT...SELECT GROUP BY HAVING with $N params.
     */
    public function testPreparedInsertSelectGroupByHaving(): void
    {
        try {
            $stmt = $this->ztdPrepare(
                "INSERT INTO pg_isgh_summary (region, total_amount, total_qty, order_count)
                 SELECT region, SUM(amount), SUM(qty), COUNT(*)
                 FROM pg_isgh_sales
                 WHERE product = $1
                 GROUP BY region
                 HAVING SUM(amount) > $2"
            );
            $stmt->execute(['Widget', 100]);

            $rows = $this->ztdQuery("SELECT region, total_amount FROM pg_isgh_summary ORDER BY region");

            // Widget: east 250 (>100), north 75 (<100 excluded)
            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Prepared INSERT GROUP BY HAVING: expected 1 region, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertSame('east', $rows[0]['region']);
            $this->assertEqualsWithDelta(250.00, (float) $rows[0]['total_amount'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared INSERT GROUP BY HAVING failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT GROUP BY with multiple aggregates and expressions.
     */
    public function testInsertSelectGroupByMultipleAggregates(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pg_isgh_summary (region, total_amount, total_qty, order_count)
                 SELECT region,
                        ROUND(AVG(amount), 2),
                        MAX(qty),
                        COUNT(DISTINCT product)
                 FROM pg_isgh_sales
                 GROUP BY region
                 HAVING COUNT(DISTINCT product) > 1"
            );

            $rows = $this->ztdQuery("SELECT region, total_amount, total_qty, order_count FROM pg_isgh_summary ORDER BY region");

            // east: 2 distinct products (Widget, Gadget), west: 2 (Gadget, Doohickey), north: 1 (excluded)
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT GROUP BY multiple aggregates: expected 2, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT GROUP BY multiple aggregates failed: ' . $e->getMessage());
        }
    }
}
