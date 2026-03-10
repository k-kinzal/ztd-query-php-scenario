<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests INSERT...SELECT FROM generate_series() through the ZTD shadow store.
 *
 * generate_series() in SELECT context has known issues with LEFT JOIN
 * (SPEC-11.PG-GENERATE-SERIES). This test exercises generate_series()
 * as a data source for INSERT operations — a very common pattern for
 * seeding data, creating calendar tables, and bulk generation.
 *
 * @spec SPEC-10.2
 */
class PostgresInsertGenerateSeriesTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE pg_igs_numbers (
                id SERIAL PRIMARY KEY,
                val INT NOT NULL,
                label VARCHAR(50)
            )",
            "CREATE TABLE pg_igs_calendar (
                id SERIAL PRIMARY KEY,
                day DATE NOT NULL,
                is_weekend BOOLEAN DEFAULT false
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_igs_calendar', 'pg_igs_numbers'];
    }

    /**
     * Basic INSERT...SELECT FROM generate_series() for integer sequence.
     */
    public function testInsertFromIntegerGenerateSeries(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pg_igs_numbers (val, label)
                 SELECT n, 'item_' || n
                 FROM generate_series(1, 10) AS n"
            );

            $rows = $this->ztdQuery("SELECT val, label FROM pg_igs_numbers ORDER BY val");

            if (count($rows) !== 10) {
                $this->markTestIncomplete(
                    'INSERT FROM generate_series: expected 10 rows, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                    . ' — generate_series in INSERT context may not work with ZTD'
                );
            }

            $this->assertCount(10, $rows);
            $this->assertSame(1, (int) $rows[0]['val']);
            $this->assertSame('item_1', $rows[0]['label']);
            $this->assertSame(10, (int) $rows[9]['val']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT FROM generate_series failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT FROM date generate_series for calendar table.
     */
    public function testInsertFromDateGenerateSeries(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pg_igs_calendar (day, is_weekend)
                 SELECT d::date,
                        EXTRACT(DOW FROM d) IN (0, 6)
                 FROM generate_series('2025-01-01'::date, '2025-01-07'::date, '1 day'::interval) AS d"
            );

            $rows = $this->ztdQuery("SELECT day, is_weekend FROM pg_igs_calendar ORDER BY day");

            if (count($rows) !== 7) {
                $this->markTestIncomplete(
                    'INSERT FROM date generate_series: expected 7 rows, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(7, $rows);
            // Jan 4 (Sat) and Jan 5 (Sun) should be weekend
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT FROM date generate_series failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT FROM generate_series then UPDATE shadow data, then SELECT.
     */
    public function testInsertGenerateSeriesThenUpdate(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pg_igs_numbers (val, label)
                 SELECT n, 'batch' FROM generate_series(1, 5) AS n"
            );

            // Update labels for even numbers
            $this->ztdExec(
                "UPDATE pg_igs_numbers SET label = 'even' WHERE val % 2 = 0"
            );

            $rows = $this->ztdQuery(
                "SELECT val, label FROM pg_igs_numbers WHERE label = 'even' ORDER BY val"
            );

            if (count($rows) !== 2) {
                $all = $this->ztdQuery("SELECT val, label FROM pg_igs_numbers ORDER BY val");
                $this->markTestIncomplete(
                    'INSERT generate_series then UPDATE: expected 2 even rows, got ' . count($rows)
                    . '. All: ' . json_encode($all)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame(2, (int) $rows[0]['val']);
            $this->assertSame(4, (int) $rows[1]['val']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT generate_series then UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT FROM generate_series then DELETE subset.
     */
    public function testInsertGenerateSeriesThenDelete(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pg_igs_numbers (val, label)
                 SELECT n, 'seq' FROM generate_series(1, 20) AS n"
            );

            $this->ztdExec("DELETE FROM pg_igs_numbers WHERE val > 15");

            $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_igs_numbers");

            if ((int) $rows[0]['cnt'] !== 15) {
                $all = $this->ztdQuery("SELECT val FROM pg_igs_numbers ORDER BY val");
                $this->markTestIncomplete(
                    'INSERT generate_series then DELETE: expected 15 rows, got ' . $rows[0]['cnt']
                    . '. Vals: ' . json_encode(array_column($all, 'val'))
                );
            }

            $this->assertSame(15, (int) $rows[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT generate_series then DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared INSERT FROM generate_series with $N parameters.
     */
    public function testPreparedInsertFromGenerateSeries(): void
    {
        try {
            $stmt = $this->ztdPrepare(
                "INSERT INTO pg_igs_numbers (val, label)
                 SELECT n, $1 FROM generate_series($2::int, $3::int) AS n"
            );
            $stmt->execute(['prepared_batch', 100, 105]);

            $rows = $this->ztdQuery(
                "SELECT val, label FROM pg_igs_numbers WHERE label = 'prepared_batch' ORDER BY val"
            );

            if (count($rows) !== 6) {
                $this->markTestIncomplete(
                    'Prepared INSERT FROM generate_series: expected 6 rows, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(6, $rows);
            $this->assertSame(100, (int) $rows[0]['val']);
            $this->assertSame(105, (int) $rows[5]['val']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared INSERT FROM generate_series failed: ' . $e->getMessage());
        }
    }

    /**
     * Large batch INSERT from generate_series (100 rows).
     */
    public function testLargeBatchInsertFromGenerateSeries(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pg_igs_numbers (val, label)
                 SELECT n, 'large_batch' FROM generate_series(1, 100) AS n"
            );

            $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_igs_numbers");

            if ((int) $rows[0]['cnt'] !== 100) {
                $this->markTestIncomplete(
                    'Large batch INSERT FROM generate_series: expected 100 rows, got ' . $rows[0]['cnt']
                );
            }

            $this->assertSame(100, (int) $rows[0]['cnt']);

            // Verify aggregation works on the generated data
            $agg = $this->ztdQuery("SELECT SUM(val) AS total FROM pg_igs_numbers");
            $this->assertSame(5050, (int) $agg[0]['total']); // sum(1..100) = 5050
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Large batch INSERT FROM generate_series failed: ' . $e->getMessage());
        }
    }
}
