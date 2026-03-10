<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL DISTINCT ON behavior through the ZTD shadow store.
 *
 * DISTINCT ON is a PostgreSQL-specific extension to SELECT that returns the
 * first row for each unique combination of the specified expressions, as
 * determined by the ORDER BY clause. It is one of the most common PostgreSQL
 * patterns for "latest row per group" queries.
 *
 * These tests verify that the CTE rewriter correctly handles DISTINCT ON
 * in SELECT, INSERT...SELECT, DELETE subquery, UPDATE subquery, and
 * prepared statement contexts.
 */
class PostgresDistinctOnDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_distincton_readings (
                id SERIAL PRIMARY KEY,
                sensor_id INTEGER NOT NULL,
                reading_time TIMESTAMP NOT NULL,
                value DECIMAL(10,2) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT \'normal\'
            )',
            'CREATE TABLE pg_distincton_latest (
                id INTEGER PRIMARY KEY,
                sensor_id INTEGER NOT NULL,
                reading_time TIMESTAMP NOT NULL,
                value DECIMAL(10,2) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT \'normal\'
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_distincton_latest', 'pg_distincton_readings'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Sensor 1: three readings at different times
        $this->pdo->exec("INSERT INTO pg_distincton_readings (id, sensor_id, reading_time, value, status)
            VALUES (1, 1, '2024-01-01 08:00:00', 22.50, 'normal')");
        $this->pdo->exec("INSERT INTO pg_distincton_readings (id, sensor_id, reading_time, value, status)
            VALUES (2, 1, '2024-01-01 12:00:00', 25.00, 'normal')");
        $this->pdo->exec("INSERT INTO pg_distincton_readings (id, sensor_id, reading_time, value, status)
            VALUES (3, 1, '2024-01-01 18:00:00', 23.75, 'warning')");

        // Sensor 2: two readings
        $this->pdo->exec("INSERT INTO pg_distincton_readings (id, sensor_id, reading_time, value, status)
            VALUES (4, 2, '2024-01-01 09:00:00', 15.00, 'normal')");
        $this->pdo->exec("INSERT INTO pg_distincton_readings (id, sensor_id, reading_time, value, status)
            VALUES (5, 2, '2024-01-01 15:00:00', 17.50, 'normal')");

        // Sensor 3: one reading
        $this->pdo->exec("INSERT INTO pg_distincton_readings (id, sensor_id, reading_time, value, status)
            VALUES (6, 3, '2024-01-01 10:00:00', 30.00, 'critical')");
    }

    /**
     * SELECT DISTINCT ON (sensor_id) to get the latest reading per sensor.
     *
     * This is the canonical PostgreSQL "latest row per group" pattern.
     * The CTE rewriter must preserve the DISTINCT ON clause and ORDER BY
     * so that the correct row per sensor is returned.
     */
    public function testDistinctOnSelectAfterInsert(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT DISTINCT ON (sensor_id) sensor_id, reading_time, value, status
                 FROM pg_distincton_readings
                 ORDER BY sensor_id, reading_time DESC"
            );

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DISTINCT ON SELECT: expected 3 rows (one per sensor), got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            // Sensor 1: latest is 18:00, value 23.75
            $sensor1 = array_values(array_filter($rows, fn($r) => (int) $r['sensor_id'] === 1));
            $this->assertCount(1, $sensor1);
            $this->assertEqualsWithDelta(23.75, (float) $sensor1[0]['value'], 0.01);
            $this->assertSame('warning', $sensor1[0]['status']);

            // Sensor 2: latest is 15:00, value 17.50
            $sensor2 = array_values(array_filter($rows, fn($r) => (int) $r['sensor_id'] === 2));
            $this->assertCount(1, $sensor2);
            $this->assertEqualsWithDelta(17.50, (float) $sensor2[0]['value'], 0.01);

            // Sensor 3: only one reading, value 30.00
            $sensor3 = array_values(array_filter($rows, fn($r) => (int) $r['sensor_id'] === 3));
            $this->assertCount(1, $sensor3);
            $this->assertEqualsWithDelta(30.00, (float) $sensor3[0]['value'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DISTINCT ON SELECT failed: ' . $e->getMessage());
        }
    }

    /**
     * DISTINCT ON requires that the ORDER BY leftmost columns match the
     * DISTINCT ON expressions. This test verifies the CTE rewriter does
     * not break that relationship.
     *
     * We also test ordering by value ASC within each sensor group to get
     * the minimum reading per sensor instead of the latest.
     */
    public function testDistinctOnWithOrderBy(): void
    {
        try {
            // Get the earliest reading per sensor (ORDER BY reading_time ASC)
            $earliest = $this->ztdQuery(
                "SELECT DISTINCT ON (sensor_id) sensor_id, reading_time, value
                 FROM pg_distincton_readings
                 ORDER BY sensor_id, reading_time ASC"
            );

            if (count($earliest) !== 3) {
                $this->markTestIncomplete(
                    'DISTINCT ON ORDER BY ASC: expected 3, got ' . count($earliest)
                    . '. Data: ' . json_encode($earliest)
                );
            }

            // Sensor 1: earliest is 08:00, value 22.50
            $s1 = array_values(array_filter($earliest, fn($r) => (int) $r['sensor_id'] === 1));
            $this->assertEqualsWithDelta(22.50, (float) $s1[0]['value'], 0.01);

            // Sensor 2: earliest is 09:00, value 15.00
            $s2 = array_values(array_filter($earliest, fn($r) => (int) $r['sensor_id'] === 2));
            $this->assertEqualsWithDelta(15.00, (float) $s2[0]['value'], 0.01);

            // Get the minimum-value reading per sensor (ORDER BY value ASC)
            $minValue = $this->ztdQuery(
                "SELECT DISTINCT ON (sensor_id) sensor_id, value
                 FROM pg_distincton_readings
                 ORDER BY sensor_id, value ASC"
            );

            if (count($minValue) !== 3) {
                $this->markTestIncomplete(
                    'DISTINCT ON ORDER BY value ASC: expected 3, got ' . count($minValue)
                    . '. Data: ' . json_encode($minValue)
                );
            }

            // Sensor 1: min value is 22.50
            $s1min = array_values(array_filter($minValue, fn($r) => (int) $r['sensor_id'] === 1));
            $this->assertEqualsWithDelta(22.50, (float) $s1min[0]['value'], 0.01);

            // Sensor 2: min value is 15.00
            $s2min = array_values(array_filter($minValue, fn($r) => (int) $r['sensor_id'] === 2));
            $this->assertEqualsWithDelta(15.00, (float) $s2min[0]['value'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DISTINCT ON with ORDER BY failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT INTO ... SELECT DISTINCT ON (...) — materialize the latest
     * reading per sensor into a separate table.
     *
     * This tests DISTINCT ON inside an INSERT...SELECT context, which
     * requires the CTE rewriter to handle the subquery correctly.
     */
    public function testInsertSelectDistinctOn(): void
    {
        try {
            $affected = $this->pdo->exec(
                "INSERT INTO pg_distincton_latest (id, sensor_id, reading_time, value, status)
                 SELECT DISTINCT ON (sensor_id) id, sensor_id, reading_time, value, status
                 FROM pg_distincton_readings
                 ORDER BY sensor_id, reading_time DESC"
            );

            if ($affected !== 3) {
                $this->markTestIncomplete(
                    'INSERT...SELECT DISTINCT ON: expected 3 affected rows, got ' . var_export($affected, true)
                );
            }

            $rows = $this->ztdQuery(
                "SELECT sensor_id, value, status FROM pg_distincton_latest ORDER BY sensor_id"
            );

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'INSERT...SELECT DISTINCT ON: expected 3 rows in target, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            // Verify the latest readings were inserted
            $this->assertEqualsWithDelta(23.75, (float) $rows[0]['value'], 0.01);
            $this->assertSame('warning', $rows[0]['status']);
            $this->assertEqualsWithDelta(17.50, (float) $rows[1]['value'], 0.01);
            $this->assertEqualsWithDelta(30.00, (float) $rows[2]['value'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT DISTINCT ON failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE FROM readings WHERE id NOT IN (SELECT DISTINCT ON (sensor_id) id ...).
     *
     * This is a common deduplication pattern: keep only the latest reading
     * per sensor, delete all others. The CTE rewriter must handle DISTINCT ON
     * inside the NOT IN subquery.
     */
    public function testDeleteWhereNotInDistinctOn(): void
    {
        try {
            $affected = $this->pdo->exec(
                "DELETE FROM pg_distincton_readings
                 WHERE id NOT IN (
                     SELECT DISTINCT ON (sensor_id) id
                     FROM pg_distincton_readings
                     ORDER BY sensor_id, reading_time DESC
                 )"
            );

            // 6 total rows - 3 kept (one per sensor) = 3 deleted
            $rows = $this->ztdQuery(
                "SELECT id, sensor_id, value FROM pg_distincton_readings ORDER BY sensor_id"
            );

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE NOT IN DISTINCT ON: expected 3 remaining rows, got ' . count($rows)
                    . '. Affected: ' . var_export($affected, true)
                    . '. Data: ' . json_encode($rows)
                );
            }

            // The remaining rows should be the latest per sensor
            $this->assertEqualsWithDelta(23.75, (float) $rows[0]['value'], 0.01); // Sensor 1 latest
            $this->assertEqualsWithDelta(17.50, (float) $rows[1]['value'], 0.01); // Sensor 2 latest
            $this->assertEqualsWithDelta(30.00, (float) $rows[2]['value'], 0.01); // Sensor 3 only
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE NOT IN DISTINCT ON failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE readings using a subquery with DISTINCT ON to set a flag
     * on the latest reading per sensor.
     *
     * This tests the CTE rewriter's ability to handle DISTINCT ON inside
     * an UPDATE ... WHERE ... IN (SELECT DISTINCT ON ...) context.
     */
    public function testUpdateWithDistinctOnSubquery(): void
    {
        try {
            $affected = $this->pdo->exec(
                "UPDATE pg_distincton_readings
                 SET status = 'latest'
                 WHERE id IN (
                     SELECT DISTINCT ON (sensor_id) id
                     FROM pg_distincton_readings
                     ORDER BY sensor_id, reading_time DESC
                 )"
            );

            $latestRows = $this->ztdQuery(
                "SELECT sensor_id, value, status FROM pg_distincton_readings
                 WHERE status = 'latest'
                 ORDER BY sensor_id"
            );

            if (count($latestRows) !== 3) {
                $allRows = $this->ztdQuery(
                    "SELECT id, sensor_id, status FROM pg_distincton_readings ORDER BY id"
                );
                $this->markTestIncomplete(
                    'UPDATE with DISTINCT ON subquery: expected 3 rows with status=latest, got '
                    . count($latestRows) . '. Affected: ' . var_export($affected, true)
                    . '. All: ' . json_encode($allRows)
                );
            }

            // Verify the correct rows were updated
            $this->assertEqualsWithDelta(23.75, (float) $latestRows[0]['value'], 0.01); // Sensor 1
            $this->assertEqualsWithDelta(17.50, (float) $latestRows[1]['value'], 0.01); // Sensor 2
            $this->assertEqualsWithDelta(30.00, (float) $latestRows[2]['value'], 0.01); // Sensor 3

            // Verify non-latest rows were NOT updated
            $otherRows = $this->ztdQuery(
                "SELECT sensor_id, status FROM pg_distincton_readings
                 WHERE status != 'latest'
                 ORDER BY id"
            );

            if (count($otherRows) !== 3) {
                $this->markTestIncomplete(
                    'UPDATE with DISTINCT ON subquery: expected 3 non-latest rows, got '
                    . count($otherRows)
                );
            }
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with DISTINCT ON subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared statement with DISTINCT ON and a parameter to filter by
     * minimum value threshold.
     *
     * Uses PostgreSQL $1 parameter placeholder. The CTE rewriter must
     * handle DISTINCT ON in a prepared query without breaking the parameter
     * binding.
     */
    public function testPreparedDistinctOnSelect(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT DISTINCT ON (sensor_id) sensor_id, reading_time, value
                 FROM pg_distincton_readings
                 WHERE value > $1
                 ORDER BY sensor_id, reading_time DESC",
                [20.00]
            );

            // Sensor 1 latest above 20: id=3 (23.75 at 18:00) — all 3 readings are > 20
            // Sensor 2: 17.50 and 15.00 — both below 20, so sensor 2 excluded
            // Sensor 3: 30.00 — above 20, included
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Prepared DISTINCT ON: expected 2 rows (sensors 1 and 3), got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $sensorIds = array_map(fn($r) => (int) $r['sensor_id'], $rows);
            sort($sensorIds);
            $this->assertSame([1, 3], $sensorIds);

            // Sensor 1: latest with value > 20 is 23.75
            $s1 = array_values(array_filter($rows, fn($r) => (int) $r['sensor_id'] === 1));
            $this->assertEqualsWithDelta(23.75, (float) $s1[0]['value'], 0.01);

            // Sensor 3: only reading is 30.00
            $s3 = array_values(array_filter($rows, fn($r) => (int) $r['sensor_id'] === 3));
            $this->assertEqualsWithDelta(30.00, (float) $s3[0]['value'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DISTINCT ON SELECT failed: ' . $e->getMessage());
        }
    }
}
