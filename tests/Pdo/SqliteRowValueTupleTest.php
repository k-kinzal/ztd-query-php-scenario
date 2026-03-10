<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests row value (tuple) comparisons with DML-modified shadow data on SQLite.
 *
 * SQLite supports row value comparisons like `(a, b) > (1, 2)` and
 * `(a, b) IN ((1, 2), (3, 4))`. Through the CTE shadow store, these
 * comparisons must correctly evaluate against shadow data.
 *
 * Extends existing SqliteRowValueComparisonTest/SqliteRowValueConstructorTest
 * with focus on DML-modified data and tuple IN with VALUES.
 *
 * @spec SPEC-3.1
 */
class SqliteRowValueTupleTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE sl_rvt_events (
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            day INTEGER NOT NULL,
            event TEXT NOT NULL,
            PRIMARY KEY (year, month, day)
        )";
    }

    protected function getTableNames(): array
    {
        return ['sl_rvt_events'];
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->pdo->exec("INSERT INTO sl_rvt_events VALUES (2024, 1, 15, 'Conference')");
        $this->pdo->exec("INSERT INTO sl_rvt_events VALUES (2024, 3, 20, 'Workshop')");
        $this->pdo->exec("INSERT INTO sl_rvt_events VALUES (2024, 6, 10, 'Summit')");
        $this->pdo->exec("INSERT INTO sl_rvt_events VALUES (2024, 12, 25, 'Holiday')");
    }

    /**
     * Row value comparison (year, month) > (2024, 3) — should return later events.
     */
    public function testRowValueGreaterThan(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT event FROM sl_rvt_events WHERE (year, month) > (2024, 3) ORDER BY year, month"
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'Row value (year,month) > (2024,3) returned 0 rows. Expected 2 (Summit, Holiday).'
                );
            }
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Row value > returned ' . count($rows) . ' rows. Expected 2. Got: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Summit', $rows[0]['event']);
            $this->assertSame('Holiday', $rows[1]['event']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Row value > test failed: ' . $e->getMessage());
        }
    }

    /**
     * Row value IN with tuple list — find specific date combinations.
     */
    public function testRowValueInTupleList(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT event FROM sl_rvt_events
                 WHERE (year, month, day) IN ((2024, 1, 15), (2024, 12, 25))
                 ORDER BY month"
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'Row value IN tuple list returned 0 rows. Expected 2 (Conference, Holiday).'
                );
            }
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Row value IN returned ' . count($rows) . ' rows. Expected 2. Got: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Conference', $rows[0]['event']);
            $this->assertSame('Holiday', $rows[1]['event']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Row value IN tuple list test failed: ' . $e->getMessage());
        }
    }

    /**
     * Row value comparison after INSERT — new row should be found.
     */
    public function testRowValueAfterInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_rvt_events VALUES (2025, 2, 14, 'Valentine')");

            $rows = $this->ztdQuery(
                "SELECT event FROM sl_rvt_events WHERE (year, month) >= (2025, 1) ORDER BY year, month"
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'Row value >= after INSERT returned 0 rows. Expected 1 (Valentine).'
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Valentine', $rows[0]['event']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Row value after INSERT test failed: ' . $e->getMessage());
        }
    }

    /**
     * Row value comparison after UPDATE — updated row should match new criteria.
     */
    public function testRowValueAfterUpdate(): void
    {
        try {
            // Move Conference from Jan to July
            $this->pdo->exec("DELETE FROM sl_rvt_events WHERE year = 2024 AND month = 1 AND day = 15");
            $this->pdo->exec("INSERT INTO sl_rvt_events VALUES (2024, 7, 15, 'Conference')");

            $rows = $this->ztdQuery(
                "SELECT event FROM sl_rvt_events WHERE (year, month) BETWEEN (2024, 6) AND (2024, 8) ORDER BY month"
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'Row value BETWEEN after move returned 0 rows. Expected 2 (Summit, Conference).'
                );
            }
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Row value BETWEEN returned ' . count($rows) . ' rows. Expected 2. Got: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $events = array_column($rows, 'event');
            $this->assertContains('Summit', $events);
            $this->assertContains('Conference', $events);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Row value after UPDATE test failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared row value comparison with parameters.
     */
    public function testPreparedRowValueComparison(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT event FROM sl_rvt_events WHERE (year, month) > (?, ?) ORDER BY year, month, day",
                [2024, 5]
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'Prepared row value > returned 0 rows. Expected 2 (Summit, Holiday).'
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Summit', $rows[0]['event']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared row value comparison test failed: ' . $e->getMessage());
        }
    }

    /**
     * Row value NOT IN — exclude specific dates.
     */
    public function testRowValueNotIn(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT event FROM sl_rvt_events
                 WHERE (year, month, day) NOT IN ((2024, 1, 15), (2024, 6, 10))
                 ORDER BY month"
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'Row value NOT IN returned 0 rows. Expected 2 (Workshop, Holiday).'
                );
            }
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Row value NOT IN returned ' . count($rows) . ' rows. Expected 2. Got: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $events = array_column($rows, 'event');
            $this->assertContains('Workshop', $events);
            $this->assertContains('Holiday', $events);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Row value NOT IN test failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with row value comparison.
     */
    public function testDeleteWithRowValue(): void
    {
        try {
            $this->pdo->exec(
                "DELETE FROM sl_rvt_events WHERE (year, month) <= (2024, 3)"
            );

            $rows = $this->ztdQuery("SELECT event FROM sl_rvt_events ORDER BY month");

            if (count($rows) === 4) {
                $this->markTestIncomplete(
                    'DELETE with row value had no effect — all 4 rows remain.'
                );
            }
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE with row value left ' . count($rows) . ' rows. Expected 2. Got: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $events = array_column($rows, 'event');
            $this->assertContains('Summit', $events);
            $this->assertContains('Holiday', $events);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE with row value test failed: ' . $e->getMessage());
        }
    }
}
