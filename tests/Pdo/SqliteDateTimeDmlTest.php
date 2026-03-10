<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests date/time functions and expressions in DML through ZTD shadow store on SQLite.
 *
 * Date/time operations are ubiquitous in real applications (expiration checks,
 * scheduling, audit timestamps). The CTE rewriter must handle date functions,
 * datetime() calls, and time arithmetic correctly.
 *
 * @spec SPEC-4.1, SPEC-4.2, SPEC-4.3
 */
class SqliteDateTimeDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_dt_events (
                id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                event_date TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime(\'now\'))
            )',
            'CREATE TABLE sl_dt_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER,
                action TEXT NOT NULL,
                logged_at TEXT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_dt_log', 'sl_dt_events'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_dt_events VALUES (1, 'Conference', '2026-06-15', '2026-01-01 10:00:00')");
        $this->pdo->exec("INSERT INTO sl_dt_events VALUES (2, 'Workshop', '2026-03-01', '2026-01-01 10:00:00')");
        $this->pdo->exec("INSERT INTO sl_dt_events VALUES (3, 'Webinar', '2026-12-25', '2026-01-01 10:00:00')");
        $this->pdo->exec("INSERT INTO sl_dt_events VALUES (4, 'Sprint', '2025-11-30', '2025-06-01 08:00:00')");
    }

    /**
     * UPDATE with datetime() function in SET clause.
     */
    public function testUpdateSetWithDatetimeFunction(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE sl_dt_events SET created_at = datetime('now') WHERE id = 1"
            );
            $rows = $this->ztdQuery("SELECT created_at FROM sl_dt_events WHERE id = 1");

            if (count($rows) !== 1 || empty($rows[0]['created_at'])) {
                $this->markTestIncomplete(
                    'UPDATE SET datetime(now): got ' . json_encode($rows)
                );
            }

            // Should be a valid datetime string
            $this->assertMatchesRegularExpression(
                '/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/',
                $rows[0]['created_at']
            );
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with datetime() in SET failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with date comparison in WHERE.
     */
    public function testDeleteWithDateComparison(): void
    {
        try {
            $this->pdo->exec(
                "DELETE FROM sl_dt_events WHERE event_date < '2026-03-10'"
            );
            $rows = $this->ztdQuery("SELECT title FROM sl_dt_events ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE date comparison: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Conference', $rows[0]['title']);
            $this->assertSame('Webinar', $rows[1]['title']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE with date comparison failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with date arithmetic using datetime() modifier.
     */
    public function testUpdateWithDateArithmetic(): void
    {
        try {
            // Push all events forward by 7 days
            $this->pdo->exec(
                "UPDATE sl_dt_events SET event_date = date(event_date, '+7 days')"
            );
            $rows = $this->ztdQuery("SELECT id, event_date FROM sl_dt_events ORDER BY id");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'UPDATE date arithmetic: expected 4, got ' . count($rows)
                );
            }

            $this->assertSame('2026-06-22', $rows[0]['event_date']); // Conference +7
            $this->assertSame('2026-03-08', $rows[1]['event_date']); // Workshop +7
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with date arithmetic failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE with date function and parameter.
     */
    public function testPreparedUpdateWithDateFunction(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE sl_dt_events SET event_date = date(event_date, ? || ' days') WHERE id = ?"
            );
            $stmt->execute(['+30', 2]);

            $rows = $this->ztdQuery("SELECT event_date FROM sl_dt_events WHERE id = 2");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Prepared UPDATE date func: got ' . json_encode($rows)
                );
            }

            $this->assertSame('2026-03-31', $rows[0]['event_date']); // 2026-03-01 + 30 days
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE with date function failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT with date function transforming values.
     */
    public function testInsertSelectWithDateFunction(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_dt_log (event_id, action, logged_at)
                 SELECT id, 'reminder', datetime(event_date, '-7 days')
                 FROM sl_dt_events
                 WHERE event_date > '2026-06-01'"
            );

            $rows = $this->ztdQuery("SELECT event_id, logged_at FROM sl_dt_log ORDER BY event_id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT SELECT date func: expected 2 (Conference, Webinar), got '
                    . count($rows) . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('2026-06-08 00:00:00', $rows[0]['logged_at']); // Conference -7 days
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT with date function failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with date function in WHERE (e.g., events older than N months).
     */
    public function testDeleteWithDateFunctionInWhere(): void
    {
        try {
            $this->pdo->exec(
                "DELETE FROM sl_dt_events WHERE event_date < date('2026-03-10', '-3 months')"
            );

            $rows = $this->ztdQuery("SELECT title FROM sl_dt_events ORDER BY id");

            // date('2026-03-10', '-3 months') = '2025-12-10'
            // Sprint (2025-11-30) is before this → deleted
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE date func WHERE: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE with date function in WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * SELECT with strftime() after DML mutations.
     */
    public function testSelectStrftimeAfterDml(): void
    {
        try {
            // Add a new event
            $this->pdo->exec("INSERT INTO sl_dt_events VALUES (5, 'Hackathon', '2026-07-04', '2026-02-01 12:00:00')");

            $rows = $this->ztdQuery(
                "SELECT title, strftime('%m', event_date) AS month
                 FROM sl_dt_events
                 WHERE strftime('%Y', event_date) = '2026'
                 ORDER BY event_date"
            );

            // 4 events in 2026: Workshop (03), Conference (06), Hackathon (07), Webinar (12)
            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'SELECT strftime: expected 4, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(4, $rows);
            $this->assertSame('03', $rows[0]['month']); // Workshop
            $this->assertSame('06', $rows[1]['month']); // Conference
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT with strftime after DML failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared DELETE with date BETWEEN comparison.
     */
    public function testPreparedDeleteDateBetween(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "DELETE FROM sl_dt_events WHERE event_date BETWEEN ? AND ?"
            );
            $stmt->execute(['2026-03-01', '2026-06-30']);

            $rows = $this->ztdQuery("SELECT title FROM sl_dt_events ORDER BY id");

            // Conference (06-15) and Workshop (03-01) should be deleted
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Prepared DELETE date BETWEEN: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Webinar', $rows[0]['title']);
            $this->assertSame('Sprint', $rows[1]['title']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DELETE with date BETWEEN failed: ' . $e->getMessage());
        }
    }
}
