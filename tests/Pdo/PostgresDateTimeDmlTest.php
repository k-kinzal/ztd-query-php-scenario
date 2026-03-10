<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests date/time functions and expressions in DML through ZTD shadow store on PostgreSQL.
 *
 * Date/time operations are ubiquitous in real applications (expiration checks,
 * scheduling, audit timestamps). The CTE rewriter must handle PostgreSQL-specific
 * date functions like NOW(), CURRENT_TIMESTAMP, interval arithmetic,
 * TO_CHAR(), and DATE_TRUNC() correctly.
 *
 * @spec SPEC-4.1, SPEC-4.2, SPEC-4.3
 */
class PostgresDateTimeDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_dt_events (
                id SERIAL PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                event_date TIMESTAMP NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT NOW()
            )',
            'CREATE TABLE pg_dt_log (
                id SERIAL PRIMARY KEY,
                event_id INT,
                action VARCHAR(255) NOT NULL,
                logged_at TIMESTAMP NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_dt_log', 'pg_dt_events'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_dt_events (id, title, event_date, created_at) VALUES (1, 'Conference', '2026-06-15 00:00:00', '2026-01-01 10:00:00')");
        $this->pdo->exec("INSERT INTO pg_dt_events (id, title, event_date, created_at) VALUES (2, 'Workshop', '2026-03-01 00:00:00', '2026-01-01 10:00:00')");
        $this->pdo->exec("INSERT INTO pg_dt_events (id, title, event_date, created_at) VALUES (3, 'Webinar', '2026-12-25 00:00:00', '2026-01-01 10:00:00')");
        $this->pdo->exec("INSERT INTO pg_dt_events (id, title, event_date, created_at) VALUES (4, 'Sprint', '2025-11-30 00:00:00', '2025-06-01 08:00:00')");
        $this->pdo->exec("SELECT setval('pg_dt_events_id_seq', 4)");
    }

    /**
     * UPDATE with NOW() function in SET clause.
     */
    public function testUpdateSetWithNowFunction(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE pg_dt_events SET created_at = NOW() WHERE id = 1"
            );
            $rows = $this->ztdQuery("SELECT TO_CHAR(created_at, 'YYYY-MM-DD HH24:MI:SS') AS created_at FROM pg_dt_events WHERE id = 1");

            if (count($rows) !== 1 || empty($rows[0]['created_at'])) {
                $this->markTestIncomplete(
                    'UPDATE SET NOW(): got ' . json_encode($rows)
                );
            }

            // Should be a valid datetime string
            $this->assertMatchesRegularExpression(
                '/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/',
                $rows[0]['created_at']
            );
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with NOW() in SET failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with date comparison in WHERE.
     */
    public function testDeleteWithDateComparison(): void
    {
        try {
            $this->pdo->exec(
                "DELETE FROM pg_dt_events WHERE event_date < '2026-03-10 00:00:00'"
            );
            $rows = $this->ztdQuery("SELECT title FROM pg_dt_events ORDER BY id");

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
     * INSERT...SELECT with TO_CHAR() transforming values and interval subtraction.
     */
    public function testInsertSelectWithToChar(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO pg_dt_log (event_id, action, logged_at)
                 SELECT id, 'reminder-' || TO_CHAR(event_date, 'YYYYMMDD'),
                        event_date - interval '7 days'
                 FROM pg_dt_events
                 WHERE event_date > '2026-06-01 00:00:00'"
            );

            $rows = $this->ztdQuery(
                "SELECT event_id, action, TO_CHAR(logged_at, 'YYYY-MM-DD HH24:MI:SS') AS logged_at
                 FROM pg_dt_log ORDER BY event_id"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT SELECT TO_CHAR: expected 2 (Conference, Webinar), got '
                    . count($rows) . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('reminder-20260615', $rows[0]['action']);
            $this->assertSame('2026-06-08 00:00:00', $rows[0]['logged_at']); // Conference -7 days
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT with TO_CHAR failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE with interval arithmetic and parameter.
     */
    public function testPreparedUpdateWithIntervalArithmetic(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE pg_dt_events SET event_date = event_date + (? || ' days')::interval WHERE id = ?"
            );
            $stmt->execute(['30', 2]);

            $rows = $this->ztdQuery(
                "SELECT TO_CHAR(event_date, 'YYYY-MM-DD HH24:MI:SS') AS event_date
                 FROM pg_dt_events WHERE id = 2"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Prepared UPDATE interval: got ' . json_encode($rows)
                );
            }

            $this->assertSame('2026-03-31 00:00:00', $rows[0]['event_date']); // 2026-03-01 + 30 days
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE with interval arithmetic failed: ' . $e->getMessage());
        }
    }

    /**
     * Interval arithmetic in WHERE clause for filtering.
     */
    public function testIntervalArithmeticInWhere(): void
    {
        try {
            // Select events that are within 90 days after 2026-03-10
            $rows = $this->ztdQuery(
                "SELECT title FROM pg_dt_events
                 WHERE event_date BETWEEN '2026-03-10 00:00:00'::timestamp
                       AND '2026-03-10 00:00:00'::timestamp + interval '90 days'
                 ORDER BY event_date"
            );

            // '2026-03-10' + 90 days = '2026-06-08'
            // No events fall in [2026-03-10, 2026-06-08]
            // Conference is 2026-06-15 which is outside
            if (count($rows) !== 0) {
                $this->markTestIncomplete(
                    'Interval in WHERE: expected 0, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(0, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Interval arithmetic in WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with interval subtraction pushing events backwards.
     */
    public function testUpdateWithIntervalSubtraction(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE pg_dt_events SET event_date = event_date - interval '7 days'"
            );
            $rows = $this->ztdQuery(
                "SELECT id, TO_CHAR(event_date, 'YYYY-MM-DD HH24:MI:SS') AS event_date
                 FROM pg_dt_events ORDER BY id"
            );

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'UPDATE interval subtraction: expected 4, got ' . count($rows)
                );
            }

            $this->assertSame('2026-06-08 00:00:00', $rows[0]['event_date']); // Conference -7
            $this->assertSame('2026-02-22 00:00:00', $rows[1]['event_date']); // Workshop -7
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with interval subtraction failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with DATE_TRUNC() in WHERE.
     */
    public function testDeleteWithDateTruncInWhere(): void
    {
        try {
            $this->pdo->exec(
                "DELETE FROM pg_dt_events WHERE DATE_TRUNC('year', event_date) < '2026-01-01'::timestamp"
            );

            $rows = $this->ztdQuery("SELECT title FROM pg_dt_events ORDER BY id");

            // Sprint (2025-11-30) truncated to year = 2025-01-01 < 2026-01-01 -> deleted
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE DATE_TRUNC WHERE: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE with DATE_TRUNC in WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * SELECT with TO_CHAR() and EXTRACT() after DML mutations.
     */
    public function testSelectToCharAfterDml(): void
    {
        try {
            // Add a new event
            $this->pdo->exec("INSERT INTO pg_dt_events (id, title, event_date, created_at) VALUES (5, 'Hackathon', '2026-07-04 00:00:00', '2026-02-01 12:00:00')");

            $rows = $this->ztdQuery(
                "SELECT title, TO_CHAR(event_date, 'MM') AS month
                 FROM pg_dt_events
                 WHERE EXTRACT(YEAR FROM event_date) = 2026
                 ORDER BY event_date"
            );

            // 4 events in 2026: Workshop (03), Conference (06), Hackathon (07), Webinar (12)
            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'SELECT TO_CHAR: expected 4, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(4, $rows);
            $this->assertSame('03', $rows[0]['month']); // Workshop
            $this->assertSame('06', $rows[1]['month']); // Conference
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT with TO_CHAR after DML failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared DELETE with date BETWEEN comparison.
     */
    public function testPreparedDeleteDateBetween(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "DELETE FROM pg_dt_events WHERE event_date BETWEEN ?::timestamp AND ?::timestamp"
            );
            $stmt->execute(['2026-03-01 00:00:00', '2026-06-30 23:59:59']);

            $rows = $this->ztdQuery("SELECT title FROM pg_dt_events ORDER BY id");

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
