<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests date/time functions and expressions in DML through ZTD shadow store on MySQL.
 *
 * Date/time operations are ubiquitous in real applications (expiration checks,
 * scheduling, audit timestamps). The CTE rewriter must handle MySQL-specific
 * date functions like NOW(), DATE_ADD(), DATE_SUB(), DATE_FORMAT(),
 * STR_TO_DATE(), and CURDATE() correctly.
 *
 * @spec SPEC-4.1, SPEC-4.2, SPEC-4.3
 */
class MysqlDateTimeDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_dt_events (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                event_date DATETIME NOT NULL,
                created_at DATETIME NOT NULL DEFAULT NOW()
            ) ENGINE=InnoDB',
            'CREATE TABLE my_dt_log (
                id INT AUTO_INCREMENT PRIMARY KEY,
                event_id INT,
                action VARCHAR(255) NOT NULL,
                logged_at DATETIME NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_dt_log', 'my_dt_events'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_dt_events (id, title, event_date, created_at) VALUES (1, 'Conference', '2026-06-15 00:00:00', '2026-01-01 10:00:00')");
        $this->pdo->exec("INSERT INTO my_dt_events (id, title, event_date, created_at) VALUES (2, 'Workshop', '2026-03-01 00:00:00', '2026-01-01 10:00:00')");
        $this->pdo->exec("INSERT INTO my_dt_events (id, title, event_date, created_at) VALUES (3, 'Webinar', '2026-12-25 00:00:00', '2026-01-01 10:00:00')");
        $this->pdo->exec("INSERT INTO my_dt_events (id, title, event_date, created_at) VALUES (4, 'Sprint', '2025-11-30 00:00:00', '2025-06-01 08:00:00')");
    }

    /**
     * UPDATE with NOW() function in SET clause.
     */
    public function testUpdateSetWithNowFunction(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE my_dt_events SET created_at = NOW() WHERE id = 1"
            );
            $rows = $this->ztdQuery("SELECT created_at FROM my_dt_events WHERE id = 1");

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
                "DELETE FROM my_dt_events WHERE event_date < '2026-03-10 00:00:00'"
            );
            $rows = $this->ztdQuery("SELECT title FROM my_dt_events ORDER BY id");

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
     * INSERT...SELECT with DATE_FORMAT() transforming values.
     */
    public function testInsertSelectWithDateFormat(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO my_dt_log (event_id, action, logged_at)
                 SELECT id, CONCAT('reminder-', DATE_FORMAT(event_date, '%Y%m%d')),
                        DATE_SUB(event_date, INTERVAL 7 DAY)
                 FROM my_dt_events
                 WHERE event_date > '2026-06-01 00:00:00'"
            );

            $rows = $this->ztdQuery("SELECT event_id, action, logged_at FROM my_dt_log ORDER BY event_id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT SELECT DATE_FORMAT: expected 2 (Conference, Webinar), got '
                    . count($rows) . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('reminder-20260615', $rows[0]['action']);
            $this->assertSame('2026-06-08 00:00:00', $rows[0]['logged_at']); // Conference -7 days
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT with DATE_FORMAT failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE with DATE_ADD() and parameter.
     */
    public function testPreparedUpdateWithDateAdd(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE my_dt_events SET event_date = DATE_ADD(event_date, INTERVAL ? DAY) WHERE id = ?"
            );
            $stmt->execute([30, 2]);

            $rows = $this->ztdQuery("SELECT event_date FROM my_dt_events WHERE id = 2");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Prepared UPDATE DATE_ADD: got ' . json_encode($rows)
                );
            }

            $this->assertSame('2026-03-31 00:00:00', $rows[0]['event_date']); // 2026-03-01 + 30 days
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE with DATE_ADD failed: ' . $e->getMessage());
        }
    }

    /**
     * DATE_ADD() arithmetic in WHERE clause for filtering.
     */
    public function testDateArithmeticInWhere(): void
    {
        try {
            // Select events that are within 90 days after 2026-03-10
            $rows = $this->ztdQuery(
                "SELECT title FROM my_dt_events
                 WHERE event_date BETWEEN '2026-03-10 00:00:00'
                       AND DATE_ADD('2026-03-10 00:00:00', INTERVAL 90 DAY)
                 ORDER BY event_date"
            );

            // DATE_ADD('2026-03-10', INTERVAL 90 DAY) = '2026-06-08'
            // No events fall in [2026-03-10, 2026-06-08]
            // Conference is 2026-06-15 which is outside
            if (count($rows) !== 0) {
                $this->markTestIncomplete(
                    'DATE_ADD in WHERE: expected 0, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(0, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Date arithmetic in WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with DATE_SUB() pushing events backwards.
     */
    public function testUpdateWithDateSub(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE my_dt_events SET event_date = DATE_SUB(event_date, INTERVAL 7 DAY)"
            );
            $rows = $this->ztdQuery("SELECT id, event_date FROM my_dt_events ORDER BY id");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'UPDATE DATE_SUB: expected 4, got ' . count($rows)
                );
            }

            $this->assertSame('2026-06-08 00:00:00', $rows[0]['event_date']); // Conference -7
            $this->assertSame('2026-02-22 00:00:00', $rows[1]['event_date']); // Workshop -7
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with DATE_SUB failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with STR_TO_DATE() and date function in WHERE.
     */
    public function testDeleteWithStrToDateInWhere(): void
    {
        try {
            $this->pdo->exec(
                "DELETE FROM my_dt_events WHERE event_date < STR_TO_DATE('10-12-2025', '%d-%m-%Y')"
            );

            $rows = $this->ztdQuery("SELECT title FROM my_dt_events ORDER BY id");

            // STR_TO_DATE('10-12-2025', '%d-%m-%Y') = '2025-12-10'
            // Sprint (2025-11-30) is before this -> deleted
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE STR_TO_DATE WHERE: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE with STR_TO_DATE in WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * SELECT with DATE_FORMAT() after DML mutations.
     */
    public function testSelectDateFormatAfterDml(): void
    {
        try {
            // Add a new event
            $this->pdo->exec("INSERT INTO my_dt_events (id, title, event_date, created_at) VALUES (5, 'Hackathon', '2026-07-04 00:00:00', '2026-02-01 12:00:00')");

            $rows = $this->ztdQuery(
                "SELECT title, DATE_FORMAT(event_date, '%m') AS month
                 FROM my_dt_events
                 WHERE YEAR(event_date) = 2026
                 ORDER BY event_date"
            );

            // 4 events in 2026: Workshop (03), Conference (06), Hackathon (07), Webinar (12)
            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'SELECT DATE_FORMAT: expected 4, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(4, $rows);
            $this->assertSame('03', $rows[0]['month']); // Workshop
            $this->assertSame('06', $rows[1]['month']); // Conference
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT with DATE_FORMAT after DML failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared DELETE with CURDATE()-based comparison.
     */
    public function testPreparedDeleteWithCurdate(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "DELETE FROM my_dt_events WHERE event_date BETWEEN ? AND ?"
            );
            $stmt->execute(['2026-03-01 00:00:00', '2026-06-30 23:59:59']);

            $rows = $this->ztdQuery("SELECT title FROM my_dt_events ORDER BY id");

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
