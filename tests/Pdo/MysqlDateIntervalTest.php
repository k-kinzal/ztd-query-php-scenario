<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests date arithmetic and interval queries through the shadow store.
 * MySQL uses DATE_ADD/DATE_SUB for date arithmetic, DATE_FORMAT for formatting,
 * and DATEDIFF for day differences.
 * @spec SPEC-10.2.18
 */
class MysqlDateIntervalTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_di_events (
                id INT PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                event_date DATE NOT NULL,
                end_date DATE,
                category VARCHAR(50) NOT NULL,
                amount DECIMAL(10,2) NOT NULL DEFAULT 0
            )',
            'CREATE TABLE mp_di_calendar (
                day_date DATE PRIMARY KEY
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_di_events', 'mp_di_calendar'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Events spanning several months with various categories
        $this->pdo->exec("INSERT INTO mp_di_events VALUES (1, 'Sprint Planning', '2024-01-08', '2024-01-08', 'meeting', 0)");
        $this->pdo->exec("INSERT INTO mp_di_events VALUES (2, 'Conference', '2024-01-15', '2024-01-17', 'event', 500)");
        $this->pdo->exec("INSERT INTO mp_di_events VALUES (3, 'Workshop A', '2024-01-22', '2024-01-23', 'training', 200)");
        $this->pdo->exec("INSERT INTO mp_di_events VALUES (4, 'Sprint Review', '2024-02-05', '2024-02-05', 'meeting', 0)");
        $this->pdo->exec("INSERT INTO mp_di_events VALUES (5, 'Team Offsite', '2024-02-12', '2024-02-14', 'event', 1500)");
        $this->pdo->exec("INSERT INTO mp_di_events VALUES (6, 'Workshop B', '2024-02-19', '2024-02-20', 'training', 300)");
        $this->pdo->exec("INSERT INTO mp_di_events VALUES (7, 'Retro', '2024-03-04', '2024-03-04', 'meeting', 0)");
        $this->pdo->exec("INSERT INTO mp_di_events VALUES (8, 'Hackathon', '2024-03-11', '2024-03-13', 'event', 800)");
        $this->pdo->exec("INSERT INTO mp_di_events VALUES (9, 'Sprint Planning Q2', '2024-04-01', '2024-04-01', 'meeting', 0)");
        $this->pdo->exec("INSERT INTO mp_di_events VALUES (10, 'Workshop C', '2024-04-15', '2024-04-16', 'training', 250)");

        // Calendar days for February (for gap detection)
        for ($d = 1; $d <= 29; $d++) {
            $date = sprintf('2024-02-%02d', $d);
            $this->pdo->exec("INSERT INTO mp_di_calendar VALUES ('$date')");
        }
    }

    /**
     * Date range BETWEEN query.
     */
    public function testDateRangeBetween(): void
    {
        $rows = $this->ztdQuery(
            "SELECT title FROM mp_di_events WHERE event_date BETWEEN '2024-01-01' AND '2024-01-31' ORDER BY event_date"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Sprint Planning', $rows[0]['title']);
        $this->assertSame('Conference', $rows[1]['title']);
        $this->assertSame('Workshop A', $rows[2]['title']);
    }

    /**
     * Date arithmetic: DATE_ADD(col, INTERVAL 7 DAY).
     */
    public function testDateArithmeticPlusDays(): void
    {
        $rows = $this->ztdQuery(
            "SELECT title, DATE_ADD(event_date, INTERVAL 7 DAY) AS follow_up_date FROM mp_di_events WHERE id = 1"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('2024-01-15', $rows[0]['follow_up_date']);
    }

    /**
     * Date arithmetic: DATE_SUB(col, INTERVAL 1 MONTH).
     */
    public function testDateArithmeticMinusMonth(): void
    {
        $rows = $this->ztdQuery(
            "SELECT title, DATE_SUB(event_date, INTERVAL 1 MONTH) AS month_before FROM mp_di_events WHERE id = 4"
        );

        $this->assertCount(1, $rows);
        // 2024-02-05 minus 1 month = 2024-01-05
        $this->assertSame('2024-01-05', $rows[0]['month_before']);
    }

    /**
     * GROUP BY month using DATE_FORMAT(col, '%Y-%m').
     */
    public function testGroupByMonth(): void
    {
        $rows = $this->ztdQuery(
            "SELECT DATE_FORMAT(event_date, '%Y-%m') AS month, COUNT(*) AS cnt, SUM(amount) AS total_cost
             FROM mp_di_events
             GROUP BY DATE_FORMAT(event_date, '%Y-%m')
             ORDER BY month"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('2024-01', $rows[0]['month']);
        $this->assertSame(3, (int) $rows[0]['cnt']);
        $this->assertEqualsWithDelta(700.0, (float) $rows[0]['total_cost'], 0.01);

        $this->assertSame('2024-02', $rows[1]['month']);
        $this->assertSame(3, (int) $rows[1]['cnt']);

        $this->assertSame('2024-03', $rows[2]['month']);
        $this->assertSame(2, (int) $rows[2]['cnt']);

        $this->assertSame('2024-04', $rows[3]['month']);
        $this->assertSame(2, (int) $rows[3]['cnt']);
    }

    /**
     * DATEDIFF to compute duration between two dates.
     */
    public function testDatediffDuration(): void
    {
        $rows = $this->ztdQuery(
            "SELECT title, DATEDIFF(end_date, event_date) AS duration_days
             FROM mp_di_events
             WHERE end_date IS NOT NULL
             ORDER BY duration_days DESC, title ASC
             LIMIT 3"
        );

        $this->assertCount(3, $rows);
        // Team Offsite: 2024-02-12 to 2024-02-14 = 2 days
        // Conference: 2024-01-15 to 2024-01-17 = 2 days
        // Hackathon: 2024-03-11 to 2024-03-13 = 2 days
        $this->assertSame(2, (int) $rows[0]['duration_days']);
    }

    /**
     * Overlapping date range detection: find events that overlap with a given range.
     * Overlap condition: event_start < range_end AND event_end > range_start.
     */
    public function testOverlappingDateRangeDetection(): void
    {
        // Find events overlapping with 2024-01-16 to 2024-01-25
        $rows = $this->ztdPrepareAndExecute(
            "SELECT title FROM mp_di_events
             WHERE event_date < ? AND end_date > ?
             ORDER BY event_date",
            ['2024-01-25', '2024-01-16']
        );

        // Conference: 2024-01-15 to 2024-01-17 -- start(15) < 25 AND end(17) > 16 => YES
        // Workshop A: 2024-01-22 to 2024-01-23 -- start(22) < 25 AND end(23) > 16 => YES
        $this->assertCount(2, $rows);
        $this->assertSame('Conference', $rows[0]['title']);
        $this->assertSame('Workshop A', $rows[1]['title']);
    }

    /**
     * Gaps detection: find February dates that have no events starting.
     */
    public function testGapsDetectionNoEventsOnDate(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.day_date
             FROM mp_di_calendar c
             LEFT JOIN mp_di_events e ON c.day_date = e.event_date
             WHERE e.id IS NULL AND c.day_date BETWEEN '2024-02-01' AND '2024-02-29'
             ORDER BY c.day_date"
        );

        // February has events on 5th, 12th, 19th. All other days are gaps.
        // 29 days - 3 event days = 26 gap days
        $this->assertCount(26, $rows);

        // Verify specific gap days
        $dates = array_column($rows, 'day_date');
        $this->assertContains('2024-02-01', $dates);
        $this->assertNotContains('2024-02-05', $dates);
        $this->assertNotContains('2024-02-12', $dates);
        $this->assertNotContains('2024-02-19', $dates);
        $this->assertContains('2024-02-28', $dates);
    }

    /**
     * Date comparison in UPDATE WHERE clause.
     */
    public function testDateComparisonInUpdateWhere(): void
    {
        // Double the cost for all events in Q1 (Jan-Mar)
        $this->pdo->exec(
            "UPDATE mp_di_events SET amount = amount * 2 WHERE event_date < '2024-04-01' AND amount > 0"
        );

        // Verify Q1 events are doubled
        $rows = $this->ztdQuery(
            "SELECT title, amount FROM mp_di_events WHERE id = 2"
        );
        $this->assertEqualsWithDelta(1000.0, (float) $rows[0]['amount'], 0.01); // 500 * 2

        // Verify Q2 events are unchanged
        $rows = $this->ztdQuery(
            "SELECT title, amount FROM mp_di_events WHERE id = 10"
        );
        $this->assertEqualsWithDelta(250.0, (float) $rows[0]['amount'], 0.01);
    }

    /**
     * INSERT then query by relative date range using DATE_ADD/DATE_SUB.
     */
    public function testInsertThenQueryByRelativeDateRange(): void
    {
        // Insert a new event
        $this->pdo->exec("INSERT INTO mp_di_events VALUES (11, 'New Workshop', '2024-02-10', '2024-02-11', 'training', 100)");

        // Query events within 7 days before and after the reference date
        $rows = $this->ztdQuery(
            "SELECT title FROM mp_di_events
             WHERE event_date BETWEEN DATE_SUB('2024-02-10', INTERVAL 7 DAY) AND DATE_ADD('2024-02-10', INTERVAL 7 DAY)
             ORDER BY event_date"
        );

        // 2024-02-03 to 2024-02-17
        // Sprint Review (02-05), New Workshop (02-10), Team Offsite (02-12)
        $this->assertCount(3, $rows);
        $this->assertSame('Sprint Review', $rows[0]['title']);
        $this->assertSame('New Workshop', $rows[1]['title']);
        $this->assertSame('Team Offsite', $rows[2]['title']);
    }

    /**
     * Prepared date range query with bound parameters.
     */
    public function testPreparedDateRangeWithBoundParams(): void
    {
        $stmt = $this->ztdPrepare(
            "SELECT title, amount FROM mp_di_events WHERE event_date >= ? AND event_date <= ? ORDER BY event_date"
        );

        // Query February events
        $stmt->execute(['2024-02-01', '2024-02-29']);
        $febRows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $febRows);
        $this->assertSame('Sprint Review', $febRows[0]['title']);
        $this->assertSame('Team Offsite', $febRows[1]['title']);
        $this->assertSame('Workshop B', $febRows[2]['title']);
    }

    /**
     * Physical isolation -- shadow data does not reach physical table.
     */
    public function testPhysicalIsolation(): void
    {
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_di_events");
        $this->assertSame(10, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query("SELECT COUNT(*) FROM mp_di_events");
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
