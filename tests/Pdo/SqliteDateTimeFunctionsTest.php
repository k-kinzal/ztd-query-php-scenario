<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests SQLite date/time functions through CTE shadow store.
 * SQLite uses strftime(), date(), time(), datetime() functions.
 * @spec SPEC-3.3
 */
class SqliteDateTimeFunctionsTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_dt_events (id INTEGER PRIMARY KEY, name TEXT, event_date TEXT, event_ts TEXT)';
    }

    protected function getTableNames(): array
    {
        return ['sl_dt_events'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_dt_events VALUES (1, 'Launch', '2024-01-15', '2024-01-15 10:30:00')");
        $this->pdo->exec("INSERT INTO sl_dt_events VALUES (2, 'Update', '2024-06-20', '2024-06-20 14:00:00')");
        $this->pdo->exec("INSERT INTO sl_dt_events VALUES (3, 'Release', '2024-12-01', '2024-12-01 09:00:00')");
    }

    public function testStrftimeYearMonth(): void
    {
        $rows = $this->ztdQuery("SELECT strftime('%Y-%m', event_date) AS ym FROM sl_dt_events WHERE id = 1");
        $this->assertSame('2024-01', $rows[0]['ym']);
    }

    public function testStrftimeYearExtraction(): void
    {
        $rows = $this->ztdQuery("SELECT strftime('%Y', event_date) AS yr FROM sl_dt_events WHERE id = 1");
        $this->assertSame('2024', $rows[0]['yr']);
    }

    public function testStrftimeMonthDayExtraction(): void
    {
        $rows = $this->ztdQuery("SELECT strftime('%m', event_date) AS m, strftime('%d', event_date) AS d FROM sl_dt_events WHERE id = 1");
        $this->assertSame('01', $rows[0]['m']);
        $this->assertSame('15', $rows[0]['d']);
    }

    public function testDateComparisonInWhere(): void
    {
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_dt_events WHERE event_date >= '2024-06-01'");
        $this->assertSame(2, (int) $rows[0]['cnt']);
    }

    public function testDateRangeWithBetween(): void
    {
        $rows = $this->ztdQuery("SELECT name FROM sl_dt_events WHERE event_date BETWEEN '2024-01-01' AND '2024-06-30' ORDER BY event_date");
        $this->assertCount(2, $rows);
        $this->assertSame('Launch', $rows[0]['name']);
        $this->assertSame('Update', $rows[1]['name']);
    }

    public function testJulianDayDiff(): void
    {
        $rows = $this->ztdQuery("SELECT CAST(julianday(event_date) - julianday('2024-01-01') AS INTEGER) AS days_since FROM sl_dt_events WHERE id = 1");
        $this->assertSame(14, (int) $rows[0]['days_since']);
    }

    public function testStrftimeGroupBy(): void
    {
        $rows = $this->ztdQuery("
            SELECT strftime('%Y', event_date) AS yr, COUNT(*) AS cnt
            FROM sl_dt_events
            GROUP BY strftime('%Y', event_date)
        ");
        $this->assertCount(1, $rows);
        $this->assertSame('2024', $rows[0]['yr']);
        $this->assertSame(3, (int) $rows[0]['cnt']);
    }

    public function testDateOrdering(): void
    {
        $rows = $this->ztdQuery('SELECT name FROM sl_dt_events ORDER BY event_date DESC');
        $this->assertSame('Release', $rows[0]['name']);
        $this->assertSame('Launch', $rows[2]['name']);
    }

    public function testStrftimeHourMinuteExtraction(): void
    {
        $rows = $this->ztdQuery("SELECT strftime('%H', event_ts) AS h, strftime('%M', event_ts) AS min FROM sl_dt_events WHERE id = 2");
        $this->assertSame('14', $rows[0]['h']);
        $this->assertSame('00', $rows[0]['min']);
    }

    public function testDateFunctionInSelect(): void
    {
        $rows = $this->ztdQuery("SELECT date('now') AS today, name FROM sl_dt_events WHERE id = 1");
        $this->assertSame('Launch', $rows[0]['name']);
        $this->assertNotEmpty($rows[0]['today']);
    }

    public function testPreparedDateRangeQuery(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            'SELECT name FROM sl_dt_events WHERE event_date BETWEEN ? AND ? ORDER BY event_date',
            ['2024-01-01', '2024-06-30']
        );
        $this->assertCount(2, $rows);
        $this->assertSame('Launch', $rows[0]['name']);
    }

    public function testDateAfterInsertAndUpdate(): void
    {
        $this->pdo->exec("INSERT INTO sl_dt_events VALUES (4, 'Patch', '2025-03-01', '2025-03-01 12:00:00')");
        $this->pdo->exec("UPDATE sl_dt_events SET event_date = '2025-04-01' WHERE id = 4");

        $rows = $this->ztdQuery("SELECT strftime('%m', event_date) AS m FROM sl_dt_events WHERE id = 4");
        $this->assertSame('04', $rows[0]['m']);
    }

    public function testDateModifiers(): void
    {
        $rows = $this->ztdQuery("SELECT date(event_date, '+1 month') AS next_month FROM sl_dt_events WHERE id = 1");
        $this->assertSame('2024-02-15', $rows[0]['next_month']);
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_dt_events');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
