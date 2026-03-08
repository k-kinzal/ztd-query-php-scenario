<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests MySQL date/time functions through CTE shadow store via PDO.
 * @spec SPEC-3.3
 */
class MysqlDateTimeFunctionsTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mp_dt_events (id INT PRIMARY KEY, name VARCHAR(100), event_date DATE, event_ts DATETIME)';
    }

    protected function getTableNames(): array
    {
        return ['mp_dt_events'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_dt_events VALUES (1, 'Launch', '2024-01-15', '2024-01-15 10:30:00')");
        $this->pdo->exec("INSERT INTO mp_dt_events VALUES (2, 'Update', '2024-06-20', '2024-06-20 14:00:00')");
        $this->pdo->exec("INSERT INTO mp_dt_events VALUES (3, 'Release', '2024-12-01', '2024-12-01 09:00:00')");
    }

    public function testDateFormatFunction(): void
    {
        $rows = $this->ztdQuery("SELECT DATE_FORMAT(event_date, '%Y-%m') AS ym FROM mp_dt_events WHERE id = 1");
        $this->assertSame('2024-01', $rows[0]['ym']);
    }

    public function testYearMonthDayExtraction(): void
    {
        $rows = $this->ztdQuery("SELECT YEAR(event_date) AS y, MONTH(event_date) AS m, DAY(event_date) AS d FROM mp_dt_events WHERE id = 1");
        $this->assertSame(2024, (int) $rows[0]['y']);
        $this->assertSame(1, (int) $rows[0]['m']);
        $this->assertSame(15, (int) $rows[0]['d']);
    }

    public function testDateComparisonInWhere(): void
    {
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_dt_events WHERE event_date >= '2024-06-01'");
        $this->assertSame(2, (int) $rows[0]['cnt']);
    }

    public function testDateRangeWithBetween(): void
    {
        $rows = $this->ztdQuery("SELECT name FROM mp_dt_events WHERE event_date BETWEEN '2024-01-01' AND '2024-06-30' ORDER BY event_date");
        $this->assertCount(2, $rows);
        $this->assertSame('Launch', $rows[0]['name']);
        $this->assertSame('Update', $rows[1]['name']);
    }

    public function testDateDiffFunction(): void
    {
        $rows = $this->ztdQuery("SELECT DATEDIFF(event_date, '2024-01-01') AS days_since FROM mp_dt_events WHERE id = 1");
        $this->assertSame(14, (int) $rows[0]['days_since']);
    }

    public function testDateFormatGroupBy(): void
    {
        $rows = $this->ztdQuery("
            SELECT DATE_FORMAT(event_date, '%Y') AS yr, COUNT(*) AS cnt
            FROM mp_dt_events
            GROUP BY DATE_FORMAT(event_date, '%Y')
        ");
        $this->assertCount(1, $rows);
        $this->assertSame('2024', $rows[0]['yr']);
        $this->assertSame(3, (int) $rows[0]['cnt']);
    }

    public function testDateOrdering(): void
    {
        $rows = $this->ztdQuery('SELECT name FROM mp_dt_events ORDER BY event_date DESC');
        $this->assertSame('Release', $rows[0]['name']);
        $this->assertSame('Launch', $rows[2]['name']);
    }

    public function testTimestampHourExtraction(): void
    {
        $rows = $this->ztdQuery("SELECT HOUR(event_ts) AS h, MINUTE(event_ts) AS min FROM mp_dt_events WHERE id = 2");
        $this->assertSame(14, (int) $rows[0]['h']);
        $this->assertSame(0, (int) $rows[0]['min']);
    }

    public function testCurrentDateInSelect(): void
    {
        $rows = $this->ztdQuery("SELECT CURDATE() AS today, name FROM mp_dt_events WHERE id = 1");
        $this->assertSame('Launch', $rows[0]['name']);
        $this->assertNotEmpty($rows[0]['today']);
    }

    public function testPreparedDateRangeQuery(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            'SELECT name FROM mp_dt_events WHERE event_date BETWEEN ? AND ? ORDER BY event_date',
            ['2024-01-01', '2024-06-30']
        );
        $this->assertCount(2, $rows);
        $this->assertSame('Launch', $rows[0]['name']);
    }

    public function testDateAfterInsertAndUpdate(): void
    {
        $this->pdo->exec("INSERT INTO mp_dt_events VALUES (4, 'Patch', '2025-03-01', '2025-03-01 12:00:00')");
        $this->pdo->exec("UPDATE mp_dt_events SET event_date = '2025-04-01' WHERE id = 4");

        $rows = $this->ztdQuery("SELECT MONTH(event_date) AS m FROM mp_dt_events WHERE id = 4");
        $this->assertSame(4, (int) $rows[0]['m']);
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM mp_dt_events');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
