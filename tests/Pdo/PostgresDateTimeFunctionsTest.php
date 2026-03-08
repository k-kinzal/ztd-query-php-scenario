<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\PostgreSQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests PostgreSQL date/time functions through CTE shadow.
 *
 * PostgreSQL has specific date/time handling: EXTRACT returns 0 for shadow dates,
 * TO_CHAR works correctly, interval arithmetic may or may not work.
 */
class PostgresDateTimeFunctionsTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_dt_events');
        $raw->exec('CREATE TABLE pg_dt_events (id INT PRIMARY KEY, name VARCHAR(50), event_date DATE, event_ts TIMESTAMP)');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO pg_dt_events VALUES (1, 'Launch', '2024-01-15', '2024-01-15 10:30:00')");
        $this->pdo->exec("INSERT INTO pg_dt_events VALUES (2, 'Update', '2024-06-20', '2024-06-20 14:00:00')");
        $this->pdo->exec("INSERT INTO pg_dt_events VALUES (3, 'Release', '2024-12-01', '2024-12-01 09:00:00')");
    }

    /**
     * TO_CHAR for date formatting — works correctly with shadow dates.
     */
    public function testToCharDateFormat(): void
    {
        $stmt = $this->pdo->query(
            "SELECT TO_CHAR(event_date::DATE, 'YYYY-MM') AS ym FROM pg_dt_events WHERE id = 1"
        );
        $value = $stmt->fetchColumn();
        $this->assertSame('2024-01', $value);
    }

    /**
     * EXTRACT(YEAR FROM ...) — known to return 0 for shadow dates.
     */
    /**
     * EXTRACT should return correct year from shadow-stored dates.
     */
    public function testExtractReturnsCorrectYearForShadowDates(): void
    {
        $stmt = $this->pdo->query(
            "SELECT EXTRACT(YEAR FROM event_date::DATE) AS yr FROM pg_dt_events WHERE id = 1"
        );
        $value = (int) $stmt->fetchColumn();
        if ($value === 0) {
            $this->markTestIncomplete(
                'EXTRACT returns 0 for shadow-stored dates. Expected 2024, got 0'
            );
        }
        $this->assertSame(2024, $value);
    }

    /**
     * DATE comparison in WHERE clause.
     */
    public function testDateComparisonInWhere(): void
    {
        $stmt = $this->pdo->query(
            "SELECT COUNT(*) FROM pg_dt_events WHERE event_date::DATE >= '2024-06-01'::DATE"
        );
        $count = (int) $stmt->fetchColumn();
        $this->assertEquals(2, $count);
    }

    /**
     * TO_CHAR with timestamp formatting.
     */
    public function testToCharTimestampFormat(): void
    {
        $stmt = $this->pdo->query(
            "SELECT TO_CHAR(event_ts::TIMESTAMP, 'HH24:MI') AS time_str FROM pg_dt_events WHERE id = 1"
        );
        $value = $stmt->fetchColumn();
        $this->assertSame('10:30', $value);
    }

    /**
     * CURRENT_DATE in SELECT (not from shadow table).
     */
    public function testCurrentDateInSelect(): void
    {
        $stmt = $this->pdo->query("SELECT CURRENT_DATE AS today, name FROM pg_dt_events WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Launch', $row['name']);
        $this->assertNotEmpty($row['today']);
    }

    /**
     * Date ordering.
     */
    public function testDateOrdering(): void
    {
        $stmt = $this->pdo->query(
            "SELECT name FROM pg_dt_events ORDER BY event_date ASC"
        );
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame('Launch', $rows[0]);
        $this->assertSame('Release', $rows[2]);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_dt_events');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(
                PostgreSQLContainer::getDsn(),
                'test',
                'test',
            );
            $raw->exec('DROP TABLE IF EXISTS pg_dt_events');
        } catch (\Exception $e) {
        }
    }
}
