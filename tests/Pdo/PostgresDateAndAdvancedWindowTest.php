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
 * Tests date/time functions and advanced window functions on PostgreSQL PDO.
 */
class PostgresDateAndAdvancedWindowTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS pg_daw_events');
        $raw->exec('CREATE TABLE pg_daw_events (id INT PRIMARY KEY, title VARCHAR(255), event_date DATE, category VARCHAR(50), amount NUMERIC(10,2))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO pg_daw_events (id, title, event_date, category, amount) VALUES (1, 'A', '2024-01-10', 'work', 100)");
        $this->pdo->exec("INSERT INTO pg_daw_events (id, title, event_date, category, amount) VALUES (2, 'B', '2024-01-20', 'work', 200)");
        $this->pdo->exec("INSERT INTO pg_daw_events (id, title, event_date, category, amount) VALUES (3, 'C', '2024-02-05', 'personal', 300)");
        $this->pdo->exec("INSERT INTO pg_daw_events (id, title, event_date, category, amount) VALUES (4, 'D', '2024-02-15', 'personal', 400)");
    }

    /**
     * EXTRACT on shadow-stored DATE fails — the CTE rewriter stores dates as strings
     * and PostgreSQL EXTRACT cannot parse them. The query may return false (no rows)
     * or 0 depending on the CTE rewriter version. Use TO_CHAR instead.
     */
    public function testExtractOnShadowDateFails(): void
    {
        $stmt = $this->pdo->query("SELECT EXTRACT(YEAR FROM event_date)::INT AS yr, EXTRACT(MONTH FROM event_date)::INT AS mo FROM pg_daw_events WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        if ($row === false) {
            // Query returned no results — EXTRACT completely failed
            $this->assertFalse($row);
        } else {
            // EXTRACT returned 0 (string-to-date cast produced epoch)
            $this->assertSame(0, (int) $row['yr']);
            $this->assertSame(0, (int) $row['mo']);
        }
    }

    public function testToCharWorksForDateParts(): void
    {
        $stmt = $this->pdo->query("SELECT TO_CHAR(event_date, 'YYYY') AS yr, TO_CHAR(event_date, 'MM') AS mo FROM pg_daw_events WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('2024', $row['yr']);
        $this->assertSame('01', $row['mo']);
    }

    public function testDateArithmeticInterval(): void
    {
        $stmt = $this->pdo->query("SELECT (event_date + INTERVAL '7 days')::DATE AS next_week FROM pg_daw_events WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('2024-01-17', $row['next_week']);
    }

    public function testDateComparisonInWhere(): void
    {
        $stmt = $this->pdo->query("SELECT title FROM pg_daw_events WHERE event_date >= '2024-02-01' ORDER BY event_date");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('C', $rows[0]['title']);
    }

    public function testGroupByMonth(): void
    {
        $stmt = $this->pdo->query("
            SELECT TO_CHAR(event_date, 'YYYY-MM') AS month, SUM(amount) AS total
            FROM pg_daw_events
            GROUP BY TO_CHAR(event_date, 'YYYY-MM')
            ORDER BY month
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('2024-01', $rows[0]['month']);
        $this->assertEqualsWithDelta(300.0, (float) $rows[0]['total'], 0.01);
    }

    public function testNtile(): void
    {
        $stmt = $this->pdo->query("
            SELECT title, amount, NTILE(2) OVER (ORDER BY amount) AS tile
            FROM pg_daw_events
            ORDER BY amount
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(4, $rows);
        $this->assertSame(1, (int) $rows[0]['tile']);
        $this->assertSame(2, (int) $rows[3]['tile']);
    }

    public function testFirstValueLastValue(): void
    {
        $stmt = $this->pdo->query("
            SELECT title, amount,
                   FIRST_VALUE(amount) OVER (ORDER BY amount) AS first_amt,
                   LAST_VALUE(amount) OVER (ORDER BY amount ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_amt
            FROM pg_daw_events
            ORDER BY amount
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(4, $rows);
        $this->assertEqualsWithDelta(100.0, (float) $rows[0]['first_amt'], 0.01);
        $this->assertEqualsWithDelta(400.0, (float) $rows[0]['last_amt'], 0.01);
    }

    public function testPartitionByWithWindowFunction(): void
    {
        $stmt = $this->pdo->query("
            SELECT title, category, amount,
                   ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount) AS rn,
                   SUM(amount) OVER (PARTITION BY category) AS cat_total
            FROM pg_daw_events
            ORDER BY category, amount
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(4, $rows);
        $this->assertSame('personal', $rows[0]['category']);
        $this->assertSame(1, (int) $rows[0]['rn']);
        $this->assertEqualsWithDelta(700.0, (float) $rows[0]['cat_total'], 0.01);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_daw_events');
    }
}
