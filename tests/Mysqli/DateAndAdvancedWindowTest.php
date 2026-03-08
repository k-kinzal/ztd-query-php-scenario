<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests date/time functions and advanced window functions on MySQLi.
 * @spec pending
 */
class DateAndAdvancedWindowTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_daw_events (id INT PRIMARY KEY, title VARCHAR(255), event_date DATE, category VARCHAR(50), amount DECIMAL(10,2))';
    }

    protected function getTableNames(): array
    {
        return ['mi_daw_events'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_daw_events (id, title, event_date, category, amount) VALUES (1, 'A', '2024-01-10', 'work', 100)");
        $this->mysqli->query("INSERT INTO mi_daw_events (id, title, event_date, category, amount) VALUES (2, 'B', '2024-01-20', 'work', 200)");
        $this->mysqli->query("INSERT INTO mi_daw_events (id, title, event_date, category, amount) VALUES (3, 'C', '2024-02-05', 'personal', 300)");
        $this->mysqli->query("INSERT INTO mi_daw_events (id, title, event_date, category, amount) VALUES (4, 'D', '2024-02-15', 'personal', 400)");
    }

    public function testYearMonthExtract(): void
    {
        $result = $this->mysqli->query("SELECT YEAR(event_date) AS yr, MONTH(event_date) AS mo FROM mi_daw_events WHERE id = 1");
        $row = $result->fetch_assoc();
        $this->assertSame(2024, (int) $row['yr']);
        $this->assertSame(1, (int) $row['mo']);
    }

    public function testDateAdd(): void
    {
        $result = $this->mysqli->query("SELECT DATE_ADD(event_date, INTERVAL 7 DAY) AS next_week FROM mi_daw_events WHERE id = 1");
        $row = $result->fetch_assoc();
        $this->assertSame('2024-01-17', $row['next_week']);
    }

    public function testGroupByMonth(): void
    {
        $result = $this->mysqli->query("
            SELECT DATE_FORMAT(event_date, '%Y-%m') AS month, SUM(amount) AS total
            FROM mi_daw_events
            GROUP BY DATE_FORMAT(event_date, '%Y-%m')
            ORDER BY month
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('2024-01', $rows[0]['month']);
        $this->assertEqualsWithDelta(300.0, (float) $rows[0]['total'], 0.01);
    }

    public function testNtile(): void
    {
        $result = $this->mysqli->query("
            SELECT title, amount, NTILE(2) OVER (ORDER BY amount) AS tile
            FROM mi_daw_events
            ORDER BY amount
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(4, $rows);
        $this->assertSame(1, (int) $rows[0]['tile']);
        $this->assertSame(2, (int) $rows[3]['tile']);
    }

    public function testPartitionByWithWindowFunction(): void
    {
        $result = $this->mysqli->query("
            SELECT title, category, amount,
                   ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount) AS rn,
                   SUM(amount) OVER (PARTITION BY category) AS cat_total
            FROM mi_daw_events
            ORDER BY category, amount
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(4, $rows);
        $this->assertSame('personal', $rows[0]['category']);
        $this->assertSame(1, (int) $rows[0]['rn']);
        $this->assertEqualsWithDelta(700.0, (float) $rows[0]['cat_total'], 0.01);
    }
}
