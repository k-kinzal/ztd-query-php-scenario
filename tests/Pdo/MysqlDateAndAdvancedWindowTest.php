<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests date/time functions and advanced window functions on MySQL PDO.
 * @spec SPEC-10.2.18
 */
class MysqlDateAndAdvancedWindowTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mysql_daw_events (id INT PRIMARY KEY, title VARCHAR(255), event_date DATE, category VARCHAR(50), amount DECIMAL(10,2))';
    }

    protected function getTableNames(): array
    {
        return ['mysql_daw_events'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mysql_daw_events (id, title, event_date, category, amount) VALUES (1, 'A', '2024-01-10', 'work', 100)");
        $this->pdo->exec("INSERT INTO mysql_daw_events (id, title, event_date, category, amount) VALUES (2, 'B', '2024-01-20', 'work', 200)");
        $this->pdo->exec("INSERT INTO mysql_daw_events (id, title, event_date, category, amount) VALUES (3, 'C', '2024-02-05', 'personal', 300)");
        $this->pdo->exec("INSERT INTO mysql_daw_events (id, title, event_date, category, amount) VALUES (4, 'D', '2024-02-15', 'personal', 400)");
    }

    public function testYearMonthExtract(): void
    {
        $stmt = $this->pdo->query("SELECT YEAR(event_date) AS yr, MONTH(event_date) AS mo FROM mysql_daw_events WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(2024, (int) $row['yr']);
        $this->assertSame(1, (int) $row['mo']);
    }

    public function testDateAdd(): void
    {
        $stmt = $this->pdo->query("SELECT DATE_ADD(event_date, INTERVAL 7 DAY) AS next_week FROM mysql_daw_events WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('2024-01-17', $row['next_week']);
    }

    public function testDateComparisonInWhere(): void
    {
        $stmt = $this->pdo->query("SELECT title FROM mysql_daw_events WHERE event_date >= '2024-02-01' ORDER BY event_date");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('C', $rows[0]['title']);
    }

    public function testGroupByMonth(): void
    {
        $stmt = $this->pdo->query("
            SELECT DATE_FORMAT(event_date, '%Y-%m') AS month, SUM(amount) AS total
            FROM mysql_daw_events
            GROUP BY DATE_FORMAT(event_date, '%Y-%m')
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
            FROM mysql_daw_events
            ORDER BY amount
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(4, $rows);
        $this->assertSame(1, (int) $rows[0]['tile']);
        $this->assertSame(1, (int) $rows[1]['tile']);
        $this->assertSame(2, (int) $rows[2]['tile']);
        $this->assertSame(2, (int) $rows[3]['tile']);
    }

    public function testFirstValueLastValue(): void
    {
        $stmt = $this->pdo->query("
            SELECT title, amount,
                   FIRST_VALUE(amount) OVER (ORDER BY amount) AS first_amt,
                   LAST_VALUE(amount) OVER (ORDER BY amount ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_amt
            FROM mysql_daw_events
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
            FROM mysql_daw_events
            ORDER BY category, amount
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(4, $rows);
        $this->assertSame('personal', $rows[0]['category']);
        $this->assertSame(1, (int) $rows[0]['rn']);
        $this->assertEqualsWithDelta(700.0, (float) $rows[0]['cat_total'], 0.01);
    }
}
