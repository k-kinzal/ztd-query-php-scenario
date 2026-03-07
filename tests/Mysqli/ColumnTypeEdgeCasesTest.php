<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests column type edge cases on MySQLi: TIME, BOOLEAN, mixed-type arithmetic,
 * UPDATE with arithmetic expression.
 */
class ColumnTypeEdgeCasesTest extends TestCase
{
    private ZtdMysqli $mysqli;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS mi_cte_events');
        $raw->query('DROP TABLE IF EXISTS mi_cte_metrics');
        $raw->query('CREATE TABLE mi_cte_events (id INT PRIMARY KEY, name VARCHAR(50), event_time TIME, event_date DATE, is_active TINYINT)');
        $raw->query('CREATE TABLE mi_cte_metrics (id INT PRIMARY KEY, label VARCHAR(20), int_val INT, float_val DOUBLE, text_val VARCHAR(50))');
        $raw->close();
    }

    protected function setUp(): void
    {
        $this->mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
    }

    public function testTimeValuesInShadowStore(): void
    {
        $this->mysqli->query("INSERT INTO mi_cte_events VALUES (1, 'Morning', '09:30:00', '2024-01-15', 1)");
        $this->mysqli->query("INSERT INTO mi_cte_events VALUES (2, 'Lunch', '12:00:00', '2024-01-15', 1)");
        $this->mysqli->query("INSERT INTO mi_cte_events VALUES (3, 'Evening', '17:30:00', '2024-01-15', 0)");

        $result = $this->mysqli->query("SELECT name, event_time FROM mi_cte_events WHERE event_time > '12:00:00' ORDER BY event_time");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Evening', $rows[0]['name']);
    }

    public function testMixedTypeArithmetic(): void
    {
        $this->mysqli->query("INSERT INTO mi_cte_metrics VALUES (1, 'test', 10, 3.14, '42')");

        $result = $this->mysqli->query("SELECT int_val + float_val AS sum1, int_val * 2 AS doubled FROM mi_cte_metrics WHERE id = 1");
        $row = $result->fetch_assoc();
        $this->assertEqualsWithDelta(13.14, (float) $row['sum1'], 0.01);
        $this->assertSame(20, (int) $row['doubled']);
    }

    public function testUpdateWithArithmeticExpression(): void
    {
        $this->mysqli->query("INSERT INTO mi_cte_metrics VALUES (1, 'price', 100, 19.99, 'product')");
        $this->mysqli->query("UPDATE mi_cte_metrics SET float_val = float_val * 1.1 WHERE id = 1");

        $result = $this->mysqli->query("SELECT float_val FROM mi_cte_metrics WHERE id = 1");
        $row = $result->fetch_assoc();
        $this->assertEqualsWithDelta(21.989, (float) $row['float_val'], 0.01);
    }

    public function testBooleanToggle(): void
    {
        $this->mysqli->query("INSERT INTO mi_cte_events VALUES (1, 'Active', '10:00:00', '2024-01-15', 1)");
        $this->mysqli->query("UPDATE mi_cte_events SET is_active = 0 WHERE id = 1");

        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_cte_events WHERE is_active = 1");
        $row = $result->fetch_assoc();
        $this->assertSame(0, (int) $row['cnt']);
    }

    protected function tearDown(): void
    {
        $this->mysqli->close();
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS mi_cte_events');
        $raw->query('DROP TABLE IF EXISTS mi_cte_metrics');
        $raw->close();
    }
}
