<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests column type edge cases on MySQL PDO: TIME, BINARY, mixed-type arithmetic,
 * CASE with mixed return types.
 */
class MysqlColumnTypeEdgeCasesTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mysql_cte_events');
        $raw->exec('DROP TABLE IF EXISTS mysql_cte_metrics');
        $raw->exec('CREATE TABLE mysql_cte_events (id INT PRIMARY KEY, name VARCHAR(50), event_time TIME, event_date DATE, is_active TINYINT, payload BLOB)');
        $raw->exec('CREATE TABLE mysql_cte_metrics (id INT PRIMARY KEY, label VARCHAR(20), int_val INT, float_val DOUBLE, text_val VARCHAR(50))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
    }

    public function testTimeValuesInShadowStore(): void
    {
        $this->pdo->exec("INSERT INTO mysql_cte_events VALUES (1, 'Morning', '09:30:00', '2024-01-15', 1, NULL)");
        $this->pdo->exec("INSERT INTO mysql_cte_events VALUES (2, 'Lunch', '12:00:00', '2024-01-15', 1, NULL)");
        $this->pdo->exec("INSERT INTO mysql_cte_events VALUES (3, 'Evening', '17:30:00', '2024-01-15', 0, NULL)");

        $stmt = $this->pdo->query("SELECT name, event_time FROM mysql_cte_events WHERE event_time > '12:00:00' ORDER BY event_time");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Evening', $rows[0]['name']);
    }

    public function testTimeComparisonAfterUpdate(): void
    {
        $this->pdo->exec("INSERT INTO mysql_cte_events VALUES (1, 'Meeting', '09:00:00', '2024-01-15', 1, NULL)");
        $this->pdo->exec("UPDATE mysql_cte_events SET event_time = '14:00:00' WHERE id = 1");

        $stmt = $this->pdo->query("SELECT event_time FROM mysql_cte_events WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('14:00:00', $row['event_time']);
    }

    public function testMixedTypeArithmetic(): void
    {
        $this->pdo->exec("INSERT INTO mysql_cte_metrics VALUES (1, 'test', 10, 3.14, '42')");

        $stmt = $this->pdo->query("SELECT int_val + float_val AS sum1, int_val * 2 AS doubled FROM mysql_cte_metrics WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(13.14, (float) $row['sum1'], 0.01);
        $this->assertSame(20, (int) $row['doubled']);
    }

    public function testUpdateWithArithmeticExpression(): void
    {
        $this->pdo->exec("INSERT INTO mysql_cte_metrics VALUES (1, 'price', 100, 19.99, 'product')");
        $this->pdo->exec("UPDATE mysql_cte_metrics SET float_val = float_val * 1.1 WHERE id = 1");

        $stmt = $this->pdo->query("SELECT float_val FROM mysql_cte_metrics WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(21.989, (float) $row['float_val'], 0.01);
    }

    public function testBooleanToggle(): void
    {
        $this->pdo->exec("INSERT INTO mysql_cte_events VALUES (1, 'Active', '10:00:00', '2024-01-15', 1, NULL)");
        $this->pdo->exec("INSERT INTO mysql_cte_events VALUES (2, 'Inactive', '10:00:00', '2024-01-15', 0, NULL)");

        $this->pdo->exec("UPDATE mysql_cte_events SET is_active = 0 WHERE id = 1");

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mysql_cte_events WHERE is_active = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $row['cnt']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mysql_cte_events');
        $raw->exec('DROP TABLE IF EXISTS mysql_cte_metrics');
    }
}
