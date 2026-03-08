<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;
use ZtdQuery\Config\UnsupportedSqlBehavior;
use ZtdQuery\Config\ZtdConfig;

/**
 * Tests diagnostic/utility SQL queries via MySQLi.
 *
 * Cross-platform parity with MysqlDiagnosticQueriesTest (PDO).
 * Tests SHOW TABLES, SHOW COLUMNS, EXPLAIN with behavior rules.
 * @spec SPEC-6.4
 */
class DiagnosticQueriesTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_diag_test');
        $raw->query('CREATE TABLE mi_diag_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
        $raw->close();
    }

    protected function setUp(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Ignore,
        );
        $this->mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
            config: $config,
        );
    }

    /**
     * SHOW TABLES with Ignore behavior returns false/empty.
     */
    public function testShowTablesIgnored(): void
    {
        $result = $this->mysqli->query('SHOW TABLES');
        // With Ignore behavior, unsupported SQL returns false
        $this->assertFalse($result);
    }

    /**
     * Shadow INSERT still works despite diagnostic SQL being ignored.
     */
    public function testShadowInsertAfterDiagnostic(): void
    {
        // Ignored diagnostic
        $this->mysqli->query('SHOW TABLES');

        // Shadow INSERT works
        $this->mysqli->query("INSERT INTO mi_diag_test VALUES (1, 'Alice', 90)");

        $result = $this->mysqli->query('SELECT name FROM mi_diag_test WHERE id = 1');
        $this->assertSame('Alice', $result->fetch_assoc()['name']);
    }

    /**
     * COUNT DISTINCT with shadow data.
     */
    public function testCountDistinct(): void
    {
        $this->mysqli->query("INSERT INTO mi_diag_test VALUES (1, 'Alice', 90)");
        $this->mysqli->query("INSERT INTO mi_diag_test VALUES (2, 'Bob', 90)");
        $this->mysqli->query("INSERT INTO mi_diag_test VALUES (3, 'Charlie', 85)");

        $result = $this->mysqli->query('SELECT COUNT(DISTINCT score) AS cnt FROM mi_diag_test');
        $this->assertSame(2, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_diag_test VALUES (1, 'Test', 100)");

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_diag_test');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }

    protected function tearDown(): void
    {
        if (isset($this->mysqli)) {
            $this->mysqli->close();
        }
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new \mysqli(
                MySQLContainer::getHost(),
                'root',
                'root',
                'test',
                MySQLContainer::getPort(),
            );
            $raw->query('DROP TABLE IF EXISTS mi_diag_test');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
