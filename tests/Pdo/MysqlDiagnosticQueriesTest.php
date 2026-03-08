<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;
use ZtdQuery\Config\UnsupportedSqlBehavior;
use ZtdQuery\Config\ZtdConfig;

/**
 * Tests diagnostic/utility SQL queries on MySQL PDO ZTD.
 *
 * MySQL diagnostic queries include:
 *   - SHOW TABLES, SHOW COLUMNS
 *   - EXPLAIN SELECT
 *   - SELECT from information_schema
 *
 * These may be unsupported by the SQL parser and require behavior rules.
 */
class MysqlDiagnosticQueriesTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
        $raw->exec('DROP TABLE IF EXISTS pdo_diag_test');
        $raw->exec('CREATE TABLE pdo_diag_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
    }

    protected function setUp(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Ignore,
        );
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            config: $config,
        );

        $this->pdo->exec("INSERT INTO pdo_diag_test (id, name, score) VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO pdo_diag_test (id, name, score) VALUES (2, 'Bob', 80)");
    }

    /**
     * SHOW TABLES — MySQL admin query.
     *
     * SHOW is not standard SQL and is likely unsupported by the CTE rewriter.
     * With Ignore behavior, it should be skipped silently.
     */
    public function testShowTablesSkippedWithIgnore(): void
    {
        // With Ignore behavior, unsupported SQL is skipped
        $result = $this->pdo->exec('SHOW TABLES');
        $this->assertIsInt($result);
    }

    /**
     * SHOW COLUMNS — schema introspection.
     */
    public function testShowColumnsSkippedWithIgnore(): void
    {
        $result = $this->pdo->exec('SHOW COLUMNS FROM pdo_diag_test');
        $this->assertIsInt($result);
    }

    /**
     * SHOW TABLES with Exception behavior throws.
     */
    public function testShowTablesThrowsWithExceptionBehavior(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Exception,
        );
        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            config: $config,
        );

        $this->expectException(\Throwable::class);
        $pdo->exec('SHOW TABLES');
    }

    /**
     * SELECT EXISTS() on shadow data.
     */
    public function testSelectExists(): void
    {
        $stmt = $this->pdo->query("SELECT EXISTS(SELECT 1 FROM pdo_diag_test WHERE name = 'Alice') AS found");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(1, (int) $row['found']);
    }

    /**
     * SELECT with CASE expression.
     */
    public function testSelectCaseExpression(): void
    {
        $stmt = $this->pdo->query("SELECT name, CASE WHEN score >= 85 THEN 'high' ELSE 'low' END AS grade FROM pdo_diag_test ORDER BY id");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('high', $rows[0]['grade']);
        $this->assertSame('low', $rows[1]['grade']);
    }

    /**
     * SELECT with inline subquery.
     */
    public function testSelectWithInlineSubquery(): void
    {
        $stmt = $this->pdo->query('SELECT name, (SELECT MAX(score) FROM pdo_diag_test) AS max_score FROM pdo_diag_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
        $this->assertSame(90, (int) $row['max_score']);
    }

    /**
     * SELECT COUNT(DISTINCT ...) on shadow data.
     */
    public function testSelectCountDistinct(): void
    {
        $this->pdo->exec("INSERT INTO pdo_diag_test (id, name, score) VALUES (3, 'Alice', 85)");

        $stmt = $this->pdo->query('SELECT COUNT(DISTINCT name) AS cnt FROM pdo_diag_test');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(2, (int) $row['cnt']);
    }

    /**
     * SELECT with GROUP BY and HAVING.
     */
    public function testSelectGroupByHaving(): void
    {
        $this->pdo->exec("INSERT INTO pdo_diag_test (id, name, score) VALUES (3, 'Alice', 85)");

        $stmt = $this->pdo->query('SELECT name, COUNT(*) AS cnt FROM pdo_diag_test GROUP BY name HAVING COUNT(*) > 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    /**
     * SELECT with IF() — MySQL-specific.
     */
    public function testSelectWithIf(): void
    {
        $stmt = $this->pdo->query("SELECT name, IF(score >= 85, 'pass', 'fail') AS result FROM pdo_diag_test ORDER BY id");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('pass', $rows[0]['result']);
        $this->assertSame('fail', $rows[1]['result']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $stmt = $this->pdo->query("SELECT EXISTS(SELECT 1 FROM pdo_diag_test WHERE name = 'Alice') AS found");
        $this->assertSame(1, (int) $stmt->fetch(PDO::FETCH_ASSOC)['found']);

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query("SELECT EXISTS(SELECT 1 FROM pdo_diag_test WHERE name = 'Alice') AS found");
        $this->assertSame(0, (int) $stmt->fetch(PDO::FETCH_ASSOC)['found']);
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
            $raw->exec('DROP TABLE IF EXISTS pdo_diag_test');
        } catch (\Exception $e) {
        }
    }
}
