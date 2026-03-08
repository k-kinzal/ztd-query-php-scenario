<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\PostgreSQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;
use ZtdQuery\Config\UnsupportedSqlBehavior;
use ZtdQuery\Config\ZtdConfig;

/**
 * Tests diagnostic/utility SQL queries on PostgreSQL PDO ZTD.
 *
 * PostgreSQL diagnostic queries include:
 *   - SELECT from information_schema
 *   - SELECT from pg_catalog
 *   - EXPLAIN SELECT
 *
 * information_schema and pg_catalog are system tables, not user tables,
 * so they should not be affected by CTE rewriting.
 * @spec SPEC-6.4
 */
class PostgresDiagnosticQueriesTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS pg_diag_test');
        $raw->exec('CREATE TABLE pg_diag_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
    }

    protected function setUp(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Ignore,
        );
        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            config: $config,
        );

        $this->pdo->exec("INSERT INTO pg_diag_test (id, name, score) VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO pg_diag_test (id, name, score) VALUES (2, 'Bob', 80)");
    }

    /**
     * SELECT from information_schema.tables — schema introspection.
     *
     * information_schema is a system catalog, not a user table.
     * It should be accessible via ZTD without CTE rewriting.
     */
    public function testSelectInformationSchemaTables(): void
    {
        $stmt = $this->pdo->query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'pg_diag_test'");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('pg_diag_test', $rows[0]['table_name']);
    }

    /**
     * SELECT from information_schema.columns — column introspection.
     */
    public function testSelectInformationSchemaColumns(): void
    {
        $stmt = $this->pdo->query("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'pg_diag_test' ORDER BY ordinal_position");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertGreaterThanOrEqual(3, count($rows));
        $columnNames = array_column($rows, 'column_name');
        $this->assertContains('id', $columnNames);
        $this->assertContains('name', $columnNames);
        $this->assertContains('score', $columnNames);
    }

    /**
     * SELECT EXISTS() on shadow data.
     */
    public function testSelectExists(): void
    {
        $stmt = $this->pdo->query("SELECT EXISTS(SELECT 1 FROM pg_diag_test WHERE name = 'Alice') AS found");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        // PostgreSQL returns boolean 't' or 'f' for EXISTS
        $this->assertTrue((bool) $row['found']);
    }

    /**
     * SELECT with CASE expression.
     */
    public function testSelectCaseExpression(): void
    {
        $stmt = $this->pdo->query("SELECT name, CASE WHEN score >= 85 THEN 'high' ELSE 'low' END AS grade FROM pg_diag_test ORDER BY id");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('high', $rows[0]['grade']);
        $this->assertSame('low', $rows[1]['grade']);
    }

    /**
     * SELECT with inline subquery.
     */
    public function testSelectWithInlineSubquery(): void
    {
        $stmt = $this->pdo->query('SELECT name, (SELECT MAX(score) FROM pg_diag_test) AS max_score FROM pg_diag_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
        $this->assertSame(90, (int) $row['max_score']);
    }

    /**
     * SELECT COUNT(DISTINCT ...).
     */
    public function testSelectCountDistinct(): void
    {
        $this->pdo->exec("INSERT INTO pg_diag_test (id, name, score) VALUES (3, 'Alice', 85)");

        $stmt = $this->pdo->query('SELECT COUNT(DISTINCT name) AS cnt FROM pg_diag_test');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(2, (int) $row['cnt']);
    }

    /**
     * SELECT with GROUP BY and HAVING.
     */
    public function testSelectGroupByHaving(): void
    {
        $this->pdo->exec("INSERT INTO pg_diag_test (id, name, score) VALUES (3, 'Alice', 85)");

        $stmt = $this->pdo->query('SELECT name, COUNT(*) AS cnt FROM pg_diag_test GROUP BY name HAVING COUNT(*) > 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    /**
     * SELECT with COALESCE and NULLIF.
     */
    public function testSelectCoalesceAndNullif(): void
    {
        $this->pdo->exec("INSERT INTO pg_diag_test (id, name, score) VALUES (3, NULL, 0)");

        $stmt = $this->pdo->query("SELECT COALESCE(name, 'unknown') AS safe_name, NULLIF(score, 0) AS non_zero_score FROM pg_diag_test WHERE id = 3");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('unknown', $row['safe_name']);
        $this->assertNull($row['non_zero_score']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_diag_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(PostgreSQLContainer::getDsn(), 'test', 'test');
            $raw->exec('DROP TABLE IF EXISTS pg_diag_test');
        } catch (\Exception $e) {
        }
    }
}
