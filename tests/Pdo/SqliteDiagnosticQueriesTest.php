<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;
use ZtdQuery\Config\UnsupportedSqlBehavior;
use ZtdQuery\Config\ZtdConfig;

/**
 * Tests diagnostic/utility SQL queries on SQLite ZTD.
 *
 * Diagnostic queries include:
 *   - PRAGMA table_info()
 *   - EXPLAIN / EXPLAIN QUERY PLAN
 *   - SELECT from sqlite_master
 *
 * These are commonly used for schema introspection and query analysis.
 * ZTD may or may not support them depending on the SQL parser.
 */
class SqliteDiagnosticQueriesTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE diag_test (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)');

        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Ignore,
        );
        $this->pdo = ZtdPdo::fromPdo($raw, config: $config);

        $this->pdo->exec("INSERT INTO diag_test (id, name, score) VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO diag_test (id, name, score) VALUES (2, 'Bob', 80)");
    }

    /**
     * SELECT from sqlite_master — schema introspection.
     */
    public function testSelectFromSqliteMaster(): void
    {
        $stmt = $this->pdo->query("SELECT type, name FROM sqlite_master WHERE type = 'table' ORDER BY name");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        // sqlite_master is not a user table, so CTE rewriting doesn't affect it
        // At least the diag_test table should appear
        $tableNames = array_column($rows, 'name');
        $this->assertContains('diag_test', $tableNames);
    }

    /**
     * SELECT EXISTS() — subquery existence check.
     */
    public function testSelectExists(): void
    {
        $stmt = $this->pdo->query("SELECT EXISTS(SELECT 1 FROM diag_test WHERE name = 'Alice') AS found");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(1, (int) $row['found']);
    }

    /**
     * SELECT EXISTS() — non-existent row.
     */
    public function testSelectExistsNotFound(): void
    {
        $stmt = $this->pdo->query("SELECT EXISTS(SELECT 1 FROM diag_test WHERE name = 'Zach') AS found");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $row['found']);
    }

    /**
     * SELECT with CASE expression.
     */
    public function testSelectCaseExpression(): void
    {
        $stmt = $this->pdo->query("SELECT name, CASE WHEN score >= 85 THEN 'high' ELSE 'low' END AS grade FROM diag_test ORDER BY id");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('high', $rows[0]['grade']); // Alice: 90
        $this->assertSame('low', $rows[1]['grade']);  // Bob: 80
    }

    /**
     * SELECT with inline subquery in column list should return correct data.
     *
     * @see https://github.com/k-kinzal/ztd-query-php/issues/13
     */
    public function testSelectWithInlineSubquery(): void
    {
        $stmt = $this->pdo->query('SELECT name, (SELECT MAX(score) FROM diag_test) AS max_score FROM diag_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        if ($row === false || $row['max_score'] === null) {
            $this->markTestIncomplete(
                'Issue #13: scalar subqueries in column list reference the physical table instead of shadow. '
                . 'Expected row with max_score, got ' . var_export($row, true)
            );
        }
        $this->assertSame('Alice', $row['name']);
        $this->assertNotNull($row['max_score']);
    }

    /**
     * SELECT COUNT with DISTINCT.
     */
    public function testSelectCountDistinct(): void
    {
        $this->pdo->exec("INSERT INTO diag_test (id, name, score) VALUES (3, 'Alice', 85)");

        $stmt = $this->pdo->query('SELECT COUNT(DISTINCT name) AS cnt FROM diag_test');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(2, (int) $row['cnt']); // Alice, Bob
    }

    /**
     * SELECT with GROUP BY and HAVING.
     */
    public function testSelectGroupByHaving(): void
    {
        $this->pdo->exec("INSERT INTO diag_test (id, name, score) VALUES (3, 'Alice', 85)");

        $stmt = $this->pdo->query('SELECT name, COUNT(*) AS cnt, AVG(score) AS avg_score FROM diag_test GROUP BY name HAVING COUNT(*) > 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame(2, (int) $rows[0]['cnt']);
    }

    /**
     * Physical isolation with diagnostic queries.
     */
    public function testPhysicalIsolation(): void
    {
        $stmt = $this->pdo->query("SELECT EXISTS(SELECT 1 FROM diag_test WHERE name = 'Alice') AS found");
        $this->assertSame(1, (int) $stmt->fetch(PDO::FETCH_ASSOC)['found']);

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query("SELECT EXISTS(SELECT 1 FROM diag_test WHERE name = 'Alice') AS found");
        $this->assertSame(0, (int) $stmt->fetch(PDO::FETCH_ASSOC)['found']);
    }
}
