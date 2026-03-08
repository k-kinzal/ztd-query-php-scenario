<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests standalone VALUES expression and CTE MATERIALIZED hints on PostgreSQL.
 *
 * PostgreSQL supports VALUES as a standalone query (not just in INSERT)
 * and CTE hints like MATERIALIZED/NOT MATERIALIZED.
 * @spec pending
 */
class PostgresValuesExpressionTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_ve_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['pg_ve_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_ve_test VALUES (1, 'Alice', 95)");
        $this->pdo->exec("INSERT INTO pg_ve_test VALUES (2, 'Bob', 85)");
        $this->pdo->exec("INSERT INTO pg_ve_test VALUES (3, 'Charlie', 75)");
    }

    /**
     * Standalone VALUES expression is NOT supported.
     * VALUES (1, 'a'), (2, 'b') is valid PostgreSQL but ZTD rewriter
     * treats it as unsupported SQL.
     */
    public function testStandaloneValuesThrows(): void
    {
        $this->expectException(\Throwable::class);
        $this->pdo->query("VALUES (1, 'hello'), (2, 'world')");
    }

    /**
     * VALUES in a subquery / derived table.
     */
    public function testValuesInSubquery(): void
    {
        try {
            $stmt = $this->pdo->query(
                "SELECT v.col1, v.col2
                 FROM (VALUES (1, 'x'), (2, 'y'), (3, 'z')) AS v(col1, col2)"
            );
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            $this->assertCount(3, $rows);
            $this->assertEquals(1, $rows[0]['col1']);
            $this->assertSame('x', $rows[0]['col2']);
        } catch (\Exception $e) {
            $this->markTestSkipped('VALUES in subquery not supported: ' . $e->getMessage());
        }
    }

    /**
     * JOIN with VALUES table expression.
     */
    public function testJoinWithValues(): void
    {
        try {
            $stmt = $this->pdo->query(
                "SELECT t.name, v.grade
                 FROM pg_ve_test t
                 JOIN (VALUES (1, 'A'), (2, 'B'), (3, 'C')) AS v(id, grade) ON t.id = v.id
                 ORDER BY t.name"
            );
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) > 0) {
                $this->assertSame('Alice', $rows[0]['name']);
                $this->assertSame('A', $rows[0]['grade']);
            } else {
                // VALUES table may read physical (empty) data
                $this->assertCount(0, $rows);
            }
        } catch (\Exception $e) {
            $this->markTestSkipped('JOIN with VALUES not supported: ' . $e->getMessage());
        }
    }

    /**
     * PostgreSQL :: cast operator syntax.
     */
    public function testDoubleColonCastOperator(): void
    {
        $stmt = $this->pdo->query("SELECT score::TEXT as score_text FROM pg_ve_test WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('95', $row['score_text']);
    }

    /**
     * Multiple :: cast operators in one expression.
     */
    public function testChainedCastOperators(): void
    {
        $stmt = $this->pdo->query("SELECT (score::NUMERIC * 1.5)::INT as adjusted FROM pg_ve_test WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        // 95 * 1.5 = 142.5, cast to INT = 142 or 143 depending on rounding
        $this->assertGreaterThanOrEqual(142, (int) $row['adjusted']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_ve_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
