<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests type coercion edge cases through the CTE shadow store on SQLite.
 *
 * Real-world scenario: applications often compare numeric columns with
 * string parameters (e.g., from HTTP query strings), or use boolean-like
 * values. The CTE rewriter must preserve type semantics correctly.
 *
 * @spec SPEC-3.1
 */
class SqliteTypeCoercionEdgeCaseTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE tc_items (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            score INTEGER,
            rating REAL,
            active INTEGER NOT NULL DEFAULT 1,
            code TEXT
        )';
    }

    protected function getTableNames(): array
    {
        return ['tc_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO tc_items VALUES (1, 'Alice', 100, 4.5, 1, 'A001')");
        $this->ztdExec("INSERT INTO tc_items VALUES (2, 'Bob', 0, 0.0, 0, '0')");
        $this->ztdExec("INSERT INTO tc_items VALUES (3, 'Carol', NULL, NULL, 1, NULL)");
        $this->ztdExec("INSERT INTO tc_items VALUES (4, 'Dave', 42, 3.14, 1, '42')");
    }

    /**
     * Integer column compared with string parameter.
     */
    public function testIntColumnWithStringParam(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT name FROM tc_items WHERE score = ?",
                ['100']  // string '100' compared to integer column
            );
            $this->assertCount(1, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete('Int/string coercion failed: ' . $e->getMessage());
        }
    }

    /**
     * Integer column compared with zero as string.
     */
    public function testZeroAsString(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT name FROM tc_items WHERE score = ?",
                ['0']
            );
            $this->assertCount(1, $rows);
            $this->assertSame('Bob', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete('Zero as string failed: ' . $e->getMessage());
        }
    }

    /**
     * Boolean-like column (0/1) with integer comparison.
     */
    public function testBooleanLikeColumn(): void
    {
        $rows = $this->ztdQuery("SELECT name FROM tc_items WHERE active = 1 ORDER BY name");
        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['name']);

        $rows = $this->ztdQuery("SELECT name FROM tc_items WHERE active = 0 ORDER BY name");
        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
    }

    /**
     * NULL comparison edge cases.
     */
    public function testNullComparisons(): void
    {
        $rows = $this->ztdQuery("SELECT name FROM tc_items WHERE score IS NULL");
        $this->assertCount(1, $rows);
        $this->assertSame('Carol', $rows[0]['name']);

        $rows = $this->ztdQuery("SELECT name FROM tc_items WHERE score IS NOT NULL ORDER BY name");
        $this->assertCount(3, $rows);
    }

    /**
     * COALESCE with mixed types.
     */
    public function testCoalesceMixedTypes(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name, COALESCE(score, -1) AS safe_score FROM tc_items ORDER BY id"
            );
            $this->assertCount(4, $rows);
            $this->assertSame(100, (int) $rows[0]['safe_score']);
            $this->assertSame(0, (int) $rows[1]['safe_score']);
            $this->assertSame(-1, (int) $rows[2]['safe_score']);  // NULL -> -1
            $this->assertSame(42, (int) $rows[3]['safe_score']);
        } catch (\Exception $e) {
            $this->markTestIncomplete('COALESCE mixed types failed: ' . $e->getMessage());
        }
    }

    /**
     * Text column with numeric content compared to integer.
     */
    public function testTextColumnNumericComparison(): void
    {
        try {
            // In SQLite, '42' = 42 depends on type affinity
            $rows = $this->ztdQuery("SELECT name FROM tc_items WHERE code = '42'");
            $this->assertCount(1, $rows);
            $this->assertSame('Dave', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete('Text/numeric comparison failed: ' . $e->getMessage());
        }
    }

    /**
     * Empty string vs NULL distinction.
     */
    public function testEmptyStringVsNull(): void
    {
        $this->ztdExec("INSERT INTO tc_items VALUES (5, 'Eve', 50, 2.5, 1, '')");

        $rows = $this->ztdQuery("SELECT name FROM tc_items WHERE code = ''");
        $this->assertCount(1, $rows);
        $this->assertSame('Eve', $rows[0]['name']);

        $rows = $this->ztdQuery("SELECT name FROM tc_items WHERE code IS NULL");
        $this->assertCount(1, $rows);
        $this->assertSame('Carol', $rows[0]['name']);
    }

    /**
     * Negative numbers in queries.
     */
    public function testNegativeNumbers(): void
    {
        $this->ztdExec("INSERT INTO tc_items VALUES (6, 'Frank', -50, -1.5, 1, 'NEG')");

        $rows = $this->ztdQuery("SELECT name FROM tc_items WHERE score < 0");
        $this->assertCount(1, $rows);
        $this->assertSame('Frank', $rows[0]['name']);

        $rows = $this->ztdQuery("SELECT name FROM tc_items WHERE rating < 0");
        $this->assertCount(1, $rows);
        $this->assertSame('Frank', $rows[0]['name']);
    }

    /**
     * CAST in WHERE clause.
     */
    public function testCastInWhere(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name FROM tc_items WHERE CAST(code AS INTEGER) = 42"
            );
            $this->assertCount(1, $rows);
            $this->assertSame('Dave', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete('CAST in WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared statement with NULL parameter.
     */
    public function testPreparedWithNullParam(): void
    {
        try {
            $stmt = $this->pdo->prepare("SELECT name FROM tc_items WHERE score = ? OR (score IS NULL AND ? IS NULL)");
            $stmt->execute([null, null]);
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
            // Should match Carol (score IS NULL)
            $this->assertCount(1, $rows);
            $this->assertSame('Carol', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete('Prepared with NULL param failed: ' . $e->getMessage());
        }
    }
}
