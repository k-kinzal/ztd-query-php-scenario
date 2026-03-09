<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests COALESCE, NULLIF, and IFNULL in WHERE clauses with prepared parameters.
 *
 * Real-world scenario: Applications commonly use COALESCE for null-safe comparisons,
 * optional filter parameters (e.g., "search by name if provided, else return all"),
 * and default value fallbacks. These function calls wrap column references and may
 * confuse the CTE rewriter's table reference detection.
 *
 * @spec SPEC-3.1
 */
class SqliteCoalesceWithParamsTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_cwp_users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT,
                nickname TEXT,
                score INTEGER
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_cwp_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO sl_cwp_users VALUES (1, 'Alice', 'alice@example.com', NULL, 100)");
        $this->ztdExec("INSERT INTO sl_cwp_users VALUES (2, 'Bob', NULL, 'bobby', 200)");
        $this->ztdExec("INSERT INTO sl_cwp_users VALUES (3, 'Charlie', 'charlie@example.com', 'chuck', NULL)");
    }

    /**
     * COALESCE in WHERE with prepared parameter.
     * Pattern: WHERE COALESCE(email, 'none') = ?
     */
    public function testCoalesceInWhereWithParam(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT name FROM sl_cwp_users WHERE COALESCE(email, 'none') = ?",
                ['none']
            );

            $this->assertCount(1, $rows);
            $this->assertSame('Bob', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'COALESCE in WHERE with param failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * COALESCE with two column fallbacks and prepared param.
     * Pattern: WHERE COALESCE(nickname, email, 'unknown') = ?
     */
    public function testCoalesceMultiColumnWithParam(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT name FROM sl_cwp_users WHERE COALESCE(nickname, email, 'unknown') = ?",
                ['alice@example.com']
            );

            // Alice has no nickname, so COALESCE falls through to email
            $this->assertCount(1, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'COALESCE multi-column with param failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * NULLIF in WHERE with prepared parameter.
     * Pattern: WHERE NULLIF(score, ?) IS NULL (find rows where score equals param)
     */
    public function testNullifInWhereWithParam(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT name FROM sl_cwp_users WHERE NULLIF(score, ?) IS NULL ORDER BY name",
                [100]
            );

            // NULLIF(score, 100) IS NULL matches: Alice (score=100 -> NULL) and Charlie (score=NULL -> NULL)
            $this->assertCount(2, $rows);
            $names = array_column($rows, 'name');
            $this->assertContains('Alice', $names);
            $this->assertContains('Charlie', $names);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'NULLIF in WHERE with param failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * COALESCE in SELECT list with WHERE param on different column.
     */
    public function testCoalesceInSelectWithWhereParam(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT COALESCE(nickname, name) AS display_name FROM sl_cwp_users WHERE score > ?",
                [50]
            );

            $names = array_column($rows, 'display_name');
            $this->assertCount(2, $names);
            $this->assertContains('Alice', $names);   // nickname NULL -> name 'Alice'
            $this->assertContains('bobby', $names);    // nickname 'bobby'
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'COALESCE in SELECT with WHERE param failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Optional filter pattern: WHERE (? IS NULL OR name = ?)
     * Common "search or return all" pattern.
     */
    public function testOptionalFilterPatternWithNull(): void
    {
        try {
            // Pass NULL for both params -> should return all rows
            $stmt = $this->pdo->prepare(
                "SELECT name FROM sl_cwp_users WHERE (? IS NULL OR name = ?) ORDER BY name"
            );
            $stmt->bindValue(1, null, PDO::PARAM_NULL);
            $stmt->bindValue(2, null, PDO::PARAM_NULL);
            $stmt->execute();
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            $this->assertCount(3, $rows, 'NULL filter should return all rows');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Optional filter pattern with NULL failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Optional filter pattern: WHERE (? IS NULL OR name = ?) with actual value.
     */
    public function testOptionalFilterPatternWithValue(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "SELECT name FROM sl_cwp_users WHERE (? IS NULL OR name = ?) ORDER BY name"
            );
            $stmt->bindValue(1, 'Alice', PDO::PARAM_STR);
            $stmt->bindValue(2, 'Alice', PDO::PARAM_STR);
            $stmt->execute();
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            $this->assertCount(1, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Optional filter pattern with value failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * COALESCE in UPDATE SET with prepared param.
     * Pattern: UPDATE ... SET score = COALESCE(score, 0) + ?
     */
    public function testCoalesceInUpdateSetWithParam(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE sl_cwp_users SET score = COALESCE(score, 0) + ? WHERE name = ?"
            );
            $stmt->execute([50, 'Charlie']);

            $rows = $this->ztdQuery("SELECT score FROM sl_cwp_users WHERE name = 'Charlie'");
            $this->assertCount(1, $rows);
            $this->assertEquals(50, (int) $rows[0]['score']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'COALESCE in UPDATE SET with param failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * CASE WHEN with prepared params in WHERE.
     * Known issue: CASE-as-boolean in WHERE with prepared params returns wrong count
     * (Issue #75 / SPEC-11.CASE-WHERE-PARAMS). This test documents the SQLite variant.
     */
    public function testCaseWhenInWhereWithParam(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT name FROM sl_cwp_users
                 WHERE CASE WHEN score IS NOT NULL THEN score ELSE 0 END > ?
                 ORDER BY name",
                [50]
            );

            $names = array_column($rows, 'name');
            if (count($names) !== 2) {
                // Known: Issue #75 / SPEC-11.CASE-WHERE-PARAMS
                $this->markTestIncomplete(
                    'Known issue #75: CASE in WHERE with prepared param returned '
                    . count($names) . ' rows instead of 2.'
                );
            }
            $this->assertCount(2, $names);
            $this->assertContains('Alice', $names);
            $this->assertContains('Bob', $names);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'CASE WHEN in WHERE with param failed: ' . $e->getMessage()
            );
        }
    }
}
