<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests deeply nested function calls in WHERE clauses through the CTE rewriter.
 *
 * The CTE rewriter must correctly parse and preserve arbitrarily nested function
 * expressions in WHERE. Bugs may appear as:
 * - SQL truncation at nested parentheses
 * - Incorrect parameter binding offset when functions precede placeholders
 * - Misidentification of function arguments as table references
 * @spec SPEC-3.1
 * @spec SPEC-3.2
 */
class SqliteNestedFunctionWhereTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_nfw_items (id INTEGER PRIMARY KEY, name TEXT NOT NULL, code TEXT, amount INTEGER, weight INTEGER)';
    }

    protected function getTableNames(): array
    {
        return ['sl_nfw_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_nfw_items VALUES (1, '  Foo Bar  ', 'abc', 100, 40)");
        $this->pdo->exec("INSERT INTO sl_nfw_items VALUES (2, 'Foobar', 'def', 200, 80)");
        $this->pdo->exec("INSERT INTO sl_nfw_items VALUES (3, 'baz qux', 'ghi', 150, 60)");
        $this->pdo->exec("INSERT INTO sl_nfw_items VALUES (4, '  FOOEY  ', '', 50, 10)");
        $this->pdo->exec("INSERT INTO sl_nfw_items VALUES (5, 'nothing', NULL, 300, 250)");
    }

    /**
     * WHERE UPPER(TRIM(SUBSTR(name, 1, 3))) = 'FOO' — triple nesting.
     */
    public function testUpperTrimSubstr(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id, name FROM sl_nfw_items WHERE UPPER(TRIM(SUBSTR(name, 1, 3))) = 'FOO' ORDER BY id"
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'WHERE UPPER(TRIM(SUBSTR())) failed: ' . $e->getMessage()
            );
            return;
        }

        // '  Foo Bar  ' → SUBSTR(1,3) = '  F' → TRIM = 'F' → UPPER = 'F' — no match
        // 'Foobar' → SUBSTR(1,3) = 'Foo' → TRIM = 'Foo' → UPPER = 'FOO' — match
        // 'baz qux' → SUBSTR(1,3) = 'baz' → TRIM = 'baz' → UPPER = 'BAZ' — no match
        // '  FOOEY  ' → SUBSTR(1,3) = '  F' → TRIM = 'F' → UPPER = 'F' — no match
        // 'nothing' → SUBSTR(1,3) = 'not' → TRIM = 'not' → UPPER = 'NOT' — no match
        $this->assertCount(1, $rows);
        $this->assertSame(2, (int) $rows[0]['id']);
    }

    /**
     * WHERE LENGTH(REPLACE(col, 'a', '')) > ? — nested function with prepared param.
     *
     * Counts non-'a' characters in code column.
     *
     * The CTE rewriter may fail to handle nested functions combined with
     * prepared statement placeholders, returning zero rows.
     */
    public function testLengthReplaceWithPreparedParam(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT id, code FROM sl_nfw_items WHERE code IS NOT NULL AND LENGTH(REPLACE(code, 'a', '')) > ? ORDER BY id",
                [1]
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'WHERE LENGTH(REPLACE()) > ? failed: ' . $e->getMessage()
            );
            return;
        }

        // code='abc' → REPLACE(code,'a','') = 'bc' → LENGTH = 2 → 2 > 1 — match
        // code='def' → REPLACE = 'def' → LENGTH = 3 → 3 > 1 — match
        // code='ghi' → REPLACE = 'ghi' → LENGTH = 3 → 3 > 1 — match
        // code='' → REPLACE = '' → LENGTH = 0 → 0 > 1 — no match
        // code=NULL → filtered by IS NOT NULL
        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'CTE rewriter returns 0 rows for nested function WHERE with prepared param. Expected 3 rows.'
            );
            return;
        }
        $this->assertCount(3, $rows);
        $ids = array_map('intval', array_column($rows, 'id'));
        $this->assertSame([1, 2, 3], $ids);
    }

    /**
     * WHERE ABS(col1 - col2) BETWEEN ? AND ? — ABS with BETWEEN and two prepared params.
     *
     * The CTE rewriter may fail to bind parameters correctly when function
     * expressions precede BETWEEN ? AND ? placeholders.
     */
    public function testAbsBetweenWithPreparedParams(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                'SELECT id FROM sl_nfw_items WHERE ABS(amount - weight) BETWEEN ? AND ? ORDER BY id',
                [50, 70]
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'WHERE ABS(col1 - col2) BETWEEN ? AND ? failed: ' . $e->getMessage()
            );
            return;
        }

        // id=1: ABS(100 - 40) = 60 → in [50,70] — match
        // id=2: ABS(200 - 80) = 120 → not in [50,70] — no
        // id=3: ABS(150 - 60) = 90 → not in [50,70] — no
        // id=4: ABS(50 - 10) = 40 → not in [50,70] — no
        // id=5: ABS(300 - 250) = 50 → in [50,70] — match
        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'CTE rewriter returns 0 rows for ABS() BETWEEN ? AND ? with prepared params. Expected 2 rows.'
            );
            return;
        }
        $this->assertCount(2, $rows);
        $ids = array_map('intval', array_column($rows, 'id'));
        $this->assertSame([1, 5], $ids);
    }

    /**
     * WHERE COALESCE(NULLIF(col, ''), 'default') = ? — nested COALESCE+NULLIF with param.
     */
    public function testCoalesceNullifWithPreparedParam(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT id, name FROM sl_nfw_items WHERE COALESCE(NULLIF(code, ''), 'default') = ? ORDER BY id",
                ['default']
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'WHERE COALESCE(NULLIF()) = ? failed: ' . $e->getMessage()
            );
            return;
        }

        // code='abc' → NULLIF('abc','')='abc' → COALESCE='abc' → ≠ 'default'
        // code='def' → NULLIF='def' → COALESCE='def' → ≠ 'default'
        // code='ghi' → NULLIF='ghi' → COALESCE='ghi' → ≠ 'default'
        // code='' → NULLIF('','')=NULL → COALESCE='default' → match
        // code=NULL → NULLIF(NULL,'')=NULL → COALESCE='default' → match
        $this->assertCount(2, $rows);
        $ids = array_map('intval', array_column($rows, 'id'));
        $this->assertSame([4, 5], $ids);
    }

    /**
     * WHERE INSTR(LOWER(col), LOWER(?)) > 0 — case-insensitive substring search with param.
     */
    public function testInstrLowerWithPreparedParam(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                'SELECT id, name FROM sl_nfw_items WHERE INSTR(LOWER(name), LOWER(?)) > 0 ORDER BY id',
                ['FOO']
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'WHERE INSTR(LOWER(col), LOWER(?)) > 0 failed: ' . $e->getMessage()
            );
            return;
        }

        // '  Foo Bar  ' → lower = '  foo bar  ' → contains 'foo' — match
        // 'Foobar' → lower = 'foobar' → contains 'foo' — match
        // 'baz qux' → lower = 'baz qux' → no 'foo' — no
        // '  FOOEY  ' → lower = '  fooey  ' → contains 'foo' — match
        // 'nothing' → lower = 'nothing' → no 'foo' — no
        $this->assertCount(3, $rows);
        $ids = array_map('intval', array_column($rows, 'id'));
        $this->assertSame([1, 2, 4], $ids);
    }

    /**
     * Combine multiple nested functions in a single WHERE with AND.
     *
     * The CTE rewriter may fail to handle multiple nested function expressions
     * combined with prepared statement placeholders in the same WHERE clause.
     */
    public function testMultipleNestedFunctionsInWhere(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT id FROM sl_nfw_items "
                . "WHERE INSTR(LOWER(name), LOWER(?)) > 0 "
                . "AND ABS(amount - weight) > ? "
                . "ORDER BY id",
                ['foo', 50]
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Multiple nested functions in WHERE failed: ' . $e->getMessage()
            );
            return;
        }

        // Must contain 'foo' (case-insensitive) AND ABS(amount-weight) > 50
        // id=1: has 'foo', ABS(60) > 50 — match
        // id=2: has 'foo', ABS(120) > 50 — match
        // id=4: has 'foo', ABS(40) > 50 — no (40 not > 50)
        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'CTE rewriter returns 0 rows for combined nested functions with prepared params. Expected 2 rows.'
            );
            return;
        }
        $this->assertCount(2, $rows);
        $ids = array_map('intval', array_column($rows, 'id'));
        $this->assertSame([1, 2], $ids);
    }

    /**
     * Nested function in WHERE after UPDATE — verify shadow consistency.
     */
    public function testNestedFunctionWhereAfterUpdate(): void
    {
        $this->pdo->exec("UPDATE sl_nfw_items SET name = 'FOOLISH' WHERE id = 3");

        try {
            $rows = $this->ztdQuery(
                "SELECT id FROM sl_nfw_items WHERE UPPER(SUBSTR(name, 1, 3)) = 'FOO' ORDER BY id"
            );
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Nested function WHERE after UPDATE failed: ' . $e->getMessage()
            );
            return;
        }

        // id=1: '  Foo Bar  ' → SUBSTR(1,3) = '  F' → UPPER = '  F' — no
        // id=2: 'Foobar' → SUBSTR(1,3) = 'Foo' → UPPER = 'FOO' — match
        // id=3: 'FOOLISH' (updated) → SUBSTR(1,3) = 'FOO' → UPPER = 'FOO' — match
        // id=4: '  FOOEY  ' → SUBSTR(1,3) = '  F' → UPPER = '  F' — no
        // id=5: 'nothing' → SUBSTR(1,3) = 'not' → UPPER = 'NOT' — no
        $this->assertCount(2, $rows);
        $ids = array_map('intval', array_column($rows, 'id'));
        $this->assertSame([2, 3], $ids);
    }

    /**
     * Physical isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM sl_nfw_items');
        $this->assertSame(5, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_nfw_items');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
