<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests NULL-safe comparison operators and NULL-handling functions
 * through the SQLite PDO CTE shadow store.
 *
 * SQLite uses IS/IS NOT for null-safe comparisons.
 * COALESCE and NULLIF must preserve semantics through CTE rewriting.
 *
 * @spec SPEC-3.1, SPEC-10.3
 */
class SqliteNullSafeComparisonTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_nsc (
            id INTEGER PRIMARY KEY,
            name TEXT,
            manager_id INTEGER,
            bonus REAL,
            dept TEXT
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_nsc'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_nsc VALUES (1, 'Alice', NULL, 100.00, 'Engineering')");
        $this->pdo->exec("INSERT INTO sl_nsc VALUES (2, 'Bob', 1, NULL, 'Engineering')");
        $this->pdo->exec("INSERT INTO sl_nsc VALUES (3, 'Carol', NULL, 200.00, NULL)");
        $this->pdo->exec("INSERT INTO sl_nsc VALUES (4, 'Dave', 1, 150.00, 'Sales')");
        $this->pdo->exec("INSERT INTO sl_nsc VALUES (5, 'Eve', NULL, NULL, NULL)");
    }

    /**
     * IS operator: NULL IS NULL = TRUE in SQLite.
     */
    public function testIsOperatorBothNull(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, name FROM sl_nsc WHERE manager_id IS bonus ORDER BY id"
        );
        $names = array_column($rows, 'name');
        $this->assertContains('Eve', $names, 'NULL IS NULL should match');
    }

    /**
     * IS with NULL literal.
     */
    public function testIsNull(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, name FROM sl_nsc WHERE manager_id IS NULL ORDER BY id"
        );
        $this->assertCount(3, $rows);
        $names = array_column($rows, 'name');
        $this->assertContains('Alice', $names);
        $this->assertContains('Carol', $names);
        $this->assertContains('Eve', $names);
    }

    /**
     * IS NOT operator.
     */
    public function testIsNot(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name FROM sl_nsc WHERE dept IS NOT 'Engineering' ORDER BY name"
        );
        // Carol (NULL), Dave (Sales), Eve (NULL)
        $this->assertCount(3, $rows);
        $names = array_column($rows, 'name');
        $this->assertContains('Carol', $names);
        $this->assertContains('Dave', $names);
        $this->assertContains('Eve', $names);
    }

    /**
     * COALESCE in SELECT list.
     */
    public function testCoalesceInSelect(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, COALESCE(dept, 'Unassigned') AS dept_name
             FROM sl_nsc ORDER BY id"
        );
        $this->assertCount(5, $rows);
        $this->assertSame('Engineering', $rows[0]['dept_name']);
        $this->assertSame('Unassigned', $rows[2]['dept_name']);
        $this->assertSame('Unassigned', $rows[4]['dept_name']);
    }

    /**
     * COALESCE in WHERE clause.
     */
    public function testCoalesceInWhere(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name FROM sl_nsc
             WHERE COALESCE(bonus, 0) > 100
             ORDER BY name"
        );
        $this->assertCount(2, $rows);
        $names = array_column($rows, 'name');
        $this->assertContains('Carol', $names);
        $this->assertContains('Dave', $names);
    }

    /**
     * NULLIF.
     */
    public function testNullif(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, NULLIF(dept, 'Engineering') AS non_eng_dept
             FROM sl_nsc ORDER BY id"
        );
        $this->assertNull($rows[0]['non_eng_dept']);
        $this->assertNull($rows[1]['non_eng_dept']);
        $this->assertSame('Sales', $rows[3]['non_eng_dept']);
    }

    /**
     * IIF (SQLite-specific conditional).
     */
    public function testIif(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, IIF(bonus IS NOT NULL, bonus, -1) AS bonus_val
             FROM sl_nsc ORDER BY id"
        );
        $this->assertCount(5, $rows);
        $this->assertEquals(100.00, (float) $rows[0]['bonus_val']);
        $this->assertEquals(-1, (float) $rows[1]['bonus_val']);
        $this->assertEquals(-1, (float) $rows[4]['bonus_val']);
    }

    /**
     * COALESCE with multiple fallbacks.
     */
    public function testCoalesceMultipleFallbacks(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, COALESCE(bonus, manager_id, -999) AS first_non_null
             FROM sl_nsc ORDER BY id"
        );
        $this->assertEquals(-999, (float) $rows[4]['first_non_null']);
        $this->assertEquals(1, (float) $rows[1]['first_non_null']);
    }

    /**
     * COALESCE in GROUP BY.
     */
    public function testCoalesceInGroupBy(): void
    {
        $rows = $this->ztdQuery(
            "SELECT COALESCE(dept, 'None') AS dept_group, COUNT(*) AS cnt
             FROM sl_nsc
             GROUP BY COALESCE(dept, 'None')
             ORDER BY dept_group"
        );
        $this->assertCount(3, $rows);
        $deptCounts = [];
        foreach ($rows as $r) {
            $deptCounts[$r['dept_group']] = (int) $r['cnt'];
        }
        $this->assertSame(2, $deptCounts['Engineering']);
        $this->assertSame(2, $deptCounts['None']);
        $this->assertSame(1, $deptCounts['Sales']);
    }

    /**
     * UPDATE using COALESCE to set defaults for NULL values.
     */
    public function testUpdateWithCoalesce(): void
    {
        $this->pdo->exec(
            "UPDATE sl_nsc SET bonus = COALESCE(bonus, 50.00) WHERE bonus IS NULL"
        );
        $rows = $this->ztdQuery("SELECT id, bonus FROM sl_nsc ORDER BY id");
        $this->assertEquals(50.00, (float) $rows[1]['bonus']);
        $this->assertEquals(50.00, (float) $rows[4]['bonus']);
        $this->assertEquals(100.00, (float) $rows[0]['bonus']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_nsc');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
