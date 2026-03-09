<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests NULL-safe comparison operators and NULL-handling functions
 * through the PostgreSQL PDO CTE shadow store.
 *
 * PostgreSQL's IS NOT DISTINCT FROM / IS DISTINCT FROM operators and
 * COALESCE/NULLIF have special NULL semantics that the CTE rewriter must preserve.
 *
 * @spec SPEC-3.1, SPEC-10.2
 */
class PostgresNullSafeComparisonTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_nsc (
            id INT PRIMARY KEY,
            name VARCHAR(50),
            manager_id INT,
            bonus DECIMAL(10,2),
            dept VARCHAR(30)
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_nsc'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_nsc VALUES (1, 'Alice', NULL, 100.00, 'Engineering')");
        $this->pdo->exec("INSERT INTO pg_nsc VALUES (2, 'Bob', 1, NULL, 'Engineering')");
        $this->pdo->exec("INSERT INTO pg_nsc VALUES (3, 'Carol', NULL, 200.00, NULL)");
        $this->pdo->exec("INSERT INTO pg_nsc VALUES (4, 'Dave', 1, 150.00, 'Sales')");
        $this->pdo->exec("INSERT INTO pg_nsc VALUES (5, 'Eve', NULL, NULL, NULL)");
    }

    /**
     * IS NOT DISTINCT FROM between two INT columns where both are NULL.
     *
     * BUG: CTE rewriter does not preserve IS NOT DISTINCT FROM semantics
     * when comparing two columns. Column-to-literal works (testIsNotDistinctFromNull).
     */
    public function testIsNotDistinctFromBothNullSameType(): void
    {
        // Use a simpler table to isolate IS NOT DISTINCT FROM behavior
        $this->createTable('CREATE TABLE pg_nsc_isndf (id INT PRIMARY KEY, a INT, b INT)');
        try {
            $this->pdo->exec("INSERT INTO pg_nsc_isndf (id, a, b) VALUES (1, NULL, NULL)");
            $this->pdo->exec("INSERT INTO pg_nsc_isndf (id, a, b) VALUES (2, 1, NULL)");
            $this->pdo->exec("INSERT INTO pg_nsc_isndf (id, a, b) VALUES (3, 1, 1)");

            $rows = $this->ztdQuery(
                "SELECT id FROM pg_nsc_isndf WHERE a IS NOT DISTINCT FROM b ORDER BY id"
            );
            $ids = array_column($rows, 'id');
            // id=1: NULL IS NOT DISTINCT FROM NULL = TRUE
            // id=2: 1 IS NOT DISTINCT FROM NULL = FALSE
            // id=3: 1 IS NOT DISTINCT FROM 1 = TRUE
            $this->assertContains('1', $ids, 'NULL IS NOT DISTINCT FROM NULL should be TRUE');
            $this->assertContains('3', $ids, '1 IS NOT DISTINCT FROM 1 should be TRUE');
            $this->assertNotContains('2', $ids, '1 IS NOT DISTINCT FROM NULL should be FALSE');
        } finally {
            $this->dropTable('pg_nsc_isndf');
        }
    }

    /**
     * IS NOT DISTINCT FROM with cross-type cast (INT vs DECIMAL).
     *
     * BUG: Same as testIsNotDistinctFromBothNullSameType — column-to-expression
     * IS NOT DISTINCT FROM fails through CTE rewriter.
     */
    public function testIsNotDistinctFromWithCast(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, name FROM pg_nsc
             WHERE manager_id IS NOT DISTINCT FROM bonus::INT
             ORDER BY id"
        );
        $names = array_column($rows, 'name');
        $this->assertContains('Eve', $names, 'NULL IS NOT DISTINCT FROM NULL should match');
    }

    /**
     * IS NOT DISTINCT FROM with NULL literal.
     */
    public function testIsNotDistinctFromNull(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, name FROM pg_nsc
             WHERE manager_id IS NOT DISTINCT FROM NULL
             ORDER BY id"
        );
        $this->assertCount(3, $rows);
        $names = array_column($rows, 'name');
        $this->assertContains('Alice', $names);
        $this->assertContains('Carol', $names);
        $this->assertContains('Eve', $names);
    }

    /**
     * IS DISTINCT FROM: treats NULL != NULL as FALSE.
     */
    public function testIsDistinctFrom(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name FROM pg_nsc
             WHERE dept IS DISTINCT FROM 'Engineering'
             ORDER BY name"
        );
        // Carol (NULL), Dave (Sales), Eve (NULL) — NULL IS DISTINCT FROM 'Engineering' = TRUE
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
             FROM pg_nsc ORDER BY id"
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
            "SELECT name FROM pg_nsc
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
             FROM pg_nsc ORDER BY id"
        );
        $this->assertNull($rows[0]['non_eng_dept']);
        $this->assertNull($rows[1]['non_eng_dept']);
        $this->assertSame('Sales', $rows[3]['non_eng_dept']);
    }

    /**
     * COALESCE with multiple fallbacks.
     */
    public function testCoalesceMultipleFallbacks(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, COALESCE(bonus, manager_id, -999)::NUMERIC AS first_non_null
             FROM pg_nsc ORDER BY id"
        );
        $this->assertEquals(-999, (float) $rows[4]['first_non_null']);
        $this->assertEquals(1, (float) $rows[1]['first_non_null']);
    }

    /**
     * IS NOT DISTINCT FROM in prepared statement.
     */
    public function testIsNotDistinctFromPrepared(): void
    {
        $stmt = $this->pdo->prepare(
            "SELECT name FROM pg_nsc WHERE dept IS NOT DISTINCT FROM ? ORDER BY name"
        );
        $stmt->bindValue(1, null, PDO::PARAM_NULL);
        $stmt->execute();
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $names = array_column($rows, 'name');
        $this->assertContains('Carol', $names);
        $this->assertContains('Eve', $names);
    }

    /**
     * COALESCE in GROUP BY.
     */
    public function testCoalesceInGroupBy(): void
    {
        $rows = $this->ztdQuery(
            "SELECT COALESCE(dept, 'None') AS dept_group, COUNT(*) AS cnt
             FROM pg_nsc
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
            "UPDATE pg_nsc SET bonus = COALESCE(bonus, 50.00) WHERE bonus IS NULL"
        );
        $rows = $this->ztdQuery("SELECT id, bonus FROM pg_nsc ORDER BY id");
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
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_nsc');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
