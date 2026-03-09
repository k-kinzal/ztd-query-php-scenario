<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests NULL-safe comparison operators and NULL-handling functions
 * through the MySQL PDO CTE shadow store.
 *
 * MySQL's <=> operator and functions like COALESCE, NULLIF, IFNULL
 * have special NULL semantics that may be disrupted by CTE rewriting.
 *
 * @spec SPEC-3.1, SPEC-10.1
 */
class MysqlNullSafeComparisonTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE my_nsc (
            id INT PRIMARY KEY,
            name VARCHAR(50),
            manager_id INT,
            bonus DECIMAL(10,2),
            dept VARCHAR(30)
        )';
    }

    protected function getTableNames(): array
    {
        return ['my_nsc'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_nsc VALUES (1, 'Alice', NULL, 100.00, 'Engineering')");
        $this->pdo->exec("INSERT INTO my_nsc VALUES (2, 'Bob', 1, NULL, 'Engineering')");
        $this->pdo->exec("INSERT INTO my_nsc VALUES (3, 'Carol', NULL, 200.00, NULL)");
        $this->pdo->exec("INSERT INTO my_nsc VALUES (4, 'Dave', 1, 150.00, 'Sales')");
        $this->pdo->exec("INSERT INTO my_nsc VALUES (5, 'Eve', NULL, NULL, NULL)");
    }

    /**
     * NULL-safe equality: NULL <=> NULL should be TRUE.
     */
    public function testNullSafeEqualsBothNull(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, name FROM my_nsc WHERE manager_id <=> bonus ORDER BY id"
        );
        // Eve: NULL <=> NULL = TRUE
        // Others: mixed
        $names = array_column($rows, 'name');
        $this->assertContains('Eve', $names, 'NULL <=> NULL should match');
    }

    /**
     * NULL-safe equality: value <=> NULL should be FALSE.
     */
    public function testNullSafeEqualsValueVsNull(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, name FROM my_nsc WHERE manager_id <=> NULL ORDER BY id"
        );
        // Alice (manager_id=NULL), Carol (NULL), Eve (NULL)
        $this->assertCount(3, $rows);
        $names = array_column($rows, 'name');
        $this->assertContains('Alice', $names);
        $this->assertContains('Carol', $names);
        $this->assertContains('Eve', $names);
    }

    /**
     * <=> with non-null on both sides works like =.
     */
    public function testNullSafeEqualsNonNull(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name FROM my_nsc WHERE manager_id <=> 1 ORDER BY name"
        );
        $this->assertCount(2, $rows);
        $names = array_column($rows, 'name');
        $this->assertContains('Bob', $names);
        $this->assertContains('Dave', $names);
    }

    /**
     * COALESCE in SELECT list.
     */
    public function testCoalesceInSelect(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, COALESCE(dept, 'Unassigned') AS dept_name
             FROM my_nsc ORDER BY id"
        );
        $this->assertCount(5, $rows);
        $this->assertSame('Engineering', $rows[0]['dept_name']);
        $this->assertSame('Unassigned', $rows[2]['dept_name']); // Carol: dept=NULL
        $this->assertSame('Unassigned', $rows[4]['dept_name']); // Eve: dept=NULL
    }

    /**
     * COALESCE in WHERE clause.
     */
    public function testCoalesceInWhere(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name FROM my_nsc
             WHERE COALESCE(bonus, 0) > 100
             ORDER BY name"
        );
        $this->assertCount(2, $rows);
        $names = array_column($rows, 'name');
        $this->assertContains('Carol', $names);  // bonus=200
        $this->assertContains('Dave', $names);    // bonus=150
    }

    /**
     * NULLIF: returns NULL when both args are equal.
     */
    public function testNullif(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, NULLIF(dept, 'Engineering') AS non_eng_dept
             FROM my_nsc ORDER BY id"
        );
        $this->assertCount(5, $rows);
        $this->assertNull($rows[0]['non_eng_dept']); // Alice: Engineering -> NULL
        $this->assertNull($rows[1]['non_eng_dept']); // Bob: Engineering -> NULL
        $this->assertSame('Sales', $rows[3]['non_eng_dept']); // Dave
    }

    /**
     * IFNULL (MySQL-specific).
     */
    public function testIfnull(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, IFNULL(bonus, -1) AS bonus_val
             FROM my_nsc ORDER BY id"
        );
        $this->assertCount(5, $rows);
        $this->assertEquals(100.00, (float) $rows[0]['bonus_val']);
        $this->assertEquals(-1, (float) $rows[1]['bonus_val']); // Bob: NULL -> -1
        $this->assertEquals(-1, (float) $rows[4]['bonus_val']); // Eve: NULL -> -1
    }

    /**
     * COALESCE with multiple fallbacks.
     */
    public function testCoalesceMultipleFallbacks(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, COALESCE(bonus, manager_id, -999) AS first_non_null
             FROM my_nsc ORDER BY id"
        );
        // Eve: bonus=NULL, manager_id=NULL => -999
        $this->assertEquals(-999, (float) $rows[4]['first_non_null']);
        // Bob: bonus=NULL, manager_id=1 => 1
        $this->assertEquals(1, (float) $rows[1]['first_non_null']);
    }

    /**
     * <=> in prepared statement.
     */
    public function testNullSafeEqualsPrepared(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT name FROM my_nsc WHERE dept <=> ? ORDER BY name",
            [null]
        );
        // Carol and Eve have dept=NULL
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
             FROM my_nsc
             GROUP BY COALESCE(dept, 'None')
             ORDER BY dept_group"
        );
        $this->assertCount(3, $rows);
        // Engineering=2, None=2 (Carol+Eve), Sales=1
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
            "UPDATE my_nsc SET bonus = COALESCE(bonus, 50.00) WHERE bonus IS NULL"
        );
        $rows = $this->ztdQuery("SELECT id, bonus FROM my_nsc ORDER BY id");
        $this->assertEquals(50.00, (float) $rows[1]['bonus']); // Bob: was NULL
        $this->assertEquals(50.00, (float) $rows[4]['bonus']); // Eve: was NULL
        $this->assertEquals(100.00, (float) $rows[0]['bonus']); // Alice: unchanged
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM my_nsc');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
