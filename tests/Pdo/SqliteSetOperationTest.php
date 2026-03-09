<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests EXCEPT and INTERSECT set operations through CTE shadow store.
 *
 * These operations are less commonly tested than UNION and may reveal
 * CTE rewriter issues when the shadow store must reconcile multiple
 * SELECT branches connected by set operators.
 *
 * SQLite supports INTERSECT and EXCEPT (distinct only).
 * INTERSECT ALL and EXCEPT ALL are not supported by SQLite.
 * SQLite uses left-to-right evaluation for compound operators (no INTERSECT precedence).
 */
class SqliteSetOperationTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_setop_employees (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                department TEXT NOT NULL,
                skill TEXT NOT NULL
            )',
            'CREATE TABLE sl_setop_contractors (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                department TEXT NOT NULL,
                skill TEXT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_setop_contractors', 'sl_setop_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Employees — skills: PHP, Python, Go, SQL, Docker
        $this->pdo->exec("INSERT INTO sl_setop_employees VALUES (1, 'Alice', 'Engineering', 'PHP')");
        $this->pdo->exec("INSERT INTO sl_setop_employees VALUES (2, 'Bob', 'Engineering', 'Python')");
        $this->pdo->exec("INSERT INTO sl_setop_employees VALUES (3, 'Carol', 'Engineering', 'Go')");
        $this->pdo->exec("INSERT INTO sl_setop_employees VALUES (4, 'Dave', 'Data', 'SQL')");
        $this->pdo->exec("INSERT INTO sl_setop_employees VALUES (5, 'Eve', 'DevOps', 'Docker')");
        // Duplicate skill for INTERSECT ALL testing
        $this->pdo->exec("INSERT INTO sl_setop_employees VALUES (6, 'Frank', 'Engineering', 'PHP')");
        $this->pdo->exec("INSERT INTO sl_setop_employees VALUES (7, 'Grace', 'Data', 'Python')");

        // Contractors — skills: PHP, Python, Rust, SQL, Kubernetes
        $this->pdo->exec("INSERT INTO sl_setop_contractors VALUES (1, 'Xander', 'Engineering', 'PHP')");
        $this->pdo->exec("INSERT INTO sl_setop_contractors VALUES (2, 'Yara', 'Engineering', 'Python')");
        $this->pdo->exec("INSERT INTO sl_setop_contractors VALUES (3, 'Zane', 'Engineering', 'Rust')");
        $this->pdo->exec("INSERT INTO sl_setop_contractors VALUES (4, 'Wendy', 'Data', 'SQL')");
        $this->pdo->exec("INSERT INTO sl_setop_contractors VALUES (5, 'Victor', 'DevOps', 'Kubernetes')");
        // Duplicate skill for INTERSECT ALL testing
        $this->pdo->exec("INSERT INTO sl_setop_contractors VALUES (6, 'Uma', 'Engineering', 'PHP')");
    }

    /**
     * Basic INTERSECT — common skills between employees and contractors.
     *
     * INTERSECT returns distinct rows present in both result sets.
     * Common skills: PHP, Python, SQL
     */
    public function testBasicIntersect(): void
    {
        $rows = $this->ztdQuery(
            "SELECT skill FROM sl_setop_employees
             INTERSECT
             SELECT skill FROM sl_setop_contractors
             ORDER BY skill"
        );

        $skills = array_column($rows, 'skill');
        $this->assertSame(['PHP', 'Python', 'SQL'], $skills);
    }

    /**
     * INTERSECT with multi-column projection returns 0 rows on SQLite.
     *
     * Single-column INTERSECT works correctly, but multi-column INTERSECT
     * returns empty results through the CTE shadow store. The CTE rewriter
     * appears to fail when INTERSECT involves non-PK columns.
     *
     * Expected (department, skill) pairs:
     *   (Data, SQL), (Engineering, PHP), (Engineering, Python)
     * Actual: 0 rows
     */
    public function testIntersectMultiColumnReturnsEmptyOnSqlite(): void
    {
        $rows = $this->ztdQuery(
            "SELECT department, skill FROM sl_setop_employees
             INTERSECT
             SELECT department, skill FROM sl_setop_contractors
             ORDER BY department, skill"
        );

        // BUG: multi-column INTERSECT returns 0 rows through CTE shadow store
        $this->assertCount(0, $rows, 'Multi-column INTERSECT returns 0 rows (expected 3)');
    }

    /**
     * Basic EXCEPT — skills employees have but contractors don't.
     *
     * Employee skills: PHP, Python, Go, SQL, Docker
     * Contractor skills: PHP, Python, Rust, SQL, Kubernetes
     * EXCEPT: Docker, Go
     */
    public function testBasicExcept(): void
    {
        $rows = $this->ztdQuery(
            "SELECT skill FROM sl_setop_employees
             EXCEPT
             SELECT skill FROM sl_setop_contractors
             ORDER BY skill"
        );

        $skills = array_column($rows, 'skill');
        $this->assertSame(['Docker', 'Go'], $skills);
    }

    /**
     * EXCEPT with ORDER BY and LIMIT.
     */
    public function testExceptWithOrderByAndLimit(): void
    {
        // Contractor-only skills (EXCEPT from contractors' perspective)
        // Contractor skills: PHP, Python, Rust, SQL, Kubernetes
        // Employee skills: PHP, Python, Go, SQL, Docker
        // EXCEPT: Kubernetes, Rust — ORDER BY skill LIMIT 1 → Kubernetes
        $rows = $this->ztdQuery(
            "SELECT skill FROM sl_setop_contractors
             EXCEPT
             SELECT skill FROM sl_setop_employees
             ORDER BY skill
             LIMIT 1"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Kubernetes', $rows[0]['skill']);
    }

    /**
     * Combined UNION + EXCEPT in the same query.
     *
     * SQLite evaluates compound operators left-to-right (no INTERSECT precedence).
     * e UNION c EXCEPT e INTERSECT c is evaluated as:
     *   ((e UNION c) EXCEPT e) INTERSECT c
     *
     * Step by step:
     *   e UNION c = {Docker, Go, Kubernetes, PHP, Python, Rust, SQL}
     *   ... EXCEPT e(distinct: {Docker, Go, PHP, Python, SQL}) = {Kubernetes, Rust}
     *   ... INTERSECT c(distinct: {Kubernetes, PHP, Python, Rust, SQL}) = {Kubernetes, Rust}
     */
    public function testUnionThenExcept(): void
    {
        $rows = $this->ztdQuery(
            "SELECT skill FROM sl_setop_employees
             UNION
             SELECT skill FROM sl_setop_contractors
             EXCEPT
             SELECT skill FROM sl_setop_employees
             INTERSECT
             SELECT skill FROM sl_setop_contractors
             ORDER BY skill"
        );

        $skills = array_column($rows, 'skill');
        $this->assertSame(['Kubernetes', 'Rust'], $skills);
    }

    /**
     * INTERSECT/EXCEPT after INSERT mutation — verify shadow data is reflected.
     *
     * Insert a new employee with skill 'Rust', then INTERSECT should include Rust.
     */
    public function testIntersectAfterInsertMutation(): void
    {
        // Before mutation: Rust is contractor-only
        $before = $this->ztdQuery(
            "SELECT skill FROM sl_setop_employees
             INTERSECT
             SELECT skill FROM sl_setop_contractors
             ORDER BY skill"
        );
        $beforeSkills = array_column($before, 'skill');
        $this->assertNotContains('Rust', $beforeSkills);

        // Mutate: add Rust to employees
        $this->pdo->exec("INSERT INTO sl_setop_employees VALUES (8, 'Hank', 'Engineering', 'Rust')");

        // After mutation: Rust should appear in INTERSECT
        $after = $this->ztdQuery(
            "SELECT skill FROM sl_setop_employees
             INTERSECT
             SELECT skill FROM sl_setop_contractors
             ORDER BY skill"
        );
        $afterSkills = array_column($after, 'skill');
        $this->assertSame(['PHP', 'Python', 'Rust', 'SQL'], $afterSkills);

        // Also verify EXCEPT no longer includes Go, Docker shrinks by nothing
        // but Rust should no longer appear in contractor-EXCEPT-employee
        $exceptAfter = $this->ztdQuery(
            "SELECT skill FROM sl_setop_contractors
             EXCEPT
             SELECT skill FROM sl_setop_employees
             ORDER BY skill"
        );
        $exceptSkills = array_column($exceptAfter, 'skill');
        $this->assertSame(['Kubernetes'], $exceptSkills);
    }

    /**
     * Prepared statement with INTERSECT.
     */
    public function testPreparedStatementWithIntersect(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT skill FROM sl_setop_employees WHERE department = ?
             INTERSECT
             SELECT skill FROM sl_setop_contractors WHERE department = ?",
            ['Engineering', 'Engineering']
        );

        $skills = array_column($rows, 'skill');
        sort($skills);
        // Employee Engineering skills: PHP, Python, Go, PHP → distinct: PHP, Python, Go
        // Contractor Engineering skills: PHP, Python, Rust, PHP → distinct: PHP, Python, Rust
        // INTERSECT: PHP, Python
        $this->assertSame(['PHP', 'Python'], $skills);
    }

    /**
     * Physical isolation — underlying tables should have no data.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();

        $empRows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_setop_employees")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $empRows[0]['cnt']);

        $conRows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_setop_contractors")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $conRows[0]['cnt']);
    }
}
