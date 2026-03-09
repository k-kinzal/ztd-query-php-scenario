<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests EXCEPT and INTERSECT set operations through CTE shadow store.
 *
 * PostgreSQL has supported INTERSECT, INTERSECT ALL, EXCEPT, and EXCEPT ALL
 * since at least version 7.x. All targeted versions (14-18) fully support these.
 *
 * PostgreSQL follows the SQL standard: INTERSECT has higher precedence than
 * UNION and EXCEPT.
 */
class PostgresSetOperationTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_setop_employees (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                department TEXT NOT NULL,
                skill TEXT NOT NULL
            )',
            'CREATE TABLE pg_setop_contractors (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                department TEXT NOT NULL,
                skill TEXT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_setop_contractors', 'pg_setop_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Employees — skills: PHP, Python, Go, SQL, Docker
        $this->pdo->exec("INSERT INTO pg_setop_employees VALUES (1, 'Alice', 'Engineering', 'PHP')");
        $this->pdo->exec("INSERT INTO pg_setop_employees VALUES (2, 'Bob', 'Engineering', 'Python')");
        $this->pdo->exec("INSERT INTO pg_setop_employees VALUES (3, 'Carol', 'Engineering', 'Go')");
        $this->pdo->exec("INSERT INTO pg_setop_employees VALUES (4, 'Dave', 'Data', 'SQL')");
        $this->pdo->exec("INSERT INTO pg_setop_employees VALUES (5, 'Eve', 'DevOps', 'Docker')");
        // Duplicate skills for ALL testing
        $this->pdo->exec("INSERT INTO pg_setop_employees VALUES (6, 'Frank', 'Engineering', 'PHP')");
        $this->pdo->exec("INSERT INTO pg_setop_employees VALUES (7, 'Grace', 'Data', 'Python')");

        // Contractors — skills: PHP, Python, Rust, SQL, Kubernetes
        $this->pdo->exec("INSERT INTO pg_setop_contractors VALUES (1, 'Xander', 'Engineering', 'PHP')");
        $this->pdo->exec("INSERT INTO pg_setop_contractors VALUES (2, 'Yara', 'Engineering', 'Python')");
        $this->pdo->exec("INSERT INTO pg_setop_contractors VALUES (3, 'Zane', 'Engineering', 'Rust')");
        $this->pdo->exec("INSERT INTO pg_setop_contractors VALUES (4, 'Wendy', 'Data', 'SQL')");
        $this->pdo->exec("INSERT INTO pg_setop_contractors VALUES (5, 'Victor', 'DevOps', 'Kubernetes')");
        // Duplicate skill for ALL testing
        $this->pdo->exec("INSERT INTO pg_setop_contractors VALUES (6, 'Uma', 'Engineering', 'PHP')");
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
            "SELECT skill FROM pg_setop_employees
             INTERSECT
             SELECT skill FROM pg_setop_contractors
             ORDER BY skill"
        );

        $skills = array_column($rows, 'skill');
        $this->assertSame(['PHP', 'Python', 'SQL'], $skills);
    }

    /**
     * INTERSECT ALL — preserves duplicates based on minimum count per side.
     *
     * Employee skills: PHP(2), Python(2), Go(1), SQL(1), Docker(1)
     * Contractor skills: PHP(2), Python(1), Rust(1), SQL(1), Kubernetes(1)
     *
     * INTERSECT ALL yields: PHP(min(2,2)=2), Python(min(2,1)=1), SQL(min(1,1)=1)
     */
    public function testIntersectAll(): void
    {
        $rows = $this->ztdQuery(
            "SELECT skill FROM pg_setop_employees
             INTERSECT ALL
             SELECT skill FROM pg_setop_contractors
             ORDER BY skill"
        );

        $skills = array_column($rows, 'skill');
        $this->assertSame(['PHP', 'PHP', 'Python', 'SQL'], $skills);
    }

    /**
     * INTERSECT with multi-column projection.
     *
     * INTERSECT on (department, skill) pairs — tests that the CTE rewriter
     * handles multi-column set operations correctly.
     *
     * Common (department, skill) pairs:
     *   (Data, SQL), (Engineering, PHP), (Engineering, Python)
     */
    public function testIntersectMultiColumn(): void
    {
        $rows = $this->ztdQuery(
            "SELECT department, skill FROM pg_setop_employees
             INTERSECT
             SELECT department, skill FROM pg_setop_contractors
             ORDER BY department, skill"
        );

        $this->assertCount(3, $rows);
        $this->assertSame(['department' => 'Data', 'skill' => 'SQL'], $rows[0]);
        $this->assertSame(['department' => 'Engineering', 'skill' => 'PHP'], $rows[1]);
        $this->assertSame(['department' => 'Engineering', 'skill' => 'Python'], $rows[2]);
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
            "SELECT skill FROM pg_setop_employees
             EXCEPT
             SELECT skill FROM pg_setop_contractors
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
        // Contractor-only skills: Kubernetes, Rust
        // ORDER BY skill LIMIT 1 → Kubernetes
        $rows = $this->ztdQuery(
            "SELECT skill FROM pg_setop_contractors
             EXCEPT
             SELECT skill FROM pg_setop_employees
             ORDER BY skill
             LIMIT 1"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Kubernetes', $rows[0]['skill']);
    }

    /**
     * Combined UNION + EXCEPT in the same query.
     *
     * PostgreSQL follows SQL standard precedence: INTERSECT binds tighter
     * than UNION/EXCEPT. Then UNION and EXCEPT are left-to-right.
     *
     * e UNION c EXCEPT e INTERSECT c  parses as:
     *   (e UNION c) EXCEPT (e INTERSECT c)
     *
     * Step by step:
     *   e UNION c = {Docker, Go, Kubernetes, PHP, Python, Rust, SQL}
     *   e INTERSECT c = {PHP, Python, SQL}
     *   Result = {Docker, Go, Kubernetes, Rust}
     */
    public function testUnionThenExcept(): void
    {
        $rows = $this->ztdQuery(
            "SELECT skill FROM pg_setop_employees
             UNION
             SELECT skill FROM pg_setop_contractors
             EXCEPT
             SELECT skill FROM pg_setop_employees
             INTERSECT
             SELECT skill FROM pg_setop_contractors
             ORDER BY skill"
        );

        $skills = array_column($rows, 'skill');
        $this->assertSame(['Docker', 'Go', 'Kubernetes', 'Rust'], $skills);
    }

    /**
     * INTERSECT/EXCEPT after INSERT mutation — verify shadow data is reflected.
     */
    public function testIntersectAfterInsertMutation(): void
    {
        // Before mutation: Rust is contractor-only
        $before = $this->ztdQuery(
            "SELECT skill FROM pg_setop_employees
             INTERSECT
             SELECT skill FROM pg_setop_contractors
             ORDER BY skill"
        );
        $beforeSkills = array_column($before, 'skill');
        $this->assertNotContains('Rust', $beforeSkills);

        // Mutate: add Rust to employees
        $this->pdo->exec("INSERT INTO pg_setop_employees VALUES (8, 'Hank', 'Engineering', 'Rust')");

        // After mutation: Rust should appear in INTERSECT
        $after = $this->ztdQuery(
            "SELECT skill FROM pg_setop_employees
             INTERSECT
             SELECT skill FROM pg_setop_contractors
             ORDER BY skill"
        );
        $afterSkills = array_column($after, 'skill');
        $this->assertSame(['PHP', 'Python', 'Rust', 'SQL'], $afterSkills);

        // Verify contractor EXCEPT employee no longer includes Rust
        $exceptAfter = $this->ztdQuery(
            "SELECT skill FROM pg_setop_contractors
             EXCEPT
             SELECT skill FROM pg_setop_employees
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
            "SELECT skill FROM pg_setop_employees WHERE department = ?
             INTERSECT
             SELECT skill FROM pg_setop_contractors WHERE department = ?",
            ['Engineering', 'Engineering']
        );

        $skills = array_column($rows, 'skill');
        sort($skills);
        // Employee Engineering: PHP, Python, Go, PHP → distinct: PHP, Python, Go
        // Contractor Engineering: PHP, Python, Rust, PHP → distinct: PHP, Python, Rust
        // INTERSECT: PHP, Python
        $this->assertSame(['PHP', 'Python'], $skills);
    }

    /**
     * Physical isolation — underlying tables should have no data.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();

        $empRows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_setop_employees")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $empRows[0]['cnt']);

        $conRows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_setop_contractors")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $conRows[0]['cnt']);
    }
}
