<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;
use ZtdQuery\Adapter\Mysqli\ZtdMysqliException;

/**
 * Tests EXCEPT and INTERSECT set operations through CTE shadow store (MySQLi adapter).
 *
 * Finding: INTERSECT and EXCEPT are rejected by the MySQL CTE rewriter as
 * "Multi-statement SQL statement" — the parser misidentifies them as
 * statement separators. UNION works correctly.
 * See SPEC-11.MYSQL-EXCEPT-INTERSECT [Issue #14].
 *
 * @spec SPEC-3.3d
 */
class SetOperationTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_setop_employees (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                department VARCHAR(100) NOT NULL,
                skill VARCHAR(100) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE mi_setop_contractors (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                department VARCHAR(100) NOT NULL,
                skill VARCHAR(100) NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_setop_contractors', 'mi_setop_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Employees — skills: PHP, Python, Go, SQL, Docker
        $this->mysqli->query("INSERT INTO mi_setop_employees VALUES (1, 'Alice', 'Engineering', 'PHP')");
        $this->mysqli->query("INSERT INTO mi_setop_employees VALUES (2, 'Bob', 'Engineering', 'Python')");
        $this->mysqli->query("INSERT INTO mi_setop_employees VALUES (3, 'Carol', 'Engineering', 'Go')");
        $this->mysqli->query("INSERT INTO mi_setop_employees VALUES (4, 'Dave', 'Data', 'SQL')");
        $this->mysqli->query("INSERT INTO mi_setop_employees VALUES (5, 'Eve', 'DevOps', 'Docker')");
        $this->mysqli->query("INSERT INTO mi_setop_employees VALUES (6, 'Frank', 'Engineering', 'PHP')");
        $this->mysqli->query("INSERT INTO mi_setop_employees VALUES (7, 'Grace', 'Data', 'Python')");

        // Contractors — skills: PHP, Python, Rust, SQL, Kubernetes
        $this->mysqli->query("INSERT INTO mi_setop_contractors VALUES (1, 'Xander', 'Engineering', 'PHP')");
        $this->mysqli->query("INSERT INTO mi_setop_contractors VALUES (2, 'Yara', 'Engineering', 'Python')");
        $this->mysqli->query("INSERT INTO mi_setop_contractors VALUES (3, 'Zane', 'Engineering', 'Rust')");
        $this->mysqli->query("INSERT INTO mi_setop_contractors VALUES (4, 'Wendy', 'Data', 'SQL')");
        $this->mysqli->query("INSERT INTO mi_setop_contractors VALUES (5, 'Victor', 'DevOps', 'Kubernetes')");
        $this->mysqli->query("INSERT INTO mi_setop_contractors VALUES (6, 'Uma', 'Engineering', 'PHP')");
    }

    /**
     * INTERSECT is rejected as "Multi-statement SQL" on MySQL.
     * See SPEC-11.MYSQL-EXCEPT-INTERSECT [Issue #14].
     */
    public function testIntersectRejectedAsMultiStatement(): void
    {
        $this->expectException(ZtdMysqliException::class);
        $this->expectExceptionMessageMatches('/Multi-statement/');

        $this->ztdQuery(
            "SELECT skill FROM mi_setop_employees
             INTERSECT
             SELECT skill FROM mi_setop_contractors
             ORDER BY skill"
        );
    }

    /**
     * EXCEPT is rejected as "Multi-statement SQL" on MySQL.
     */
    public function testExceptRejectedAsMultiStatement(): void
    {
        $this->expectException(ZtdMysqliException::class);
        $this->expectExceptionMessageMatches('/Multi-statement/');

        $this->ztdQuery(
            "SELECT skill FROM mi_setop_employees
             EXCEPT
             SELECT skill FROM mi_setop_contractors
             ORDER BY skill"
        );
    }

    /**
     * INTERSECT ALL is also rejected on MySQL.
     */
    public function testIntersectAllRejectedAsMultiStatement(): void
    {
        $this->expectException(ZtdMysqliException::class);
        $this->expectExceptionMessageMatches('/Multi-statement/');

        $this->ztdQuery(
            "SELECT skill FROM mi_setop_employees
             INTERSECT ALL
             SELECT skill FROM mi_setop_contractors
             ORDER BY skill"
        );
    }

    /**
     * Prepared INTERSECT is also rejected on MySQL.
     */
    public function testPreparedIntersectRejectedAsMultiStatement(): void
    {
        $this->expectException(ZtdMysqliException::class);
        $this->expectExceptionMessageMatches('/Multi-statement/');

        $this->ztdPrepareAndExecute(
            "SELECT skill FROM mi_setop_employees WHERE department = ?
             INTERSECT
             SELECT skill FROM mi_setop_contractors WHERE department = ?",
            ['Engineering', 'Engineering']
        );
    }

    /**
     * UNION works correctly on MySQL (not affected by multi-statement detection).
     */
    public function testUnionWorksOnMysql(): void
    {
        $rows = $this->ztdQuery(
            "SELECT skill FROM mi_setop_employees
             UNION
             SELECT skill FROM mi_setop_contractors
             ORDER BY skill"
        );

        $skills = array_column($rows, 'skill');
        $this->assertSame(['Docker', 'Go', 'Kubernetes', 'PHP', 'Python', 'Rust', 'SQL'], $skills);
    }

    /**
     * Workaround: IN subquery for INTERSECT.
     */
    public function testInSubqueryWorkaroundForIntersect(): void
    {
        $rows = $this->ztdQuery(
            "SELECT DISTINCT skill FROM mi_setop_employees
             WHERE skill IN (SELECT skill FROM mi_setop_contractors)
             ORDER BY skill"
        );

        $skills = array_column($rows, 'skill');
        $this->assertSame(['PHP', 'Python', 'SQL'], $skills);
    }

    /**
     * Workaround: NOT IN subquery for EXCEPT.
     */
    public function testNotInSubqueryWorkaroundForExcept(): void
    {
        $rows = $this->ztdQuery(
            "SELECT DISTINCT skill FROM mi_setop_employees
             WHERE skill NOT IN (SELECT skill FROM mi_setop_contractors)
             ORDER BY skill"
        );

        $skills = array_column($rows, 'skill');
        $this->assertSame(['Docker', 'Go'], $skills);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->disableZtd();

        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_setop_employees");
        $empRows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertEquals(0, (int) $empRows[0]['cnt']);
    }
}
