<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdoException;

/**
 * Tests EXCEPT and INTERSECT set operations through CTE shadow store on MySQL.
 *
 * Finding: INTERSECT and EXCEPT are rejected by the MySQL CTE rewriter as
 * "Multi-statement SQL statement" — the parser misidentifies them as
 * statement separators. UNION works correctly.
 * See SPEC-11.MYSQL-EXCEPT-INTERSECT [Issue #14].
 *
 * @spec SPEC-3.3d
 */
class MysqlSetOperationTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_setop_employees (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                department VARCHAR(100) NOT NULL,
                skill VARCHAR(100) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE my_setop_contractors (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                department VARCHAR(100) NOT NULL,
                skill VARCHAR(100) NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_setop_contractors', 'my_setop_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Employees — skills: PHP, Python, Go, SQL, Docker
        $this->pdo->exec("INSERT INTO my_setop_employees VALUES (1, 'Alice', 'Engineering', 'PHP')");
        $this->pdo->exec("INSERT INTO my_setop_employees VALUES (2, 'Bob', 'Engineering', 'Python')");
        $this->pdo->exec("INSERT INTO my_setop_employees VALUES (3, 'Carol', 'Engineering', 'Go')");
        $this->pdo->exec("INSERT INTO my_setop_employees VALUES (4, 'Dave', 'Data', 'SQL')");
        $this->pdo->exec("INSERT INTO my_setop_employees VALUES (5, 'Eve', 'DevOps', 'Docker')");
        $this->pdo->exec("INSERT INTO my_setop_employees VALUES (6, 'Frank', 'Engineering', 'PHP')");
        $this->pdo->exec("INSERT INTO my_setop_employees VALUES (7, 'Grace', 'Data', 'Python')");

        // Contractors — skills: PHP, Python, Rust, SQL, Kubernetes
        $this->pdo->exec("INSERT INTO my_setop_contractors VALUES (1, 'Xander', 'Engineering', 'PHP')");
        $this->pdo->exec("INSERT INTO my_setop_contractors VALUES (2, 'Yara', 'Engineering', 'Python')");
        $this->pdo->exec("INSERT INTO my_setop_contractors VALUES (3, 'Zane', 'Engineering', 'Rust')");
        $this->pdo->exec("INSERT INTO my_setop_contractors VALUES (4, 'Wendy', 'Data', 'SQL')");
        $this->pdo->exec("INSERT INTO my_setop_contractors VALUES (5, 'Victor', 'DevOps', 'Kubernetes')");
        $this->pdo->exec("INSERT INTO my_setop_contractors VALUES (6, 'Uma', 'Engineering', 'PHP')");
    }

    /**
     * INTERSECT is rejected as "Multi-statement SQL" on MySQL.
     * See SPEC-11.MYSQL-EXCEPT-INTERSECT [Issue #14].
     */
    public function testIntersectRejectedAsMultiStatement(): void
    {
        $this->expectException(ZtdPdoException::class);
        $this->expectExceptionMessageMatches('/Multi-statement/');

        $this->ztdQuery(
            "SELECT skill FROM my_setop_employees
             INTERSECT
             SELECT skill FROM my_setop_contractors
             ORDER BY skill"
        );
    }

    /**
     * EXCEPT is rejected as "Multi-statement SQL" on MySQL.
     * See SPEC-11.MYSQL-EXCEPT-INTERSECT [Issue #14].
     */
    public function testExceptRejectedAsMultiStatement(): void
    {
        $this->expectException(ZtdPdoException::class);
        $this->expectExceptionMessageMatches('/Multi-statement/');

        $this->ztdQuery(
            "SELECT skill FROM my_setop_employees
             EXCEPT
             SELECT skill FROM my_setop_contractors
             ORDER BY skill"
        );
    }

    /**
     * INTERSECT ALL is also rejected on MySQL.
     */
    public function testIntersectAllRejectedAsMultiStatement(): void
    {
        $this->expectException(ZtdPdoException::class);
        $this->expectExceptionMessageMatches('/Multi-statement/');

        $this->ztdQuery(
            "SELECT skill FROM my_setop_employees
             INTERSECT ALL
             SELECT skill FROM my_setop_contractors
             ORDER BY skill"
        );
    }

    /**
     * Prepared INTERSECT is also rejected on MySQL.
     */
    public function testPreparedIntersectRejectedAsMultiStatement(): void
    {
        $this->expectException(ZtdPdoException::class);
        $this->expectExceptionMessageMatches('/Multi-statement/');

        $this->ztdPrepareAndExecute(
            "SELECT skill FROM my_setop_employees WHERE department = ?
             INTERSECT
             SELECT skill FROM my_setop_contractors WHERE department = ?",
            ['Engineering', 'Engineering']
        );
    }

    /**
     * UNION works correctly on MySQL (not affected by multi-statement detection).
     */
    public function testUnionWorksOnMysql(): void
    {
        $rows = $this->ztdQuery(
            "SELECT skill FROM my_setop_employees
             UNION
             SELECT skill FROM my_setop_contractors
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
            "SELECT DISTINCT skill FROM my_setop_employees
             WHERE skill IN (SELECT skill FROM my_setop_contractors)
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
            "SELECT DISTINCT skill FROM my_setop_employees
             WHERE skill NOT IN (SELECT skill FROM my_setop_contractors)
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
        $this->pdo->disableZtd();

        $empRows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM my_setop_employees")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $empRows[0]['cnt']);
    }
}
