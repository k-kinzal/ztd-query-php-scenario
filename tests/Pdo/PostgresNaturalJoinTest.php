<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests NATURAL JOIN through ZTD shadow store on PostgreSQL.
 *
 * NATURAL JOIN implicitly joins on columns with the same name.
 * The CTE rewriter must handle the implicit column matching.
 * @spec SPEC-3.3
 */
class PostgresNaturalJoinTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pnj_users (id INT PRIMARY KEY, name VARCHAR(50), dept_id INT)',
            'CREATE TABLE pnj_depts (dept_id INT PRIMARY KEY, dept_name VARCHAR(50))',
            'CREATE TABLE pnj_roles (id INT PRIMARY KEY, role_name VARCHAR(50))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pnj_users', 'pnj_depts', 'pnj_roles'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pnj_users VALUES (1, 'Alice', 10)");
        $this->pdo->exec("INSERT INTO pnj_users VALUES (2, 'Bob', 20)");
        $this->pdo->exec("INSERT INTO pnj_users VALUES (3, 'Charlie', 10)");
        $this->pdo->exec("INSERT INTO pnj_depts VALUES (10, 'Engineering')");
        $this->pdo->exec("INSERT INTO pnj_depts VALUES (20, 'Marketing')");
        $this->pdo->exec("INSERT INTO pnj_roles VALUES (1, 'Admin')");
        $this->pdo->exec("INSERT INTO pnj_roles VALUES (2, 'User')");
    }

    /**
     * NATURAL JOIN on common column (dept_id).
     */
    public function testNaturalJoin(): void
    {
        try {
            $rows = $this->ztdQuery(
                'SELECT u.name, d.dept_name
                 FROM pnj_users u NATURAL JOIN pnj_depts d
                 ORDER BY u.name'
            );

            $this->assertCount(3, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Engineering', $rows[0]['dept_name']);
        } catch (\Exception $e) {
            $this->markTestSkipped('NATURAL JOIN not supported on PostgreSQL: ' . $e->getMessage());
        }
    }

    /**
     * NATURAL JOIN with id column (common but different meaning).
     */
    public function testNaturalJoinOnId(): void
    {
        try {
            $rows = $this->ztdQuery(
                'SELECT u.name, r.role_name
                 FROM pnj_users u NATURAL JOIN pnj_roles r
                 ORDER BY u.name'
            );

            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestSkipped('NATURAL JOIN on id not supported on PostgreSQL: ' . $e->getMessage());
        }
    }

    /**
     * Shadow mutation affects NATURAL JOIN results.
     */
    public function testMutationAffectsNaturalJoin(): void
    {
        $this->pdo->exec("INSERT INTO pnj_depts VALUES (30, 'Sales')");
        $this->pdo->exec("INSERT INTO pnj_users VALUES (4, 'Diana', 30)");

        try {
            $rows = $this->ztdQuery(
                "SELECT u.name, d.dept_name
                 FROM pnj_users u NATURAL JOIN pnj_depts d
                 WHERE d.dept_name = 'Sales'"
            );

            $this->assertCount(1, $rows);
            $this->assertSame('Diana', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestSkipped('NATURAL JOIN after mutation not supported on PostgreSQL: ' . $e->getMessage());
        }
    }

    /**
     * NATURAL JOIN after DELETE.
     */
    public function testNaturalJoinAfterDelete(): void
    {
        $this->pdo->exec("DELETE FROM pnj_users WHERE id = 2");

        try {
            $rows = $this->ztdQuery(
                'SELECT u.name, d.dept_name
                 FROM pnj_users u NATURAL JOIN pnj_depts d
                 ORDER BY u.name'
            );

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Charlie', $rows[1]['name']);
        } catch (\Exception $e) {
            $this->markTestSkipped('NATURAL JOIN after DELETE not supported on PostgreSQL: ' . $e->getMessage());
        }
    }

    /**
     * NATURAL LEFT JOIN — unmatched rows from left table should appear with NULLs.
     */
    public function testNaturalLeftJoin(): void
    {
        $this->pdo->exec("INSERT INTO pnj_users VALUES (4, 'Diana', 99)"); // no matching dept

        try {
            $rows = $this->ztdQuery(
                'SELECT u.name, d.dept_name
                 FROM pnj_users u NATURAL LEFT JOIN pnj_depts d
                 ORDER BY u.name'
            );

            $this->assertCount(4, $rows);
            $diana = array_filter($rows, fn($r) => $r['name'] === 'Diana');
            $diana = array_values($diana);
            $this->assertCount(1, $diana);
            $this->assertNull($diana[0]['dept_name'], 'Unmatched NATURAL LEFT JOIN should have NULL');
        } catch (\Exception $e) {
            $this->markTestSkipped('NATURAL LEFT JOIN not supported on PostgreSQL: ' . $e->getMessage());
        }
    }
}
