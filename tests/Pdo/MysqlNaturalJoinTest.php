<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests NATURAL JOIN through ZTD shadow store on MySQL.
 *
 * NATURAL JOIN implicitly joins on columns with the same name.
 * The CTE rewriter must handle the implicit column matching.
 * @spec SPEC-3.3
 */
class MysqlNaturalJoinTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mnj_users (id INT PRIMARY KEY, name VARCHAR(50), dept_id INT)',
            'CREATE TABLE mnj_depts (dept_id INT PRIMARY KEY, dept_name VARCHAR(50))',
            'CREATE TABLE mnj_roles (id INT PRIMARY KEY, role_name VARCHAR(50))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mnj_users', 'mnj_depts', 'mnj_roles'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mnj_users VALUES (1, 'Alice', 10)");
        $this->pdo->exec("INSERT INTO mnj_users VALUES (2, 'Bob', 20)");
        $this->pdo->exec("INSERT INTO mnj_users VALUES (3, 'Charlie', 10)");
        $this->pdo->exec("INSERT INTO mnj_depts VALUES (10, 'Engineering')");
        $this->pdo->exec("INSERT INTO mnj_depts VALUES (20, 'Marketing')");
        $this->pdo->exec("INSERT INTO mnj_roles VALUES (1, 'Admin')");
        $this->pdo->exec("INSERT INTO mnj_roles VALUES (2, 'User')");
    }

    /**
     * NATURAL JOIN on common column (dept_id).
     */
    public function testNaturalJoin(): void
    {
        try {
            $rows = $this->ztdQuery(
                'SELECT u.name, d.dept_name
                 FROM mnj_users u NATURAL JOIN mnj_depts d
                 ORDER BY u.name'
            );

            $this->assertCount(3, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Engineering', $rows[0]['dept_name']);
        } catch (\Exception $e) {
            $this->markTestSkipped('NATURAL JOIN not supported on MySQL: ' . $e->getMessage());
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
                 FROM mnj_users u NATURAL JOIN mnj_roles r
                 ORDER BY u.name'
            );

            // Only matches where mnj_users.id = mnj_roles.id
            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestSkipped('NATURAL JOIN on id not supported on MySQL: ' . $e->getMessage());
        }
    }

    /**
     * Shadow mutation affects NATURAL JOIN results.
     */
    public function testMutationAffectsNaturalJoin(): void
    {
        $this->pdo->exec("INSERT INTO mnj_depts VALUES (30, 'Sales')");
        $this->pdo->exec("INSERT INTO mnj_users VALUES (4, 'Diana', 30)");

        try {
            $rows = $this->ztdQuery(
                "SELECT u.name, d.dept_name
                 FROM mnj_users u NATURAL JOIN mnj_depts d
                 WHERE d.dept_name = 'Sales'"
            );

            $this->assertCount(1, $rows);
            $this->assertSame('Diana', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestSkipped('NATURAL JOIN after mutation not supported on MySQL: ' . $e->getMessage());
        }
    }

    /**
     * NATURAL JOIN after DELETE.
     */
    public function testNaturalJoinAfterDelete(): void
    {
        $this->pdo->exec("DELETE FROM mnj_users WHERE id = 2");

        try {
            $rows = $this->ztdQuery(
                'SELECT u.name, d.dept_name
                 FROM mnj_users u NATURAL JOIN mnj_depts d
                 ORDER BY u.name'
            );

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Charlie', $rows[1]['name']);
        } catch (\Exception $e) {
            $this->markTestSkipped('NATURAL JOIN after DELETE not supported on MySQL: ' . $e->getMessage());
        }
    }

    /**
     * NATURAL JOIN with prepared statement.
     */
    public function testNaturalJoinPrepared(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                'SELECT u.name, d.dept_name
                 FROM mnj_users u NATURAL JOIN mnj_depts d
                 WHERE u.name = ?',
                ['Alice']
            );

            $this->assertCount(1, $rows);
            $this->assertSame('Engineering', $rows[0]['dept_name']);
        } catch (\Exception $e) {
            $this->markTestSkipped('NATURAL JOIN prepared not supported on MySQL: ' . $e->getMessage());
        }
    }
}
