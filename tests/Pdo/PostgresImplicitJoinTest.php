<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests implicit JOIN (comma syntax in FROM) through ZTD shadow store on PostgreSQL.
 *
 * Pattern: SELECT ... FROM a, b WHERE a.fk = b.pk
 * Many legacy applications use comma-separated FROM clauses.
 * @spec SPEC-3.3
 */
class PostgresImplicitJoinTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pij_users (id INT PRIMARY KEY, name VARCHAR(50), dept_id INT)',
            'CREATE TABLE pij_depts (id INT PRIMARY KEY, dept_name VARCHAR(50))',
            'CREATE TABLE pij_roles (id INT PRIMARY KEY, user_id INT, role_name VARCHAR(50))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pij_users', 'pij_depts', 'pij_roles'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pij_depts VALUES (1, 'Engineering')");
        $this->pdo->exec("INSERT INTO pij_depts VALUES (2, 'Marketing')");
        $this->pdo->exec("INSERT INTO pij_users VALUES (1, 'Alice', 1)");
        $this->pdo->exec("INSERT INTO pij_users VALUES (2, 'Bob', 2)");
        $this->pdo->exec("INSERT INTO pij_users VALUES (3, 'Charlie', 1)");
        $this->pdo->exec("INSERT INTO pij_roles VALUES (1, 1, 'admin')");
        $this->pdo->exec("INSERT INTO pij_roles VALUES (2, 2, 'editor')");
    }

    /**
     * Two-table implicit JOIN.
     */
    public function testTwoTableImplicitJoin(): void
    {
        $rows = $this->ztdQuery(
            'SELECT u.name, d.dept_name
             FROM pij_users u, pij_depts d
             WHERE u.dept_id = d.id
             ORDER BY u.name'
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Engineering', $rows[0]['dept_name']);
    }

    /**
     * Three-table implicit JOIN.
     */
    public function testThreeTableImplicitJoin(): void
    {
        $rows = $this->ztdQuery(
            'SELECT u.name, d.dept_name, r.role_name
             FROM pij_users u, pij_depts d, pij_roles r
             WHERE u.dept_id = d.id AND r.user_id = u.id
             ORDER BY u.name'
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('admin', $rows[0]['role_name']);
    }

    /**
     * Implicit JOIN after shadow mutation.
     */
    public function testImplicitJoinAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO pij_users VALUES (4, 'Diana', 2)");

        $rows = $this->ztdQuery(
            'SELECT u.name, d.dept_name
             FROM pij_users u, pij_depts d
             WHERE u.dept_id = d.id AND u.id = 4'
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Diana', $rows[0]['name']);
        $this->assertSame('Marketing', $rows[0]['dept_name']);
    }

    /**
     * Implicit JOIN with aggregate.
     */
    public function testImplicitJoinWithAggregate(): void
    {
        $rows = $this->ztdQuery(
            'SELECT d.dept_name, COUNT(*) AS cnt
             FROM pij_users u, pij_depts d
             WHERE u.dept_id = d.id
             GROUP BY d.dept_name
             ORDER BY d.dept_name'
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Engineering', $rows[0]['dept_name']);
        $this->assertSame('2', (string) $rows[0]['cnt']);
    }

    /**
     * Implicit JOIN after DELETE.
     */
    public function testImplicitJoinAfterDelete(): void
    {
        $this->pdo->exec("DELETE FROM pij_users WHERE id = 2");

        $rows = $this->ztdQuery(
            'SELECT u.name, d.dept_name
             FROM pij_users u, pij_depts d
             WHERE u.dept_id = d.id
             ORDER BY u.name'
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Charlie', $rows[1]['name']);
    }

    /**
     * Implicit CROSS JOIN (no WHERE filter).
     */
    public function testImplicitCrossJoin(): void
    {
        $rows = $this->ztdQuery(
            'SELECT u.name, d.dept_name
             FROM pij_users u, pij_depts d
             ORDER BY u.name, d.dept_name'
        );

        // 3 users × 2 depts = 6 rows
        $this->assertCount(6, $rows);
    }

    /**
     * Implicit JOIN with subquery in WHERE.
     */
    public function testImplicitJoinWithSubquery(): void
    {
        $rows = $this->ztdQuery(
            "SELECT u.name
             FROM pij_users u, pij_depts d
             WHERE u.dept_id = d.id
               AND d.dept_name IN (SELECT dept_name FROM pij_depts WHERE dept_name = 'Engineering')
             ORDER BY u.name"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Charlie', $rows[1]['name']);
    }
}
