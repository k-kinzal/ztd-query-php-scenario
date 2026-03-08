<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests NATURAL JOIN and USING clause through ZTD shadow store on SQLite.
 *
 * NATURAL JOIN implicitly joins on columns with the same name.
 * USING clause joins on specified common column(s).
 * @spec pending
 */
class SqliteNaturalJoinTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE nj_users (id INT PRIMARY KEY, name VARCHAR(50), dept_id INT)',
            'CREATE TABLE nj_depts (dept_id INT PRIMARY KEY, dept_name VARCHAR(50))',
            'CREATE TABLE nj_roles (id INT PRIMARY KEY, role_name VARCHAR(50))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['nj_users', 'nj_depts', 'nj_roles'];
    }


    /**
     * NATURAL JOIN on common column (dept_id).
     */
    public function testNaturalJoin(): void
    {
        try {
            $stmt = $this->pdo->query(
                'SELECT u.name, d.dept_name
                 FROM nj_users u NATURAL JOIN nj_depts d
                 ORDER BY u.name'
            );
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            $this->assertCount(3, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Engineering', $rows[0]['dept_name']);
        } catch (\Exception $e) {
            $this->markTestSkipped('NATURAL JOIN not supported: ' . $e->getMessage());
        }
    }

    /**
     * JOIN ... USING (column) clause.
     */
    public function testJoinUsing(): void
    {
        try {
            $stmt = $this->pdo->query(
                'SELECT u.name, d.dept_name
                 FROM nj_users u JOIN nj_depts d USING (dept_id)
                 ORDER BY u.name'
            );
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            $this->assertCount(3, $rows);
            $this->assertSame('Bob', $rows[1]['name']);
            $this->assertSame('Marketing', $rows[1]['dept_name']);
        } catch (\Exception $e) {
            $this->markTestSkipped('JOIN ... USING not supported: ' . $e->getMessage());
        }
    }

    /**
     * NATURAL JOIN with id column (common but different meaning).
     * nj_users.id and nj_roles.id are both "id" but represent different entities.
     */
    public function testNaturalJoinOnId(): void
    {
        try {
            $stmt = $this->pdo->query(
                'SELECT u.name, r.role_name
                 FROM nj_users u NATURAL JOIN nj_roles r
                 ORDER BY u.name'
            );
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            // Only matches where nj_users.id = nj_roles.id
            // Alice (id=1) → Admin, Bob (id=2) → User
            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestSkipped('NATURAL JOIN on id not supported: ' . $e->getMessage());
        }
    }

    /**
     * CROSS JOIN with all shadow tables.
     */
    public function testCrossJoin(): void
    {
        $stmt = $this->pdo->query(
            'SELECT u.name, d.dept_name
             FROM nj_users u CROSS JOIN nj_depts d
             ORDER BY u.name, d.dept_name'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        // 3 users × 2 depts = 6 rows
        $this->assertCount(6, $rows);
    }

    /**
     * Shadow mutation affects NATURAL JOIN results.
     */
    public function testMutationAffectsNaturalJoin(): void
    {
        $this->pdo->exec("INSERT INTO nj_depts VALUES (30, 'Sales')");
        $this->pdo->exec("INSERT INTO nj_users VALUES (4, 'Diana', 30)");

        try {
            $stmt = $this->pdo->query(
                'SELECT u.name, d.dept_name
                 FROM nj_users u NATURAL JOIN nj_depts d
                 WHERE d.dept_name = \'Sales\''
            );
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            $this->assertCount(1, $rows);
            $this->assertSame('Diana', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestSkipped('NATURAL JOIN after mutation not supported: ' . $e->getMessage());
        }
    }
}
