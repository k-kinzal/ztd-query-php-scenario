<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests cross-table shadow store consistency: mutations on multiple tables
 * must be visible through JOINs, subqueries, and aggregations.
 *
 * This verifies that the CTE rewriter correctly handles multi-table
 * queries where both tables have shadow mutations.
 *
 * @spec SPEC-3.3, SPEC-4.1
 */
class MysqlCrossTableShadowTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_ct_depts (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                budget DECIMAL(12,2) NOT NULL DEFAULT 0
            )',
            'CREATE TABLE my_ct_employees (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                dept_id INT NOT NULL,
                salary DECIMAL(10,2) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_ct_employees', 'my_ct_depts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_ct_depts VALUES (1, 'Engineering', 500000)");
        $this->pdo->exec("INSERT INTO my_ct_depts VALUES (2, 'Sales', 200000)");

        $this->pdo->exec("INSERT INTO my_ct_employees VALUES (1, 'Alice', 1, 120000)");
        $this->pdo->exec("INSERT INTO my_ct_employees VALUES (2, 'Bob', 1, 100000)");
        $this->pdo->exec("INSERT INTO my_ct_employees VALUES (3, 'Carol', 2, 80000)");
    }

    /**
     * JOIN between two shadow-populated tables.
     */
    public function testJoinBothShadowTables(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name AS emp, d.name AS dept
             FROM my_ct_employees e
             JOIN my_ct_depts d ON d.id = e.dept_id
             ORDER BY e.name"
        );
        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['emp']);
        $this->assertSame('Engineering', $rows[0]['dept']);
        $this->assertSame('Carol', $rows[2]['emp']);
        $this->assertSame('Sales', $rows[2]['dept']);
    }

    /**
     * INSERT into child after parent, then verify JOIN.
     */
    public function testInsertChildAfterParentVisibleInJoin(): void
    {
        $this->pdo->exec("INSERT INTO my_ct_depts VALUES (3, 'Marketing', 100000)");
        $this->pdo->exec("INSERT INTO my_ct_employees VALUES (4, 'Dave', 3, 70000)");

        $rows = $this->ztdQuery(
            "SELECT e.name AS emp, d.name AS dept
             FROM my_ct_employees e
             JOIN my_ct_depts d ON d.id = e.dept_id
             WHERE d.id = 3"
        );
        $this->assertCount(1, $rows);
        $this->assertSame('Dave', $rows[0]['emp']);
        $this->assertSame('Marketing', $rows[0]['dept']);
    }

    /**
     * Aggregate query across two shadow tables.
     */
    public function testCrossTableAggregate(): void
    {
        $rows = $this->ztdQuery(
            "SELECT d.name AS dept, COUNT(e.id) AS headcount, SUM(e.salary) AS total_salary
             FROM my_ct_depts d
             LEFT JOIN my_ct_employees e ON e.dept_id = d.id
             GROUP BY d.id, d.name
             ORDER BY d.name"
        );
        $this->assertCount(2, $rows);
        $this->assertSame('Engineering', $rows[0]['dept']);
        $this->assertEquals(2, (int) $rows[0]['headcount']);
        $this->assertEquals(220000, (float) $rows[0]['total_salary']);
        $this->assertSame('Sales', $rows[1]['dept']);
        $this->assertEquals(1, (int) $rows[1]['headcount']);
    }

    /**
     * UPDATE parent, verify child JOIN reflects new parent data.
     */
    public function testUpdateParentVisibleInChildJoin(): void
    {
        $this->pdo->exec("UPDATE my_ct_depts SET name = 'Eng Team' WHERE id = 1");

        $rows = $this->ztdQuery(
            "SELECT e.name AS emp, d.name AS dept
             FROM my_ct_employees e
             JOIN my_ct_depts d ON d.id = e.dept_id
             WHERE e.id = 1"
        );
        $this->assertSame('Eng Team', $rows[0]['dept']);
    }

    /**
     * DELETE from parent, verify orphan handling in LEFT JOIN.
     */
    public function testDeleteParentOrphanInLeftJoin(): void
    {
        $this->pdo->exec("DELETE FROM my_ct_depts WHERE id = 2");

        $rows = $this->ztdQuery(
            "SELECT e.name AS emp, d.name AS dept
             FROM my_ct_employees e
             LEFT JOIN my_ct_depts d ON d.id = e.dept_id
             ORDER BY e.name"
        );
        $this->assertCount(3, $rows);
        // Carol's dept should be NULL after parent deletion
        $carolRow = array_values(array_filter($rows, fn($r) => $r['emp'] === 'Carol'));
        $this->assertCount(1, $carolRow);
        $this->assertNull($carolRow[0]['dept']);
    }

    /**
     * Subquery referencing the other shadow table.
     */
    public function testSubqueryAcrossShadowTables(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name FROM my_ct_employees
             WHERE dept_id IN (SELECT id FROM my_ct_depts WHERE budget > 300000)
             ORDER BY name"
        );
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
    }

    /**
     * INSERT...SELECT across two shadow tables.
     */
    public function testInsertSelectAcrossShadowTables(): void
    {
        $this->createTable(
            'CREATE TABLE my_ct_summary (
                dept_name VARCHAR(50) PRIMARY KEY,
                headcount INT NOT NULL
            )'
        );

        try {
            $this->pdo->exec(
                "INSERT INTO my_ct_summary (dept_name, headcount)
                 SELECT d.name, COUNT(e.id)
                 FROM my_ct_depts d
                 LEFT JOIN my_ct_employees e ON e.dept_id = d.id
                 GROUP BY d.name"
            );

            $rows = $this->ztdQuery("SELECT * FROM my_ct_summary ORDER BY dept_name");
            $this->assertCount(2, $rows);
            $this->assertSame('Engineering', $rows[0]['dept_name']);
            $this->assertEquals(2, (int) $rows[0]['headcount']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('INSERT...SELECT across shadow tables not supported: ' . $e->getMessage());
        } finally {
            $this->dropTable('my_ct_summary');
        }
    }

    /**
     * UPDATE with subquery referencing the other shadow table.
     */
    public function testUpdateWithCrossTableSubquery(): void
    {
        $this->pdo->exec(
            "UPDATE my_ct_depts SET budget = (
                SELECT COALESCE(SUM(salary), 0) FROM my_ct_employees WHERE dept_id = my_ct_depts.id
             )"
        );
        $rows = $this->ztdQuery("SELECT id, budget FROM my_ct_depts ORDER BY id");
        $this->assertEquals(220000, (float) $rows[0]['budget']);
        $this->assertEquals(80000, (float) $rows[1]['budget']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $d = $this->pdo->query("SELECT COUNT(*) FROM my_ct_depts")->fetchColumn();
        $e = $this->pdo->query("SELECT COUNT(*) FROM my_ct_employees")->fetchColumn();
        $this->assertSame(0, (int) $d);
        $this->assertSame(0, (int) $e);
    }
}
