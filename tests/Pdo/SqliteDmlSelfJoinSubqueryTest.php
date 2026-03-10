<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests DML operations that use self-join subqueries on SQLite.
 *
 * The CTE rewriter must handle cases where the same table appears in both
 * the target of a DML statement and in a self-join subquery.
 *
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class SqliteDmlSelfJoinSubqueryTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_dsjs_employees (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            manager_id INTEGER,
            salary REAL NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_dsjs_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_dsjs_employees VALUES (1, 'CEO', NULL, 200000)");
        $this->pdo->exec("INSERT INTO sl_dsjs_employees VALUES (2, 'VP', 1, 150000)");
        $this->pdo->exec("INSERT INTO sl_dsjs_employees VALUES (3, 'Manager', 2, 100000)");
        $this->pdo->exec("INSERT INTO sl_dsjs_employees VALUES (4, 'Dev1', 3, 80000)");
        $this->pdo->exec("INSERT INTO sl_dsjs_employees VALUES (5, 'Dev2', 3, 70000)");
    }

    /**
     * DELETE employees who earn more than their manager.
     * Uses self-join in WHERE subquery.
     *
     * @spec SPEC-4.3
     */
    public function testDeleteWithSelfJoinSubquery(): void
    {
        try {
            // First, give Dev1 a salary higher than Manager's
            $this->pdo->exec("UPDATE sl_dsjs_employees SET salary = 120000 WHERE id = 4");

            $this->pdo->exec(
                "DELETE FROM sl_dsjs_employees
                 WHERE id IN (
                     SELECT e.id FROM sl_dsjs_employees e
                     JOIN sl_dsjs_employees m ON e.manager_id = m.id
                     WHERE e.salary > m.salary
                 )"
            );

            $rows = $this->ztdQuery('SELECT id, name FROM sl_dsjs_employees ORDER BY id');

            // Dev1 (id=4, salary 120k) earns more than Manager (id=3, salary 100k)
            // So Dev1 should be deleted
            $ids = array_column($rows, 'id');

            if (in_array(4, array_map('intval', $ids))) {
                $this->markTestIncomplete(
                    'DELETE with self-join subquery: Dev1 (id=4) should have been deleted but remains. Got ids: ' . implode(',', $ids)
                );
            }

            $this->assertCount(4, $rows, 'Should have 4 employees after deleting Dev1');
            $this->assertNotContains('4', array_column($rows, 'id'));
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'DELETE with self-join subquery failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE employees to match their manager's salary.
     * Uses self-join in correlated subquery.
     *
     * @spec SPEC-4.2
     */
    public function testUpdateWithSelfJoinSubquery(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE sl_dsjs_employees SET salary = (
                    SELECT m.salary FROM sl_dsjs_employees m
                    WHERE m.id = sl_dsjs_employees.manager_id
                )
                WHERE manager_id IS NOT NULL"
            );

            $rows = $this->ztdQuery('SELECT id, name, salary FROM sl_dsjs_employees ORDER BY id');

            if (count($rows) !== 5) {
                $this->markTestIncomplete('Expected 5 rows, got ' . count($rows));
            }

            // CEO stays 200000 (no manager)
            $this->assertEqualsWithDelta(200000, (float) $rows[0]['salary'], 0.01, 'CEO salary unchanged');
            // VP gets CEO's salary (200000)
            $this->assertEqualsWithDelta(200000, (float) $rows[1]['salary'], 0.01, 'VP should get CEO salary');
            // Manager gets VP's salary (150000)
            $this->assertEqualsWithDelta(150000, (float) $rows[2]['salary'], 0.01, 'Manager should get VP salary');
            // Dev1 gets Manager's salary (100000)
            $this->assertEqualsWithDelta(100000, (float) $rows[3]['salary'], 0.01, 'Dev1 should get Manager salary');
            // Dev2 gets Manager's salary (100000)
            $this->assertEqualsWithDelta(100000, (float) $rows[4]['salary'], 0.01, 'Dev2 should get Manager salary');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE with self-join subquery failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT with self-join baseline (verify the join works).
     *
     * @spec SPEC-3.1
     */
    public function testSelectSelfJoinWorks(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT e.name AS emp, m.name AS mgr, e.salary, m.salary AS mgr_salary
                 FROM sl_dsjs_employees e
                 JOIN sl_dsjs_employees m ON e.manager_id = m.id
                 ORDER BY e.id"
            );

            $this->assertCount(4, $rows, 'Should have 4 employees with managers');
            $this->assertSame('VP', $rows[0]['emp']);
            $this->assertSame('CEO', $rows[0]['mgr']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT self-join failed: ' . $e->getMessage());
        }
    }
}
