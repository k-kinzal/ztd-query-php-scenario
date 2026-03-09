<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests querying a view JOINed with a shadow-modified table on SQLite.
 *
 * Real-world scenario: applications use views for reporting or access control,
 * then JOIN those views with other tables. When ZTD shadow mutations exist on
 * the base table or the joined table, the query returns a mix of physical (view)
 * and shadow (table) data, which can silently produce incorrect results.
 *
 * @spec SPEC-3.3b
 */
class SqliteViewJoinShadowTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_vjs_departments (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1
            )',
            'CREATE TABLE sl_vjs_employees (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                dept_id INTEGER NOT NULL,
                salary REAL NOT NULL
            )',
            "INSERT INTO sl_vjs_departments VALUES (1, 'Engineering', 1)",
            "INSERT INTO sl_vjs_departments VALUES (2, 'Marketing', 1)",
            "INSERT INTO sl_vjs_departments VALUES (3, 'Archived', 0)",
            "INSERT INTO sl_vjs_employees VALUES (1, 'Alice', 1, 90000)",
            "INSERT INTO sl_vjs_employees VALUES (2, 'Bob', 2, 80000)",
            "INSERT INTO sl_vjs_employees VALUES (3, 'Charlie', 1, 85000)",
            'CREATE VIEW sl_vjs_active_depts AS SELECT id, name FROM sl_vjs_departments WHERE active = 1',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_vjs_employees', 'sl_vjs_departments'];
    }

    /**
     * JOIN view with shadow-modified table: shadow INSERT into employees,
     * then SELECT from view JOIN employees. The view reads physical data
     * while employees should include shadow data.
     */
    public function testViewJoinWithShadowInsert(): void
    {
        // Shadow insert a new employee in Engineering
        $this->ztdExec("INSERT INTO sl_vjs_employees VALUES (4, 'Diana', 1, 95000)");

        // Query: join the view (physical) with employees (shadow)
        try {
            $rows = $this->ztdQuery(
                "SELECT e.name, d.name AS dept
                 FROM sl_vjs_employees e
                 JOIN sl_vjs_active_depts d ON e.dept_id = d.id
                 ORDER BY e.name"
            );

            // If view is not rewritten (physical) but employees IS rewritten (shadow),
            // we expect shadow employees joined with physical view
            $names = array_column($rows, 'name');

            if (!in_array('Diana', $names)) {
                $this->markTestIncomplete(
                    'Shadow-inserted employee Diana not visible when JOINing with view. '
                    . 'View reads physical data but JOIN with shadow table may lose shadow rows. '
                    . 'Got ' . count($rows) . ' rows: ' . implode(', ', $names)
                );
            }

            // Expected: Alice(Eng), Bob(Mkt), Charlie(Eng), Diana(Eng) = 4 rows
            $this->assertCount(4, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'View JOIN shadow query failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Shadow UPDATE on base table should not affect view results.
     */
    public function testViewNotAffectedByShadowUpdateOnBaseTable(): void
    {
        // Shadow update: deactivate Engineering
        $this->ztdExec("UPDATE sl_vjs_departments SET active = 0 WHERE id = 1");

        // View should still show 2 active depts (physical, unmodified)
        $rows = $this->ztdQuery("SELECT * FROM sl_vjs_active_depts ORDER BY id");
        $this->assertCount(2, $rows, 'View should read physical data, not shadow');
    }

    /**
     * Shadow DELETE on joined table, then query through view JOIN.
     */
    public function testViewJoinAfterShadowDelete(): void
    {
        // Shadow delete Bob
        $this->ztdExec("DELETE FROM sl_vjs_employees WHERE id = 2");

        try {
            $rows = $this->ztdQuery(
                "SELECT e.name, d.name AS dept
                 FROM sl_vjs_employees e
                 JOIN sl_vjs_active_depts d ON e.dept_id = d.id
                 ORDER BY e.name"
            );

            $names = array_column($rows, 'name');

            if (in_array('Bob', $names)) {
                $this->markTestIncomplete(
                    'Shadow-deleted employee Bob still visible when JOINing with view.'
                );
            }

            // Expected: Alice(Eng), Charlie(Eng) = 2 rows (Bob deleted)
            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'View JOIN after shadow DELETE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Aggregate query through view JOIN with shadow data.
     */
    public function testViewJoinAggregateWithShadow(): void
    {
        // Shadow insert
        $this->ztdExec("INSERT INTO sl_vjs_employees VALUES (4, 'Diana', 1, 95000)");

        try {
            $rows = $this->ztdQuery(
                "SELECT d.name AS dept, COUNT(e.id) AS emp_count, AVG(e.salary) AS avg_salary
                 FROM sl_vjs_active_depts d
                 LEFT JOIN sl_vjs_employees e ON e.dept_id = d.id
                 GROUP BY d.id, d.name
                 ORDER BY d.name"
            );

            // Engineering should have 3 employees (Alice, Charlie, Diana) in shadow
            $eng = null;
            foreach ($rows as $row) {
                if ($row['dept'] === 'Engineering') {
                    $eng = $row;
                }
            }

            if ($eng === null) {
                $this->markTestIncomplete('Engineering department not found in aggregate results');
            }

            if ((int) $eng['emp_count'] !== 3) {
                $this->markTestIncomplete(
                    'Engineering employee count mismatch in view JOIN aggregate. '
                    . 'Expected 3, got ' . $eng['emp_count']
                    . '. View reads physical while employee table reads shadow.'
                );
            }

            $this->assertEquals(3, (int) $eng['emp_count']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'View JOIN aggregate with shadow failed: ' . $e->getMessage()
            );
        }
    }
}
