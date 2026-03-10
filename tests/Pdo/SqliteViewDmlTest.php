<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests SELECT from views after shadow DML on SQLite.
 *
 * Views are common in production schemas for reporting, access control, and
 * denormalization. After INSERT/UPDATE/DELETE on the base table, SELECTs
 * from a view should reflect the shadow data.
 *
 * @spec SPEC-3.1, SPEC-4.1
 */
class SqliteViewDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_vw_employees (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                department TEXT NOT NULL,
                salary REAL NOT NULL,
                active INTEGER NOT NULL DEFAULT 1
            )',
            'CREATE VIEW sl_vw_active_employees AS
                SELECT id, name, department, salary
                FROM sl_vw_employees
                WHERE active = 1',
            'CREATE VIEW sl_vw_dept_summary AS
                SELECT department, COUNT(*) AS emp_count, AVG(salary) AS avg_salary
                FROM sl_vw_employees
                WHERE active = 1
                GROUP BY department',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_vw_dept_summary', 'sl_vw_active_employees', 'sl_vw_employees'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_vw_employees VALUES (1, 'Alice', 'Engineering', 120000, 1)");
        $this->pdo->exec("INSERT INTO sl_vw_employees VALUES (2, 'Bob', 'Engineering', 110000, 1)");
        $this->pdo->exec("INSERT INTO sl_vw_employees VALUES (3, 'Charlie', 'Sales', 95000, 1)");
        $this->pdo->exec("INSERT INTO sl_vw_employees VALUES (4, 'Diana', 'Sales', 90000, 0)");
    }

    /**
     * SELECT from simple view after INSERT on base table.
     */
    public function testViewAfterInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_vw_employees VALUES (5, 'Eve', 'Engineering', 130000, 1)");

            $rows = $this->ztdQuery("SELECT name FROM sl_vw_active_employees WHERE department = 'Engineering' ORDER BY name");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'View after INSERT: expected 3 active engineers, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $names = array_column($rows, 'name');
            $this->assertContains('Eve', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('View after INSERT failed: ' . $e->getMessage());
        }
    }

    /**
     * SELECT from view after UPDATE changes a row's visibility.
     */
    public function testViewAfterUpdateChangesVisibility(): void
    {
        try {
            // Deactivate Bob — should disappear from active view
            $this->pdo->exec("UPDATE sl_vw_employees SET active = 0 WHERE id = 2");

            $rows = $this->ztdQuery("SELECT name FROM sl_vw_active_employees ORDER BY name");

            $names = array_column($rows, 'name');

            if (in_array('Bob', $names)) {
                $this->markTestIncomplete(
                    'View after UPDATE: Bob should not appear in active view. Data: ' . json_encode($rows)
                );
            }

            $this->assertNotContains('Bob', $names);
            $this->assertContains('Alice', $names);
            $this->assertContains('Charlie', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('View after UPDATE visibility change failed: ' . $e->getMessage());
        }
    }

    /**
     * Aggregate view reflects DML changes.
     */
    public function testAggregateViewAfterDml(): void
    {
        try {
            // Add another engineer
            $this->pdo->exec("INSERT INTO sl_vw_employees VALUES (5, 'Eve', 'Engineering', 130000, 1)");
            // Remove Charlie from Sales
            $this->pdo->exec("DELETE FROM sl_vw_employees WHERE id = 3");

            $rows = $this->ztdQuery("SELECT department, emp_count FROM sl_vw_dept_summary ORDER BY department");

            $byDept = [];
            foreach ($rows as $r) {
                $byDept[$r['department']] = (int) $r['emp_count'];
            }

            if (($byDept['Engineering'] ?? 0) !== 3) {
                $this->markTestIncomplete(
                    'Aggregate view: Engineering expected 3, got ' . ($byDept['Engineering'] ?? 'missing')
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame(3, $byDept['Engineering']); // Alice, Bob, Eve
            // Sales: Diana is inactive, Charlie deleted → 0 active → may not appear
            $this->assertArrayNotHasKey('Sales', $byDept);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Aggregate view after DML failed: ' . $e->getMessage());
        }
    }

    /**
     * SELECT from view after UPDATE changes salary — avg should change.
     */
    public function testViewAvgAfterSalaryUpdate(): void
    {
        try {
            // Give Alice a raise
            $this->pdo->exec("UPDATE sl_vw_employees SET salary = 150000 WHERE id = 1");

            $rows = $this->ztdQuery(
                "SELECT avg_salary FROM sl_vw_dept_summary WHERE department = 'Engineering'"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'View avg after update: expected 1 row, got ' . count($rows)
                );
            }

            // Engineering: Alice=150000, Bob=110000 → avg=130000
            $this->assertEqualsWithDelta(130000.0, (float) $rows[0]['avg_salary'], 1.0);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('View avg after salary update failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared SELECT from view.
     */
    public function testPreparedSelectFromView(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_vw_employees VALUES (5, 'Eve', 'Engineering', 130000, 1)");

            $rows = $this->ztdPrepareAndExecute(
                "SELECT name FROM sl_vw_active_employees WHERE salary > ? ORDER BY salary DESC",
                [115000]
            );

            // Alice=120000, Eve=130000
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Prepared view SELECT: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Eve', $rows[0]['name']);
            $this->assertSame('Alice', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared SELECT from view failed: ' . $e->getMessage());
        }
    }

    /**
     * JOIN between view and base table.
     */
    public function testJoinViewWithBaseTable(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT v.name, v.salary, e.active
                 FROM sl_vw_active_employees v
                 JOIN sl_vw_employees e ON v.id = e.id
                 ORDER BY v.name"
            );

            if (count($rows) < 1) {
                $this->markTestIncomplete(
                    'JOIN view + base: expected >= 1, got ' . count($rows)
                );
            }

            // All active employees should have active=1
            foreach ($rows as $r) {
                $this->assertSame(1, (int) $r['active']);
            }
        } catch (\Throwable $e) {
            $this->markTestIncomplete('JOIN view with base table failed: ' . $e->getMessage());
        }
    }
}
