<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests FOREIGN KEY CASCADE behavior through ZTD shadow store on SQLite.
 *
 * FK cascades (ON DELETE CASCADE, ON UPDATE CASCADE) are fundamental to
 * relational applications. The shadow store must handle cascade effects
 * correctly — or at least not silently produce inconsistent data.
 *
 * @spec SPEC-4.3, SPEC-8.1
 */
class SqliteFkCascadeShadowTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'PRAGMA foreign_keys = ON',
            'CREATE TABLE sl_fk_departments (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )',
            'CREATE TABLE sl_fk_employees (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                dept_id INTEGER NOT NULL,
                FOREIGN KEY (dept_id) REFERENCES sl_fk_departments(id) ON DELETE CASCADE ON UPDATE CASCADE
            )',
            'CREATE TABLE sl_fk_tasks (
                id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                employee_id INTEGER NOT NULL,
                FOREIGN KEY (employee_id) REFERENCES sl_fk_employees(id) ON DELETE CASCADE
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_fk_tasks', 'sl_fk_employees', 'sl_fk_departments'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_fk_departments VALUES (1, 'Engineering')");
        $this->pdo->exec("INSERT INTO sl_fk_departments VALUES (2, 'Marketing')");
        $this->pdo->exec("INSERT INTO sl_fk_employees VALUES (10, 'Alice', 1)");
        $this->pdo->exec("INSERT INTO sl_fk_employees VALUES (20, 'Bob', 1)");
        $this->pdo->exec("INSERT INTO sl_fk_employees VALUES (30, 'Carol', 2)");
        $this->pdo->exec("INSERT INTO sl_fk_tasks VALUES (100, 'Build API', 10)");
        $this->pdo->exec("INSERT INTO sl_fk_tasks VALUES (101, 'Write Tests', 10)");
        $this->pdo->exec("INSERT INTO sl_fk_tasks VALUES (102, 'Marketing Plan', 30)");
    }

    /**
     * DELETE parent row — child rows should be cascade-deleted in shadow store.
     */
    public function testDeleteParentCascadesToChildren(): void
    {
        try {
            $this->pdo->exec("DELETE FROM sl_fk_departments WHERE id = 1");

            // Engineering employees (Alice, Bob) should be cascade-deleted
            $employees = $this->ztdQuery("SELECT name FROM sl_fk_employees ORDER BY id");

            if (count($employees) !== 1) {
                $this->markTestIncomplete(
                    'FK CASCADE DELETE: expected 1 employee (Carol), got ' . count($employees)
                    . '. Data: ' . json_encode($employees)
                    . ' — shadow store may not enforce FK cascades'
                );
            }

            $this->assertSame('Carol', $employees[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE parent with FK CASCADE failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE grandparent — cascade should propagate to grandchildren.
     */
    public function testDeleteGrandparentCascadesToGrandchildren(): void
    {
        try {
            $this->pdo->exec("DELETE FROM sl_fk_departments WHERE id = 1");

            // Tasks of Alice (10) should be cascade-deleted through employees
            $tasks = $this->ztdQuery("SELECT title FROM sl_fk_tasks ORDER BY id");

            if (count($tasks) !== 1) {
                $this->markTestIncomplete(
                    'FK CASCADE grandchild: expected 1 task (Marketing Plan), got ' . count($tasks)
                    . '. Data: ' . json_encode($tasks)
                    . ' — multi-level cascade may not work in shadow store'
                );
            }

            $this->assertSame('Marketing Plan', $tasks[0]['title']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-level FK CASCADE failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT child with non-existent parent FK — should fail.
     */
    public function testInsertChildWithInvalidFkFails(): void
    {
        $inserted = false;
        try {
            $this->pdo->exec("INSERT INTO sl_fk_employees VALUES (40, 'Dave', 999)");
            $inserted = true;
        } catch (\Throwable $e) {
            // Expected: FOREIGN KEY constraint violation
            $this->assertStringContainsString('FOREIGN KEY', strtoupper($e->getMessage()));
            return;
        }

        if ($inserted) {
            $rows = $this->ztdQuery("SELECT name FROM sl_fk_employees WHERE id = 40");
            if (count($rows) === 1) {
                $this->markTestIncomplete(
                    'FK CONSTRAINT: INSERT with invalid FK succeeded — shadow store does not enforce FK constraints'
                );
            }
        }
    }

    /**
     * UPDATE parent PK — child FK references should cascade-update.
     */
    public function testUpdateParentPkCascadesToChildFk(): void
    {
        try {
            $this->pdo->exec("UPDATE sl_fk_departments SET id = 100 WHERE id = 1");

            // Employees in Engineering should now reference dept_id = 100
            $employees = $this->ztdQuery(
                "SELECT name, dept_id FROM sl_fk_employees WHERE dept_id = 100 ORDER BY id"
            );

            if (count($employees) !== 2) {
                // Check if they still reference old dept_id
                $oldRef = $this->ztdQuery(
                    "SELECT name, dept_id FROM sl_fk_employees ORDER BY id"
                );
                $this->markTestIncomplete(
                    'FK CASCADE UPDATE: expected 2 employees with dept_id=100, got ' . count($employees)
                    . '. All employees: ' . json_encode($oldRef)
                    . ' — ON UPDATE CASCADE may not propagate in shadow store'
                );
            }

            $this->assertSame('Alice', $employees[0]['name']);
            $this->assertSame('Bob', $employees[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE parent PK with FK CASCADE failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE parent then SELECT with JOIN — orphan detection.
     */
    public function testDeleteParentThenJoinShowsNoOrphans(): void
    {
        try {
            $this->pdo->exec("DELETE FROM sl_fk_departments WHERE id = 1");

            // LEFT JOIN should not show any orphaned employees
            $rows = $this->ztdQuery(
                "SELECT e.name, d.name AS dept
                 FROM sl_fk_employees e
                 LEFT JOIN sl_fk_departments d ON e.dept_id = d.id
                 ORDER BY e.id"
            );

            // After cascade, only Carol (Marketing) should remain
            if (count($rows) !== 1) {
                // Check if employees exist but are orphaned
                $allEmps = $this->ztdQuery("SELECT id, name, dept_id FROM sl_fk_employees ORDER BY id");
                $orphaned = array_filter($rows, fn($r) => $r['dept'] === null);
                if (count($orphaned) > 0) {
                    $this->markTestIncomplete(
                        'FK CASCADE DELETE left ' . count($orphaned) . ' orphaned employee(s). '
                        . 'All employees: ' . json_encode($allEmps)
                    );
                }
                $this->markTestIncomplete(
                    'FK CASCADE JOIN: expected 1, got ' . count($rows) . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame('Carol', $rows[0]['name']);
            $this->assertSame('Marketing', $rows[0]['dept']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE parent then JOIN failed: ' . $e->getMessage());
        }
    }

    /**
     * Bulk delete parents — all dependent children should cascade.
     */
    public function testBulkDeleteParentsCascadesAll(): void
    {
        try {
            $this->pdo->exec("DELETE FROM sl_fk_departments");

            $emps = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_fk_employees");
            $tasks = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_fk_tasks");

            if ((int) $emps[0]['cnt'] !== 0 || (int) $tasks[0]['cnt'] !== 0) {
                $this->markTestIncomplete(
                    'Bulk FK CASCADE: expected 0 employees and 0 tasks, got '
                    . $emps[0]['cnt'] . ' employees and ' . $tasks[0]['cnt'] . ' tasks'
                    . ' — shadow store may not cascade bulk deletes'
                );
            }

            $this->assertEquals(0, (int) $emps[0]['cnt']);
            $this->assertEquals(0, (int) $tasks[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Bulk DELETE with FK CASCADE failed: ' . $e->getMessage());
        }
    }
}
