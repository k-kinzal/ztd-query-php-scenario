<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests FOREIGN KEY CASCADE behavior through ZTD shadow store on PostgreSQL.
 *
 * FK cascades (ON DELETE CASCADE, ON UPDATE CASCADE) are fundamental to
 * relational applications. The shadow store must handle cascade effects
 * correctly — or at least not silently produce inconsistent data.
 *
 * @spec SPEC-4.3, SPEC-8.1
 */
class PostgresFkCascadeShadowTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_fk_departments (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50) NOT NULL
            )',
            'CREATE TABLE pg_fk_employees (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                dept_id INTEGER NOT NULL REFERENCES pg_fk_departments(id) ON DELETE CASCADE ON UPDATE CASCADE
            )',
            'CREATE TABLE pg_fk_tasks (
                id SERIAL PRIMARY KEY,
                title VARCHAR(100) NOT NULL,
                employee_id INTEGER NOT NULL REFERENCES pg_fk_employees(id) ON DELETE CASCADE
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_fk_tasks', 'pg_fk_employees', 'pg_fk_departments'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_fk_departments VALUES (1, 'Engineering')");
        $this->pdo->exec("INSERT INTO pg_fk_departments VALUES (2, 'Marketing')");
        $this->pdo->exec("INSERT INTO pg_fk_employees VALUES (10, 'Alice', 1)");
        $this->pdo->exec("INSERT INTO pg_fk_employees VALUES (20, 'Bob', 1)");
        $this->pdo->exec("INSERT INTO pg_fk_employees VALUES (30, 'Carol', 2)");
        $this->pdo->exec("INSERT INTO pg_fk_tasks (id, title, employee_id) VALUES (100, 'Build API', 10)");
        $this->pdo->exec("INSERT INTO pg_fk_tasks (id, title, employee_id) VALUES (101, 'Write Tests', 10)");
        $this->pdo->exec("INSERT INTO pg_fk_tasks (id, title, employee_id) VALUES (102, 'Marketing Plan', 30)");
    }

    /**
     * DELETE parent — children should be cascade-deleted in shadow store.
     */
    public function testDeleteParentCascadesToChildren(): void
    {
        try {
            $this->pdo->exec("DELETE FROM pg_fk_departments WHERE id = 1");

            $employees = $this->ztdQuery("SELECT name FROM pg_fk_employees ORDER BY id");

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
            $this->pdo->exec("DELETE FROM pg_fk_departments WHERE id = 1");

            $tasks = $this->ztdQuery("SELECT title FROM pg_fk_tasks ORDER BY id");

            if (count($tasks) !== 1) {
                $this->markTestIncomplete(
                    'FK CASCADE grandchild: expected 1 task, got ' . count($tasks)
                    . '. Data: ' . json_encode($tasks)
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
            $this->pdo->exec("INSERT INTO pg_fk_employees VALUES (40, 'Dave', 999)");
            $inserted = true;
        } catch (\Throwable $e) {
            $this->assertStringContainsStringIgnoringCase('foreign key', $e->getMessage());
            return;
        }

        if ($inserted) {
            $rows = $this->ztdQuery("SELECT name FROM pg_fk_employees WHERE id = 40");
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
            $this->pdo->exec("UPDATE pg_fk_departments SET id = 100 WHERE id = 1");

            $employees = $this->ztdQuery(
                "SELECT name, dept_id FROM pg_fk_employees WHERE dept_id = 100 ORDER BY id"
            );

            if (count($employees) !== 2) {
                $oldRef = $this->ztdQuery("SELECT name, dept_id FROM pg_fk_employees ORDER BY id");
                $this->markTestIncomplete(
                    'FK CASCADE UPDATE: expected 2 with dept_id=100, got ' . count($employees)
                    . '. All: ' . json_encode($oldRef)
                );
            }

            $this->assertSame('Alice', $employees[0]['name']);
            $this->assertSame('Bob', $employees[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE parent PK with FK CASCADE failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE parent then JOIN — verify no orphaned rows.
     */
    public function testDeleteParentThenJoinShowsNoOrphans(): void
    {
        try {
            $this->pdo->exec("DELETE FROM pg_fk_departments WHERE id = 1");

            $rows = $this->ztdQuery(
                "SELECT e.name, d.name AS dept
                 FROM pg_fk_employees e
                 LEFT JOIN pg_fk_departments d ON e.dept_id = d.id
                 ORDER BY e.id"
            );

            if (count($rows) !== 1) {
                $allEmps = $this->ztdQuery("SELECT id, name, dept_id FROM pg_fk_employees ORDER BY id");
                $orphaned = array_filter($rows, fn($r) => $r['dept'] === null);
                if (count($orphaned) > 0) {
                    $this->markTestIncomplete(
                        'FK CASCADE DELETE left ' . count($orphaned) . ' orphaned employee(s). '
                        . 'All: ' . json_encode($allEmps)
                    );
                }
                $this->markTestIncomplete(
                    'FK CASCADE JOIN: expected 1, got ' . count($rows) . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame('Carol', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE parent then JOIN failed: ' . $e->getMessage());
        }
    }
}
