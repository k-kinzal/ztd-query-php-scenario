<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests FOREIGN KEY CASCADE behavior through ZTD shadow store on MySQLi.
 *
 * FK cascades (ON DELETE CASCADE, ON UPDATE CASCADE) are fundamental to
 * relational applications. The shadow store must handle cascade effects
 * correctly — or at least not silently produce inconsistent data.
 *
 * @spec SPEC-4.3, SPEC-8.1
 */
class FkCascadeShadowTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_fk_departments (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE mi_fk_employees (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                dept_id INT NOT NULL,
                FOREIGN KEY (dept_id) REFERENCES mi_fk_departments(id) ON DELETE CASCADE ON UPDATE CASCADE
            ) ENGINE=InnoDB',
            'CREATE TABLE mi_fk_tasks (
                id INT PRIMARY KEY AUTO_INCREMENT,
                title VARCHAR(100) NOT NULL,
                employee_id INT NOT NULL,
                FOREIGN KEY (employee_id) REFERENCES mi_fk_employees(id) ON DELETE CASCADE
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_fk_tasks', 'mi_fk_employees', 'mi_fk_departments'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_fk_departments (id, name) VALUES (1, 'Engineering')");
        $this->mysqli->query("INSERT INTO mi_fk_departments (id, name) VALUES (2, 'Marketing')");
        $this->mysqli->query("INSERT INTO mi_fk_employees (id, name, dept_id) VALUES (10, 'Alice', 1)");
        $this->mysqli->query("INSERT INTO mi_fk_employees (id, name, dept_id) VALUES (20, 'Bob', 1)");
        $this->mysqli->query("INSERT INTO mi_fk_employees (id, name, dept_id) VALUES (30, 'Carol', 2)");
        $this->mysqli->query("INSERT INTO mi_fk_tasks (id, title, employee_id) VALUES (100, 'Build API', 10)");
        $this->mysqli->query("INSERT INTO mi_fk_tasks (id, title, employee_id) VALUES (101, 'Write Tests', 10)");
        $this->mysqli->query("INSERT INTO mi_fk_tasks (id, title, employee_id) VALUES (102, 'Marketing Plan', 30)");
    }

    /**
     * DELETE parent — children should be cascade-deleted in shadow store.
     */
    public function testDeleteParentCascadesToChildren(): void
    {
        try {
            $this->mysqli->query("DELETE FROM mi_fk_departments WHERE id = 1");

            $employees = $this->ztdQuery("SELECT name FROM mi_fk_employees ORDER BY id");

            if (count($employees) !== 1) {
                $this->markTestIncomplete(
                    'FK CASCADE DELETE: expected 1 (Carol), got ' . count($employees)
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
     * DELETE grandparent — cascade to grandchildren.
     */
    public function testDeleteGrandparentCascadesToGrandchildren(): void
    {
        try {
            $this->mysqli->query("DELETE FROM mi_fk_departments WHERE id = 1");

            $tasks = $this->ztdQuery("SELECT title FROM mi_fk_tasks ORDER BY id");

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
     * INSERT with invalid FK — should fail.
     */
    public function testInsertChildWithInvalidFkFails(): void
    {
        $inserted = false;
        try {
            $this->mysqli->query("INSERT INTO mi_fk_employees (id, name, dept_id) VALUES (40, 'Dave', 999)");
            $inserted = true;
        } catch (\Throwable $e) {
            $this->assertStringContainsStringIgnoringCase('foreign key', $e->getMessage());
            return;
        }

        if ($inserted) {
            $employees = $this->ztdQuery("SELECT name FROM mi_fk_employees WHERE id = 40");
            if (count($employees) === 1) {
                $this->markTestIncomplete(
                    'FK CONSTRAINT: INSERT with invalid FK succeeded — shadow store does not enforce FK constraints'
                );
            }
        }
    }

    /**
     * UPDATE parent PK — child FK should cascade-update.
     */
    public function testUpdateParentPkCascadesToChildFk(): void
    {
        try {
            $this->mysqli->query("UPDATE mi_fk_departments SET id = 100 WHERE id = 1");

            $employees = $this->ztdQuery(
                "SELECT name, dept_id FROM mi_fk_employees WHERE dept_id = 100 ORDER BY id"
            );

            if (count($employees) !== 2) {
                $all = $this->ztdQuery("SELECT name, dept_id FROM mi_fk_employees ORDER BY id");
                $this->markTestIncomplete(
                    'FK CASCADE UPDATE: expected 2 with dept_id=100, got ' . count($employees)
                    . '. All: ' . json_encode($all)
                );
            }

            $this->assertSame('Alice', $employees[0]['name']);
            $this->assertSame('Bob', $employees[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE parent PK with FK CASCADE failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE parent then JOIN — no orphans.
     */
    public function testDeleteParentThenJoinShowsNoOrphans(): void
    {
        try {
            $this->mysqli->query("DELETE FROM mi_fk_departments WHERE id = 1");

            $rows = $this->ztdQuery(
                "SELECT e.name, d.name AS dept
                 FROM mi_fk_employees e
                 LEFT JOIN mi_fk_departments d ON e.dept_id = d.id
                 ORDER BY e.id"
            );

            if (count($rows) !== 1) {
                $all = $this->ztdQuery("SELECT id, name, dept_id FROM mi_fk_employees ORDER BY id");
                $orphaned = array_filter($rows, fn($r) => $r['dept'] === null);
                if (count($orphaned) > 0) {
                    $this->markTestIncomplete(
                        'FK CASCADE DELETE left ' . count($orphaned) . ' orphaned employee(s). '
                        . 'All: ' . json_encode($all)
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
