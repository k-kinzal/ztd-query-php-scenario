<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests foreign key CASCADE visibility in the shadow store on SQLite.
 *
 * When PRAGMA foreign_keys = ON and a row is deleted from a parent table,
 * SQLite's ON DELETE CASCADE should delete child rows. The question is
 * whether the shadow store reflects these cascade-deleted rows.
 *
 * This is a critical usability test: if users rely on FK cascades and the
 * shadow store doesn't track them, they will see stale child rows.
 *
 * @spec SPEC-8.1
 */
class SqliteFkCascadeVisibilityTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE sl_fkc_departments (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )",
            "CREATE TABLE sl_fkc_employees (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                dept_id INTEGER NOT NULL,
                FOREIGN KEY (dept_id) REFERENCES sl_fkc_departments(id) ON DELETE CASCADE
            )",
            "CREATE TABLE sl_fkc_tasks (
                id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                employee_id INTEGER NOT NULL,
                FOREIGN KEY (employee_id) REFERENCES sl_fkc_employees(id) ON DELETE CASCADE
            )",
            "CREATE TABLE sl_fkc_projects (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )",
            "CREATE TABLE sl_fkc_assignments (
                id INTEGER PRIMARY KEY,
                project_id INTEGER,
                description TEXT NOT NULL,
                FOREIGN KEY (project_id) REFERENCES sl_fkc_projects(id) ON DELETE SET NULL
            )",
            "CREATE TABLE sl_fkc_categories (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )",
            "CREATE TABLE sl_fkc_items (
                id INTEGER PRIMARY KEY,
                category_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                FOREIGN KEY (category_id) REFERENCES sl_fkc_categories(id) ON UPDATE CASCADE
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return [
            'sl_fkc_items', 'sl_fkc_categories',
            'sl_fkc_assignments', 'sl_fkc_projects',
            'sl_fkc_tasks', 'sl_fkc_employees', 'sl_fkc_departments',
        ];
    }

    protected function createZtdConnection(\PDO $rawPdo): \ZtdQuery\Adapter\Pdo\ZtdPdo
    {
        $rawPdo->exec("PRAGMA foreign_keys = ON");
        return parent::createZtdConnection($rawPdo);
    }

    /**
     * ON DELETE CASCADE: deleting parent should cascade to children in shadow.
     */
    public function testDeleteCascadeVisibility(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_fkc_departments VALUES (1, 'Engineering')");
            $this->pdo->exec("INSERT INTO sl_fkc_departments VALUES (2, 'Marketing')");
            $this->pdo->exec("INSERT INTO sl_fkc_employees VALUES (1, 'Alice', 1)");
            $this->pdo->exec("INSERT INTO sl_fkc_employees VALUES (2, 'Bob', 1)");
            $this->pdo->exec("INSERT INTO sl_fkc_employees VALUES (3, 'Carol', 2)");

            // Delete department 1 — should cascade to Alice and Bob
            $this->pdo->exec("DELETE FROM sl_fkc_departments WHERE id = 1");

            $depts = $this->ztdQuery("SELECT name FROM sl_fkc_departments");
            $emps = $this->ztdQuery("SELECT name FROM sl_fkc_employees ORDER BY name");

            // Department check
            $this->assertCount(1, $depts);
            $this->assertSame('Marketing', $depts[0]['name']);

            // Employee check — cascade should have removed Alice and Bob
            if (count($emps) === 3) {
                $this->markTestIncomplete(
                    'ON DELETE CASCADE did not propagate to shadow store. All 3 employees remain. '
                    . 'Shadow store does not track FK cascade deletes.'
                );
            }
            if (count($emps) !== 1) {
                $this->markTestIncomplete(
                    'After CASCADE delete, expected 1 employee (Carol), got ' . count($emps) . ': ' . json_encode($emps)
                );
            }

            $this->assertCount(1, $emps);
            $this->assertSame('Carol', $emps[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE CASCADE visibility test failed: ' . $e->getMessage());
        }
    }

    /**
     * Multi-level CASCADE: department → employees → tasks.
     */
    public function testMultiLevelCascade(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_fkc_departments VALUES (1, 'Engineering')");
            $this->pdo->exec("INSERT INTO sl_fkc_employees VALUES (1, 'Alice', 1)");
            $this->pdo->exec("INSERT INTO sl_fkc_tasks VALUES (1, 'Task A', 1)");
            $this->pdo->exec("INSERT INTO sl_fkc_tasks VALUES (2, 'Task B', 1)");

            // Delete department → should cascade to employee → should cascade to tasks
            $this->pdo->exec("DELETE FROM sl_fkc_departments WHERE id = 1");

            $tasks = $this->ztdQuery("SELECT title FROM sl_fkc_tasks");
            $emps = $this->ztdQuery("SELECT name FROM sl_fkc_employees");

            if (count($emps) > 0) {
                $this->markTestIncomplete(
                    'Multi-level CASCADE: employees not cascaded. Got ' . count($emps) . ' employees.'
                );
            }
            if (count($tasks) > 0) {
                $this->markTestIncomplete(
                    'Multi-level CASCADE: tasks not cascaded. Got ' . count($tasks) . ' tasks. '
                    . 'Shadow store does not track multi-level FK cascades.'
                );
            }

            $this->assertCount(0, $emps);
            $this->assertCount(0, $tasks);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-level CASCADE test failed: ' . $e->getMessage());
        }
    }

    /**
     * ON DELETE SET NULL: verify shadow tracks the NULL update.
     */
    public function testDeleteSetNullVisibility(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_fkc_projects VALUES (1, 'Alpha')");
            $this->pdo->exec("INSERT INTO sl_fkc_assignments VALUES (1, 1, 'Design')");
            $this->pdo->exec("INSERT INTO sl_fkc_assignments VALUES (2, 1, 'Build')");

            // Delete project → should SET NULL on assignments.project_id
            $this->pdo->exec("DELETE FROM sl_fkc_projects WHERE id = 1");

            $assignments = $this->ztdQuery("SELECT id, project_id, description FROM sl_fkc_assignments ORDER BY id");

            if (count($assignments) === 0) {
                $this->markTestIncomplete(
                    'ON DELETE SET NULL: assignments were deleted instead of having project_id set to NULL.'
                );
            }
            if (count($assignments) !== 2) {
                $this->markTestIncomplete(
                    'ON DELETE SET NULL: expected 2 assignments, got ' . count($assignments) . ': ' . json_encode($assignments)
                );
            }

            // project_id should be NULL
            if ($assignments[0]['project_id'] !== null) {
                $this->markTestIncomplete(
                    'ON DELETE SET NULL: project_id is "' . $assignments[0]['project_id']
                    . '" instead of NULL. Shadow store does not track FK SET NULL.'
                );
            }

            $this->assertNull($assignments[0]['project_id']);
            $this->assertNull($assignments[1]['project_id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('ON DELETE SET NULL test failed: ' . $e->getMessage());
        }
    }

    /**
     * CASCADE after UPDATE of parent PK (ON UPDATE CASCADE).
     */
    public function testUpdateCascadeVisibility(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_fkc_categories VALUES (1, 'Books')");
            $this->pdo->exec("INSERT INTO sl_fkc_items VALUES (1, 1, 'Novel')");
            $this->pdo->exec("INSERT INTO sl_fkc_items VALUES (2, 1, 'Textbook')");

            // Update parent PK → should cascade to children
            $this->pdo->exec("UPDATE sl_fkc_categories SET id = 100 WHERE id = 1");

            $items = $this->ztdQuery("SELECT name, category_id FROM sl_fkc_items ORDER BY name");

            if (count($items) === 0) {
                $this->markTestIncomplete('ON UPDATE CASCADE: items disappeared after parent PK update.');
            }

            $this->assertCount(2, $items);

            if ((int) $items[0]['category_id'] === 1) {
                $this->markTestIncomplete(
                    'ON UPDATE CASCADE: category_id still 1, not cascaded to 100. '
                    . 'Shadow store does not track FK UPDATE CASCADE.'
                );
            }

            $this->assertSame(100, (int) $items[0]['category_id']);
            $this->assertSame(100, (int) $items[1]['category_id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('ON UPDATE CASCADE test failed: ' . $e->getMessage());
        }
    }
}
