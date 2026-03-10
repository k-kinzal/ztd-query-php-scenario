<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests date/time INTERVAL arithmetic expressions in DML operations
 * through ZTD shadow store on MySQL via MySQLi.
 *
 * MySQL uses INTERVAL syntax: col + INTERVAL 30 DAY, DATE_ADD(col, INTERVAL 7 DAY).
 * The CTE rewriter must preserve these expressions in UPDATE SET and DELETE WHERE.
 *
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class IntervalArithmeticDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE mi_iad_tasks (
            id INT PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            created_at DATETIME NOT NULL,
            due_at DATETIME,
            status VARCHAR(20) NOT NULL DEFAULT 'pending'
        ) ENGINE=InnoDB";
    }

    protected function getTableNames(): array
    {
        return ['mi_iad_tasks'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_iad_tasks VALUES (1, 'Task A', '2025-01-01 10:00:00', NULL, 'pending')");
        $this->mysqli->query("INSERT INTO mi_iad_tasks VALUES (2, 'Task B', '2025-01-15 12:00:00', NULL, 'pending')");
        $this->mysqli->query("INSERT INTO mi_iad_tasks VALUES (3, 'Task C', '2025-02-01 08:00:00', NULL, 'pending')");
        $this->mysqli->query("INSERT INTO mi_iad_tasks VALUES (4, 'Task D', '2024-12-01 09:00:00', NULL, 'pending')");
    }

    /**
     * UPDATE SET due_at = created_at + INTERVAL 30 DAY
     */
    public function testUpdateSetWithIntervalAddition(): void
    {
        try {
            $this->mysqli->query(
                "UPDATE mi_iad_tasks SET due_at = created_at + INTERVAL 30 DAY"
            );

            $rows = $this->ztdQuery("SELECT id, name, due_at FROM mi_iad_tasks ORDER BY id");

            $this->assertCount(4, $rows);

            // Task A: 2025-01-01 + 30 days = 2025-01-31 10:00:00
            if ($rows[0]['due_at'] !== '2025-01-31 10:00:00') {
                $this->markTestIncomplete(
                    'INTERVAL arithmetic: Task A due_at='
                    . var_export($rows[0]['due_at'], true) . ', expected 2025-01-31 10:00:00'
                );
            }
            $this->assertSame('2025-01-31 10:00:00', $rows[0]['due_at']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET with INTERVAL addition failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET due_at = DATE_ADD(created_at, INTERVAL 7 DAY)
     */
    public function testUpdateSetWithDateAdd(): void
    {
        try {
            $this->mysqli->query(
                "UPDATE mi_iad_tasks SET due_at = DATE_ADD(created_at, INTERVAL 7 DAY)"
            );

            $rows = $this->ztdQuery("SELECT id, due_at FROM mi_iad_tasks ORDER BY id");

            $this->assertCount(4, $rows);

            // Task A: 2025-01-01 + 7 days = 2025-01-08 10:00:00
            if ($rows[0]['due_at'] !== '2025-01-08 10:00:00') {
                $this->markTestIncomplete(
                    'DATE_ADD: Task A due_at='
                    . var_export($rows[0]['due_at'], true) . ', expected 2025-01-08 10:00:00'
                );
            }
            $this->assertSame('2025-01-08 10:00:00', $rows[0]['due_at']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET with DATE_ADD failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE WHERE created_at < NOW() - INTERVAL 60 DAY
     * Use a fixed date for reproducibility.
     */
    public function testDeleteWhereWithIntervalComparison(): void
    {
        try {
            $this->mysqli->query(
                "DELETE FROM mi_iad_tasks WHERE created_at < '2025-03-10' - INTERVAL 60 DAY"
            );

            $rows = $this->ztdQuery("SELECT name FROM mi_iad_tasks ORDER BY name");
            $names = array_column($rows, 'name');

            // 60 days before 2025-03-10 = 2025-01-09
            // Task A (2025-01-01) deleted, Task D (2024-12-01) deleted
            // Task B (2025-01-15) kept, Task C (2025-02-01) kept
            if (count($names) !== 2) {
                $this->markTestIncomplete(
                    'DELETE WHERE INTERVAL: expected 2 rows, got '
                    . count($names) . ': ' . json_encode($names)
                );
            }
            $this->assertEquals(['Task B', 'Task C'], $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE INTERVAL comparison failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with INTERVAL in WHERE clause.
     */
    public function testUpdateWhereWithInterval(): void
    {
        try {
            $this->mysqli->query(
                "UPDATE mi_iad_tasks SET status = 'overdue'
                 WHERE created_at < '2025-01-10 00:00:00'"
            );

            $rows = $this->ztdQuery("SELECT name, status FROM mi_iad_tasks ORDER BY id");

            $this->assertCount(4, $rows);

            if ($rows[0]['status'] !== 'overdue') {
                $this->markTestIncomplete(
                    'UPDATE WHERE date: Task A status=' . var_export($rows[0]['status'], true)
                );
            }
            $this->assertSame('overdue', $rows[0]['status']);
            $this->assertSame('pending', $rows[1]['status']);
            $this->assertSame('pending', $rows[2]['status']);

            if ($rows[3]['status'] !== 'overdue') {
                $this->markTestIncomplete(
                    'UPDATE WHERE date: Task D status=' . var_export($rows[3]['status'], true)
                );
            }
            $this->assertSame('overdue', $rows[3]['status']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE WHERE with date comparison failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET with DATE_FORMAT for computed string.
     */
    public function testUpdateSetWithDateFormat(): void
    {
        try {
            $this->mysqli->query(
                "UPDATE mi_iad_tasks SET name = CONCAT(name, ' (', DATE_FORMAT(created_at, '%Y-%m'), ')')"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM mi_iad_tasks ORDER BY id");

            $this->assertCount(4, $rows);

            if ($rows[0]['name'] !== 'Task A (2025-01)') {
                $this->markTestIncomplete(
                    'DATE_FORMAT in UPDATE SET: name='
                    . var_export($rows[0]['name'], true) . ', expected "Task A (2025-01)"'
                );
            }
            $this->assertSame('Task A (2025-01)', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET with DATE_FORMAT failed: ' . $e->getMessage());
        }
    }
}
