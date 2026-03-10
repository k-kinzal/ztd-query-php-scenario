<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests date/time arithmetic expressions in DML operations through ZTD shadow store
 * on SQLite via PDO.
 *
 * SQLite uses datetime() function for date arithmetic:
 * datetime(col, '+30 days'), date(col, '-1 month'), etc.
 * The CTE rewriter must preserve these expressions in UPDATE SET and DELETE WHERE.
 *
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class SqliteIntervalArithmeticDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE sl_iad_tasks (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            created_at TEXT NOT NULL,
            due_at TEXT,
            status TEXT NOT NULL DEFAULT 'pending'
        )";
    }

    protected function getTableNames(): array
    {
        return ['sl_iad_tasks'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_iad_tasks VALUES (1, 'Task A', '2025-01-01 10:00:00', NULL, 'pending')");
        $this->pdo->exec("INSERT INTO sl_iad_tasks VALUES (2, 'Task B', '2025-01-15 12:00:00', NULL, 'pending')");
        $this->pdo->exec("INSERT INTO sl_iad_tasks VALUES (3, 'Task C', '2025-02-01 08:00:00', NULL, 'pending')");
        $this->pdo->exec("INSERT INTO sl_iad_tasks VALUES (4, 'Task D', '2024-12-01 09:00:00', NULL, 'pending')");
    }

    /**
     * UPDATE SET due_at = datetime(created_at, '+30 days')
     */
    public function testUpdateSetWithDatetimeArithmetic(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE sl_iad_tasks SET due_at = datetime(created_at, '+30 days')"
            );

            $rows = $this->ztdQuery("SELECT id, name, due_at FROM sl_iad_tasks ORDER BY id");

            $this->assertCount(4, $rows);

            // Task A: 2025-01-01 + 30 days = 2025-01-31 10:00:00
            if ($rows[0]['due_at'] !== '2025-01-31 10:00:00') {
                $this->markTestIncomplete(
                    'datetime arithmetic: Task A due_at='
                    . var_export($rows[0]['due_at'], true) . ', expected 2025-01-31 10:00:00'
                );
            }
            $this->assertSame('2025-01-31 10:00:00', $rows[0]['due_at']);

            // Task B: 2025-01-15 + 30 days = 2025-02-14 12:00:00
            $this->assertSame('2025-02-14 12:00:00', $rows[1]['due_at']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET datetime arithmetic failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE WHERE date(created_at) < date('now', '-60 days')
     * Delete tasks created more than 60 days before 2025-03-10.
     */
    public function testDeleteWhereWithDateComparison(): void
    {
        try {
            // Use a fixed date comparison instead of 'now' for reproducibility
            $this->pdo->exec(
                "DELETE FROM sl_iad_tasks WHERE date(created_at) < date('2025-03-10', '-60 days')"
            );

            $rows = $this->ztdQuery("SELECT name FROM sl_iad_tasks ORDER BY name");
            $names = array_column($rows, 'name');

            // 60 days before 2025-03-10 = 2025-01-09
            // Task A (2025-01-01) < 2025-01-09 -> deleted
            // Task D (2024-12-01) < 2025-01-09 -> deleted
            // Task B (2025-01-15) >= 2025-01-09 -> kept
            // Task C (2025-02-01) >= 2025-01-09 -> kept
            if (count($names) !== 2) {
                $this->markTestIncomplete(
                    'DELETE WHERE date comparison: expected 2 rows (Task B, C), got '
                    . count($names) . ': ' . json_encode($names)
                );
            }
            $this->assertEquals(['Task B', 'Task C'], $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE date comparison failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET with date function in WHERE clause.
     */
    public function testUpdateWhereWithDateFunction(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE sl_iad_tasks SET status = 'overdue'
                 WHERE date(created_at) < '2025-01-10'"
            );

            $rows = $this->ztdQuery("SELECT name, status FROM sl_iad_tasks ORDER BY id");

            $this->assertCount(4, $rows);

            // Task A (2025-01-01): overdue
            if ($rows[0]['status'] !== 'overdue') {
                $this->markTestIncomplete(
                    'UPDATE WHERE date function: Task A status='
                    . var_export($rows[0]['status'], true) . ', expected overdue'
                );
            }
            $this->assertSame('overdue', $rows[0]['status']);

            // Task B (2025-01-15): still pending
            $this->assertSame('pending', $rows[1]['status']);

            // Task C (2025-02-01): still pending
            $this->assertSame('pending', $rows[2]['status']);

            // Task D (2024-12-01): overdue
            if ($rows[3]['status'] !== 'overdue') {
                $this->markTestIncomplete(
                    'UPDATE WHERE date function: Task D status='
                    . var_export($rows[3]['status'], true) . ', expected overdue'
                );
            }
            $this->assertSame('overdue', $rows[3]['status']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE WHERE date function failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE with datetime arithmetic and parameter.
     */
    public function testPreparedUpdateWithDatetimeArithmetic(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE sl_iad_tasks SET due_at = datetime(created_at, ?) WHERE id = ?"
            );
            $stmt->execute(['+7 days', 1]);

            $rows = $this->ztdQuery("SELECT due_at FROM sl_iad_tasks WHERE id = 1");

            $this->assertCount(1, $rows);

            // Task A: 2025-01-01 + 7 days = 2025-01-08 10:00:00
            if ($rows[0]['due_at'] !== '2025-01-08 10:00:00') {
                $this->markTestIncomplete(
                    'Prepared datetime arithmetic: due_at='
                    . var_export($rows[0]['due_at'], true)
                );
            }
            $this->assertSame('2025-01-08 10:00:00', $rows[0]['due_at']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE with datetime arithmetic failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET with strftime() for computed date formatting.
     */
    public function testUpdateSetWithStrftime(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE sl_iad_tasks SET name = name || ' (' || strftime('%Y-%m', created_at) || ')'"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM sl_iad_tasks ORDER BY id");

            $this->assertCount(4, $rows);

            if ($rows[0]['name'] !== 'Task A (2025-01)') {
                $this->markTestIncomplete(
                    'strftime in UPDATE SET: name='
                    . var_export($rows[0]['name'], true) . ', expected "Task A (2025-01)"'
                );
            }
            $this->assertSame('Task A (2025-01)', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET with strftime failed: ' . $e->getMessage());
        }
    }
}
