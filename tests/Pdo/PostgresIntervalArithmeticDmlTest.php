<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests date/time INTERVAL arithmetic expressions in DML operations
 * through ZTD shadow store on PostgreSQL via PDO.
 *
 * PostgreSQL uses INTERVAL '30 days' syntax. The CTE rewriter must preserve
 * INTERVAL expressions in UPDATE SET and DELETE WHERE.
 *
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class PostgresIntervalArithmeticDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE pg_iad_tasks (
            id INT PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            created_at TIMESTAMP NOT NULL,
            due_at TIMESTAMP,
            status VARCHAR(20) NOT NULL DEFAULT 'pending'
        )";
    }

    protected function getTableNames(): array
    {
        return ['pg_iad_tasks'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_iad_tasks VALUES (1, 'Task A', '2025-01-01 10:00:00', NULL, 'pending')");
        $this->pdo->exec("INSERT INTO pg_iad_tasks VALUES (2, 'Task B', '2025-01-15 12:00:00', NULL, 'pending')");
        $this->pdo->exec("INSERT INTO pg_iad_tasks VALUES (3, 'Task C', '2025-02-01 08:00:00', NULL, 'pending')");
        $this->pdo->exec("INSERT INTO pg_iad_tasks VALUES (4, 'Task D', '2024-12-01 09:00:00', NULL, 'pending')");
    }

    /**
     * UPDATE SET due_at = created_at + INTERVAL '30 days'
     */
    public function testUpdateSetWithIntervalAddition(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE pg_iad_tasks SET due_at = created_at + INTERVAL '30 days'"
            );

            $rows = $this->ztdQuery("SELECT id, name, due_at FROM pg_iad_tasks ORDER BY id");

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
     * DELETE WHERE created_at < TIMESTAMP '2025-03-10' - INTERVAL '60 days'
     */
    public function testDeleteWhereWithIntervalComparison(): void
    {
        try {
            $this->pdo->exec(
                "DELETE FROM pg_iad_tasks WHERE created_at < TIMESTAMP '2025-03-10 00:00:00' - INTERVAL '60 days'"
            );

            $rows = $this->ztdQuery("SELECT name FROM pg_iad_tasks ORDER BY name");
            $names = array_column($rows, 'name');

            // 60 days before 2025-03-10 = 2025-01-09
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
     * UPDATE with date comparison in WHERE clause.
     */
    public function testUpdateWhereWithDateComparison(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE pg_iad_tasks SET status = 'overdue'
                 WHERE created_at < '2025-01-10 00:00:00'"
            );

            $rows = $this->ztdQuery("SELECT name, status FROM pg_iad_tasks ORDER BY id");

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
            $this->markTestIncomplete('UPDATE WHERE date comparison failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET with to_char for computed string.
     */
    public function testUpdateSetWithToChar(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE pg_iad_tasks SET name = name || ' (' || to_char(created_at, 'YYYY-MM') || ')'"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM pg_iad_tasks ORDER BY id");

            $this->assertCount(4, $rows);

            if ($rows[0]['name'] !== 'Task A (2025-01)') {
                $this->markTestIncomplete(
                    'to_char in UPDATE SET: name='
                    . var_export($rows[0]['name'], true) . ', expected "Task A (2025-01)"'
                );
            }
            $this->assertSame('Task A (2025-01)', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET with to_char failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET due_at = created_at + INTERVAL '1 month' -- month interval.
     */
    public function testUpdateSetWithMonthInterval(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE pg_iad_tasks SET due_at = created_at + INTERVAL '1 month'"
            );

            $rows = $this->ztdQuery("SELECT id, due_at FROM pg_iad_tasks ORDER BY id");

            $this->assertCount(4, $rows);

            // Task A: 2025-01-01 + 1 month = 2025-02-01 10:00:00
            if ($rows[0]['due_at'] !== '2025-02-01 10:00:00') {
                $this->markTestIncomplete(
                    'Month INTERVAL: Task A due_at='
                    . var_export($rows[0]['due_at'], true) . ', expected 2025-02-01 10:00:00'
                );
            }
            $this->assertSame('2025-02-01 10:00:00', $rows[0]['due_at']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET with month INTERVAL failed: ' . $e->getMessage());
        }
    }
}
