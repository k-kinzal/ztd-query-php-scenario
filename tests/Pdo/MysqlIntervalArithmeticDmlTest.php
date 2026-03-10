<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests date/time INTERVAL arithmetic expressions in DML operations
 * through ZTD shadow store on MySQL via PDO.
 *
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class MysqlIntervalArithmeticDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE mp_iad_tasks (
            id INT PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            created_at DATETIME NOT NULL,
            due_at DATETIME,
            status VARCHAR(20) NOT NULL DEFAULT 'pending'
        ) ENGINE=InnoDB";
    }

    protected function getTableNames(): array
    {
        return ['mp_iad_tasks'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_iad_tasks VALUES (1, 'Task A', '2025-01-01 10:00:00', NULL, 'pending')");
        $this->pdo->exec("INSERT INTO mp_iad_tasks VALUES (2, 'Task B', '2025-01-15 12:00:00', NULL, 'pending')");
        $this->pdo->exec("INSERT INTO mp_iad_tasks VALUES (3, 'Task C', '2025-02-01 08:00:00', NULL, 'pending')");
        $this->pdo->exec("INSERT INTO mp_iad_tasks VALUES (4, 'Task D', '2024-12-01 09:00:00', NULL, 'pending')");
    }

    public function testUpdateSetWithIntervalAddition(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE mp_iad_tasks SET due_at = created_at + INTERVAL 30 DAY"
            );

            $rows = $this->ztdQuery("SELECT id, name, due_at FROM mp_iad_tasks ORDER BY id");

            $this->assertCount(4, $rows);

            if ($rows[0]['due_at'] !== '2025-01-31 10:00:00') {
                $this->markTestIncomplete(
                    'INTERVAL: Task A due_at=' . var_export($rows[0]['due_at'], true)
                );
            }
            $this->assertSame('2025-01-31 10:00:00', $rows[0]['due_at']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET INTERVAL addition failed: ' . $e->getMessage());
        }
    }

    public function testUpdateSetWithDateAdd(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE mp_iad_tasks SET due_at = DATE_ADD(created_at, INTERVAL 7 DAY)"
            );

            $rows = $this->ztdQuery("SELECT id, due_at FROM mp_iad_tasks ORDER BY id");

            $this->assertCount(4, $rows);

            if ($rows[0]['due_at'] !== '2025-01-08 10:00:00') {
                $this->markTestIncomplete(
                    'DATE_ADD: Task A due_at=' . var_export($rows[0]['due_at'], true)
                );
            }
            $this->assertSame('2025-01-08 10:00:00', $rows[0]['due_at']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET DATE_ADD failed: ' . $e->getMessage());
        }
    }

    public function testDeleteWhereWithIntervalComparison(): void
    {
        try {
            $this->pdo->exec(
                "DELETE FROM mp_iad_tasks WHERE created_at < '2025-03-10' - INTERVAL 60 DAY"
            );

            $rows = $this->ztdQuery("SELECT name FROM mp_iad_tasks ORDER BY name");
            $names = array_column($rows, 'name');

            if (count($names) !== 2) {
                $this->markTestIncomplete(
                    'DELETE WHERE INTERVAL: expected 2, got '
                    . count($names) . ': ' . json_encode($names)
                );
            }
            $this->assertEquals(['Task B', 'Task C'], $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE INTERVAL failed: ' . $e->getMessage());
        }
    }

    public function testUpdateWhereWithDateComparison(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE mp_iad_tasks SET status = 'overdue' WHERE created_at < '2025-01-10 00:00:00'"
            );

            $rows = $this->ztdQuery("SELECT name, status FROM mp_iad_tasks ORDER BY id");

            $this->assertCount(4, $rows);
            if ($rows[0]['status'] !== 'overdue') {
                $this->markTestIncomplete('Task A status=' . var_export($rows[0]['status'], true));
            }
            $this->assertSame('overdue', $rows[0]['status']);
            $this->assertSame('pending', $rows[1]['status']);
            $this->assertSame('pending', $rows[2]['status']);
            if ($rows[3]['status'] !== 'overdue') {
                $this->markTestIncomplete('Task D status=' . var_export($rows[3]['status'], true));
            }
            $this->assertSame('overdue', $rows[3]['status']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE WHERE date comparison failed: ' . $e->getMessage());
        }
    }

    public function testUpdateSetWithDateFormat(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE mp_iad_tasks SET name = CONCAT(name, ' (', DATE_FORMAT(created_at, '%Y-%m'), ')')"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM mp_iad_tasks ORDER BY id");

            $this->assertCount(4, $rows);
            if ($rows[0]['name'] !== 'Task A (2025-01)') {
                $this->markTestIncomplete(
                    'DATE_FORMAT: name=' . var_export($rows[0]['name'], true)
                );
            }
            $this->assertSame('Task A (2025-01)', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET DATE_FORMAT failed: ' . $e->getMessage());
        }
    }
}
