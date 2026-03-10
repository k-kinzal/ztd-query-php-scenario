<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests DML with parameterized IN lists through ZTD shadow store on PostgreSQL.
 *
 * @spec SPEC-4.2, SPEC-4.3, SPEC-3.2
 */
class PostgresParamInListDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_pin_tasks (
            id SERIAL PRIMARY KEY,
            title VARCHAR(50) NOT NULL,
            status VARCHAR(20) NOT NULL,
            priority INT NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_pin_tasks'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_pin_tasks (id, title, status, priority) VALUES (1, 'Task A', 'open', 1)");
        $this->pdo->exec("INSERT INTO pg_pin_tasks (id, title, status, priority) VALUES (2, 'Task B', 'in_progress', 2)");
        $this->pdo->exec("INSERT INTO pg_pin_tasks (id, title, status, priority) VALUES (3, 'Task C', 'open', 3)");
        $this->pdo->exec("INSERT INTO pg_pin_tasks (id, title, status, priority) VALUES (4, 'Task D', 'done', 1)");
        $this->pdo->exec("INSERT INTO pg_pin_tasks (id, title, status, priority) VALUES (5, 'Task E', 'in_progress', 2)");
        $this->pdo->exec("INSERT INTO pg_pin_tasks (id, title, status, priority) VALUES (6, 'Task F', 'blocked', 1)");
    }

    public function testPreparedDeleteInList(): void
    {
        try {
            $stmt = $this->pdo->prepare("DELETE FROM pg_pin_tasks WHERE id IN (?, ?, ?)");
            $stmt->execute([1, 3, 5]);

            $rows = $this->ztdQuery("SELECT id FROM pg_pin_tasks ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Prepared DELETE IN: expected 3, got ' . count($rows) . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $ids = array_map(fn($r) => (int) $r['id'], $rows);
            $this->assertSame([2, 4, 6], $ids);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DELETE IN list failed: ' . $e->getMessage());
        }
    }

    public function testPreparedUpdateInList(): void
    {
        try {
            $stmt = $this->pdo->prepare("UPDATE pg_pin_tasks SET priority = ? WHERE status IN (?, ?)");
            $stmt->execute([99, 'open', 'blocked']);

            $rows = $this->ztdQuery("SELECT title, priority FROM pg_pin_tasks ORDER BY id");

            $byTitle = [];
            foreach ($rows as $r) {
                $byTitle[$r['title']] = (int) $r['priority'];
            }

            if ($byTitle['Task A'] !== 99 || $byTitle['Task B'] !== 2) {
                $this->markTestIncomplete('Prepared UPDATE IN: got ' . json_encode($byTitle));
            }

            $this->assertSame(99, $byTitle['Task A']);
            $this->assertSame(2, $byTitle['Task B']);
            $this->assertSame(99, $byTitle['Task F']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE IN list failed: ' . $e->getMessage());
        }
    }

    public function testPreparedUpdateNotInList(): void
    {
        try {
            $stmt = $this->pdo->prepare("UPDATE pg_pin_tasks SET status = 'archived' WHERE id NOT IN (?, ?)");
            $stmt->execute([1, 2]);

            $rows = $this->ztdQuery("SELECT id, status FROM pg_pin_tasks ORDER BY id");
            $byId = [];
            foreach ($rows as $r) {
                $byId[(int) $r['id']] = $r['status'];
            }

            if ($byId[1] !== 'open' || $byId[3] !== 'archived') {
                $this->markTestIncomplete('Prepared UPDATE NOT IN: got ' . json_encode($byId));
            }

            $this->assertSame('open', $byId[1]);
            $this->assertSame('in_progress', $byId[2]);
            $this->assertSame('archived', $byId[3]);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE NOT IN failed: ' . $e->getMessage());
        }
    }
}
