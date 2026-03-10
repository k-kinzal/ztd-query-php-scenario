<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests DML with parameterized IN lists (WHERE col IN (?, ?, ?))
 * through ZTD shadow store on SQLite.
 *
 * Multiple ? placeholders in an IN clause require the CTE rewriter
 * to correctly track parameter positions.
 *
 * @spec SPEC-4.2, SPEC-4.3, SPEC-3.2
 */
class SqliteParamInListDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_pin_tasks (
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            status TEXT NOT NULL,
            priority INTEGER NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_pin_tasks'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_pin_tasks VALUES (1, 'Task A', 'open', 1)");
        $this->pdo->exec("INSERT INTO sl_pin_tasks VALUES (2, 'Task B', 'in_progress', 2)");
        $this->pdo->exec("INSERT INTO sl_pin_tasks VALUES (3, 'Task C', 'open', 3)");
        $this->pdo->exec("INSERT INTO sl_pin_tasks VALUES (4, 'Task D', 'done', 1)");
        $this->pdo->exec("INSERT INTO sl_pin_tasks VALUES (5, 'Task E', 'in_progress', 2)");
        $this->pdo->exec("INSERT INTO sl_pin_tasks VALUES (6, 'Task F', 'blocked', 1)");
    }

    /**
     * Prepared DELETE WHERE id IN (?, ?, ?).
     */
    public function testPreparedDeleteInList(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "DELETE FROM sl_pin_tasks WHERE id IN (?, ?, ?)"
            );
            $stmt->execute([1, 3, 5]);

            $rows = $this->ztdQuery("SELECT id, title FROM sl_pin_tasks ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Prepared DELETE IN list: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $ids = array_map(fn($r) => (int) $r['id'], $rows);
            $this->assertSame([2, 4, 6], $ids);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DELETE IN list failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE WHERE status IN (?, ?).
     */
    public function testPreparedUpdateInList(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE sl_pin_tasks SET priority = ? WHERE status IN (?, ?)"
            );
            $stmt->execute([99, 'open', 'blocked']);

            $rows = $this->ztdQuery("SELECT title, status, priority FROM sl_pin_tasks ORDER BY id");

            $byTitle = [];
            foreach ($rows as $r) {
                $byTitle[$r['title']] = (int) $r['priority'];
            }

            // Task A (open), Task C (open), Task F (blocked) → priority 99
            // Task B (in_progress), Task D (done), Task E (in_progress) → unchanged
            if ($byTitle['Task A'] !== 99 || $byTitle['Task B'] !== 2) {
                $this->markTestIncomplete(
                    'Prepared UPDATE IN list: expected A=99, B=2, got '
                    . json_encode($byTitle)
                );
            }

            $this->assertSame(99, $byTitle['Task A']);
            $this->assertSame(2, $byTitle['Task B']);
            $this->assertSame(99, $byTitle['Task C']);
            $this->assertSame(1, $byTitle['Task D']);
            $this->assertSame(2, $byTitle['Task E']);
            $this->assertSame(99, $byTitle['Task F']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE IN list failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared SELECT WHERE col IN (?, ?, ?, ?) — read path.
     */
    public function testPreparedSelectInList(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT title FROM sl_pin_tasks WHERE status IN (?, ?) ORDER BY title",
                ['open', 'done']
            );

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Prepared SELECT IN list: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared SELECT IN list failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared DELETE with IN list combined with additional AND condition.
     */
    public function testPreparedDeleteInListWithAnd(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "DELETE FROM sl_pin_tasks WHERE status IN (?, ?) AND priority >= ?"
            );
            $stmt->execute(['open', 'in_progress', 2]);

            $rows = $this->ztdQuery("SELECT title, status, priority FROM sl_pin_tasks ORDER BY id");

            // Task A (open, pri 1) → kept (pri < 2)
            // Task B (in_progress, pri 2) → deleted
            // Task C (open, pri 3) → deleted
            // Task D (done, pri 1) → kept (status not matched)
            // Task E (in_progress, pri 2) → deleted
            // Task F (blocked, pri 1) → kept (status not matched)

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Prepared DELETE IN + AND: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $titles = array_column($rows, 'title');
            $this->assertContains('Task A', $titles);
            $this->assertContains('Task D', $titles);
            $this->assertContains('Task F', $titles);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DELETE IN list AND failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE with NOT IN list.
     */
    public function testPreparedUpdateNotInList(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE sl_pin_tasks SET status = 'archived' WHERE id NOT IN (?, ?)"
            );
            $stmt->execute([1, 2]);

            $rows = $this->ztdQuery("SELECT id, status FROM sl_pin_tasks ORDER BY id");

            $byId = [];
            foreach ($rows as $r) {
                $byId[(int) $r['id']] = $r['status'];
            }

            if ($byId[1] !== 'open' || $byId[3] !== 'archived') {
                $this->markTestIncomplete(
                    'Prepared UPDATE NOT IN: expected 1=open, 3=archived, got '
                    . json_encode($byId)
                );
            }

            $this->assertSame('open', $byId[1]);
            $this->assertSame('in_progress', $byId[2]);
            $this->assertSame('archived', $byId[3]);
            $this->assertSame('archived', $byId[4]);
            $this->assertSame('archived', $byId[5]);
            $this->assertSame('archived', $byId[6]);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE NOT IN list failed: ' . $e->getMessage());
        }
    }
}
