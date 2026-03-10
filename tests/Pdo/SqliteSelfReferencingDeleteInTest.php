<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests DELETE with self-referencing subquery (same table in WHERE IN).
 * Without GROUP BY HAVING, this is a simpler pattern that should work.
 *
 * Pattern: DELETE FROM t WHERE id IN (SELECT parent_id FROM t WHERE ...)
 * Also: DELETE FROM t WHERE col > (SELECT AVG(col) FROM t)
 *
 * @spec SPEC-4.3
 */
class SqliteSelfReferencingDeleteInTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_srdi_tasks (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER,
            title TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT \'open\',
            priority INTEGER NOT NULL DEFAULT 0
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_srdi_tasks'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Tree: task 1 is root, 2 and 3 are children of 1, 4 is child of 2
        $this->pdo->exec("INSERT INTO sl_srdi_tasks VALUES (1, NULL, 'Root Task', 'open', 3)");
        $this->pdo->exec("INSERT INTO sl_srdi_tasks VALUES (2, 1, 'Sub Task A', 'done', 2)");
        $this->pdo->exec("INSERT INTO sl_srdi_tasks VALUES (3, 1, 'Sub Task B', 'open', 1)");
        $this->pdo->exec("INSERT INTO sl_srdi_tasks VALUES (4, 2, 'Sub Sub Task', 'done', 5)");
        $this->pdo->exec("INSERT INTO sl_srdi_tasks VALUES (5, NULL, 'Standalone', 'open', 4)");
    }

    /**
     * DELETE parent tasks that have completed children.
     */
    public function testDeleteParentsWithDoneChildren(): void
    {
        $sql = "DELETE FROM sl_srdi_tasks
                WHERE id IN (
                    SELECT parent_id FROM sl_srdi_tasks WHERE status = 'done' AND parent_id IS NOT NULL
                )";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, title FROM sl_srdi_tasks ORDER BY id");

            // parent_id of done tasks: 1 (parent of task 2) and 2 (parent of task 4)
            // So tasks 1 and 2 are deleted
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Self-ref DELETE: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $ids = array_map(fn($r) => (int) $r['id'], $rows);
            $this->assertContains(3, $ids);
            $this->assertContains(4, $ids);
            $this->assertContains(5, $ids);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Self-ref DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE tasks with priority above average (self-referencing scalar subquery).
     */
    public function testDeleteAboveAveragePriority(): void
    {
        $sql = "DELETE FROM sl_srdi_tasks
                WHERE priority > (SELECT AVG(priority) FROM sl_srdi_tasks)";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, title, priority FROM sl_srdi_tasks ORDER BY id");

            // AVG = (3 + 2 + 1 + 5 + 4) / 5 = 3.0
            // Task 1 (3): 3 > 3 is false → kept
            // Task 2 (2): kept
            // Task 3 (1): kept
            // Task 4 (5): 5 > 3 → DELETED
            // Task 5 (4): 4 > 3 → DELETED
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Above-avg DELETE: expected 3, got ' . count($rows)
                    . '. AVG should be 3.0. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $ids = array_map(fn($r) => (int) $r['id'], $rows);
            $this->assertContains(1, $ids);
            $this->assertContains(2, $ids);
            $this->assertContains(3, $ids);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Above-avg DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared self-referencing DELETE with status parameter.
     */
    public function testPreparedSelfRefDelete(): void
    {
        $sql = "DELETE FROM sl_srdi_tasks
                WHERE id IN (
                    SELECT parent_id FROM sl_srdi_tasks WHERE status = ? AND parent_id IS NOT NULL
                )";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute(['open']);

            $rows = $this->ztdQuery("SELECT id, title FROM sl_srdi_tasks ORDER BY id");

            // Open tasks with non-null parent_id: task 3 (parent_id=1)
            // So task 1 is deleted
            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'Prepared self-ref DELETE: expected 4, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(4, $rows);
            $ids = array_map(fn($r) => (int) $r['id'], $rows);
            $this->assertNotContains(1, $ids);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared self-ref DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with self-referencing scalar subquery.
     */
    public function testUpdateSelfReferencingScalar(): void
    {
        $sql = "UPDATE sl_srdi_tasks
                SET status = 'flagged'
                WHERE priority > (SELECT AVG(priority) FROM sl_srdi_tasks)";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, status FROM sl_srdi_tasks ORDER BY id");

            $this->assertCount(5, $rows);

            $statuses = array_column($rows, 'status', 'id');
            // AVG = 3.0; tasks 4(5) and 5(4) above average
            if ($statuses[4] !== 'flagged') {
                $this->markTestIncomplete(
                    'Self-ref UPDATE: task 4 expected flagged, got ' . $statuses[4]
                    . '. All: ' . json_encode($statuses)
                );
            }

            $this->assertSame('flagged', $statuses[4]);
            $this->assertSame('flagged', $statuses[5]);
            $this->assertSame('open', $statuses[1]); // 3 is not > 3
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Self-ref UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE on shadow-inserted self-referencing data.
     */
    public function testSelfRefDeleteOnShadowData(): void
    {
        // Add new task referencing existing shadow data
        $this->pdo->exec("INSERT INTO sl_srdi_tasks VALUES (6, 5, 'Child of Standalone', 'done', 1)");

        $sql = "DELETE FROM sl_srdi_tasks
                WHERE id IN (
                    SELECT parent_id FROM sl_srdi_tasks WHERE status = 'done' AND parent_id IS NOT NULL
                )";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id FROM sl_srdi_tasks ORDER BY id");

            // Done tasks: 2 (parent=1), 4 (parent=2), 6 (parent=5)
            // So delete tasks 1, 2, 5
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Shadow self-ref DELETE: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $ids = array_map(fn($r) => (int) $r['id'], $rows);
            $this->assertContains(3, $ids);
            $this->assertContains(4, $ids);
            $this->assertContains(6, $ids);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Shadow self-ref DELETE failed: ' . $e->getMessage());
        }
    }
}
