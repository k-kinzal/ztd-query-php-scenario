<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Soft delete: UPDATE SET deleted_at = timestamp instead of DELETE.
 * @spec SPEC-4.2, SPEC-3.1
 */
class SqliteSoftDeletePatternTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sd_tasks (id INTEGER PRIMARY KEY, title TEXT, status TEXT, deleted_at TEXT NULL DEFAULT NULL)';
    }

    protected function getTableNames(): array
    {
        return ['sd_tasks'];
    }

    private function seed(): void
    {
        $this->pdo->exec("INSERT INTO sd_tasks (id, title, status, deleted_at) VALUES (1, 'Task A', 'open', NULL)");
        $this->pdo->exec("INSERT INTO sd_tasks (id, title, status, deleted_at) VALUES (2, 'Task B', 'open', NULL)");
        $this->pdo->exec("INSERT INTO sd_tasks (id, title, status, deleted_at) VALUES (3, 'Task C', 'done', NULL)");
    }

    public function testSelectActiveRecords(): void
    {
        $this->seed();

        $rows = $this->ztdQuery('SELECT id FROM sd_tasks WHERE deleted_at IS NULL ORDER BY id');
        $this->assertCount(3, $rows);
    }

    public function testSoftDeleteSetsTimestamp(): void
    {
        $this->seed();

        $this->pdo->exec("UPDATE sd_tasks SET deleted_at = '2024-06-01 12:00:00' WHERE id = 2");

        $rows = $this->ztdQuery('SELECT id FROM sd_tasks WHERE deleted_at IS NULL ORDER BY id');
        $this->assertCount(2, $rows);
        $this->assertSame(1, (int) $rows[0]['id']);
        $this->assertSame(3, (int) $rows[1]['id']);
    }

    public function testSoftDeletedRowStillInFullCount(): void
    {
        $this->seed();

        $this->pdo->exec("UPDATE sd_tasks SET deleted_at = '2024-06-01 12:00:00' WHERE id = 2");

        $rows = $this->ztdQuery('SELECT COUNT(*) AS c FROM sd_tasks');
        $this->assertSame(3, (int) $rows[0]['c']);
    }

    public function testRestoreSoftDeletedRow(): void
    {
        $this->seed();

        $this->pdo->exec("UPDATE sd_tasks SET deleted_at = '2024-06-01 12:00:00' WHERE id = 2");
        $this->pdo->exec("UPDATE sd_tasks SET deleted_at = NULL WHERE id = 2");

        $rows = $this->ztdQuery('SELECT id FROM sd_tasks WHERE deleted_at IS NULL');
        $this->assertCount(3, $rows);
    }

    public function testPreparedSoftDelete(): void
    {
        $this->seed();

        $stmt = $this->pdo->prepare("UPDATE sd_tasks SET deleted_at = ? WHERE id = ?");
        $stmt->execute(['2024-06-01 12:00:00', 1]);
        $this->assertSame(1, $stmt->rowCount());

        $rows = $this->ztdQuery('SELECT id FROM sd_tasks WHERE deleted_at IS NULL ORDER BY id');
        $this->assertCount(2, $rows);
    }

    public function testCountActiveVsDeleted(): void
    {
        $this->seed();

        $this->pdo->exec("UPDATE sd_tasks SET deleted_at = '2024-06-01 12:00:00' WHERE id = 1");
        $this->pdo->exec("UPDATE sd_tasks SET deleted_at = '2024-06-02 12:00:00' WHERE id = 3");

        $rows = $this->ztdQuery('SELECT COUNT(*) AS c FROM sd_tasks WHERE deleted_at IS NULL');
        $this->assertSame(1, (int) $rows[0]['c']);

        $rows = $this->ztdQuery('SELECT COUNT(*) AS c FROM sd_tasks WHERE deleted_at IS NOT NULL');
        $this->assertSame(2, (int) $rows[0]['c']);
    }

    public function testHardDeleteSoftDeletedRecords(): void
    {
        $this->seed();

        $this->pdo->exec("UPDATE sd_tasks SET deleted_at = '2024-06-01 12:00:00' WHERE id = 2");

        $affected = $this->pdo->exec("DELETE FROM sd_tasks WHERE deleted_at IS NOT NULL");
        $this->assertSame(1, $affected);

        $rows = $this->ztdQuery('SELECT COUNT(*) AS c FROM sd_tasks');
        $this->assertSame(2, (int) $rows[0]['c']);
    }
}
