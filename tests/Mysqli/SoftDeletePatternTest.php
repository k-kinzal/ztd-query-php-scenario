<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Soft delete: UPDATE SET deleted_at = NOW() instead of DELETE.
 * Most ORMs (Laravel, Doctrine) default to this pattern.
 * @spec SPEC-4.2, SPEC-3.1
 */
class SoftDeletePatternTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_sd_tasks (id INT PRIMARY KEY, title VARCHAR(255), status VARCHAR(20), deleted_at DATETIME NULL DEFAULT NULL)';
    }

    protected function getTableNames(): array
    {
        return ['mi_sd_tasks'];
    }

    private function seed(): void
    {
        $this->mysqli->query("INSERT INTO mi_sd_tasks (id, title, status, deleted_at) VALUES (1, 'Task A', 'open', NULL)");
        $this->mysqli->query("INSERT INTO mi_sd_tasks (id, title, status, deleted_at) VALUES (2, 'Task B', 'open', NULL)");
        $this->mysqli->query("INSERT INTO mi_sd_tasks (id, title, status, deleted_at) VALUES (3, 'Task C', 'done', NULL)");
    }

    public function testSelectOnlyActiveRecords(): void
    {
        $this->seed();

        $rows = $this->ztdQuery('SELECT id, title FROM mi_sd_tasks WHERE deleted_at IS NULL ORDER BY id');
        $this->assertCount(3, $rows);
    }

    public function testSoftDeleteSetsTimestamp(): void
    {
        $this->seed();

        $this->mysqli->query("UPDATE mi_sd_tasks SET deleted_at = '2024-06-01 12:00:00' WHERE id = 2");

        $rows = $this->ztdQuery('SELECT id FROM mi_sd_tasks WHERE deleted_at IS NULL ORDER BY id');
        $this->assertCount(2, $rows);
        $this->assertSame(1, (int) $rows[0]['id']);
        $this->assertSame(3, (int) $rows[1]['id']);
    }

    public function testSoftDeletedRowStillExistsInFullQuery(): void
    {
        $this->seed();

        $this->mysqli->query("UPDATE mi_sd_tasks SET deleted_at = '2024-06-01 12:00:00' WHERE id = 2");

        // Without filter, all rows still visible
        $rows = $this->ztdQuery('SELECT COUNT(*) AS c FROM mi_sd_tasks');
        $this->assertSame(3, (int) $rows[0]['c']);
    }

    public function testCountActiveVsDeleted(): void
    {
        $this->seed();

        $this->mysqli->query("UPDATE mi_sd_tasks SET deleted_at = '2024-06-01 12:00:00' WHERE id = 1");
        $this->mysqli->query("UPDATE mi_sd_tasks SET deleted_at = '2024-06-02 12:00:00' WHERE id = 3");

        $rows = $this->ztdQuery('SELECT COUNT(*) AS c FROM mi_sd_tasks WHERE deleted_at IS NULL');
        $this->assertSame(1, (int) $rows[0]['c']);

        $rows = $this->ztdQuery('SELECT COUNT(*) AS c FROM mi_sd_tasks WHERE deleted_at IS NOT NULL');
        $this->assertSame(2, (int) $rows[0]['c']);
    }

    public function testRestoreSoftDeletedRow(): void
    {
        $this->seed();

        $this->mysqli->query("UPDATE mi_sd_tasks SET deleted_at = '2024-06-01 12:00:00' WHERE id = 2");
        $rows = $this->ztdQuery('SELECT id FROM mi_sd_tasks WHERE deleted_at IS NULL');
        $this->assertCount(2, $rows);

        // Restore
        $this->mysqli->query("UPDATE mi_sd_tasks SET deleted_at = NULL WHERE id = 2");
        $rows = $this->ztdQuery('SELECT id FROM mi_sd_tasks WHERE deleted_at IS NULL');
        $this->assertCount(3, $rows);
    }

    public function testSoftDeleteWithStatusFilter(): void
    {
        $this->seed();

        $this->mysqli->query("UPDATE mi_sd_tasks SET deleted_at = '2024-06-01 12:00:00' WHERE id = 3");

        // Active + open tasks only
        $rows = $this->ztdQuery("SELECT id FROM mi_sd_tasks WHERE deleted_at IS NULL AND status = 'open' ORDER BY id");
        $this->assertCount(2, $rows);
        $this->assertSame(1, (int) $rows[0]['id']);
        $this->assertSame(2, (int) $rows[1]['id']);
    }

    public function testPreparedSoftDelete(): void
    {
        $this->seed();

        $stmt = $this->mysqli->prepare("UPDATE mi_sd_tasks SET deleted_at = ? WHERE id = ?");
        $ts = '2024-06-01 12:00:00';
        $id = 1;
        $stmt->bind_param('si', $ts, $id);
        $stmt->execute();
        $this->assertSame(1, $stmt->ztdAffectedRows());

        $rows = $this->ztdQuery('SELECT id FROM mi_sd_tasks WHERE deleted_at IS NULL ORDER BY id');
        $this->assertCount(2, $rows);
        $this->assertSame(2, (int) $rows[0]['id']);
    }

    public function testHardDeleteAfterSoftDelete(): void
    {
        $this->seed();

        // Soft-delete first
        $this->mysqli->query("UPDATE mi_sd_tasks SET deleted_at = '2024-06-01 12:00:00' WHERE id = 2");

        // Then hard-delete the soft-deleted records
        $this->mysqli->query("DELETE FROM mi_sd_tasks WHERE deleted_at IS NOT NULL");
        $affected = $this->mysqli->lastAffectedRows();
        $this->assertSame(1, $affected);

        $rows = $this->ztdQuery('SELECT COUNT(*) AS c FROM mi_sd_tasks');
        $this->assertSame(2, (int) $rows[0]['c']);
    }
}
