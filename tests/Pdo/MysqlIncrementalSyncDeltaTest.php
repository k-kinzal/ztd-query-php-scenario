<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Incremental sync delta scenario: change detection and incremental data
 * synchronization for ETL, data replication, and API sync patterns (MySQL PDO).
 * @spec SPEC-10.2.123
 */
class MysqlIncrementalSyncDeltaTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_sd_source_records (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255),
                email VARCHAR(255),
                updated_at VARCHAR(255),
                is_deleted INT
            )',
            'CREATE TABLE mp_sd_sync_log (
                id INT AUTO_INCREMENT PRIMARY KEY,
                record_id INT,
                sync_action VARCHAR(255),
                synced_at VARCHAR(255)
            )',
            'CREATE TABLE mp_sd_sync_watermark (
                id INT AUTO_INCREMENT PRIMARY KEY,
                last_sync_at VARCHAR(255)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_sd_sync_log', 'mp_sd_sync_watermark', 'mp_sd_source_records'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Source records (6)
        $this->pdo->exec("INSERT INTO mp_sd_source_records VALUES (1, 'Alice', 'alice@example.com', '2026-03-09 08:00:00', 0)");
        $this->pdo->exec("INSERT INTO mp_sd_source_records VALUES (2, 'Bob', 'bob@example.com', '2026-03-09 08:00:00', 0)");
        $this->pdo->exec("INSERT INTO mp_sd_source_records VALUES (3, 'Charlie', 'charlie@example.com', '2026-03-09 09:00:00', 0)");
        $this->pdo->exec("INSERT INTO mp_sd_source_records VALUES (4, 'Diana', 'diana@example.com', '2026-03-09 09:30:00', 0)");
        $this->pdo->exec("INSERT INTO mp_sd_source_records VALUES (5, 'Eve', 'eve@old.com', '2026-03-09 09:15:00', 0)");
        $this->pdo->exec("INSERT INTO mp_sd_source_records VALUES (6, 'Frank', 'frank@example.com', '2026-03-09 09:45:00', 1)");

        // Sync log from previous sync (3)
        $this->pdo->exec("INSERT INTO mp_sd_sync_log VALUES (1, 1, 'insert', '2026-03-09 08:30:00')");
        $this->pdo->exec("INSERT INTO mp_sd_sync_log VALUES (2, 2, 'insert', '2026-03-09 08:30:00')");
        $this->pdo->exec("INSERT INTO mp_sd_sync_log VALUES (3, 5, 'insert', '2026-03-09 08:30:00')");

        // Sync watermark (1)
        $this->pdo->exec("INSERT INTO mp_sd_sync_watermark VALUES (1, '2026-03-09 08:30:00')");
    }

    /**
     * Detect new records that have never been synced using LEFT JOIN.
     */
    public function testDetectNewRecords(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.id, s.name
             FROM mp_sd_source_records s
             LEFT JOIN mp_sd_sync_log l ON l.record_id = s.id
             WHERE l.id IS NULL
             ORDER BY s.id"
        );

        // Charlie (3), Diana (4), Frank (6) have no sync_log entries
        $this->assertCount(3, $rows);
        $this->assertSame('3', (string) $rows[0]['id']);
        $this->assertSame('Charlie', $rows[0]['name']);
        $this->assertSame('4', (string) $rows[1]['id']);
        $this->assertSame('Diana', $rows[1]['name']);
        $this->assertSame('6', (string) $rows[2]['id']);
        $this->assertSame('Frank', $rows[2]['name']);
    }

    /**
     * Detect updated records: previously synced and updated_at after watermark.
     */
    public function testDetectUpdatedRecords(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.id, s.name, s.email
             FROM mp_sd_source_records s
             JOIN mp_sd_sync_log l ON l.record_id = s.id
             WHERE s.updated_at > '2026-03-09 08:30:00'
             ORDER BY s.id"
        );

        // Eve (5) was previously synced and updated_at 09:15 > watermark 08:30
        $this->assertCount(1, $rows);
        $this->assertSame('5', (string) $rows[0]['id']);
        $this->assertSame('Eve', $rows[0]['name']);
        $this->assertSame('eve@old.com', $rows[0]['email']);
    }

    /**
     * Detect soft-deleted records updated after watermark.
     */
    public function testDetectSoftDeleted(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, name
             FROM mp_sd_source_records
             WHERE is_deleted = 1
               AND updated_at > '2026-03-09 08:30:00'
             ORDER BY id"
        );

        // Frank (6) is soft-deleted with updated_at 09:45 > watermark 08:30
        $this->assertCount(1, $rows);
        $this->assertSame('6', (string) $rows[0]['id']);
        $this->assertSame('Frank', $rows[0]['name']);
    }

    /**
     * Delta since watermark: all records with updated_at after the stored watermark (subquery).
     */
    public function testDeltaSinceWatermark(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.id, s.name
             FROM mp_sd_source_records s
             WHERE s.updated_at > (SELECT w.last_sync_at FROM mp_sd_sync_watermark w WHERE w.id = 1)
             ORDER BY s.id"
        );

        // Charlie (09:00), Diana (09:30), Eve (09:15), Frank (09:45) all > 08:30
        $this->assertCount(4, $rows);
        $this->assertSame('3', (string) $rows[0]['id']);
        $this->assertSame('Charlie', $rows[0]['name']);
        $this->assertSame('4', (string) $rows[1]['id']);
        $this->assertSame('Diana', $rows[1]['name']);
        $this->assertSame('5', (string) $rows[2]['id']);
        $this->assertSame('Eve', $rows[2]['name']);
        $this->assertSame('6', (string) $rows[3]['id']);
        $this->assertSame('Frank', $rows[3]['name']);
    }

    /**
     * Record sync actions and count by action type using GROUP BY.
     */
    public function testRecordSyncActions(): void
    {
        // Insert new sync_log entries for the delta records
        $this->pdo->exec("INSERT INTO mp_sd_sync_log VALUES (4, 3, 'insert', '2026-03-09 10:00:00')");
        $this->pdo->exec("INSERT INTO mp_sd_sync_log VALUES (5, 4, 'insert', '2026-03-09 10:00:00')");
        $this->pdo->exec("INSERT INTO mp_sd_sync_log VALUES (6, 5, 'update', '2026-03-09 10:00:00')");
        $this->pdo->exec("INSERT INTO mp_sd_sync_log VALUES (7, 6, 'delete', '2026-03-09 10:00:00')");

        $rows = $this->ztdQuery(
            "SELECT sync_action, COUNT(*) AS cnt
             FROM mp_sd_sync_log
             GROUP BY sync_action
             ORDER BY sync_action"
        );

        $this->assertCount(3, $rows);
        // delete: 1, insert: 4 (original 3 + new 2), update: 1
        $this->assertSame('delete', $rows[0]['sync_action']);
        $this->assertSame('1', (string) $rows[0]['cnt']);
        $this->assertSame('insert', $rows[1]['sync_action']);
        $this->assertSame('5', (string) $rows[1]['cnt']);
        $this->assertSame('update', $rows[2]['sync_action']);
        $this->assertSame('1', (string) $rows[2]['cnt']);
    }

    /**
     * Update watermark to new timestamp and verify it reads back correctly.
     */
    public function testUpdateWatermark(): void
    {
        $this->pdo->exec("UPDATE mp_sd_sync_watermark SET last_sync_at = '2026-03-09 10:00:00' WHERE id = 1");

        $rows = $this->ztdQuery('SELECT last_sync_at FROM mp_sd_sync_watermark WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertSame('2026-03-09 10:00:00', $rows[0]['last_sync_at']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO mp_sd_source_records VALUES (7, 'Grace', 'grace@example.com', '2026-03-09 10:00:00', 0)");
        $this->pdo->exec("UPDATE mp_sd_sync_watermark SET last_sync_at = '2026-03-09 10:00:00' WHERE id = 1");

        // ZTD sees changes
        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM mp_sd_source_records');
        $this->assertEquals(7, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery('SELECT last_sync_at FROM mp_sd_sync_watermark WHERE id = 1');
        $this->assertSame('2026-03-09 10:00:00', $rows[0]['last_sync_at']);

        // Physical tables untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM mp_sd_source_records')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
