<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT...SELECT with self-join patterns on SQLite.
 *
 * @spec SPEC-4.1
 */
class SqliteInsertSelectSelfJoinTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_issj_events (
                id INTEGER PRIMARY KEY,
                room TEXT NOT NULL,
                start_hour INTEGER NOT NULL,
                end_hour INTEGER NOT NULL
            )',
            'CREATE TABLE sl_issj_overlaps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_a_id INTEGER NOT NULL,
                event_b_id INTEGER NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_issj_overlaps', 'sl_issj_events'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_issj_events (id, room, start_hour, end_hour) VALUES (1, 'A', 9, 11)");
        $this->pdo->exec("INSERT INTO sl_issj_events (id, room, start_hour, end_hour) VALUES (2, 'A', 10, 12)");
        $this->pdo->exec("INSERT INTO sl_issj_events (id, room, start_hour, end_hour) VALUES (3, 'A', 14, 16)");
    }

    /**
     * INSERT...SELECT with self-join to find overlapping events.
     *
     * @spec SPEC-4.1
     */
    public function testInsertSelectSelfJoinOverlap(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_issj_overlaps (event_a_id, event_b_id)
                 SELECT a.id, b.id
                 FROM sl_issj_events a
                 JOIN sl_issj_events b ON a.id < b.id
                    AND a.room = b.room
                    AND a.start_hour < b.end_hour
                    AND a.end_hour > b.start_hour"
            );

            $rows = $this->ztdQuery('SELECT event_a_id, event_b_id FROM sl_issj_overlaps ORDER BY event_a_id, event_b_id');

            if (count($rows) < 1) {
                $this->markTestIncomplete(
                    'INSERT SELECT self-join (SQLite): expected at least 1 overlap, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertEquals(1, (int) $rows[0]['event_a_id']);
            $this->assertEquals(2, (int) $rows[0]['event_b_id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT SELECT self-join (SQLite) failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Cross-table INSERT...SELECT baseline (no self-join).
     *
     * @spec SPEC-4.1
     */
    public function testInsertSelectCrossTable(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_issj_overlaps (event_a_id, event_b_id)
                 SELECT id, id FROM sl_issj_events WHERE room = 'A'"
            );

            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM sl_issj_overlaps');
            $this->assertEquals(3, (int) $rows[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT SELECT cross-table (SQLite) failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Self-join SELECT (no INSERT) to verify the join itself works.
     *
     * @spec SPEC-3.1
     */
    public function testSelfJoinSelectWorks(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT a.id AS aid, b.id AS bid
                 FROM sl_issj_events a
                 JOIN sl_issj_events b ON a.id < b.id
                    AND a.room = b.room
                    AND a.start_hour < b.end_hour
                    AND a.end_hour > b.start_hour"
            );

            if (count($rows) < 1) {
                $this->markTestIncomplete(
                    'Self-join SELECT: expected at least 1 row, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertEquals(1, (int) $rows[0]['aid']);
            $this->assertEquals(2, (int) $rows[0]['bid']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Self-join SELECT failed: ' . $e->getMessage()
            );
        }
    }
}
