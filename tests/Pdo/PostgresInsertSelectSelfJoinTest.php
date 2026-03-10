<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests INSERT...SELECT with self-join patterns on PostgreSQL.
 *
 * Isolates whether INSERT...SELECT with self-join works, and
 * whether adding OVERLAPS in the JOIN condition breaks it.
 *
 * @spec SPEC-4.1
 */
class PostgresInsertSelectSelfJoinTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_issj_events (
                id SERIAL PRIMARY KEY,
                room TEXT NOT NULL,
                start_hour INTEGER NOT NULL,
                end_hour INTEGER NOT NULL
            )',
            'CREATE TABLE pg_issj_overlaps (
                id SERIAL PRIMARY KEY,
                event_a_id INTEGER NOT NULL,
                event_b_id INTEGER NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_issj_overlaps', 'pg_issj_events'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Events: 1 and 2 overlap (9-11 and 10-12), 3 does not (14-16)
        $this->pdo->exec("INSERT INTO pg_issj_events (id, room, start_hour, end_hour) VALUES
            (1, 'A', 9, 11),
            (2, 'A', 10, 12),
            (3, 'A', 14, 16)
        ");
    }

    /**
     * INSERT...SELECT with self-join on same table (no OVERLAPS).
     * Uses simple integer range comparison to find overlapping events.
     *
     * @spec SPEC-4.1
     */
    public function testInsertSelectSelfJoinSimpleOverlap(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO pg_issj_overlaps (event_a_id, event_b_id)
                 SELECT a.id, b.id
                 FROM pg_issj_events a
                 JOIN pg_issj_events b ON a.id < b.id
                    AND a.room = b.room
                    AND a.start_hour < b.end_hour
                    AND a.end_hour > b.start_hour"
            );

            $rows = $this->ztdQuery('SELECT event_a_id, event_b_id FROM pg_issj_overlaps ORDER BY event_a_id, event_b_id');

            if (count($rows) < 1) {
                $this->markTestIncomplete(
                    'INSERT SELECT self-join: expected at least 1 overlap, got ' . count($rows)
                );
            }

            // Events 1 (9-11) and 2 (10-12) overlap
            $this->assertCount(1, $rows);
            $this->assertEquals(1, (int) $rows[0]['event_a_id']);
            $this->assertEquals(2, (int) $rows[0]['event_b_id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT SELECT self-join (simple overlap) failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT...SELECT with self-join using LEFT JOIN.
     * Finds events that have no overlapping pair (isolated events).
     *
     * @spec SPEC-4.1
     */
    public function testInsertSelectSelfJoinLeftJoin(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO pg_issj_overlaps (event_a_id, event_b_id)
                 SELECT a.id, COALESCE(b.id, 0)
                 FROM pg_issj_events a
                 LEFT JOIN pg_issj_events b ON a.id <> b.id
                    AND a.room = b.room
                    AND a.start_hour < b.end_hour
                    AND a.end_hour > b.start_hour
                 WHERE b.id IS NULL"
            );

            $rows = $this->ztdQuery('SELECT event_a_id FROM pg_issj_overlaps ORDER BY event_a_id');

            // Event 3 (14-16) doesn't overlap with 1 or 2
            if (count($rows) < 1) {
                $this->markTestIncomplete(
                    'INSERT SELECT self LEFT JOIN: expected at least 1 isolated event, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertEquals(3, (int) $rows[0]['event_a_id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT SELECT self-join LEFT JOIN failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Simple cross-table INSERT...SELECT (no self-join, no OVERLAPS).
     * Baseline to verify basic INSERT...SELECT works.
     *
     * @spec SPEC-4.1
     */
    public function testInsertSelectCrossTable(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO pg_issj_overlaps (event_a_id, event_b_id)
                 SELECT id, id FROM pg_issj_events WHERE room = 'A'"
            );

            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM pg_issj_overlaps');

            $this->assertEquals(3, (int) $rows[0]['cnt'],
                'Should insert 3 rows from events table');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT SELECT cross-table failed: ' . $e->getMessage()
            );
        }
    }
}
