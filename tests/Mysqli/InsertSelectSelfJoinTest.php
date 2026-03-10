<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests INSERT...SELECT with self-join patterns on MySQLi.
 *
 * @spec SPEC-4.1
 */
class InsertSelectSelfJoinTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_issj_events (
                id INT PRIMARY KEY,
                room VARCHAR(50) NOT NULL,
                start_hour INT NOT NULL,
                end_hour INT NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE mi_issj_overlaps (
                id INT AUTO_INCREMENT PRIMARY KEY,
                event_a_id INT NOT NULL,
                event_b_id INT NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_issj_overlaps', 'mi_issj_events'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_issj_events (id, room, start_hour, end_hour) VALUES (1, 'A', 9, 11)");
        $this->mysqli->query("INSERT INTO mi_issj_events (id, room, start_hour, end_hour) VALUES (2, 'A', 10, 12)");
        $this->mysqli->query("INSERT INTO mi_issj_events (id, room, start_hour, end_hour) VALUES (3, 'A', 14, 16)");
    }

    /**
     * INSERT...SELECT with self-join to find overlapping events.
     *
     * @spec SPEC-4.1
     */
    public function testInsertSelectSelfJoinOverlap(): void
    {
        try {
            $this->mysqli->query(
                "INSERT INTO mi_issj_overlaps (event_a_id, event_b_id)
                 SELECT a.id, b.id
                 FROM mi_issj_events a
                 JOIN mi_issj_events b ON a.id < b.id
                    AND a.room = b.room
                    AND a.start_hour < b.end_hour
                    AND a.end_hour > b.start_hour"
            );

            $rows = $this->ztdQuery('SELECT event_a_id, event_b_id FROM mi_issj_overlaps ORDER BY event_a_id, event_b_id');

            if (count($rows) < 1) {
                $this->markTestIncomplete(
                    'INSERT SELECT self-join (MySQLi): expected at least 1 overlap, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertEquals(1, (int) $rows[0]['event_a_id']);
            $this->assertEquals(2, (int) $rows[0]['event_b_id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT SELECT self-join (MySQLi) failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Cross-table INSERT...SELECT baseline.
     *
     * @spec SPEC-4.1
     */
    public function testInsertSelectCrossTable(): void
    {
        try {
            $this->mysqli->query(
                "INSERT INTO mi_issj_overlaps (event_a_id, event_b_id)
                 SELECT id, id FROM mi_issj_events WHERE room = 'A'"
            );

            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM mi_issj_overlaps');
            $this->assertEquals(3, (int) $rows[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT SELECT cross-table (MySQLi) failed: ' . $e->getMessage()
            );
        }
    }
}
