<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests appointment scheduling with BETWEEN for time ranges, EXISTS for conflict
 * detection, COALESCE for defaults, UPDATE with range conditions, and NOT EXISTS
 * anti-join patterns (MySQL PDO).
 * SQL patterns exercised: BETWEEN, EXISTS/NOT EXISTS subqueries, COALESCE,
 * UPDATE WHERE BETWEEN, COUNT with CASE, prepared BETWEEN.
 * @spec SPEC-10.2.178
 */
class MysqlAppointmentSchedulingTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_appt_room (
                id INT PRIMARY KEY,
                name VARCHAR(50),
                capacity INT,
                building VARCHAR(50)
            )',
            'CREATE TABLE mp_appt_booking (
                id INT PRIMARY KEY,
                room_id INT,
                title VARCHAR(100),
                organizer VARCHAR(50),
                start_time VARCHAR(20),
                end_time VARCHAR(20),
                status VARCHAR(20),
                notes VARCHAR(200)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_appt_booking', 'mp_appt_room'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_appt_room VALUES (1, 'Alpha', 10, 'HQ')");
        $this->pdo->exec("INSERT INTO mp_appt_room VALUES (2, 'Beta', 6, 'HQ')");
        $this->pdo->exec("INSERT INTO mp_appt_room VALUES (3, 'Gamma', 20, 'Annex')");

        $this->pdo->exec("INSERT INTO mp_appt_booking VALUES (1, 1, 'Sprint Planning', 'alice', '2025-03-10 09:00', '2025-03-10 10:00', 'confirmed', NULL)");
        $this->pdo->exec("INSERT INTO mp_appt_booking VALUES (2, 1, 'Design Review', 'bob', '2025-03-10 10:30', '2025-03-10 11:30', 'confirmed', NULL)");
        $this->pdo->exec("INSERT INTO mp_appt_booking VALUES (3, 2, 'Standup', 'carol', '2025-03-10 09:00', '2025-03-10 09:15', 'confirmed', NULL)");
        $this->pdo->exec("INSERT INTO mp_appt_booking VALUES (4, 1, 'Lunch Talk', 'dave', '2025-03-10 12:00', '2025-03-10 13:00', 'tentative', NULL)");
        $this->pdo->exec("INSERT INTO mp_appt_booking VALUES (5, 3, 'All Hands', 'alice', '2025-03-10 14:00', '2025-03-10 15:00', 'confirmed', NULL)");
        $this->pdo->exec("INSERT INTO mp_appt_booking VALUES (6, 2, 'Retrospective', 'bob', '2025-03-10 15:00', '2025-03-10 16:00', 'cancelled', NULL)");
    }

    public function testBetweenTimeRange(): void
    {
        $rows = $this->ztdQuery(
            "SELECT b.title, b.start_time, r.name AS room
             FROM mp_appt_booking b
             JOIN mp_appt_room r ON r.id = b.room_id
             WHERE b.start_time BETWEEN '2025-03-10 09:00' AND '2025-03-10 12:00'
               AND b.status = 'confirmed'
             ORDER BY b.start_time"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Sprint Planning', $rows[0]['title']);
        $this->assertSame('Standup', $rows[1]['title']);
        $this->assertSame('Design Review', $rows[2]['title']);
    }

    public function testExistsSubquery(): void
    {
        $rows = $this->ztdQuery(
            "SELECT r.name, r.building
             FROM mp_appt_room r
             WHERE EXISTS (
                 SELECT 1 FROM mp_appt_booking b
                 WHERE b.room_id = r.id AND b.status = 'confirmed'
             )
             ORDER BY r.name"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Alpha', $rows[0]['name']);
        $this->assertSame('Beta', $rows[1]['name']);
        $this->assertSame('Gamma', $rows[2]['name']);
    }

    public function testNotExistsAntiJoin(): void
    {
        $rows = $this->ztdQuery(
            "SELECT r.name
             FROM mp_appt_room r
             WHERE NOT EXISTS (
                 SELECT 1 FROM mp_appt_booking b
                 WHERE b.room_id = r.id
                   AND b.status = 'confirmed'
                   AND b.start_time >= '2025-03-10 14:00'
                   AND b.start_time < '2025-03-10 16:00'
             )
             ORDER BY r.name"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Alpha', $rows[0]['name']);
        $this->assertSame('Beta', $rows[1]['name']);
    }

    public function testCoalesceForDefaults(): void
    {
        $rows = $this->ztdQuery(
            "SELECT title, COALESCE(notes, 'No notes') AS display_notes
             FROM mp_appt_booking
             WHERE room_id = 1
             ORDER BY start_time"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('No notes', $rows[0]['display_notes']);
    }

    public function testUpdateWithBetween(): void
    {
        $this->ztdExec(
            "UPDATE mp_appt_booking SET status = 'cancelled'
             WHERE start_time BETWEEN '2025-03-10 09:00' AND '2025-03-10 11:59'
               AND status != 'cancelled'"
        );

        $rows = $this->ztdQuery(
            "SELECT title, status FROM mp_appt_booking ORDER BY id"
        );

        $this->assertSame('cancelled', $rows[0]['status']);
        $this->assertSame('cancelled', $rows[1]['status']);
        $this->assertSame('cancelled', $rows[2]['status']);
        $this->assertSame('tentative', $rows[3]['status']);
    }

    public function testCountCaseStatusSummary(): void
    {
        $rows = $this->ztdQuery(
            "SELECT r.name,
                    COUNT(b.id) AS total,
                    SUM(CASE WHEN b.status = 'confirmed' THEN 1 ELSE 0 END) AS confirmed,
                    SUM(CASE WHEN b.status = 'tentative' THEN 1 ELSE 0 END) AS tentative,
                    SUM(CASE WHEN b.status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled
             FROM mp_appt_room r
             LEFT JOIN mp_appt_booking b ON b.room_id = r.id
             GROUP BY r.id, r.name
             ORDER BY r.name"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Alpha', $rows[0]['name']);
        $this->assertEquals(3, (int) $rows[0]['total']);
        $this->assertEquals(2, (int) $rows[0]['confirmed']);
        $this->assertEquals(1, (int) $rows[0]['tentative']);
        $this->assertSame('Beta', $rows[1]['name']);
        $this->assertEquals(2, (int) $rows[1]['total']);
    }

    public function testPreparedBetween(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT b.title, r.name AS room
             FROM mp_appt_booking b
             JOIN mp_appt_room r ON r.id = b.room_id
             WHERE b.start_time BETWEEN ? AND ?
               AND b.status = ?
             ORDER BY b.start_time",
            ['2025-03-10 09:00', '2025-03-10 12:00', 'confirmed']
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Sprint Planning', $rows[0]['title']);
    }

    public function testOverlapDetection(): void
    {
        $rows = $this->ztdQuery(
            "SELECT r.name
             FROM mp_appt_room r
             WHERE r.id = 1
               AND NOT EXISTS (
                   SELECT 1 FROM mp_appt_booking b
                   WHERE b.room_id = r.id
                     AND b.status IN ('confirmed', 'tentative')
                     AND b.start_time < '2025-03-10 10:30'
                     AND b.end_time > '2025-03-10 10:00'
               )"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Alpha', $rows[0]['name']);

        $rows2 = $this->ztdQuery(
            "SELECT r.name
             FROM mp_appt_room r
             WHERE r.id = 1
               AND NOT EXISTS (
                   SELECT 1 FROM mp_appt_booking b
                   WHERE b.room_id = r.id
                     AND b.status IN ('confirmed', 'tentative')
                     AND b.start_time < '2025-03-10 10:15'
                     AND b.end_time > '2025-03-10 09:30'
               )"
        );

        $this->assertCount(0, $rows2);
    }

    public function testPhysicalIsolation(): void
    {
        $this->ztdExec("DELETE FROM mp_appt_booking WHERE status = 'cancelled'");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_appt_booking");
        $this->assertEquals(5, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mp_appt_booking")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
