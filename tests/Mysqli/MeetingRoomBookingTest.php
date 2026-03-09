<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests meeting room booking scenarios through ZTD shadow store (MySQLi).
 * Covers NOT EXISTS with compound date-range overlap detection, COUNT-based
 * availability, overlapping booking detection, per-user counts, room
 * utilization with CASE, floor-level aggregation, and physical isolation.
 * @spec SPEC-10.2.127
 */
class MeetingRoomBookingTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_mrb_rooms (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                capacity INT,
                floor_num INT
            )',
            'CREATE TABLE mi_mrb_bookings (
                id INT PRIMARY KEY,
                room_id INT,
                booked_by VARCHAR(100),
                booking_date VARCHAR(20),
                start_time VARCHAR(10),
                end_time VARCHAR(10),
                title VARCHAR(200)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_mrb_bookings', 'mi_mrb_rooms'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_mrb_rooms VALUES (1, 'Alpha', 10, 1)");
        $this->mysqli->query("INSERT INTO mi_mrb_rooms VALUES (2, 'Beta', 6, 1)");
        $this->mysqli->query("INSERT INTO mi_mrb_rooms VALUES (3, 'Gamma', 20, 2)");
        $this->mysqli->query("INSERT INTO mi_mrb_rooms VALUES (4, 'Delta', 4, 2)");

        $this->mysqli->query("INSERT INTO mi_mrb_bookings VALUES (1, 1, 'alice', '2025-10-15', '09:00', '10:00', 'Standup')");
        $this->mysqli->query("INSERT INTO mi_mrb_bookings VALUES (2, 1, 'bob', '2025-10-15', '10:30', '12:00', 'Sprint Planning')");
        $this->mysqli->query("INSERT INTO mi_mrb_bookings VALUES (3, 1, 'charlie', '2025-10-15', '14:00', '15:00', 'Retrospective')");
        $this->mysqli->query("INSERT INTO mi_mrb_bookings VALUES (4, 2, 'alice', '2025-10-15', '09:00', '09:30', 'Quick sync')");
        $this->mysqli->query("INSERT INTO mi_mrb_bookings VALUES (5, 2, 'diana', '2025-10-15', '11:00', '12:30', 'Design review')");
        $this->mysqli->query("INSERT INTO mi_mrb_bookings VALUES (6, 3, 'bob', '2025-10-15', '13:00', '15:00', 'All-hands')");
        $this->mysqli->query("INSERT INTO mi_mrb_bookings VALUES (7, 4, 'eve', '2025-10-15', '08:00', '09:00', '1:1 meeting')");
        $this->mysqli->query("INSERT INTO mi_mrb_bookings VALUES (8, 4, 'eve', '2025-10-15', '15:00', '16:00', 'Interview')");
        $this->mysqli->query("INSERT INTO mi_mrb_bookings VALUES (9, 1, 'alice', '2025-10-16', '09:00', '10:00', 'Follow-up')");
    }

    /**
     * NOT EXISTS with compound date-range overlap detection: find rooms
     * available during 10:00-11:00 on 2025-10-15.
     * Alpha is blocked by booking 2 (10:30-12:00 overlaps 10:00-11:00).
     * Beta, Delta, Gamma are available.
     */
    public function testRoomAvailabilityCheck(): void
    {
        $rows = $this->ztdQuery(
            "SELECT r.id, r.name, r.capacity
             FROM mi_mrb_rooms r
             WHERE NOT EXISTS (
                 SELECT 1 FROM mi_mrb_bookings b
                 WHERE b.room_id = r.id
                   AND b.booking_date = '2025-10-15'
                   AND b.start_time < '11:00'
                   AND b.end_time > '10:00'
             )
             ORDER BY r.name"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Beta', $rows[0]['name']);
        $this->assertSame('Delta', $rows[1]['name']);
        $this->assertSame('Gamma', $rows[2]['name']);
    }

    /**
     * COUNT bookings per room for a given date using LEFT JOIN and GROUP BY.
     * Alpha: 3, Beta: 2, Delta: 2, Gamma: 1.
     */
    public function testBookingsPerRoom(): void
    {
        $rows = $this->ztdQuery(
            "SELECT r.name, COUNT(b.id) AS booking_count
             FROM mi_mrb_rooms r
             LEFT JOIN mi_mrb_bookings b ON b.room_id = r.id AND b.booking_date = '2025-10-15'
             GROUP BY r.id, r.name
             ORDER BY booking_count DESC, r.name"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('Alpha', $rows[0]['name']);
        $this->assertEquals(3, (int) $rows[0]['booking_count']);
        $this->assertSame('Beta', $rows[1]['name']);
        $this->assertEquals(2, (int) $rows[1]['booking_count']);
        $this->assertSame('Delta', $rows[2]['name']);
        $this->assertEquals(2, (int) $rows[2]['booking_count']);
        $this->assertSame('Gamma', $rows[3]['name']);
        $this->assertEquals(1, (int) $rows[3]['booking_count']);
    }

    /**
     * Detect existing bookings that overlap with a proposed booking
     * (room 1, 09:30-10:30 on 2025-10-15).
     * Booking 1 (09:00-10:00) overlaps. Booking 2 (10:30-12:00) does not.
     */
    public function testOverlappingBookingDetection(): void
    {
        $rows = $this->ztdQuery(
            "SELECT b.id, b.title, b.start_time, b.end_time
             FROM mi_mrb_bookings b
             WHERE b.room_id = 1
               AND b.booking_date = '2025-10-15'
               AND b.start_time < '10:30'
               AND b.end_time > '09:30'
             ORDER BY b.start_time"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Standup', $rows[0]['title']);
        $this->assertSame('09:00', $rows[0]['start_time']);
        $this->assertSame('10:00', $rows[0]['end_time']);
    }

    /**
     * COUNT bookings per user across all rooms and dates.
     * alice: 3, bob: 2, eve: 2, charlie: 1, diana: 1.
     */
    public function testBookingsPerUser(): void
    {
        $rows = $this->ztdQuery(
            "SELECT booked_by, COUNT(*) AS total_bookings
             FROM mi_mrb_bookings
             GROUP BY booked_by
             ORDER BY total_bookings DESC, booked_by"
        );

        $this->assertCount(5, $rows);
        $this->assertSame('alice', $rows[0]['booked_by']);
        $this->assertEquals(3, (int) $rows[0]['total_bookings']);
        $this->assertSame('bob', $rows[1]['booked_by']);
        $this->assertEquals(2, (int) $rows[1]['total_bookings']);
        $this->assertSame('eve', $rows[2]['booked_by']);
        $this->assertEquals(2, (int) $rows[2]['total_bookings']);
        $this->assertSame('charlie', $rows[3]['booked_by']);
        $this->assertEquals(1, (int) $rows[3]['total_bookings']);
        $this->assertSame('diana', $rows[4]['booked_by']);
        $this->assertEquals(1, (int) $rows[4]['total_bookings']);
    }

    /**
     * Room utilization using CASE with COUNT thresholds.
     * Alpha: high (3), Beta: medium (2), Delta: medium (2), Gamma: low (1).
     */
    public function testRoomUtilization(): void
    {
        $rows = $this->ztdQuery(
            "SELECT r.name, r.capacity,
                    COUNT(b.id) AS bookings,
                    CASE WHEN COUNT(b.id) >= 3 THEN 'high'
                         WHEN COUNT(b.id) >= 2 THEN 'medium'
                         ELSE 'low'
                    END AS utilization
             FROM mi_mrb_rooms r
             LEFT JOIN mi_mrb_bookings b ON b.room_id = r.id AND b.booking_date = '2025-10-15'
             GROUP BY r.id, r.name, r.capacity
             ORDER BY bookings DESC, r.name"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('Alpha', $rows[0]['name']);
        $this->assertEquals(3, (int) $rows[0]['bookings']);
        $this->assertSame('high', $rows[0]['utilization']);
        $this->assertSame('Beta', $rows[1]['name']);
        $this->assertEquals(2, (int) $rows[1]['bookings']);
        $this->assertSame('medium', $rows[1]['utilization']);
        $this->assertSame('Delta', $rows[2]['name']);
        $this->assertEquals(2, (int) $rows[2]['bookings']);
        $this->assertSame('medium', $rows[2]['utilization']);
        $this->assertSame('Gamma', $rows[3]['name']);
        $this->assertEquals(1, (int) $rows[3]['bookings']);
        $this->assertSame('low', $rows[3]['utilization']);
    }

    /**
     * Aggregate bookings by floor using COUNT(DISTINCT) and COUNT.
     * Floor 1: 2 rooms, 5 bookings. Floor 2: 2 rooms, 3 bookings.
     */
    public function testFloorSummary(): void
    {
        $rows = $this->ztdQuery(
            "SELECT r.floor_num,
                    COUNT(DISTINCT r.id) AS room_count,
                    COUNT(b.id) AS total_bookings
             FROM mi_mrb_rooms r
             LEFT JOIN mi_mrb_bookings b ON b.room_id = r.id AND b.booking_date = '2025-10-15'
             GROUP BY r.floor_num
             ORDER BY r.floor_num"
        );

        $this->assertCount(2, $rows);
        $this->assertEquals(1, (int) $rows[0]['floor_num']);
        $this->assertEquals(2, (int) $rows[0]['room_count']);
        $this->assertEquals(5, (int) $rows[0]['total_bookings']);
        $this->assertEquals(2, (int) $rows[1]['floor_num']);
        $this->assertEquals(2, (int) $rows[1]['room_count']);
        $this->assertEquals(3, (int) $rows[1]['total_bookings']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_mrb_bookings VALUES (10, 1, 'frank', '2025-10-15', '16:00', '17:00', 'Wrap-up')");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_mrb_bookings");
        $this->assertSame(10, (int) $rows[0]['cnt']);

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_mrb_bookings');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
