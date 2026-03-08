<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests a reservation/booking workflow through ZTD shadow store (SQLite PDO).
 * Covers time-range queries, availability checking via anti-join, status
 * transitions, utilization reporting, and physical isolation.
 * @spec SPEC-10.2.58
 */
class SqliteReservationBookingTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_rb_venues (
                id INTEGER PRIMARY KEY,
                name TEXT,
                capacity INTEGER,
                hourly_rate REAL
            )',
            'CREATE TABLE sl_rb_time_slots (
                id INTEGER PRIMARY KEY,
                venue_id INTEGER,
                slot_date TEXT,
                start_hour INTEGER,
                end_hour INTEGER
            )',
            'CREATE TABLE sl_rb_reservations (
                id INTEGER PRIMARY KEY,
                slot_id INTEGER,
                customer_name TEXT,
                status TEXT,
                created_at TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_rb_reservations', 'sl_rb_time_slots', 'sl_rb_venues'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 3 venues
        $this->pdo->exec("INSERT INTO sl_rb_venues VALUES (1, 'Main Hall', 200, 150.00)");
        $this->pdo->exec("INSERT INTO sl_rb_venues VALUES (2, 'Meeting Room A', 20, 50.00)");
        $this->pdo->exec("INSERT INTO sl_rb_venues VALUES (3, 'Outdoor Terrace', 100, 120.00)");

        // Time slots for 2026-03-10 (each venue has slots from 9-17)
        $slotId = 1;
        foreach ([1, 2, 3] as $venueId) {
            for ($hour = 9; $hour < 17; $hour++) {
                $this->pdo->exec("INSERT INTO sl_rb_time_slots VALUES ({$slotId}, {$venueId}, '2026-03-10', {$hour}, " . ($hour + 1) . ")");
                $slotId++;
            }
        }

        // Some existing reservations
        $this->pdo->exec("INSERT INTO sl_rb_reservations VALUES (1, 1, 'Alice', 'confirmed', '2026-03-01 10:00:00')");  // Main Hall 9-10
        $this->pdo->exec("INSERT INTO sl_rb_reservations VALUES (2, 2, 'Bob', 'confirmed', '2026-03-01 11:00:00')");    // Main Hall 10-11
        $this->pdo->exec("INSERT INTO sl_rb_reservations VALUES (3, 9, 'Charlie', 'pending', '2026-03-02 09:00:00')");  // Meeting Room A 9-10
        $this->pdo->exec("INSERT INTO sl_rb_reservations VALUES (4, 17, 'Diana', 'confirmed', '2026-03-03 14:00:00')"); // Outdoor Terrace 9-10
        $this->pdo->exec("INSERT INTO sl_rb_reservations VALUES (5, 3, 'Eve', 'cancelled', '2026-03-04 08:00:00')");    // Main Hall 11-12 (cancelled)
    }

    /**
     * LEFT JOIN anti-join: find available (unreserved) time slots.
     */
    public function testAvailableSlots(): void
    {
        $rows = $this->ztdQuery(
            "SELECT ts.id AS slot_id, v.name AS venue_name,
                    ts.slot_date, ts.start_hour, ts.end_hour
             FROM sl_rb_time_slots ts
             JOIN sl_rb_venues v ON v.id = ts.venue_id
             LEFT JOIN sl_rb_reservations r ON r.slot_id = ts.id AND r.status != 'cancelled'
             WHERE r.id IS NULL
             ORDER BY v.name, ts.start_hour"
        );

        // Total slots: 24 (8 per venue × 3 venues)
        // Booked (non-cancelled): slots 1, 2, 9, 17 = 4
        // Available: 24 - 4 = 20
        // Slot 3 (Main Hall 11-12) was cancelled so it IS available
        $this->assertCount(20, $rows);

        // First available slot should be Main Hall 11-12 (slot 3, cancelled reservation)
        $mainHallSlots = array_filter($rows, fn($r) => $r['venue_name'] === 'Main Hall');
        $mainHallHours = array_column(array_values($mainHallSlots), 'start_hour');
        // Main Hall booked: 9-10 (slot 1), 10-11 (slot 2). Available: 11-16 (6 slots)
        $this->assertCount(6, $mainHallSlots);
        $this->assertEquals(11, (int) $mainHallHours[0]);
    }

    /**
     * Find available slots for a specific venue and time range.
     */
    public function testAvailableSlotsForVenueAndTimeRange(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT ts.id AS slot_id, ts.start_hour, ts.end_hour
             FROM sl_rb_time_slots ts
             LEFT JOIN sl_rb_reservations r ON r.slot_id = ts.id AND r.status != 'cancelled'
             WHERE ts.venue_id = ?
               AND ts.start_hour >= ?
               AND ts.end_hour <= ?
               AND r.id IS NULL
             ORDER BY ts.start_hour",
            [1, 12, 16]
        );

        // Main Hall (venue 1), hours 12-16: slots for 12-13, 13-14, 14-15, 15-16
        // None booked in this range
        $this->assertCount(4, $rows);
        $this->assertEquals(12, (int) $rows[0]['start_hour']);
        $this->assertEquals(15, (int) $rows[3]['start_hour']);
    }

    /**
     * Book a slot and verify it disappears from available list.
     */
    public function testBookSlotAndVerifyAvailability(): void
    {
        // Slot 11 = Meeting Room A, 11-12, currently available
        $available = $this->ztdQuery(
            "SELECT ts.id FROM sl_rb_time_slots ts
             LEFT JOIN sl_rb_reservations r ON r.slot_id = ts.id AND r.status != 'cancelled'
             WHERE ts.id = 11 AND r.id IS NULL"
        );
        $this->assertCount(1, $available);

        // Book it
        $this->pdo->exec("INSERT INTO sl_rb_reservations VALUES (6, 11, 'Frank', 'confirmed', '2026-03-09 15:00:00')");

        // Verify slot is no longer available
        $available = $this->ztdQuery(
            "SELECT ts.id FROM sl_rb_time_slots ts
             LEFT JOIN sl_rb_reservations r ON r.slot_id = ts.id AND r.status != 'cancelled'
             WHERE ts.id = 11 AND r.id IS NULL"
        );
        $this->assertCount(0, $available);

        // Verify reservation exists
        $rows = $this->ztdQuery("SELECT customer_name, status FROM sl_rb_reservations WHERE id = 6");
        $this->assertSame('Frank', $rows[0]['customer_name']);
        $this->assertSame('confirmed', $rows[0]['status']);
    }

    /**
     * Status transitions: pending -> confirmed -> cancelled with guards.
     */
    public function testReservationStatusTransitions(): void
    {
        // Reservation 3 is pending (Charlie, Meeting Room A 9-10)
        $rows = $this->ztdQuery("SELECT status FROM sl_rb_reservations WHERE id = 3");
        $this->assertSame('pending', $rows[0]['status']);

        // Confirm it
        $affected = $this->pdo->exec("UPDATE sl_rb_reservations SET status = 'confirmed' WHERE id = 3 AND status = 'pending'");
        $this->assertSame(1, $affected);

        $rows = $this->ztdQuery("SELECT status FROM sl_rb_reservations WHERE id = 3");
        $this->assertSame('confirmed', $rows[0]['status']);

        // Cancel it
        $affected = $this->pdo->exec("UPDATE sl_rb_reservations SET status = 'cancelled' WHERE id = 3 AND status = 'confirmed'");
        $this->assertSame(1, $affected);

        $rows = $this->ztdQuery("SELECT status FROM sl_rb_reservations WHERE id = 3");
        $this->assertSame('cancelled', $rows[0]['status']);

        // Invalid: try to confirm a cancelled reservation
        $affected = $this->pdo->exec("UPDATE sl_rb_reservations SET status = 'confirmed' WHERE id = 3 AND status = 'pending'");
        $this->assertSame(0, $affected);

        $rows = $this->ztdQuery("SELECT status FROM sl_rb_reservations WHERE id = 3");
        $this->assertSame('cancelled', $rows[0]['status']);
    }

    /**
     * Cancel a reservation and verify the slot becomes available again.
     */
    public function testCancelAndRebook(): void
    {
        // Slot 1 (Main Hall 9-10) is booked by Alice (confirmed)
        $available = $this->ztdQuery(
            "SELECT ts.id FROM sl_rb_time_slots ts
             LEFT JOIN sl_rb_reservations r ON r.slot_id = ts.id AND r.status != 'cancelled'
             WHERE ts.id = 1 AND r.id IS NULL"
        );
        $this->assertCount(0, $available);

        // Cancel Alice's reservation
        $this->pdo->exec("UPDATE sl_rb_reservations SET status = 'cancelled' WHERE id = 1");

        // Slot is now available
        $available = $this->ztdQuery(
            "SELECT ts.id FROM sl_rb_time_slots ts
             LEFT JOIN sl_rb_reservations r ON r.slot_id = ts.id AND r.status != 'cancelled'
             WHERE ts.id = 1 AND r.id IS NULL"
        );
        $this->assertCount(1, $available);

        // Rebook with new customer
        $this->pdo->exec("INSERT INTO sl_rb_reservations VALUES (7, 1, 'Grace', 'confirmed', '2026-03-09 16:00:00')");

        // Slot is booked again
        $available = $this->ztdQuery(
            "SELECT ts.id FROM sl_rb_time_slots ts
             LEFT JOIN sl_rb_reservations r ON r.slot_id = ts.id AND r.status != 'cancelled'
             WHERE ts.id = 1 AND r.id IS NULL"
        );
        $this->assertCount(0, $available);

        // Verify new booking
        $rows = $this->ztdQuery("SELECT customer_name FROM sl_rb_reservations WHERE slot_id = 1 AND status = 'confirmed'");
        $this->assertCount(1, $rows);
        $this->assertSame('Grace', $rows[0]['customer_name']);
    }

    /**
     * Venue utilization report: bookings per venue and revenue.
     */
    public function testVenueUtilizationReport(): void
    {
        $rows = $this->ztdQuery(
            "SELECT v.name,
                    COUNT(DISTINCT ts.id) AS total_slots,
                    COUNT(DISTINCT CASE WHEN r.status IN ('confirmed', 'pending') THEN r.id END) AS booked_count,
                    ROUND(COUNT(DISTINCT CASE WHEN r.status IN ('confirmed', 'pending') THEN r.id END) * 100.0
                          / COUNT(DISTINCT ts.id), 1) AS utilization_pct,
                    COALESCE(SUM(CASE WHEN r.status IN ('confirmed', 'pending') THEN v.hourly_rate ELSE 0 END), 0) AS revenue
             FROM sl_rb_venues v
             JOIN sl_rb_time_slots ts ON ts.venue_id = v.id
             LEFT JOIN sl_rb_reservations r ON r.slot_id = ts.id
             GROUP BY v.id, v.name
             ORDER BY v.name"
        );

        $this->assertCount(3, $rows);

        // Main Hall: 8 slots, 2 confirmed (slots 1, 2), 1 cancelled = 2 active
        $this->assertSame('Main Hall', $rows[0]['name']);
        $this->assertEquals(8, (int) $rows[0]['total_slots']);
        $this->assertEquals(2, (int) $rows[0]['booked_count']);
        $this->assertEqualsWithDelta(25.0, (float) $rows[0]['utilization_pct'], 0.1);

        // Meeting Room A: 8 slots, 1 pending (slot 9)
        $this->assertSame('Meeting Room A', $rows[1]['name']);
        $this->assertEquals(1, (int) $rows[1]['booked_count']);

        // Outdoor Terrace: 8 slots, 1 confirmed (slot 17)
        $this->assertSame('Outdoor Terrace', $rows[2]['name']);
        $this->assertEquals(1, (int) $rows[2]['booked_count']);
    }

    /**
     * Customer booking history with venue details.
     */
    public function testCustomerBookingHistory(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT r.id AS reservation_id, v.name AS venue_name,
                    ts.slot_date, ts.start_hour, ts.end_hour,
                    r.status, r.created_at
             FROM sl_rb_reservations r
             JOIN sl_rb_time_slots ts ON ts.id = r.slot_id
             JOIN sl_rb_venues v ON v.id = ts.venue_id
             WHERE r.customer_name = ?
             ORDER BY r.created_at",
            ['Alice']
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Main Hall', $rows[0]['venue_name']);
        $this->assertEquals(9, (int) $rows[0]['start_hour']);
        $this->assertEquals(10, (int) $rows[0]['end_hour']);
        $this->assertSame('confirmed', $rows[0]['status']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO sl_rb_reservations VALUES (8, 5, 'NewCustomer', 'pending', '2026-03-09 17:00:00')");
        $this->pdo->exec("UPDATE sl_rb_reservations SET status = 'cancelled' WHERE id = 1");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_rb_reservations");
        $this->assertSame(6, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_rb_reservations")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
