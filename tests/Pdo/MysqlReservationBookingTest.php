<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests a reservation/booking workflow through ZTD shadow store (MySQL PDO).
 * Covers time-range queries, availability checking via anti-join, status
 * transitions, utilization reporting, and physical isolation.
 * @spec SPEC-10.2.58
 */
class MysqlReservationBookingTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_rb_venues (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                capacity INT,
                hourly_rate DECIMAL(10,2)
            )',
            'CREATE TABLE mp_rb_time_slots (
                id INT PRIMARY KEY,
                venue_id INT,
                slot_date DATE,
                start_hour INT,
                end_hour INT
            )',
            'CREATE TABLE mp_rb_reservations (
                id INT PRIMARY KEY,
                slot_id INT,
                customer_name VARCHAR(255),
                status VARCHAR(20),
                created_at DATETIME
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_rb_reservations', 'mp_rb_time_slots', 'mp_rb_venues'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_rb_venues VALUES (1, 'Main Hall', 200, 150.00)");
        $this->pdo->exec("INSERT INTO mp_rb_venues VALUES (2, 'Meeting Room A', 20, 50.00)");
        $this->pdo->exec("INSERT INTO mp_rb_venues VALUES (3, 'Outdoor Terrace', 100, 120.00)");

        $slotId = 1;
        foreach ([1, 2, 3] as $venueId) {
            for ($hour = 9; $hour < 17; $hour++) {
                $this->pdo->exec("INSERT INTO mp_rb_time_slots VALUES ({$slotId}, {$venueId}, '2026-03-10', {$hour}, " . ($hour + 1) . ")");
                $slotId++;
            }
        }

        $this->pdo->exec("INSERT INTO mp_rb_reservations VALUES (1, 1, 'Alice', 'confirmed', '2026-03-01 10:00:00')");
        $this->pdo->exec("INSERT INTO mp_rb_reservations VALUES (2, 2, 'Bob', 'confirmed', '2026-03-01 11:00:00')");
        $this->pdo->exec("INSERT INTO mp_rb_reservations VALUES (3, 9, 'Charlie', 'pending', '2026-03-02 09:00:00')");
        $this->pdo->exec("INSERT INTO mp_rb_reservations VALUES (4, 17, 'Diana', 'confirmed', '2026-03-03 14:00:00')");
        $this->pdo->exec("INSERT INTO mp_rb_reservations VALUES (5, 3, 'Eve', 'cancelled', '2026-03-04 08:00:00')");
    }

    public function testAvailableSlots(): void
    {
        $rows = $this->ztdQuery(
            "SELECT ts.id AS slot_id, v.name AS venue_name,
                    ts.slot_date, ts.start_hour, ts.end_hour
             FROM mp_rb_time_slots ts
             JOIN mp_rb_venues v ON v.id = ts.venue_id
             LEFT JOIN mp_rb_reservations r ON r.slot_id = ts.id AND r.status != 'cancelled'
             WHERE r.id IS NULL
             ORDER BY v.name, ts.start_hour"
        );

        $this->assertCount(20, $rows);
        $mainHallSlots = array_filter($rows, fn($r) => $r['venue_name'] === 'Main Hall');
        $this->assertCount(6, $mainHallSlots);
    }

    public function testAvailableSlotsForVenueAndTimeRange(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT ts.id AS slot_id, ts.start_hour, ts.end_hour
             FROM mp_rb_time_slots ts
             LEFT JOIN mp_rb_reservations r ON r.slot_id = ts.id AND r.status != 'cancelled'
             WHERE ts.venue_id = ?
               AND ts.start_hour >= ?
               AND ts.end_hour <= ?
               AND r.id IS NULL
             ORDER BY ts.start_hour",
            [1, 12, 16]
        );

        $this->assertCount(4, $rows);
        $this->assertEquals(12, (int) $rows[0]['start_hour']);
    }

    public function testBookSlotAndVerifyAvailability(): void
    {
        $available = $this->ztdQuery(
            "SELECT ts.id FROM mp_rb_time_slots ts
             LEFT JOIN mp_rb_reservations r ON r.slot_id = ts.id AND r.status != 'cancelled'
             WHERE ts.id = 11 AND r.id IS NULL"
        );
        $this->assertCount(1, $available);

        $this->pdo->exec("INSERT INTO mp_rb_reservations VALUES (6, 11, 'Frank', 'confirmed', '2026-03-09 15:00:00')");

        $available = $this->ztdQuery(
            "SELECT ts.id FROM mp_rb_time_slots ts
             LEFT JOIN mp_rb_reservations r ON r.slot_id = ts.id AND r.status != 'cancelled'
             WHERE ts.id = 11 AND r.id IS NULL"
        );
        $this->assertCount(0, $available);
    }

    public function testReservationStatusTransitions(): void
    {
        $rows = $this->ztdQuery("SELECT status FROM mp_rb_reservations WHERE id = 3");
        $this->assertSame('pending', $rows[0]['status']);

        $affected = $this->pdo->exec("UPDATE mp_rb_reservations SET status = 'confirmed' WHERE id = 3 AND status = 'pending'");
        $this->assertSame(1, $affected);

        $rows = $this->ztdQuery("SELECT status FROM mp_rb_reservations WHERE id = 3");
        $this->assertSame('confirmed', $rows[0]['status']);

        $affected = $this->pdo->exec("UPDATE mp_rb_reservations SET status = 'cancelled' WHERE id = 3 AND status = 'confirmed'");
        $this->assertSame(1, $affected);

        $rows = $this->ztdQuery("SELECT status FROM mp_rb_reservations WHERE id = 3");
        $this->assertSame('cancelled', $rows[0]['status']);

        $affected = $this->pdo->exec("UPDATE mp_rb_reservations SET status = 'confirmed' WHERE id = 3 AND status = 'pending'");
        $this->assertSame(0, $affected);
    }

    public function testCancelAndRebook(): void
    {
        $this->pdo->exec("UPDATE mp_rb_reservations SET status = 'cancelled' WHERE id = 1");

        $available = $this->ztdQuery(
            "SELECT ts.id FROM mp_rb_time_slots ts
             LEFT JOIN mp_rb_reservations r ON r.slot_id = ts.id AND r.status != 'cancelled'
             WHERE ts.id = 1 AND r.id IS NULL"
        );
        $this->assertCount(1, $available);

        $this->pdo->exec("INSERT INTO mp_rb_reservations VALUES (7, 1, 'Grace', 'confirmed', '2026-03-09 16:00:00')");

        $available = $this->ztdQuery(
            "SELECT ts.id FROM mp_rb_time_slots ts
             LEFT JOIN mp_rb_reservations r ON r.slot_id = ts.id AND r.status != 'cancelled'
             WHERE ts.id = 1 AND r.id IS NULL"
        );
        $this->assertCount(0, $available);
    }

    public function testVenueUtilizationReport(): void
    {
        $rows = $this->ztdQuery(
            "SELECT v.name,
                    COUNT(DISTINCT ts.id) AS total_slots,
                    COUNT(DISTINCT CASE WHEN r.status IN ('confirmed', 'pending') THEN r.id END) AS booked_count
             FROM mp_rb_venues v
             JOIN mp_rb_time_slots ts ON ts.venue_id = v.id
             LEFT JOIN mp_rb_reservations r ON r.slot_id = ts.id
             GROUP BY v.id, v.name
             ORDER BY v.name"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Main Hall', $rows[0]['name']);
        $this->assertEquals(8, (int) $rows[0]['total_slots']);
        $this->assertEquals(2, (int) $rows[0]['booked_count']);
    }

    public function testCustomerBookingHistory(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT r.id AS reservation_id, v.name AS venue_name,
                    ts.slot_date, ts.start_hour, ts.end_hour,
                    r.status, r.created_at
             FROM mp_rb_reservations r
             JOIN mp_rb_time_slots ts ON ts.id = r.slot_id
             JOIN mp_rb_venues v ON v.id = ts.venue_id
             WHERE r.customer_name = ?
             ORDER BY r.created_at",
            ['Alice']
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Main Hall', $rows[0]['venue_name']);
        $this->assertSame('confirmed', $rows[0]['status']);
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO mp_rb_reservations VALUES (8, 5, 'NewCustomer', 'pending', '2026-03-09 17:00:00')");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_rb_reservations");
        $this->assertSame(6, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mp_rb_reservations")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
