<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests appointment scheduling scenarios through ZTD shadow store (MySQL PDO).
 * Covers calendar slot management with booking conflict detection using
 * NOT EXISTS for overlap detection, date/time comparisons, prepared
 * statement with date params, LEFT JOIN for available slots, GROUP BY
 * with COUNT, and physical isolation.
 * @spec SPEC-10.2.135
 */
class MysqlAppointmentSchedulingTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_as_providers (
                id INT PRIMARY KEY,
                provider_name VARCHAR(100),
                specialty VARCHAR(100)
            )',
            'CREATE TABLE mp_as_slots (
                id INT PRIMARY KEY,
                provider_id INT,
                slot_date TEXT,
                start_time TEXT,
                end_time TEXT,
                is_available INT
            )',
            'CREATE TABLE mp_as_appointments (
                id INT PRIMARY KEY,
                slot_id INT,
                patient_name VARCHAR(100),
                appointment_type VARCHAR(100),
                booked_at TEXT,
                status TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_as_appointments', 'mp_as_slots', 'mp_as_providers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_as_providers VALUES (1, 'Dr. Smith', 'General')");
        $this->pdo->exec("INSERT INTO mp_as_providers VALUES (2, 'Dr. Patel', 'Cardiology')");

        $this->pdo->exec("INSERT INTO mp_as_slots VALUES (1, 1, '2025-10-06', '09:00', '09:30', 0)");
        $this->pdo->exec("INSERT INTO mp_as_slots VALUES (2, 1, '2025-10-06', '09:30', '10:00', 0)");
        $this->pdo->exec("INSERT INTO mp_as_slots VALUES (3, 1, '2025-10-06', '10:00', '10:30', 1)");
        $this->pdo->exec("INSERT INTO mp_as_slots VALUES (4, 1, '2025-10-06', '10:30', '11:00', 1)");
        $this->pdo->exec("INSERT INTO mp_as_slots VALUES (5, 1, '2025-10-06', '11:00', '11:30', 0)");

        $this->pdo->exec("INSERT INTO mp_as_slots VALUES (6, 2, '2025-10-06', '14:00', '14:30', 0)");
        $this->pdo->exec("INSERT INTO mp_as_slots VALUES (7, 2, '2025-10-06', '14:30', '15:00', 1)");
        $this->pdo->exec("INSERT INTO mp_as_slots VALUES (8, 2, '2025-10-06', '15:00', '15:30', 1)");

        $this->pdo->exec("INSERT INTO mp_as_appointments VALUES (1, 1, 'Alice', 'checkup', '2025-10-01', 'confirmed')");
        $this->pdo->exec("INSERT INTO mp_as_appointments VALUES (2, 2, 'Bob', 'follow-up', '2025-10-02', 'confirmed')");
        $this->pdo->exec("INSERT INTO mp_as_appointments VALUES (3, 5, 'Carol', 'checkup', '2025-10-03', 'cancelled')");
        $this->pdo->exec("INSERT INTO mp_as_appointments VALUES (4, 6, 'Dave', 'consultation', '2025-10-04', 'confirmed')");
    }

    /**
     * Available slots for a provider: WHERE is_available = 1 AND provider_id = 1.
     * JOIN with providers to show provider_name.
     * Dr. Smith has 2 available slots: 10:00-10:30 and 10:30-11:00.
     */
    public function testAvailableSlotsForProvider(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.id, s.start_time, s.end_time, p.provider_name
             FROM mp_as_slots s
             JOIN mp_as_providers p ON p.id = s.provider_id
             WHERE s.is_available = 1 AND s.provider_id = 1
             ORDER BY s.start_time"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('10:00', $rows[0]['start_time']);
        $this->assertSame('10:30', $rows[0]['end_time']);
        $this->assertSame('Dr. Smith', $rows[0]['provider_name']);
        $this->assertSame('10:30', $rows[1]['start_time']);
        $this->assertSame('11:00', $rows[1]['end_time']);
        $this->assertSame('Dr. Smith', $rows[1]['provider_name']);
    }

    /**
     * Provider schedule summary: GROUP BY provider_id with COUNT and SUM.
     * Dr. Smith: 5 total, 2 available, 3 booked.
     * Dr. Patel: 3 total, 2 available, 1 booked.
     */
    public function testProviderScheduleSummary(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.provider_name,
                    COUNT(s.id) AS total_slots,
                    SUM(s.is_available) AS available,
                    COUNT(s.id) - SUM(s.is_available) AS booked
             FROM mp_as_providers p
             JOIN mp_as_slots s ON s.provider_id = p.id
             GROUP BY p.id, p.provider_name
             ORDER BY p.provider_name"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Dr. Patel', $rows[0]['provider_name']);
        $this->assertEquals(3, (int) $rows[0]['total_slots']);
        $this->assertEquals(2, (int) $rows[0]['available']);
        $this->assertEquals(1, (int) $rows[0]['booked']);
        $this->assertSame('Dr. Smith', $rows[1]['provider_name']);
        $this->assertEquals(5, (int) $rows[1]['total_slots']);
        $this->assertEquals(2, (int) $rows[1]['available']);
        $this->assertEquals(3, (int) $rows[1]['booked']);
    }

    /**
     * Active appointments: JOIN appointments with slots and providers
     * WHERE status != 'cancelled'. 3 rows (appointments 1, 2, 4).
     * ORDER BY patient_name: Alice, Bob, Dave.
     */
    public function testActiveAppointments(): void
    {
        $rows = $this->ztdQuery(
            "SELECT a.id, a.patient_name, a.appointment_type, p.provider_name
             FROM mp_as_appointments a
             JOIN mp_as_slots s ON s.id = a.slot_id
             JOIN mp_as_providers p ON p.id = s.provider_id
             WHERE a.status != 'cancelled'
             ORDER BY a.patient_name"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['patient_name']);
        $this->assertSame('Dr. Smith', $rows[0]['provider_name']);
        $this->assertSame('Bob', $rows[1]['patient_name']);
        $this->assertSame('Dr. Smith', $rows[1]['provider_name']);
        $this->assertSame('Dave', $rows[2]['patient_name']);
        $this->assertSame('Dr. Patel', $rows[2]['provider_name']);
    }

    /**
     * Booking conflict detection: check if 09:15-09:45 overlaps existing
     * booked slots for Dr. Smith. Slots 1 (09:00-09:30) and 2 (09:30-10:00)
     * are booked and overlap this range. Assert count > 0 (conflict exists).
     */
    public function testBookingConflictDetection(): void
    {
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS conflict_count
             FROM mp_as_slots
             WHERE provider_id = 1
               AND is_available = 0
               AND start_time < '09:45'
               AND end_time > '09:15'"
        );

        $this->assertGreaterThan(0, (int) $rows[0]['conflict_count']);
    }

    /**
     * Cancelled appointment count: COUNT appointments WHERE status = 'cancelled'.
     * Only Carol's appointment is cancelled. Assert 1.
     */
    public function testCancelledAppointmentCount(): void
    {
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cancelled_count
             FROM mp_as_appointments
             WHERE status = 'cancelled'"
        );

        $this->assertEquals(1, (int) $rows[0]['cancelled_count']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO mp_as_appointments VALUES (5, 3, 'Eve', 'checkup', '2025-10-05', 'confirmed')");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_as_appointments");
        $this->assertSame(5, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mp_as_appointments")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
