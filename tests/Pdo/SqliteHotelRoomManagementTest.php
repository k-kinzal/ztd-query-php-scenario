<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests a hotel room management scenario through ZTD shadow store (SQLite PDO).
 * Room availability, guest stays, occupancy rates, and revenue per room type
 * exercise GROUP BY with COUNT/SUM/AVG, multi-table JOINs, LEFT JOIN aggregation,
 * WHERE with ORDER BY, prepared statement for floor filtering,
 * and physical isolation check.
 * @spec SPEC-10.2.159
 */
class SqliteHotelRoomManagementTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_hr_rooms (
                id INTEGER PRIMARY KEY,
                room_number TEXT,
                room_type TEXT,
                floor INTEGER,
                rate_per_night REAL,
                status TEXT
            )',
            'CREATE TABLE sl_hr_guests (
                id INTEGER PRIMARY KEY,
                name TEXT,
                email TEXT,
                vip_level TEXT
            )',
            'CREATE TABLE sl_hr_stays (
                id INTEGER PRIMARY KEY,
                room_id INTEGER,
                guest_id INTEGER,
                arrival_date TEXT,
                departure_date TEXT,
                total_charge REAL,
                rating INTEGER
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_hr_stays', 'sl_hr_guests', 'sl_hr_rooms'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 6 rooms
        $this->pdo->exec("INSERT INTO sl_hr_rooms VALUES (1, '101', 'single', 1, 89.00, 'available')");
        $this->pdo->exec("INSERT INTO sl_hr_rooms VALUES (2, '102', 'double', 1, 149.00, 'occupied')");
        $this->pdo->exec("INSERT INTO sl_hr_rooms VALUES (3, '201', 'double', 2, 149.00, 'available')");
        $this->pdo->exec("INSERT INTO sl_hr_rooms VALUES (4, '202', 'suite', 2, 299.00, 'occupied')");
        $this->pdo->exec("INSERT INTO sl_hr_rooms VALUES (5, '203', 'single', 2, 89.00, 'maintenance')");
        $this->pdo->exec("INSERT INTO sl_hr_rooms VALUES (6, '301', 'suite', 3, 349.00, 'available')");

        // 4 guests
        $this->pdo->exec("INSERT INTO sl_hr_guests VALUES (1, 'Alice Chen', 'alice@example.com', 'gold')");
        $this->pdo->exec("INSERT INTO sl_hr_guests VALUES (2, 'Bob Kim', 'bob@example.com', 'silver')");
        $this->pdo->exec("INSERT INTO sl_hr_guests VALUES (3, 'Carol Davis', 'carol@example.com', 'none')");
        $this->pdo->exec("INSERT INTO sl_hr_guests VALUES (4, 'Dan Wilson', 'dan@example.com', 'gold')");

        // 6 stays
        $this->pdo->exec("INSERT INTO sl_hr_stays VALUES (1, 2, 1, '2026-02-20', '2026-02-23', 447.00, 5)");
        $this->pdo->exec("INSERT INTO sl_hr_stays VALUES (2, 4, 4, '2026-02-25', '2026-03-01', 1196.00, 4)");
        $this->pdo->exec("INSERT INTO sl_hr_stays VALUES (3, 1, 3, '2026-01-10', '2026-01-12', 178.00, 3)");
        $this->pdo->exec("INSERT INTO sl_hr_stays VALUES (4, 3, 2, '2026-01-15', '2026-01-18', 447.00, 4)");
        $this->pdo->exec("INSERT INTO sl_hr_stays VALUES (5, 6, 1, '2025-12-24', '2025-12-31', 2443.00, 5)");
        $this->pdo->exec("INSERT INTO sl_hr_stays VALUES (6, 2, 2, '2026-03-05', '2026-03-10', 745.00, NULL)");
    }

    /**
     * GROUP BY room_type, COUNT rooms per type, ORDER BY room_type.
     * Expected: double=2, single=2, suite=2.
     */
    public function testRoomCountByType(): void
    {
        $rows = $this->ztdQuery(
            "SELECT r.room_type, COUNT(*) AS room_count
             FROM sl_hr_rooms r
             GROUP BY r.room_type
             ORDER BY r.room_type"
        );

        $this->assertCount(3, $rows);

        $this->assertSame('double', $rows[0]['room_type']);
        $this->assertEquals(2, (int) $rows[0]['room_count']);

        $this->assertSame('single', $rows[1]['room_type']);
        $this->assertEquals(2, (int) $rows[1]['room_count']);

        $this->assertSame('suite', $rows[2]['room_type']);
        $this->assertEquals(2, (int) $rows[2]['room_count']);
    }

    /**
     * Currently occupied rooms: WHERE status = 'occupied', ORDER BY room_number.
     * Expected: room 102 (double, floor 1), room 202 (suite, floor 2).
     */
    public function testCurrentlyOccupiedRooms(): void
    {
        $rows = $this->ztdQuery(
            "SELECT r.room_number, r.room_type, r.floor
             FROM sl_hr_rooms r
             WHERE r.status = 'occupied'
             ORDER BY r.room_number"
        );

        $this->assertCount(2, $rows);

        $this->assertSame('102', $rows[0]['room_number']);
        $this->assertSame('double', $rows[0]['room_type']);
        $this->assertEquals(1, (int) $rows[0]['floor']);

        $this->assertSame('202', $rows[1]['room_number']);
        $this->assertSame('suite', $rows[1]['room_type']);
        $this->assertEquals(2, (int) $rows[1]['floor']);
    }

    /**
     * Revenue by room type: JOIN rooms to stays, GROUP BY room_type,
     * SUM(total_charge), ROUND(AVG(total_charge), 2), ORDER BY room_type.
     * Expected:
     * - double: total=1639.00, avg=546.33
     * - single: total=178.00, avg=178.00
     * - suite: total=3639.00, avg=1819.50
     */
    public function testRevenueByRoomType(): void
    {
        $rows = $this->ztdQuery(
            "SELECT r.room_type,
                    SUM(s.total_charge) AS total_revenue,
                    ROUND(AVG(s.total_charge), 2) AS avg_charge
             FROM sl_hr_rooms r
             JOIN sl_hr_stays s ON s.room_id = r.id
             GROUP BY r.room_type
             ORDER BY r.room_type"
        );

        $this->assertCount(3, $rows);

        // double: stays 1(447) + 4(447) + 6(745) = 1639, avg = 546.33
        $this->assertSame('double', $rows[0]['room_type']);
        $this->assertEquals(1639.00, (float) $rows[0]['total_revenue'], '', 0.01);
        $this->assertEquals(546.33, (float) $rows[0]['avg_charge'], '', 0.01);

        // single: stay 3(178), avg = 178.00
        $this->assertSame('single', $rows[1]['room_type']);
        $this->assertEquals(178.00, (float) $rows[1]['total_revenue'], '', 0.01);
        $this->assertEquals(178.00, (float) $rows[1]['avg_charge'], '', 0.01);

        // suite: stays 2(1196) + 5(2443) = 3639, avg = 1819.50
        $this->assertSame('suite', $rows[2]['room_type']);
        $this->assertEquals(3639.00, (float) $rows[2]['total_revenue'], '', 0.01);
        $this->assertEquals(1819.50, (float) $rows[2]['avg_charge'], '', 0.01);
    }

    /**
     * Guest stay history: LEFT JOIN guests to stays, COUNT stays, SUM total_charge per guest.
     * ORDER BY guest name.
     * Expected:
     * - Alice Chen: 2 stays, total=2890.00
     * - Bob Kim: 2 stays, total=1192.00
     * - Carol Davis: 1 stay, total=178.00
     * - Dan Wilson: 1 stay, total=1196.00
     */
    public function testGuestStayHistory(): void
    {
        $rows = $this->ztdQuery(
            "SELECT g.name, COUNT(s.id) AS stay_count, SUM(s.total_charge) AS total_spent
             FROM sl_hr_guests g
             LEFT JOIN sl_hr_stays s ON s.guest_id = g.id
             GROUP BY g.id, g.name
             ORDER BY g.name"
        );

        $this->assertCount(4, $rows);

        // Alice Chen: stays 1(447) + 5(2443) = 2890
        $this->assertSame('Alice Chen', $rows[0]['name']);
        $this->assertEquals(2, (int) $rows[0]['stay_count']);
        $this->assertEquals(2890.00, (float) $rows[0]['total_spent'], '', 0.01);

        // Bob Kim: stays 4(447) + 6(745) = 1192
        $this->assertSame('Bob Kim', $rows[1]['name']);
        $this->assertEquals(2, (int) $rows[1]['stay_count']);
        $this->assertEquals(1192.00, (float) $rows[1]['total_spent'], '', 0.01);

        // Carol Davis: stay 3(178)
        $this->assertSame('Carol Davis', $rows[2]['name']);
        $this->assertEquals(1, (int) $rows[2]['stay_count']);
        $this->assertEquals(178.00, (float) $rows[2]['total_spent'], '', 0.01);

        // Dan Wilson: stay 2(1196)
        $this->assertSame('Dan Wilson', $rows[3]['name']);
        $this->assertEquals(1, (int) $rows[3]['stay_count']);
        $this->assertEquals(1196.00, (float) $rows[3]['total_spent'], '', 0.01);
    }

    /**
     * High-rated stays (rating >= 4): JOIN room + guest info, ORDER BY rating DESC, arrival_date ASC.
     * Expected 4 rows:
     * - Alice Chen, 301, suite, 5, 2025-12-24
     * - Alice Chen, 102, double, 5, 2026-02-20
     * - Bob Kim, 201, double, 4, 2026-01-15
     * - Dan Wilson, 202, suite, 4, 2026-02-25
     */
    public function testHighRatedStays(): void
    {
        $rows = $this->ztdQuery(
            "SELECT g.name, r.room_number, r.room_type, s.rating, s.arrival_date
             FROM sl_hr_stays s
             JOIN sl_hr_rooms r ON r.id = s.room_id
             JOIN sl_hr_guests g ON g.id = s.guest_id
             WHERE s.rating >= 4
             ORDER BY s.rating DESC, s.arrival_date"
        );

        $this->assertCount(4, $rows);

        $this->assertSame('Alice Chen', $rows[0]['name']);
        $this->assertSame('301', $rows[0]['room_number']);
        $this->assertSame('suite', $rows[0]['room_type']);
        $this->assertEquals(5, (int) $rows[0]['rating']);
        $this->assertSame('2025-12-24', $rows[0]['arrival_date']);

        $this->assertSame('Alice Chen', $rows[1]['name']);
        $this->assertSame('102', $rows[1]['room_number']);
        $this->assertSame('double', $rows[1]['room_type']);
        $this->assertEquals(5, (int) $rows[1]['rating']);
        $this->assertSame('2026-02-20', $rows[1]['arrival_date']);

        $this->assertSame('Bob Kim', $rows[2]['name']);
        $this->assertSame('201', $rows[2]['room_number']);
        $this->assertSame('double', $rows[2]['room_type']);
        $this->assertEquals(4, (int) $rows[2]['rating']);
        $this->assertSame('2026-01-15', $rows[2]['arrival_date']);

        $this->assertSame('Dan Wilson', $rows[3]['name']);
        $this->assertSame('202', $rows[3]['room_number']);
        $this->assertSame('suite', $rows[3]['room_type']);
        $this->assertEquals(4, (int) $rows[3]['rating']);
        $this->assertSame('2026-02-25', $rows[3]['arrival_date']);
    }

    /**
     * Prepared statement: available rooms on a given floor.
     * Params: floor=2, status='available'.
     * Expected: 1 row - room 201, double, 149.00.
     */
    public function testPreparedFloorAvailability(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT r.room_number, r.room_type, r.rate_per_night
             FROM sl_hr_rooms r
             WHERE r.floor = ? AND r.status = 'available'
             ORDER BY r.room_number",
            [2]
        );

        $this->assertCount(1, $rows);

        $this->assertSame('201', $rows[0]['room_number']);
        $this->assertSame('double', $rows[0]['room_type']);
        $this->assertEquals(149.00, (float) $rows[0]['rate_per_night'], '', 0.01);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        // Insert a new stay via shadow
        $this->pdo->exec("INSERT INTO sl_hr_stays VALUES (7, 1, 3, '2026-04-01', '2026-04-03', 178.00, NULL)");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_hr_stays");
        $this->assertEquals(7, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_hr_stays")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
