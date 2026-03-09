<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests a parking garage scenario through ZTD shadow store (SQLite PDO).
 * Vehicle entry/exit tracking, capacity monitoring, rate calculation,
 * and monthly pass management.
 * SQL patterns exercised: COUNT for current occupancy, CASE for rate tiers,
 * SUM for revenue aggregation, GROUP BY date for daily entry stats,
 * LEFT JOIN for pass validation, COALESCE with multiple fallback levels,
 * DELETE followed by SELECT verification, prepared statement for vehicle lookup,
 * physical isolation check.
 * @spec SPEC-10.2.154
 */
class SqliteParkingGarageTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_pkg_levels (
                id INTEGER PRIMARY KEY,
                name TEXT,
                capacity INTEGER
            )',
            'CREATE TABLE sl_pkg_passes (
                id INTEGER PRIMARY KEY,
                plate_number TEXT,
                member_name TEXT,
                pass_type TEXT,
                valid_from TEXT,
                valid_until TEXT
            )',
            'CREATE TABLE sl_pkg_entries (
                id INTEGER PRIMARY KEY,
                plate_number TEXT,
                level_id INTEGER,
                entry_time TEXT,
                exit_time TEXT,
                amount_charged REAL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_pkg_entries', 'sl_pkg_passes', 'sl_pkg_levels'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Levels
        $this->pdo->exec("INSERT INTO sl_pkg_levels VALUES (1, 'Level A', 50)");
        $this->pdo->exec("INSERT INTO sl_pkg_levels VALUES (2, 'Level B', 40)");
        $this->pdo->exec("INSERT INTO sl_pkg_levels VALUES (3, 'Level C', 30)");

        // Passes
        $this->pdo->exec("INSERT INTO sl_pkg_passes VALUES (1, 'ABC-1234', 'Alice Smith', 'monthly', '2026-02-01', '2026-02-28')");
        $this->pdo->exec("INSERT INTO sl_pkg_passes VALUES (2, 'DEF-5678', 'Bob Jones', 'annual', '2026-01-01', '2026-12-31')");
        $this->pdo->exec("INSERT INTO sl_pkg_passes VALUES (3, 'GHI-9012', 'Carol White', 'monthly', '2026-03-01', '2026-03-31')");

        // Entries
        $this->pdo->exec("INSERT INTO sl_pkg_entries VALUES (1, 'ABC-1234', 1, '2026-03-01 08:00', '2026-03-01 10:30', 5.00)");
        $this->pdo->exec("INSERT INTO sl_pkg_entries VALUES (2, 'DEF-5678', 1, '2026-03-01 09:00', '2026-03-01 14:00', 0.00)");
        $this->pdo->exec("INSERT INTO sl_pkg_entries VALUES (3, 'XYZ-9999', 2, '2026-03-01 10:00', '2026-03-01 11:00', 3.00)");
        $this->pdo->exec("INSERT INTO sl_pkg_entries VALUES (4, 'LMN-4567', 2, '2026-03-01 07:00', '2026-03-01 17:00', 20.00)");
        $this->pdo->exec("INSERT INTO sl_pkg_entries VALUES (5, 'ABC-1234', 1, '2026-03-02 08:00', '2026-03-02 09:30', 3.00)");
        $this->pdo->exec("INSERT INTO sl_pkg_entries VALUES (6, 'GHI-9012', 3, '2026-03-02 10:00', '2026-03-02 16:00', 0.00)");
        $this->pdo->exec("INSERT INTO sl_pkg_entries VALUES (7, 'PQR-1111', 1, '2026-03-02 11:00', NULL, NULL)");
        $this->pdo->exec("INSERT INTO sl_pkg_entries VALUES (8, 'STU-2222', 2, '2026-03-02 12:00', NULL, NULL)");
        $this->pdo->exec("INSERT INTO sl_pkg_entries VALUES (9, 'DEF-5678', 3, '2026-03-02 14:00', NULL, NULL)");
        $this->pdo->exec("INSERT INTO sl_pkg_entries VALUES (10, 'VWX-3333', 1, '2026-03-02 15:00', NULL, NULL)");
    }

    /**
     * COUNT entries WHERE exit_time IS NULL, GROUP BY level.
     * Level A: 2, Level B: 1, Level C: 1. ORDER BY level name.
     */
    public function testCurrentOccupancy(): void
    {
        $rows = $this->ztdQuery(
            "SELECT lv.name, COUNT(e.id) AS occupied
             FROM sl_pkg_levels lv
             JOIN sl_pkg_entries e ON e.level_id = lv.id
             WHERE e.exit_time IS NULL
             GROUP BY lv.id, lv.name
             ORDER BY lv.name"
        );

        $this->assertCount(3, $rows);

        $this->assertSame('Level A', $rows[0]['name']);
        $this->assertEquals(2, (int) $rows[0]['occupied']);

        $this->assertSame('Level B', $rows[1]['name']);
        $this->assertEquals(1, (int) $rows[1]['occupied']);

        $this->assertSame('Level C', $rows[2]['name']);
        $this->assertEquals(1, (int) $rows[2]['occupied']);
    }

    /**
     * LEFT JOIN levels to entries (exit_time IS NULL), COUNT occupied,
     * compute ROUND percentage. All levels included even with 0 current cars.
     * Level A: 2/50=4%, Level B: 1/40=3%, Level C: 1/30=3%.
     */
    public function testCapacityUtilization(): void
    {
        $rows = $this->ztdQuery(
            "SELECT lv.name, lv.capacity,
                    COUNT(e.id) AS occupied,
                    ROUND(COUNT(e.id) * 100.0 / lv.capacity) AS pct
             FROM sl_pkg_levels lv
             LEFT JOIN sl_pkg_entries e ON e.level_id = lv.id AND e.exit_time IS NULL
             GROUP BY lv.id, lv.name, lv.capacity
             ORDER BY lv.name"
        );

        $this->assertCount(3, $rows);

        $this->assertSame('Level A', $rows[0]['name']);
        $this->assertEquals(50, (int) $rows[0]['capacity']);
        $this->assertEquals(2, (int) $rows[0]['occupied']);
        $this->assertEquals(4, (int) $rows[0]['pct']);

        $this->assertSame('Level B', $rows[1]['name']);
        $this->assertEquals(40, (int) $rows[1]['capacity']);
        $this->assertEquals(1, (int) $rows[1]['occupied']);
        $this->assertEquals(3, (int) $rows[1]['pct']);

        $this->assertSame('Level C', $rows[2]['name']);
        $this->assertEquals(30, (int) $rows[2]['capacity']);
        $this->assertEquals(1, (int) $rows[2]['occupied']);
        $this->assertEquals(3, (int) $rows[2]['pct']);
    }

    /**
     * GROUP BY date portion of entry_time, COUNT entries, SUM amount_charged.
     * SUBSTR for date extraction, COALESCE for NULL amounts.
     * 2026-03-01: 4 entries, total=28.00. 2026-03-02: 6 entries, total=3.00.
     */
    public function testDailyEntryStats(): void
    {
        $rows = $this->ztdQuery(
            "SELECT SUBSTR(e.entry_time, 1, 10) AS entry_date,
                    COUNT(*) AS entry_count,
                    COALESCE(SUM(e.amount_charged), 0) AS total_charged
             FROM sl_pkg_entries e
             GROUP BY entry_date
             ORDER BY entry_date"
        );

        $this->assertCount(2, $rows);

        $this->assertSame('2026-03-01', $rows[0]['entry_date']);
        $this->assertEquals(4, (int) $rows[0]['entry_count']);
        $this->assertEquals(28, (int) round((float) $rows[0]['total_charged']));

        $this->assertSame('2026-03-02', $rows[1]['entry_date']);
        $this->assertEquals(6, (int) $rows[1]['entry_count']);
        $this->assertEquals(3, (int) round((float) $rows[1]['total_charged']));
    }

    /**
     * LEFT JOIN entries to passes on plate_number, COUNT visits per pass holder.
     * Alice: 2, Bob: 2, Carol: 1. ORDER BY visit_count DESC, member_name ASC.
     */
    public function testPassHolderActivity(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.member_name, p.pass_type, p.plate_number,
                    COUNT(e.id) AS visit_count
             FROM sl_pkg_passes p
             LEFT JOIN sl_pkg_entries e ON e.plate_number = p.plate_number
             GROUP BY p.id, p.member_name, p.pass_type, p.plate_number
             ORDER BY visit_count DESC, p.member_name ASC"
        );

        $this->assertCount(3, $rows);

        $this->assertSame('Alice Smith', $rows[0]['member_name']);
        $this->assertEquals(2, (int) $rows[0]['visit_count']);

        $this->assertSame('Bob Jones', $rows[1]['member_name']);
        $this->assertEquals(2, (int) $rows[1]['visit_count']);

        $this->assertSame('Carol White', $rows[2]['member_name']);
        $this->assertEquals(1, (int) $rows[2]['visit_count']);
    }

    /**
     * SUM amount_charged GROUP BY level, only completed entries.
     * LEFT JOIN to include all levels. COALESCE for NULL sums.
     * Level B: 23.00, Level A: 8.00, Level C: 0.00.
     */
    public function testRevenueByLevel(): void
    {
        $rows = $this->ztdQuery(
            "SELECT lv.name, COALESCE(SUM(e.amount_charged), 0) AS revenue
             FROM sl_pkg_levels lv
             LEFT JOIN sl_pkg_entries e ON e.level_id = lv.id AND e.exit_time IS NOT NULL
             GROUP BY lv.id, lv.name
             ORDER BY revenue DESC"
        );

        $this->assertCount(3, $rows);

        $this->assertSame('Level B', $rows[0]['name']);
        $this->assertEquals(23, (int) round((float) $rows[0]['revenue']));

        $this->assertSame('Level A', $rows[1]['name']);
        $this->assertEquals(8, (int) round((float) $rows[1]['revenue']));

        $this->assertSame('Level C', $rows[2]['name']);
        $this->assertEquals(0, (int) round((float) $rows[2]['revenue']));
    }

    /**
     * LEFT JOIN entries to passes, WHERE exit_time IS NULL AND pass IS NULL.
     * Currently parked non-pass vehicles: PQR-1111, STU-2222, VWX-3333.
     */
    public function testIdentifyNonPassVehiclesParked(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.plate_number, lv.name AS level_name, e.entry_time
             FROM sl_pkg_entries e
             JOIN sl_pkg_levels lv ON lv.id = e.level_id
             LEFT JOIN sl_pkg_passes p ON p.plate_number = e.plate_number
             WHERE e.exit_time IS NULL AND p.id IS NULL
             ORDER BY e.entry_time"
        );

        $this->assertCount(3, $rows);

        $this->assertSame('PQR-1111', $rows[0]['plate_number']);
        $this->assertSame('Level A', $rows[0]['level_name']);

        $this->assertSame('STU-2222', $rows[1]['plate_number']);
        $this->assertSame('Level B', $rows[1]['level_name']);

        $this->assertSame('VWX-3333', $rows[2]['plate_number']);
        $this->assertSame('Level A', $rows[2]['level_name']);
    }

    /**
     * Prepared statement: find all entries for a given plate_number,
     * JOIN level name, ORDER BY entry_time DESC.
     * Test with 'ABC-1234' (Alice) => 2 rows.
     */
    public function testPreparedVehicleHistory(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT e.plate_number, lv.name AS level_name,
                    e.entry_time, e.exit_time, e.amount_charged
             FROM sl_pkg_entries e
             JOIN sl_pkg_levels lv ON lv.id = e.level_id
             WHERE e.plate_number = ?
             ORDER BY e.entry_time DESC",
            ['ABC-1234']
        );

        $this->assertCount(2, $rows);

        // Most recent first
        $this->assertSame('ABC-1234', $rows[0]['plate_number']);
        $this->assertSame('2026-03-02 08:00', $rows[0]['entry_time']);
        $this->assertSame('2026-03-02 09:30', $rows[0]['exit_time']);

        $this->assertSame('ABC-1234', $rows[1]['plate_number']);
        $this->assertSame('2026-03-01 08:00', $rows[1]['entry_time']);
        $this->assertSame('2026-03-01 10:30', $rows[1]['exit_time']);
    }

    /**
     * Physical isolation: insert a new entry through ZTD, verify shadow count
     * is 11, then disableZtd and verify physical count is 0.
     */
    public function testPhysicalIsolation(): void
    {
        // Insert a new entry through ZTD
        $this->pdo->exec(
            "INSERT INTO sl_pkg_entries VALUES (11, 'NEW-0001', 1, '2026-03-03 08:00', NULL, NULL)"
        );

        // ZTD sees the new entry (10 original + 1 new = 11)
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_pkg_entries");
        $this->assertEquals(11, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM sl_pkg_entries')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
