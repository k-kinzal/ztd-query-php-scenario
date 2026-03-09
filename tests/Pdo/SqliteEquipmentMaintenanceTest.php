<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests an equipment maintenance scenario through ZTD shadow store (SQLite PDO).
 * Preventive maintenance scheduling, service history, technician workload,
 * cost tracking, and overdue detection exercise GROUP BY with COUNT,
 * platform-specific date arithmetic (JULIANDAY), LEFT JOIN with SUM,
 * correlated MAX subquery, ROUND(AVG), prepared statement with JOIN,
 * and physical isolation check.
 * @spec SPEC-10.2.158
 */
class SqliteEquipmentMaintenanceTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_em_equipment (
                id INTEGER PRIMARY KEY,
                name TEXT,
                category TEXT,
                location TEXT,
                last_service_date TEXT,
                service_interval_days INTEGER
            )',
            'CREATE TABLE sl_em_technicians (
                id INTEGER PRIMARY KEY,
                name TEXT,
                specialty TEXT
            )',
            'CREATE TABLE sl_em_service_records (
                id INTEGER PRIMARY KEY,
                equipment_id INTEGER,
                technician_id INTEGER,
                service_type TEXT,
                service_date TEXT,
                cost REAL,
                notes TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_em_service_records', 'sl_em_technicians', 'sl_em_equipment'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 5 pieces of equipment
        $this->pdo->exec("INSERT INTO sl_em_equipment VALUES (1, 'Rooftop HVAC Unit', 'HVAC', 'Building A', '2025-11-15', 90)");
        $this->pdo->exec("INSERT INTO sl_em_equipment VALUES (2, 'Main Electrical Panel', 'electrical', 'Building A', '2026-01-10', 180)");
        $this->pdo->exec("INSERT INTO sl_em_equipment VALUES (3, 'Lobby HVAC Unit', 'HVAC', 'Building B', '2025-08-20', 90)");
        $this->pdo->exec("INSERT INTO sl_em_equipment VALUES (4, 'Water Heater', 'plumbing', 'Building C', '2026-02-28', 365)");
        $this->pdo->exec("INSERT INTO sl_em_equipment VALUES (5, 'Emergency Generator', 'electrical', 'Building A', '2025-06-01', 180)");

        // 3 technicians
        $this->pdo->exec("INSERT INTO sl_em_technicians VALUES (1, 'Marco', 'HVAC')");
        $this->pdo->exec("INSERT INTO sl_em_technicians VALUES (2, 'Priya', 'electrical')");
        $this->pdo->exec("INSERT INTO sl_em_technicians VALUES (3, 'Tom', 'plumbing')");

        // 7 service records
        $this->pdo->exec("INSERT INTO sl_em_service_records VALUES (1, 1, 1, 'preventive', '2025-11-15', 350.00, 'Filter replacement')");
        $this->pdo->exec("INSERT INTO sl_em_service_records VALUES (2, 2, 2, 'inspection', '2026-01-10', 150.00, 'Annual safety check')");
        $this->pdo->exec("INSERT INTO sl_em_service_records VALUES (3, 3, 1, 'repair', '2025-08-20', 820.00, 'Compressor replacement')");
        $this->pdo->exec("INSERT INTO sl_em_service_records VALUES (4, 5, 2, 'preventive', '2025-06-01', 500.00, 'Load test and oil change')");
        $this->pdo->exec("INSERT INTO sl_em_service_records VALUES (5, 1, 1, 'preventive', '2025-08-10', 280.00, 'Coil cleaning')");
        $this->pdo->exec("INSERT INTO sl_em_service_records VALUES (6, 4, 3, 'inspection', '2026-02-28', 120.00, 'Pressure valve check')");
        $this->pdo->exec("INSERT INTO sl_em_service_records VALUES (7, 5, 2, 'repair', '2025-09-15', 1200.00, 'Starter motor replacement')");
    }

    /**
     * GROUP BY category with COUNT, ordered alphabetically.
     * Expected: HVAC=2, electrical=2, plumbing=1.
     */
    public function testEquipmentByCategory(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.category, COUNT(*) AS total
             FROM sl_em_equipment e
             GROUP BY e.category
             ORDER BY e.category"
        );

        $this->assertCount(3, $rows);

        $this->assertSame('HVAC', $rows[0]['category']);
        $this->assertEquals(2, (int) $rows[0]['total']);

        $this->assertSame('electrical', $rows[1]['category']);
        $this->assertEquals(2, (int) $rows[1]['total']);

        $this->assertSame('plumbing', $rows[2]['category']);
        $this->assertEquals(1, (int) $rows[2]['total']);
    }

    /**
     * Overdue equipment detection using JULIANDAY date arithmetic.
     * Equipment where days since last service exceeds service_interval_days
     * as of 2026-03-09.
     * Expected 3 rows: Emergency Generator, Lobby HVAC Unit, Rooftop HVAC Unit.
     */
    public function testOverdueEquipment(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name, e.category, e.last_service_date, e.service_interval_days
             FROM sl_em_equipment e
             WHERE CAST(JULIANDAY('2026-03-09') - JULIANDAY(e.last_service_date) AS INTEGER) > e.service_interval_days
             ORDER BY e.name"
        );

        $this->assertCount(3, $rows);

        $this->assertSame('Emergency Generator', $rows[0]['name']);
        $this->assertSame('electrical', $rows[0]['category']);
        $this->assertSame('2025-06-01', $rows[0]['last_service_date']);
        $this->assertEquals(180, (int) $rows[0]['service_interval_days']);

        $this->assertSame('Lobby HVAC Unit', $rows[1]['name']);
        $this->assertSame('HVAC', $rows[1]['category']);
        $this->assertSame('2025-08-20', $rows[1]['last_service_date']);
        $this->assertEquals(90, (int) $rows[1]['service_interval_days']);

        $this->assertSame('Rooftop HVAC Unit', $rows[2]['name']);
        $this->assertSame('HVAC', $rows[2]['category']);
        $this->assertSame('2025-11-15', $rows[2]['last_service_date']);
        $this->assertEquals(90, (int) $rows[2]['service_interval_days']);
    }

    /**
     * LEFT JOIN technicians to service_records, COUNT total jobs, SUM cost.
     * Expected: Marco=3/1450.00, Priya=3/1850.00, Tom=1/120.00.
     */
    public function testTechnicianWorkload(): void
    {
        $rows = $this->ztdQuery(
            "SELECT t.name, COUNT(sr.id) AS total_jobs, SUM(sr.cost) AS cost_total
             FROM sl_em_technicians t
             LEFT JOIN sl_em_service_records sr ON sr.technician_id = t.id
             GROUP BY t.id, t.name
             ORDER BY t.name"
        );

        $this->assertCount(3, $rows);

        $this->assertSame('Marco', $rows[0]['name']);
        $this->assertEquals(3, (int) $rows[0]['total_jobs']);
        $this->assertEquals(1450.00, (float) $rows[0]['cost_total']);

        $this->assertSame('Priya', $rows[1]['name']);
        $this->assertEquals(3, (int) $rows[1]['total_jobs']);
        $this->assertEquals(1850.00, (float) $rows[1]['cost_total']);

        $this->assertSame('Tom', $rows[2]['name']);
        $this->assertEquals(1, (int) $rows[2]['total_jobs']);
        $this->assertEquals(120.00, (float) $rows[2]['cost_total']);
    }

    /**
     * Correlated MAX subquery: latest service record per equipment.
     * Expected 5 rows with most recent service_date and service_type per equipment.
     */
    public function testMostRecentServicePerEquipment(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name, sr.service_date, sr.service_type
             FROM sl_em_equipment e
             JOIN sl_em_service_records sr ON sr.equipment_id = e.id
             WHERE sr.service_date = (
                 SELECT MAX(sr2.service_date)
                 FROM sl_em_service_records sr2
                 WHERE sr2.equipment_id = e.id
             )
             ORDER BY e.name"
        );

        $this->assertCount(5, $rows);

        $this->assertSame('Emergency Generator', $rows[0]['name']);
        $this->assertSame('2025-09-15', $rows[0]['service_date']);
        $this->assertSame('repair', $rows[0]['service_type']);

        $this->assertSame('Lobby HVAC Unit', $rows[1]['name']);
        $this->assertSame('2025-08-20', $rows[1]['service_date']);
        $this->assertSame('repair', $rows[1]['service_type']);

        $this->assertSame('Main Electrical Panel', $rows[2]['name']);
        $this->assertSame('2026-01-10', $rows[2]['service_date']);
        $this->assertSame('inspection', $rows[2]['service_type']);

        $this->assertSame('Rooftop HVAC Unit', $rows[3]['name']);
        $this->assertSame('2025-11-15', $rows[3]['service_date']);
        $this->assertSame('preventive', $rows[3]['service_type']);

        $this->assertSame('Water Heater', $rows[4]['name']);
        $this->assertSame('2026-02-28', $rows[4]['service_date']);
        $this->assertSame('inspection', $rows[4]['service_type']);
    }

    /**
     * Cost breakdown by equipment category: SUM, AVG with ROUND.
     * Expected: electrical=1850.00/616.67, HVAC=1450.00/483.33, plumbing=120.00/120.00.
     */
    public function testCostBreakdownByCategory(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.category, SUM(sr.cost) AS total_cost, ROUND(AVG(sr.cost), 2) AS avg_cost
             FROM sl_em_service_records sr
             JOIN sl_em_equipment e ON e.id = sr.equipment_id
             GROUP BY e.category
             ORDER BY e.category"
        );

        $this->assertCount(3, $rows);

        $this->assertSame('HVAC', $rows[0]['category']);
        $this->assertEquals(1450.00, (float) $rows[0]['total_cost']);
        $this->assertEquals(483.33, (float) $rows[0]['avg_cost']);

        $this->assertSame('electrical', $rows[1]['category']);
        $this->assertEquals(1850.00, (float) $rows[1]['total_cost']);
        $this->assertEquals(616.67, (float) $rows[1]['avg_cost']);

        $this->assertSame('plumbing', $rows[2]['category']);
        $this->assertEquals(120.00, (float) $rows[2]['total_cost']);
        $this->assertEquals(120.00, (float) $rows[2]['avg_cost']);
    }

    /**
     * Prepared statement: service history for a specific technician (technician_id=1, Marco).
     * JOIN with equipment table to get equipment name.
     * Expected 3 rows ordered by service_date.
     */
    public function testPreparedTechnicianHistory(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT e.name AS equipment_name, sr.service_type, sr.service_date, sr.cost
             FROM sl_em_service_records sr
             JOIN sl_em_equipment e ON e.id = sr.equipment_id
             WHERE sr.technician_id = ?
             ORDER BY sr.service_date",
            [1]
        );

        $this->assertCount(3, $rows);

        $this->assertSame('Rooftop HVAC Unit', $rows[0]['equipment_name']);
        $this->assertSame('preventive', $rows[0]['service_type']);
        $this->assertSame('2025-08-10', $rows[0]['service_date']);
        $this->assertEquals(280.00, (float) $rows[0]['cost']);

        $this->assertSame('Lobby HVAC Unit', $rows[1]['equipment_name']);
        $this->assertSame('repair', $rows[1]['service_type']);
        $this->assertSame('2025-08-20', $rows[1]['service_date']);
        $this->assertEquals(820.00, (float) $rows[1]['cost']);

        $this->assertSame('Rooftop HVAC Unit', $rows[2]['equipment_name']);
        $this->assertSame('preventive', $rows[2]['service_type']);
        $this->assertSame('2025-11-15', $rows[2]['service_date']);
        $this->assertEquals(350.00, (float) $rows[2]['cost']);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        // Insert new equipment via shadow
        $this->pdo->exec("INSERT INTO sl_em_equipment VALUES (6, 'Backup AC Unit', 'HVAC', 'Building D', '2026-03-01', 90)");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_em_equipment");
        $this->assertEquals(6, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_em_equipment")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
