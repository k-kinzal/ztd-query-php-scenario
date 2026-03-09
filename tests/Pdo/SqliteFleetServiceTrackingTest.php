<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests a fleet service tracking scenario through ZTD shadow store (SQLite PDO).
 * Covers MAX/MIN date aggregation, overdue service detection with date comparison,
 * SUM cost analysis, and LAG window function for consecutive service comparisons.
 * @spec SPEC-10.2.130
 */
class SqliteFleetServiceTrackingTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_fs_vehicles (
                id INTEGER PRIMARY KEY,
                plate TEXT,
                make TEXT,
                model TEXT,
                year_made INTEGER,
                status TEXT,
                service_interval_months INTEGER
            )',
            'CREATE TABLE sl_fs_service_records (
                id INTEGER PRIMARY KEY,
                vehicle_id INTEGER,
                service_date TEXT,
                service_type TEXT,
                cost REAL,
                mileage INTEGER,
                notes TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_fs_service_records', 'sl_fs_vehicles'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Vehicles
        $this->pdo->exec("INSERT INTO sl_fs_vehicles VALUES (1, 'ABC-001', 'Toyota', 'Camry', 2022, 'active', 6)");
        $this->pdo->exec("INSERT INTO sl_fs_vehicles VALUES (2, 'DEF-002', 'Honda', 'Civic', 2021, 'active', 6)");
        $this->pdo->exec("INSERT INTO sl_fs_vehicles VALUES (3, 'GHI-003', 'Ford', 'F-150', 2020, 'active', 3)");
        $this->pdo->exec("INSERT INTO sl_fs_vehicles VALUES (4, 'JKL-004', 'Tesla', 'Model 3', 2023, 'inactive', 12)");
        $this->pdo->exec("INSERT INTO sl_fs_vehicles VALUES (5, 'MNO-005', 'Chevy', 'Malibu', 2019, 'active', 6)");

        // Vehicle 1 service records (regular maintenance)
        $this->pdo->exec("INSERT INTO sl_fs_service_records VALUES (1, 1, '2025-01-15', 'oil_change', 75.00, 15000, 'Regular service')");
        $this->pdo->exec("INSERT INTO sl_fs_service_records VALUES (2, 1, '2025-04-20', 'oil_change', 80.00, 18000, 'Regular service')");
        $this->pdo->exec("INSERT INTO sl_fs_service_records VALUES (3, 1, '2025-07-10', 'oil_change', 75.00, 21000, 'Regular service')");
        $this->pdo->exec("INSERT INTO sl_fs_service_records VALUES (4, 1, '2025-07-10', 'tire_rotation', 50.00, 21000, 'With oil change')");

        // Vehicle 2 service records (less frequent)
        $this->pdo->exec("INSERT INTO sl_fs_service_records VALUES (5, 2, '2025-02-01', 'oil_change', 70.00, 22000, 'Regular service')");
        $this->pdo->exec("INSERT INTO sl_fs_service_records VALUES (6, 2, '2025-06-15', 'brake_service', 350.00, 25000, 'Front brakes replaced')");
        $this->pdo->exec("INSERT INTO sl_fs_service_records VALUES (7, 2, '2025-08-20', 'oil_change', 75.00, 27000, 'Regular service')");

        // Vehicle 3 service records (expensive, frequent)
        $this->pdo->exec("INSERT INTO sl_fs_service_records VALUES (8, 3, '2025-01-05', 'oil_change', 95.00, 45000, 'Synthetic oil')");
        $this->pdo->exec("INSERT INTO sl_fs_service_records VALUES (9, 3, '2025-03-10', 'oil_change', 95.00, 48000, 'Synthetic oil')");
        $this->pdo->exec("INSERT INTO sl_fs_service_records VALUES (10, 3, '2025-05-15', 'transmission', 1200.00, 51000, 'Transmission flush')");
        $this->pdo->exec("INSERT INTO sl_fs_service_records VALUES (11, 3, '2025-06-20', 'oil_change', 95.00, 53000, 'Synthetic oil')");
        $this->pdo->exec("INSERT INTO sl_fs_service_records VALUES (12, 3, '2025-09-01', 'oil_change', 95.00, 56000, 'Synthetic oil')");

        // Vehicle 4 service records (inactive, only 1 record)
        $this->pdo->exec("INSERT INTO sl_fs_service_records VALUES (13, 4, '2025-03-01', 'inspection', 150.00, 5000, 'Annual inspection')");

        // Vehicle 5 service records (overdue)
        $this->pdo->exec("INSERT INTO sl_fs_service_records VALUES (14, 5, '2024-06-10', 'oil_change', 65.00, 60000, 'Regular service')");
        $this->pdo->exec("INSERT INTO sl_fs_service_records VALUES (15, 5, '2024-12-15', 'oil_change', 70.00, 63000, 'Regular service')");
    }

    /**
     * Get most recent service date and mileage per vehicle via MAX aggregation.
     */
    public function testLastServicePerVehicle(): void
    {
        $rows = $this->ztdQuery(
            "SELECT v.plate, v.make, v.model,
                    MAX(sr.service_date) AS last_service,
                    MAX(sr.mileage) AS last_mileage
             FROM sl_fs_vehicles v
             JOIN sl_fs_service_records sr ON sr.vehicle_id = v.id
             GROUP BY v.id, v.plate, v.make, v.model
             ORDER BY v.plate"
        );

        $this->assertCount(5, $rows);

        // ABC-001
        $this->assertSame('ABC-001', $rows[0]['plate']);
        $this->assertSame('2025-07-10', $rows[0]['last_service']);
        $this->assertEquals(21000, (int) $rows[0]['last_mileage']);

        // DEF-002
        $this->assertSame('DEF-002', $rows[1]['plate']);
        $this->assertSame('2025-08-20', $rows[1]['last_service']);
        $this->assertEquals(27000, (int) $rows[1]['last_mileage']);

        // GHI-003
        $this->assertSame('GHI-003', $rows[2]['plate']);
        $this->assertSame('2025-09-01', $rows[2]['last_service']);
        $this->assertEquals(56000, (int) $rows[2]['last_mileage']);

        // JKL-004
        $this->assertSame('JKL-004', $rows[3]['plate']);
        $this->assertSame('2025-03-01', $rows[3]['last_service']);
        $this->assertEquals(5000, (int) $rows[3]['last_mileage']);

        // MNO-005
        $this->assertSame('MNO-005', $rows[4]['plate']);
        $this->assertSame('2024-12-15', $rows[4]['last_service']);
        $this->assertEquals(63000, (int) $rows[4]['last_mileage']);
    }

    /**
     * Find active vehicles whose last service is overdue using date comparison in HAVING.
     */
    public function testOverdueServices(): void
    {
        $rows = $this->ztdQuery(
            "SELECT v.plate, v.make, v.model, v.service_interval_months,
                    MAX(sr.service_date) AS last_service
             FROM sl_fs_vehicles v
             JOIN sl_fs_service_records sr ON sr.vehicle_id = v.id
             WHERE v.status = 'active'
             GROUP BY v.id, v.plate, v.make, v.model, v.service_interval_months
             HAVING MAX(sr.service_date) < '2025-04-15'
             ORDER BY last_service"
        );

        // Only MNO-005 (last service 2024-12-15) is before cutoff 2025-04-15
        $this->assertCount(1, $rows);
        $this->assertSame('MNO-005', $rows[0]['plate']);
        $this->assertSame('Chevy', $rows[0]['make']);
        $this->assertSame('Malibu', $rows[0]['model']);
        $this->assertEquals(6, (int) $rows[0]['service_interval_months']);
        $this->assertSame('2024-12-15', $rows[0]['last_service']);
    }

    /**
     * Total service cost and count per vehicle with average cost.
     */
    public function testServiceCostPerVehicle(): void
    {
        $rows = $this->ztdQuery(
            "SELECT v.plate, v.make,
                    COUNT(sr.id) AS service_count,
                    SUM(sr.cost) AS total_cost,
                    ROUND(SUM(sr.cost) / COUNT(sr.id), 2) AS avg_cost
             FROM sl_fs_vehicles v
             JOIN sl_fs_service_records sr ON sr.vehicle_id = v.id
             GROUP BY v.id, v.plate, v.make
             ORDER BY total_cost DESC"
        );

        $this->assertCount(5, $rows);

        // GHI-003: 5 services, total=1580.00, avg=316.00
        $this->assertSame('GHI-003', $rows[0]['plate']);
        $this->assertSame('Ford', $rows[0]['make']);
        $this->assertEquals(5, (int) $rows[0]['service_count']);
        $this->assertEqualsWithDelta(1580.00, (float) $rows[0]['total_cost'], 0.01);
        $this->assertEqualsWithDelta(316.00, (float) $rows[0]['avg_cost'], 0.01);

        // DEF-002: 3 services, total=495.00, avg=165.00
        $this->assertSame('DEF-002', $rows[1]['plate']);
        $this->assertSame('Honda', $rows[1]['make']);
        $this->assertEquals(3, (int) $rows[1]['service_count']);
        $this->assertEqualsWithDelta(495.00, (float) $rows[1]['total_cost'], 0.01);
        $this->assertEqualsWithDelta(165.00, (float) $rows[1]['avg_cost'], 0.01);

        // ABC-001: 4 services, total=280.00, avg=70.00
        $this->assertSame('ABC-001', $rows[2]['plate']);
        $this->assertSame('Toyota', $rows[2]['make']);
        $this->assertEquals(4, (int) $rows[2]['service_count']);
        $this->assertEqualsWithDelta(280.00, (float) $rows[2]['total_cost'], 0.01);
        $this->assertEqualsWithDelta(70.00, (float) $rows[2]['avg_cost'], 0.01);

        // JKL-004: 1 service, total=150.00, avg=150.00
        $this->assertSame('JKL-004', $rows[3]['plate']);
        $this->assertSame('Tesla', $rows[3]['make']);
        $this->assertEquals(1, (int) $rows[3]['service_count']);
        $this->assertEqualsWithDelta(150.00, (float) $rows[3]['total_cost'], 0.01);
        $this->assertEqualsWithDelta(150.00, (float) $rows[3]['avg_cost'], 0.01);

        // MNO-005: 2 services, total=135.00, avg=67.50
        $this->assertSame('MNO-005', $rows[4]['plate']);
        $this->assertSame('Chevy', $rows[4]['make']);
        $this->assertEquals(2, (int) $rows[4]['service_count']);
        $this->assertEqualsWithDelta(135.00, (float) $rows[4]['total_cost'], 0.01);
        $this->assertEqualsWithDelta(67.50, (float) $rows[4]['avg_cost'], 0.01);
    }

    /**
     * Aggregate by service type across all vehicles.
     */
    public function testServiceTypeSummary(): void
    {
        $rows = $this->ztdQuery(
            "SELECT service_type,
                    COUNT(*) AS occurrences,
                    SUM(cost) AS total_cost,
                    ROUND(AVG(cost), 2) AS avg_cost
             FROM sl_fs_service_records
             GROUP BY service_type
             ORDER BY total_cost DESC"
        );

        $this->assertCount(5, $rows);

        // transmission: 1, 1200.00, 1200.00
        $this->assertSame('transmission', $rows[0]['service_type']);
        $this->assertEquals(1, (int) $rows[0]['occurrences']);
        $this->assertEqualsWithDelta(1200.00, (float) $rows[0]['total_cost'], 0.01);
        $this->assertEqualsWithDelta(1200.00, (float) $rows[0]['avg_cost'], 0.01);

        // oil_change: 11, 890.00, 80.91
        $this->assertSame('oil_change', $rows[1]['service_type']);
        $this->assertEquals(11, (int) $rows[1]['occurrences']);
        $this->assertEqualsWithDelta(890.00, (float) $rows[1]['total_cost'], 0.01);
        $this->assertEqualsWithDelta(80.91, (float) $rows[1]['avg_cost'], 0.01);

        // brake_service: 1, 350.00, 350.00
        $this->assertSame('brake_service', $rows[2]['service_type']);
        $this->assertEquals(1, (int) $rows[2]['occurrences']);
        $this->assertEqualsWithDelta(350.00, (float) $rows[2]['total_cost'], 0.01);
        $this->assertEqualsWithDelta(350.00, (float) $rows[2]['avg_cost'], 0.01);

        // inspection: 1, 150.00, 150.00
        $this->assertSame('inspection', $rows[3]['service_type']);
        $this->assertEquals(1, (int) $rows[3]['occurrences']);
        $this->assertEqualsWithDelta(150.00, (float) $rows[3]['total_cost'], 0.01);
        $this->assertEqualsWithDelta(150.00, (float) $rows[3]['avg_cost'], 0.01);

        // tire_rotation: 1, 50.00, 50.00
        $this->assertSame('tire_rotation', $rows[4]['service_type']);
        $this->assertEquals(1, (int) $rows[4]['occurrences']);
        $this->assertEqualsWithDelta(50.00, (float) $rows[4]['total_cost'], 0.01);
        $this->assertEqualsWithDelta(50.00, (float) $rows[4]['avg_cost'], 0.01);
    }

    /**
     * Use LAG window function to calculate mileage between consecutive services per vehicle.
     */
    public function testMileageBetweenServices(): void
    {
        $rows = $this->ztdQuery(
            "SELECT v.plate, sr.service_date, sr.mileage,
                    LAG(sr.mileage) OVER (PARTITION BY v.id ORDER BY sr.service_date, sr.id) AS prev_mileage,
                    sr.mileage - LAG(sr.mileage) OVER (PARTITION BY v.id ORDER BY sr.service_date, sr.id) AS miles_since_last
             FROM sl_fs_vehicles v
             JOIN sl_fs_service_records sr ON sr.vehicle_id = v.id
             ORDER BY v.plate, sr.service_date, sr.id"
        );

        $this->assertCount(15, $rows);

        // ABC-001 rows
        $this->assertSame('ABC-001', $rows[0]['plate']);
        $this->assertSame('2025-01-15', $rows[0]['service_date']);
        $this->assertEquals(15000, (int) $rows[0]['mileage']);
        $this->assertNull($rows[0]['prev_mileage']);
        $this->assertNull($rows[0]['miles_since_last']);

        $this->assertSame('ABC-001', $rows[1]['plate']);
        $this->assertSame('2025-04-20', $rows[1]['service_date']);
        $this->assertEquals(18000, (int) $rows[1]['mileage']);
        $this->assertEquals(15000, (int) $rows[1]['prev_mileage']);
        $this->assertEquals(3000, (int) $rows[1]['miles_since_last']);

        $this->assertSame('ABC-001', $rows[2]['plate']);
        $this->assertSame('2025-07-10', $rows[2]['service_date']);
        $this->assertEquals(21000, (int) $rows[2]['mileage']);
        $this->assertEquals(18000, (int) $rows[2]['prev_mileage']);
        $this->assertEquals(3000, (int) $rows[2]['miles_since_last']);

        // Same date, tire_rotation - prev is same mileage
        $this->assertSame('ABC-001', $rows[3]['plate']);
        $this->assertSame('2025-07-10', $rows[3]['service_date']);
        $this->assertEquals(21000, (int) $rows[3]['mileage']);
        $this->assertEquals(21000, (int) $rows[3]['prev_mileage']);
        $this->assertEquals(0, (int) $rows[3]['miles_since_last']);

        // MNO-005 rows (last two in result set)
        $this->assertSame('MNO-005', $rows[13]['plate']);
        $this->assertSame('2024-06-10', $rows[13]['service_date']);
        $this->assertEquals(60000, (int) $rows[13]['mileage']);
        $this->assertNull($rows[13]['prev_mileage']);
        $this->assertNull($rows[13]['miles_since_last']);

        $this->assertSame('MNO-005', $rows[14]['plate']);
        $this->assertSame('2024-12-15', $rows[14]['service_date']);
        $this->assertEquals(63000, (int) $rows[14]['mileage']);
        $this->assertEquals(60000, (int) $rows[14]['prev_mileage']);
        $this->assertEquals(3000, (int) $rows[14]['miles_since_last']);
    }

    /**
     * Summary stats for active vehicles only.
     */
    public function testActiveFleetSummary(): void
    {
        $rows = $this->ztdQuery(
            "SELECT COUNT(DISTINCT v.id) AS active_vehicles,
                    SUM(sr.cost) AS total_maintenance_cost,
                    MAX(sr.mileage) AS highest_mileage,
                    MIN(sr.mileage) AS lowest_mileage
             FROM sl_fs_vehicles v
             JOIN sl_fs_service_records sr ON sr.vehicle_id = v.id
             WHERE v.status = 'active'"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(4, (int) $rows[0]['active_vehicles']);
        $this->assertEqualsWithDelta(2490.00, (float) $rows[0]['total_maintenance_cost'], 0.01);
        $this->assertEquals(63000, (int) $rows[0]['highest_mileage']);
        $this->assertEquals(15000, (int) $rows[0]['lowest_mileage']);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO sl_fs_service_records VALUES (16, 1, '2025-10-01', 'oil_change', 80.00, 24000, 'New service')");
        $this->pdo->exec("UPDATE sl_fs_vehicles SET status = 'maintenance' WHERE id = 1");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_fs_service_records");
        $this->assertEquals(16, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM sl_fs_service_records')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
