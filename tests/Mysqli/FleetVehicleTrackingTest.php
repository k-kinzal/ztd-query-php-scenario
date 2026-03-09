<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests fleet vehicle tracking with trip logging and mileage accrual (MySQLi).
 * Table names deliberately overlap: "vehicle" is a prefix of "vehicle_type" and "vehicle_trip",
 * exercising the CTE rewriter's table reference detection (stripos matching).
 * SQL patterns exercised: JOIN across prefix-overlapping tables, self-referencing UPDATE
 * arithmetic (mileage += distance), GROUP BY SUM, prepared BETWEEN date range, COUNT(DISTINCT).
 * @spec SPEC-10.2.170
 */
class FleetVehicleTrackingTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_fvt_vehicle_type (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(50),
                fuel_type VARCHAR(20),
                capacity INT
            )',
            'CREATE TABLE mi_fvt_vehicle (
                id INT AUTO_INCREMENT PRIMARY KEY,
                type_id INT,
                plate VARCHAR(20),
                mileage DECIMAL(10,1),
                status VARCHAR(20)
            )',
            'CREATE TABLE mi_fvt_vehicle_trip (
                id INT AUTO_INCREMENT PRIMARY KEY,
                vehicle_id INT,
                trip_date TEXT,
                distance DECIMAL(8,1),
                driver VARCHAR(100),
                destination VARCHAR(200)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_fvt_vehicle_trip', 'mi_fvt_vehicle', 'mi_fvt_vehicle_type'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Vehicle types
        $this->mysqli->query("INSERT INTO mi_fvt_vehicle_type VALUES (1, 'Sedan', 'gasoline', 4)");
        $this->mysqli->query("INSERT INTO mi_fvt_vehicle_type VALUES (2, 'Van', 'diesel', 8)");
        $this->mysqli->query("INSERT INTO mi_fvt_vehicle_type VALUES (3, 'Truck', 'diesel', 2)");

        // Vehicles — "vehicle" is prefix of "vehicle_type" and "vehicle_trip"
        $this->mysqli->query("INSERT INTO mi_fvt_vehicle VALUES (1, 1, 'ABC-1234', 15000.0, 'active')");
        $this->mysqli->query("INSERT INTO mi_fvt_vehicle VALUES (2, 1, 'DEF-5678', 42000.0, 'active')");
        $this->mysqli->query("INSERT INTO mi_fvt_vehicle VALUES (3, 2, 'GHI-9012', 8500.0, 'active')");
        $this->mysqli->query("INSERT INTO mi_fvt_vehicle VALUES (4, 3, 'JKL-3456', 62000.0, 'maintenance')");

        // Trips
        $this->mysqli->query("INSERT INTO mi_fvt_vehicle_trip VALUES (1, 1, '2025-09-01', 120.5, 'Alice', 'Warehouse A')");
        $this->mysqli->query("INSERT INTO mi_fvt_vehicle_trip VALUES (2, 1, '2025-09-03', 85.0, 'Alice', 'Client B')");
        $this->mysqli->query("INSERT INTO mi_fvt_vehicle_trip VALUES (3, 2, '2025-09-01', 200.0, 'Bob', 'Depot C')");
        $this->mysqli->query("INSERT INTO mi_fvt_vehicle_trip VALUES (4, 3, '2025-09-02', 310.0, 'Carol', 'Distribution Hub')");
        $this->mysqli->query("INSERT INTO mi_fvt_vehicle_trip VALUES (5, 3, '2025-09-04', 150.0, 'Carol', 'Client D')");
        $this->mysqli->query("INSERT INTO mi_fvt_vehicle_trip VALUES (6, 1, '2025-09-05', 95.0, 'Dave', 'Airport')");
    }

    /**
     * 3-table JOIN across prefix-overlapping table names.
     * Verifies CTE rewriter correctly distinguishes "vehicle" from "vehicle_type" and "vehicle_trip".
     */
    public function testThreeTableJoinWithOverlappingNames(): void
    {
        $rows = $this->ztdQuery(
            "SELECT v.plate, vt.name AS type_name, t.trip_date, t.distance, t.driver
             FROM mi_fvt_vehicle v
             JOIN mi_fvt_vehicle_type vt ON vt.id = v.type_id
             JOIN mi_fvt_vehicle_trip t ON t.vehicle_id = v.id
             ORDER BY t.id"
        );

        $this->assertCount(6, $rows);

        $this->assertSame('ABC-1234', $rows[0]['plate']);
        $this->assertSame('Sedan', $rows[0]['type_name']);
        $this->assertSame('Alice', $rows[0]['driver']);

        $this->assertSame('GHI-9012', $rows[3]['plate']);
        $this->assertSame('Van', $rows[3]['type_name']);
        $this->assertSame('Carol', $rows[3]['driver']);
    }

    /**
     * GROUP BY + SUM total distance per vehicle, with LEFT JOIN for vehicle with no trips.
     * Vehicle 1: 300.5, Vehicle 2: 200.0, Vehicle 3: 460.0, Vehicle 4: 0 (maintenance, no trips).
     */
    public function testTotalDistancePerVehicle(): void
    {
        $rows = $this->ztdQuery(
            "SELECT v.plate,
                    COALESCE(SUM(t.distance), 0) AS total_distance,
                    COUNT(t.id) AS trip_count
             FROM mi_fvt_vehicle v
             LEFT JOIN mi_fvt_vehicle_trip t ON t.vehicle_id = v.id
             GROUP BY v.id, v.plate
             ORDER BY v.id"
        );

        $this->assertCount(4, $rows);

        $this->assertSame('ABC-1234', $rows[0]['plate']);
        $this->assertEqualsWithDelta(300.5, (float) $rows[0]['total_distance'], 0.1);
        $this->assertEquals(3, (int) $rows[0]['trip_count']);

        $this->assertSame('DEF-5678', $rows[1]['plate']);
        $this->assertEqualsWithDelta(200.0, (float) $rows[1]['total_distance'], 0.1);
        $this->assertEquals(1, (int) $rows[1]['trip_count']);

        $this->assertSame('GHI-9012', $rows[2]['plate']);
        $this->assertEqualsWithDelta(460.0, (float) $rows[2]['total_distance'], 0.1);
        $this->assertEquals(2, (int) $rows[2]['trip_count']);

        $this->assertSame('JKL-3456', $rows[3]['plate']);
        $this->assertEqualsWithDelta(0.0, (float) $rows[3]['total_distance'], 0.1);
        $this->assertEquals(0, (int) $rows[3]['trip_count']);
    }

    /**
     * GROUP BY vehicle_type with COUNT(DISTINCT vehicle_id) and SUM distance.
     * Sedan: 2 vehicles, 500.5 total; Van: 1 vehicle, 460.0; Truck: 0 trips.
     */
    public function testFleetSummaryByType(): void
    {
        $rows = $this->ztdQuery(
            "SELECT vt.name AS type_name,
                    COUNT(DISTINCT t.vehicle_id) AS active_vehicles,
                    COALESCE(SUM(t.distance), 0) AS total_distance
             FROM mi_fvt_vehicle_type vt
             LEFT JOIN mi_fvt_vehicle v ON v.type_id = vt.id
             LEFT JOIN mi_fvt_vehicle_trip t ON t.vehicle_id = v.id
             GROUP BY vt.id, vt.name
             ORDER BY vt.name"
        );

        $this->assertCount(3, $rows);

        $this->assertSame('Sedan', $rows[0]['type_name']);
        $this->assertEquals(2, (int) $rows[0]['active_vehicles']);
        $this->assertEqualsWithDelta(500.5, (float) $rows[0]['total_distance'], 0.1);

        $this->assertSame('Truck', $rows[1]['type_name']);
        $this->assertEquals(0, (int) $rows[1]['active_vehicles']);
        $this->assertEqualsWithDelta(0.0, (float) $rows[1]['total_distance'], 0.1);

        $this->assertSame('Van', $rows[2]['type_name']);
        $this->assertEquals(1, (int) $rows[2]['active_vehicles']);
        $this->assertEqualsWithDelta(460.0, (float) $rows[2]['total_distance'], 0.1);
    }

    /**
     * Self-referencing UPDATE: add trip distance to vehicle mileage.
     * Vehicle 1 starts at 15000.0, after adding 50.0 trip => 15050.0.
     */
    public function testMileageAccrualSelfRefUpdate(): void
    {
        // Add a new trip
        $this->mysqli->query("INSERT INTO mi_fvt_vehicle_trip VALUES (7, 1, '2025-09-06', 50.0, 'Alice', 'Office')");

        // Self-referencing mileage update
        $this->ztdExec("UPDATE mi_fvt_vehicle SET mileage = mileage + 50.0 WHERE id = 1");

        $rows = $this->ztdQuery("SELECT mileage FROM mi_fvt_vehicle WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertEqualsWithDelta(15050.0, (float) $rows[0]['mileage'], 0.1);
    }

    /**
     * Chained self-referencing UPDATEs: two sequential mileage increments.
     * Vehicle 2 starts at 42000.0, +200 +85 => 42285.0.
     */
    public function testChainedMileageUpdates(): void
    {
        $this->ztdExec("UPDATE mi_fvt_vehicle SET mileage = mileage + 200.0 WHERE id = 2");
        $this->ztdExec("UPDATE mi_fvt_vehicle SET mileage = mileage + 85.0 WHERE id = 2");

        $rows = $this->ztdQuery("SELECT mileage FROM mi_fvt_vehicle WHERE id = 2");
        $this->assertCount(1, $rows);
        $this->assertEqualsWithDelta(42285.0, (float) $rows[0]['mileage'], 0.1);
    }

    /**
     * Prepared statement: trips within a date range.
     * 2025-09-01 to 2025-09-03 => 4 trips (Alice 09-01, Bob 09-01, Carol 09-02, Alice 09-03).
     */
    public function testPreparedTripsByDateRange(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT t.trip_date, t.driver, t.destination, v.plate
             FROM mi_fvt_vehicle_trip t
             JOIN mi_fvt_vehicle v ON v.id = t.vehicle_id
             WHERE t.trip_date BETWEEN ? AND ?
             ORDER BY t.trip_date, t.id",
            ['2025-09-01', '2025-09-03']
        );

        $this->assertCount(4, $rows);
        $this->assertSame('Alice', $rows[0]['driver']);
        $this->assertSame('Bob', $rows[1]['driver']);
        $this->assertSame('Carol', $rows[2]['driver']);
        $this->assertSame('Alice', $rows[3]['driver']);
    }

    /**
     * Query only "vehicle" table when "vehicle_type" and "vehicle_trip" also have shadow data.
     * CTE rewriter must not confuse prefix-overlapping table names.
     */
    public function testSelectVehicleOnlyNoConfusion(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, plate, mileage, status
             FROM mi_fvt_vehicle
             WHERE status = 'active'
             ORDER BY id"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('ABC-1234', $rows[0]['plate']);
        $this->assertSame('DEF-5678', $rows[1]['plate']);
        $this->assertSame('GHI-9012', $rows[2]['plate']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_fvt_vehicle VALUES (5, 2, 'MNO-7890', 0.0, 'new')");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_fvt_vehicle");
        $this->assertEquals(5, (int) $rows[0]['cnt']);

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_fvt_vehicle');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
