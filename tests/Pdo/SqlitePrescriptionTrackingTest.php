<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests a prescription tracking system through ZTD shadow store (SQLite PDO).
 * Covers 4-table JOIN (patients, doctors, visits, prescriptions), refill count
 * tracking with self-referencing UPDATE arithmetic, active prescription detection
 * with date BETWEEN, prescriptions-per-doctor aggregation, and physical isolation.
 * @spec SPEC-10.2.139
 */
class SqlitePrescriptionTrackingTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_pt_patients (
                id INTEGER PRIMARY KEY,
                name TEXT,
                date_of_birth TEXT
            )',
            'CREATE TABLE sl_pt_doctors (
                id INTEGER PRIMARY KEY,
                name TEXT,
                specialty TEXT
            )',
            'CREATE TABLE sl_pt_visits (
                id INTEGER PRIMARY KEY,
                patient_id INTEGER,
                doctor_id INTEGER,
                visit_date TEXT,
                diagnosis TEXT
            )',
            'CREATE TABLE sl_pt_prescriptions (
                id INTEGER PRIMARY KEY,
                visit_id INTEGER,
                drug_name TEXT,
                dosage TEXT,
                refills_remaining INTEGER,
                start_date TEXT,
                end_date TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_pt_prescriptions', 'sl_pt_visits', 'sl_pt_doctors', 'sl_pt_patients'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 3 patients
        $this->pdo->exec("INSERT INTO sl_pt_patients VALUES (1, 'John Doe', '1985-03-15')");
        $this->pdo->exec("INSERT INTO sl_pt_patients VALUES (2, 'Jane Smith', '1990-07-22')");
        $this->pdo->exec("INSERT INTO sl_pt_patients VALUES (3, 'Bob Wilson', '1978-11-30')");

        // 3 doctors
        $this->pdo->exec("INSERT INTO sl_pt_doctors VALUES (1, 'Dr. Adams', 'General')");
        $this->pdo->exec("INSERT INTO sl_pt_doctors VALUES (2, 'Dr. Baker', 'Cardiology')");
        $this->pdo->exec("INSERT INTO sl_pt_doctors VALUES (3, 'Dr. Clark', 'Neurology')");

        // 5 visits
        $this->pdo->exec("INSERT INTO sl_pt_visits VALUES (1, 1, 1, '2025-06-01', 'Hypertension')");
        $this->pdo->exec("INSERT INTO sl_pt_visits VALUES (2, 1, 2, '2025-06-15', 'Heart checkup')");
        $this->pdo->exec("INSERT INTO sl_pt_visits VALUES (3, 2, 1, '2025-07-01', 'Annual physical')");
        $this->pdo->exec("INSERT INTO sl_pt_visits VALUES (4, 2, 3, '2025-07-10', 'Migraine')");
        $this->pdo->exec("INSERT INTO sl_pt_visits VALUES (5, 3, 1, '2025-08-01', 'Flu')");

        // 6 prescriptions
        $this->pdo->exec("INSERT INTO sl_pt_prescriptions VALUES (1, 1, 'Lisinopril', '10mg', 3, '2025-06-01', '2025-12-01')");
        $this->pdo->exec("INSERT INTO sl_pt_prescriptions VALUES (2, 1, 'Aspirin', '81mg', 6, '2025-06-01', '2025-12-01')");
        $this->pdo->exec("INSERT INTO sl_pt_prescriptions VALUES (3, 2, 'Metoprolol', '25mg', 2, '2025-06-15', '2025-09-15')");
        $this->pdo->exec("INSERT INTO sl_pt_prescriptions VALUES (4, 3, 'Vitamin D', '1000IU', 12, '2025-07-01', '2026-07-01')");
        $this->pdo->exec("INSERT INTO sl_pt_prescriptions VALUES (5, 4, 'Sumatriptan', '50mg', 4, '2025-07-10', '2026-01-10')");
        $this->pdo->exec("INSERT INTO sl_pt_prescriptions VALUES (6, 5, 'Oseltamivir', '75mg', 0, '2025-08-01', '2025-08-06')");
    }

    /**
     * 4-table JOIN: patients -> visits -> prescriptions -> doctors.
     * @spec SPEC-10.2.139
     */
    public function testPatientPrescriptionSummary(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.name AS patient_name, rx.drug_name, rx.dosage,
                    d.name AS doctor_name, v.visit_date
             FROM sl_pt_patients p
             JOIN sl_pt_visits v ON v.patient_id = p.id
             JOIN sl_pt_prescriptions rx ON rx.visit_id = v.id
             JOIN sl_pt_doctors d ON d.id = v.doctor_id
             ORDER BY p.name, v.visit_date, rx.drug_name"
        );

        $this->assertCount(6, $rows);

        // Bob Wilson: 1 prescription (Oseltamivir from Dr. Adams)
        $bob = array_values(array_filter($rows, fn($r) => $r['patient_name'] === 'Bob Wilson'));
        $this->assertCount(1, $bob);
        $this->assertSame('Oseltamivir', $bob[0]['drug_name']);
        $this->assertSame('Dr. Adams', $bob[0]['doctor_name']);

        // Jane Smith: 2 prescriptions (Vitamin D from Dr. Adams, Sumatriptan from Dr. Clark)
        $jane = array_values(array_filter($rows, fn($r) => $r['patient_name'] === 'Jane Smith'));
        $this->assertCount(2, $jane);
        $this->assertSame('Vitamin D', $jane[0]['drug_name']);
        $this->assertSame('Dr. Adams', $jane[0]['doctor_name']);
        $this->assertSame('Sumatriptan', $jane[1]['drug_name']);
        $this->assertSame('Dr. Clark', $jane[1]['doctor_name']);

        // John Doe: 3 prescriptions (Aspirin + Lisinopril from Dr. Adams, Metoprolol from Dr. Baker)
        $john = array_values(array_filter($rows, fn($r) => $r['patient_name'] === 'John Doe'));
        $this->assertCount(3, $john);
        $this->assertSame('Aspirin', $john[0]['drug_name']);
        $this->assertSame('Dr. Adams', $john[0]['doctor_name']);
        $this->assertSame('Lisinopril', $john[1]['drug_name']);
        $this->assertSame('Dr. Adams', $john[1]['doctor_name']);
        $this->assertSame('Metoprolol', $john[2]['drug_name']);
        $this->assertSame('Dr. Baker', $john[2]['doctor_name']);
    }

    /**
     * Self-referencing UPDATE: decrement refills_remaining by 1.
     * @spec SPEC-10.2.139
     */
    public function testRefillTracking(): void
    {
        // Lisinopril (id=1) starts with 3 refills
        $rows = $this->ztdQuery(
            "SELECT refills_remaining FROM sl_pt_prescriptions WHERE id = 1"
        );
        $this->assertEquals(3, (int) $rows[0]['refills_remaining']);

        // Decrement by 1
        $this->pdo->exec("UPDATE sl_pt_prescriptions SET refills_remaining = refills_remaining - 1 WHERE id = 1");

        $rows = $this->ztdQuery(
            "SELECT refills_remaining FROM sl_pt_prescriptions WHERE id = 1"
        );
        $this->assertEquals(2, (int) $rows[0]['refills_remaining']);

        // Decrement again
        $this->pdo->exec("UPDATE sl_pt_prescriptions SET refills_remaining = refills_remaining - 1 WHERE id = 1");

        $rows = $this->ztdQuery(
            "SELECT refills_remaining FROM sl_pt_prescriptions WHERE id = 1"
        );
        $this->assertEquals(1, (int) $rows[0]['refills_remaining']);
    }

    /**
     * Date BETWEEN: find active prescriptions on a given date.
     * @spec SPEC-10.2.139
     */
    public function testActivePrescriptions(): void
    {
        // All 6 prescriptions are active on 2025-08-01
        $rows = $this->ztdQuery(
            "SELECT rx.drug_name, rx.start_date, rx.end_date
             FROM sl_pt_prescriptions rx
             WHERE '2025-08-01' BETWEEN rx.start_date AND rx.end_date
             ORDER BY rx.drug_name"
        );
        $this->assertCount(6, $rows);

        // On 2025-10-01: Lisinopril, Aspirin, Vitamin D, Sumatriptan = 4 active
        // (Metoprolol ends 2025-09-15, Oseltamivir ends 2025-08-06)
        $rows = $this->ztdQuery(
            "SELECT rx.drug_name, rx.start_date, rx.end_date
             FROM sl_pt_prescriptions rx
             WHERE '2025-10-01' BETWEEN rx.start_date AND rx.end_date
             ORDER BY rx.drug_name"
        );
        $this->assertCount(4, $rows);
        $this->assertSame('Aspirin', $rows[0]['drug_name']);
        $this->assertSame('Lisinopril', $rows[1]['drug_name']);
        $this->assertSame('Sumatriptan', $rows[2]['drug_name']);
        $this->assertSame('Vitamin D', $rows[3]['drug_name']);
    }

    /**
     * GROUP BY doctor: COUNT prescriptions, COUNT DISTINCT patients.
     * @spec SPEC-10.2.139
     */
    public function testPrescriptionsPerDoctor(): void
    {
        $rows = $this->ztdQuery(
            "SELECT d.name AS doctor_name,
                    COUNT(rx.id) AS prescription_count,
                    COUNT(DISTINCT v.patient_id) AS patient_count
             FROM sl_pt_doctors d
             JOIN sl_pt_visits v ON v.doctor_id = d.id
             JOIN sl_pt_prescriptions rx ON rx.visit_id = v.id
             GROUP BY d.id, d.name
             ORDER BY prescription_count DESC, d.name"
        );

        $this->assertCount(3, $rows);

        // Dr. Adams: visits 1,3,5 -> prescriptions 1,2,4,6 = 4 prescriptions, patients 1,2,3 = 3 distinct
        $adams = array_values(array_filter($rows, fn($r) => $r['doctor_name'] === 'Dr. Adams'))[0];
        $this->assertEquals(4, (int) $adams['prescription_count']);
        $this->assertEquals(3, (int) $adams['patient_count']);

        // Dr. Baker: visit 2 -> prescription 3 = 1 prescription, patient 1 = 1 distinct
        $baker = array_values(array_filter($rows, fn($r) => $r['doctor_name'] === 'Dr. Baker'))[0];
        $this->assertEquals(1, (int) $baker['prescription_count']);
        $this->assertEquals(1, (int) $baker['patient_count']);

        // Dr. Clark: visit 4 -> prescription 5 = 1 prescription, patient 2 = 1 distinct
        $clark = array_values(array_filter($rows, fn($r) => $r['doctor_name'] === 'Dr. Clark'))[0];
        $this->assertEquals(1, (int) $clark['prescription_count']);
        $this->assertEquals(1, (int) $clark['patient_count']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     * @spec SPEC-10.2.139
     */
    public function testPhysicalIsolation(): void
    {
        // Insert a new prescription through ZTD
        $this->pdo->exec("INSERT INTO sl_pt_prescriptions VALUES (7, 5, 'Amoxicillin', '500mg', 1, '2025-08-01', '2025-08-11')");

        // ZTD sees 7 prescriptions
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_pt_prescriptions");
        $this->assertEquals(7, (int) $rows[0]['cnt']);

        // Physical table is empty
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_pt_prescriptions")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
