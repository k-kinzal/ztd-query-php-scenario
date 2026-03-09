<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests an asset depreciation tracking scenario through ZTD shadow store (MySQL PDO).
 * Fixed assets with straight-line depreciation schedules exercise GROUP BY
 * with COUNT and SUM, correlated MAX subquery for latest depreciation entry,
 * ROUND arithmetic for depreciation percentage, HAVING aggregate threshold
 * for fully depreciated assets, prepared JOIN with category filter,
 * and physical isolation check.
 * @spec SPEC-10.2.163
 */
class MysqlAssetDepreciationTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_dep_assets (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255),
                category VARCHAR(255),
                purchase_cost TEXT,
                purchase_date TEXT,
                useful_life_years INT,
                salvage_value TEXT,
                status VARCHAR(255)
            )',
            'CREATE TABLE mp_dep_entries (
                id INT AUTO_INCREMENT PRIMARY KEY,
                asset_id INT,
                period TEXT,
                amount TEXT,
                cumulative TEXT,
                book_value TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_dep_entries', 'mp_dep_assets'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 5 assets
        $this->pdo->exec("INSERT INTO mp_dep_assets VALUES (1, 'Office Laptop', 'equipment', '1500.00', '2024-01-15', 3, '150.00', 'active')");
        $this->pdo->exec("INSERT INTO mp_dep_assets VALUES (2, 'Delivery Van', 'vehicle', '35000.00', '2023-06-01', 7, '5000.00', 'active')");
        $this->pdo->exec("INSERT INTO mp_dep_assets VALUES (3, 'Server Rack', 'equipment', '12000.00', '2022-03-10', 5, '1000.00', 'active')");
        $this->pdo->exec("INSERT INTO mp_dep_assets VALUES (4, 'Office Desk', 'furniture', '800.00', '2023-01-20', 10, '80.00', 'active')");
        $this->pdo->exec("INSERT INTO mp_dep_assets VALUES (5, 'Old Printer', 'equipment', '500.00', '2020-01-01', 4, '50.00', 'disposed')");

        // 16 depreciation entries
        $this->pdo->exec("INSERT INTO mp_dep_entries VALUES (1, 1, '2024', '450.00', '450.00', '1050.00')");
        $this->pdo->exec("INSERT INTO mp_dep_entries VALUES (2, 1, '2025', '450.00', '900.00', '600.00')");
        $this->pdo->exec("INSERT INTO mp_dep_entries VALUES (3, 2, '2023', '4285.71', '4285.71', '30714.29')");
        $this->pdo->exec("INSERT INTO mp_dep_entries VALUES (4, 2, '2024', '4285.71', '8571.42', '26428.58')");
        $this->pdo->exec("INSERT INTO mp_dep_entries VALUES (5, 2, '2025', '4285.71', '12857.13', '22142.87')");
        $this->pdo->exec("INSERT INTO mp_dep_entries VALUES (6, 3, '2022', '2200.00', '2200.00', '9800.00')");
        $this->pdo->exec("INSERT INTO mp_dep_entries VALUES (7, 3, '2023', '2200.00', '4400.00', '7600.00')");
        $this->pdo->exec("INSERT INTO mp_dep_entries VALUES (8, 3, '2024', '2200.00', '6600.00', '5400.00')");
        $this->pdo->exec("INSERT INTO mp_dep_entries VALUES (9, 3, '2025', '2200.00', '8800.00', '3200.00')");
        $this->pdo->exec("INSERT INTO mp_dep_entries VALUES (10, 4, '2023', '72.00', '72.00', '728.00')");
        $this->pdo->exec("INSERT INTO mp_dep_entries VALUES (11, 4, '2024', '72.00', '144.00', '656.00')");
        $this->pdo->exec("INSERT INTO mp_dep_entries VALUES (12, 4, '2025', '72.00', '216.00', '584.00')");
        $this->pdo->exec("INSERT INTO mp_dep_entries VALUES (13, 5, '2020', '112.50', '112.50', '387.50')");
        $this->pdo->exec("INSERT INTO mp_dep_entries VALUES (14, 5, '2021', '112.50', '225.00', '275.00')");
        $this->pdo->exec("INSERT INTO mp_dep_entries VALUES (15, 5, '2022', '112.50', '337.50', '162.50')");
        $this->pdo->exec("INSERT INTO mp_dep_entries VALUES (16, 5, '2023', '112.50', '450.00', '50.00')");
    }

    /**
     * GROUP BY category with COUNT and SUM purchase_cost.
     * Expected 3 rows: equipment=3/14000.00, furniture=1/800.00, vehicle=1/35000.00.
     */
    public function testAssetSummaryByCategory(): void
    {
        $rows = $this->ztdQuery(
            "SELECT a.category, COUNT(*) AS asset_count, SUM(a.purchase_cost) AS total_cost
             FROM mp_dep_assets a
             GROUP BY a.category
             ORDER BY a.category"
        );

        $this->assertCount(3, $rows);

        $this->assertSame('equipment', $rows[0]['category']);
        $this->assertEquals(3, (int) $rows[0]['asset_count']);
        $this->assertEquals(14000.00, (float) $rows[0]['total_cost']);

        $this->assertSame('furniture', $rows[1]['category']);
        $this->assertEquals(1, (int) $rows[1]['asset_count']);
        $this->assertEquals(800.00, (float) $rows[1]['total_cost']);

        $this->assertSame('vehicle', $rows[2]['category']);
        $this->assertEquals(1, (int) $rows[2]['asset_count']);
        $this->assertEquals(35000.00, (float) $rows[2]['total_cost']);
    }

    /**
     * JOIN with correlated MAX subquery for latest depreciation entry per asset.
     * Expected 5 rows with current book value and period.
     */
    public function testCurrentBookValue(): void
    {
        $rows = $this->ztdQuery(
            "SELECT a.name, a.purchase_cost, d.book_value, d.period
             FROM mp_dep_assets a
             JOIN mp_dep_entries d ON d.asset_id = a.id
             WHERE d.period = (SELECT MAX(d2.period) FROM mp_dep_entries d2 WHERE d2.asset_id = a.id)
             ORDER BY a.id"
        );

        $this->assertCount(5, $rows);

        $this->assertSame('Office Laptop', $rows[0]['name']);
        $this->assertEquals(1500.00, (float) $rows[0]['purchase_cost']);
        $this->assertEquals(600.00, (float) $rows[0]['book_value']);
        $this->assertSame('2025', $rows[0]['period']);

        $this->assertSame('Delivery Van', $rows[1]['name']);
        $this->assertEquals(35000.00, (float) $rows[1]['purchase_cost']);
        $this->assertEquals(22142.87, (float) $rows[1]['book_value']);
        $this->assertSame('2025', $rows[1]['period']);

        $this->assertSame('Server Rack', $rows[2]['name']);
        $this->assertEquals(12000.00, (float) $rows[2]['purchase_cost']);
        $this->assertEquals(3200.00, (float) $rows[2]['book_value']);
        $this->assertSame('2025', $rows[2]['period']);

        $this->assertSame('Office Desk', $rows[3]['name']);
        $this->assertEquals(800.00, (float) $rows[3]['purchase_cost']);
        $this->assertEquals(584.00, (float) $rows[3]['book_value']);
        $this->assertSame('2025', $rows[3]['period']);

        $this->assertSame('Old Printer', $rows[4]['name']);
        $this->assertEquals(500.00, (float) $rows[4]['purchase_cost']);
        $this->assertEquals(50.00, (float) $rows[4]['book_value']);
        $this->assertSame('2023', $rows[4]['period']);
    }

    /**
     * ROUND arithmetic for depreciation percentage.
     * Expected 5 rows ordered by pct_depreciated DESC.
     */
    public function testDepreciationProgress(): void
    {
        $rows = $this->ztdQuery(
            "SELECT a.name,
                    d.cumulative,
                    ROUND(d.cumulative * 100.0 / (a.purchase_cost - a.salvage_value), 1) AS pct_depreciated
             FROM mp_dep_assets a
             JOIN mp_dep_entries d ON d.asset_id = a.id
             WHERE d.period = (SELECT MAX(d2.period) FROM mp_dep_entries d2 WHERE d2.asset_id = a.id)
             ORDER BY pct_depreciated DESC"
        );

        $this->assertCount(5, $rows);

        $this->assertSame('Old Printer', $rows[0]['name']);
        $this->assertEquals(450.00, (float) $rows[0]['cumulative']);
        $this->assertEquals(100.0, round((float) $rows[0]['pct_depreciated'], 1));

        $this->assertSame('Server Rack', $rows[1]['name']);
        $this->assertEquals(8800.00, (float) $rows[1]['cumulative']);
        $this->assertEquals(80.0, round((float) $rows[1]['pct_depreciated'], 1));

        $this->assertSame('Office Laptop', $rows[2]['name']);
        $this->assertEquals(900.00, (float) $rows[2]['cumulative']);
        $this->assertEquals(66.7, round((float) $rows[2]['pct_depreciated'], 1));

        $this->assertSame('Delivery Van', $rows[3]['name']);
        $this->assertEquals(12857.13, (float) $rows[3]['cumulative']);
        $this->assertEquals(42.9, round((float) $rows[3]['pct_depreciated'], 1));

        $this->assertSame('Office Desk', $rows[4]['name']);
        $this->assertEquals(216.00, (float) $rows[4]['cumulative']);
        $this->assertEquals(30.0, round((float) $rows[4]['pct_depreciated'], 1));
    }

    /**
     * HAVING with aggregate threshold for fully depreciated assets.
     * Expected 1 row: Old Printer with total_depreciation=450.00, depreciable=450.00.
     */
    public function testFullyDepreciatedAssets(): void
    {
        $rows = $this->ztdQuery(
            "SELECT a.name, a.category,
                    SUM(d.amount) AS total_depreciation,
                    a.purchase_cost - a.salvage_value AS depreciable
             FROM mp_dep_assets a
             JOIN mp_dep_entries d ON d.asset_id = a.id
             GROUP BY a.id, a.name, a.category, a.purchase_cost, a.salvage_value
             HAVING SUM(d.amount) >= a.purchase_cost - a.salvage_value
             ORDER BY a.id"
        );

        $this->assertCount(1, $rows);

        $this->assertSame('Old Printer', $rows[0]['name']);
        $this->assertSame('equipment', $rows[0]['category']);
        $this->assertEquals(450.00, (float) $rows[0]['total_depreciation']);
        $this->assertEquals(450.00, (float) $rows[0]['depreciable']);
    }

    /**
     * Prepared JOIN with category filter for equipment assets and their entries.
     * Expected 10 rows for category='equipment'.
     */
    public function testPreparedAssetLookup(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT a.name, a.purchase_cost, d.period, d.book_value
             FROM mp_dep_assets a
             JOIN mp_dep_entries d ON d.asset_id = a.id
             WHERE a.category = ?
             ORDER BY a.name, d.period",
            ['equipment']
        );

        $this->assertCount(10, $rows);

        $this->assertSame('Office Laptop', $rows[0]['name']);
        $this->assertEquals(1500.00, (float) $rows[0]['purchase_cost']);
        $this->assertSame('2024', $rows[0]['period']);
        $this->assertEquals(1050.00, (float) $rows[0]['book_value']);

        $this->assertSame('Office Laptop', $rows[1]['name']);
        $this->assertEquals(1500.00, (float) $rows[1]['purchase_cost']);
        $this->assertSame('2025', $rows[1]['period']);
        $this->assertEquals(600.00, (float) $rows[1]['book_value']);

        $this->assertSame('Old Printer', $rows[2]['name']);
        $this->assertEquals(500.00, (float) $rows[2]['purchase_cost']);
        $this->assertSame('2020', $rows[2]['period']);
        $this->assertEquals(387.50, (float) $rows[2]['book_value']);

        $this->assertSame('Old Printer', $rows[3]['name']);
        $this->assertEquals(500.00, (float) $rows[3]['purchase_cost']);
        $this->assertSame('2021', $rows[3]['period']);
        $this->assertEquals(275.00, (float) $rows[3]['book_value']);

        $this->assertSame('Old Printer', $rows[4]['name']);
        $this->assertEquals(500.00, (float) $rows[4]['purchase_cost']);
        $this->assertSame('2022', $rows[4]['period']);
        $this->assertEquals(162.50, (float) $rows[4]['book_value']);

        $this->assertSame('Old Printer', $rows[5]['name']);
        $this->assertEquals(500.00, (float) $rows[5]['purchase_cost']);
        $this->assertSame('2023', $rows[5]['period']);
        $this->assertEquals(50.00, (float) $rows[5]['book_value']);

        $this->assertSame('Server Rack', $rows[6]['name']);
        $this->assertEquals(12000.00, (float) $rows[6]['purchase_cost']);
        $this->assertSame('2022', $rows[6]['period']);
        $this->assertEquals(9800.00, (float) $rows[6]['book_value']);

        $this->assertSame('Server Rack', $rows[7]['name']);
        $this->assertEquals(12000.00, (float) $rows[7]['purchase_cost']);
        $this->assertSame('2023', $rows[7]['period']);
        $this->assertEquals(7600.00, (float) $rows[7]['book_value']);

        $this->assertSame('Server Rack', $rows[8]['name']);
        $this->assertEquals(12000.00, (float) $rows[8]['purchase_cost']);
        $this->assertSame('2024', $rows[8]['period']);
        $this->assertEquals(5400.00, (float) $rows[8]['book_value']);

        $this->assertSame('Server Rack', $rows[9]['name']);
        $this->assertEquals(12000.00, (float) $rows[9]['purchase_cost']);
        $this->assertSame('2025', $rows[9]['period']);
        $this->assertEquals(3200.00, (float) $rows[9]['book_value']);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        // Insert new asset via shadow
        $this->pdo->exec("INSERT INTO mp_dep_assets VALUES (6, 'New Monitor', 'equipment', '600.00', '2026-01-01', 5, '60.00', 'active')");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_dep_assets");
        $this->assertEquals(6, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM mp_dep_assets')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
