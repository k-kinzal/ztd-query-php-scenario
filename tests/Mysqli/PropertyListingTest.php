<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests a property listing search workflow through ZTD shadow store (MySQLi).
 * Covers faceted search with multi-condition WHERE, BETWEEN for price ranges,
 * LIMIT/OFFSET pagination, GROUP BY counts, price updates, and physical isolation.
 * @spec SPEC-10.2.83
 */
class PropertyListingTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_pl_properties (
                id INT PRIMARY KEY,
                title VARCHAR(255),
                city VARCHAR(100),
                price DECIMAL(12,2),
                bedrooms INT,
                bathrooms INT,
                sqft INT,
                status VARCHAR(20),
                listed_at DATETIME
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_pl_properties'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_pl_properties VALUES (1, 'Downtown Loft', 'New York', 450000.00, 1, 1, 750, 'active', '2026-02-01 09:00:00')");
        $this->mysqli->query("INSERT INTO mi_pl_properties VALUES (2, 'Brooklyn Brownstone', 'New York', 1200000.00, 4, 3, 2800, 'active', '2026-02-05 10:00:00')");
        $this->mysqli->query("INSERT INTO mi_pl_properties VALUES (3, 'Midtown Condo', 'New York', 850000.00, 2, 2, 1200, 'active', '2026-02-10 11:00:00')");
        $this->mysqli->query("INSERT INTO mi_pl_properties VALUES (4, 'Venice Beach House', 'Los Angeles', 1500000.00, 3, 2, 2000, 'active', '2026-02-12 09:00:00')");
        $this->mysqli->query("INSERT INTO mi_pl_properties VALUES (5, 'Silver Lake Bungalow', 'Los Angeles', 980000.00, 2, 1, 1400, 'active', '2026-02-15 10:00:00')");
        $this->mysqli->query("INSERT INTO mi_pl_properties VALUES (6, 'Hollywood Studio', 'Los Angeles', 520000.00, 1, 1, 600, 'active', '2026-02-18 11:00:00')");
        $this->mysqli->query("INSERT INTO mi_pl_properties VALUES (7, 'Lincoln Park House', 'Chicago', 680000.00, 3, 2, 1800, 'active', '2026-02-20 09:00:00')");
        $this->mysqli->query("INSERT INTO mi_pl_properties VALUES (8, 'Loop Penthouse', 'Chicago', 920000.00, 2, 2, 1500, 'active', '2026-02-22 10:00:00')");
        $this->mysqli->query("INSERT INTO mi_pl_properties VALUES (9, 'Wicker Park Flat', 'Chicago', 350000.00, 1, 1, 800, 'sold', '2026-01-15 09:00:00')");
        $this->mysqli->query("INSERT INTO mi_pl_properties VALUES (10, 'River North Studio', 'Chicago', 410000.00, 1, 1, 650, 'active', '2026-02-25 11:00:00')");
    }

    /**
     * Prepared: search properties by city.
     */
    public function testSearchByCity(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT id, title, price FROM mi_pl_properties WHERE city = ? AND status = 'active' ORDER BY id",
            ['Los Angeles']
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Venice Beach House', $rows[0]['title']);
        $this->assertSame('Silver Lake Bungalow', $rows[1]['title']);
        $this->assertSame('Hollywood Studio', $rows[2]['title']);

        // Different city
        $rows = $this->ztdPrepareAndExecute(
            "SELECT id, title, price FROM mi_pl_properties WHERE city = ? AND status = 'active' ORDER BY id",
            ['Chicago']
        );
        $this->assertCount(3, $rows);
    }

    /**
     * Prepared: filter by price range using BETWEEN, ordered by price.
     */
    public function testPriceRangeFilter(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT id, title, city, price
             FROM mi_pl_properties
             WHERE price BETWEEN ? AND ? AND status = 'active'
             ORDER BY price",
            [500000.00, 1000000.00]
        );

        $this->assertCount(5, $rows);
        $this->assertSame('Hollywood Studio', $rows[0]['title']);
        $this->assertEquals(520000.00, (float) $rows[0]['price']);
        $this->assertSame('Silver Lake Bungalow', $rows[4]['title']);
        $this->assertEquals(980000.00, (float) $rows[4]['price']);
    }

    /**
     * Prepared: multi-filter search combining city, bedrooms, and price ceiling.
     */
    public function testMultiFilterSearch(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT id, title, price, bedrooms
             FROM mi_pl_properties
             WHERE city = ? AND bedrooms >= ? AND price <= ? AND status = 'active'
             ORDER BY price",
            ['New York', 2, 1000000.00]
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Midtown Condo', $rows[0]['title']);
        $this->assertEquals(850000.00, (float) $rows[0]['price']);
        $this->assertEquals(2, (int) $rows[0]['bedrooms']);
    }

    /**
     * LIMIT/OFFSET pagination: verify page sizes and content.
     */
    public function testPaginatedResults(): void
    {
        // Page 1: first 3 active listings by price
        $page1 = $this->ztdQuery(
            "SELECT id, title, price
             FROM mi_pl_properties
             WHERE status = 'active'
             ORDER BY price
             LIMIT 3 OFFSET 0"
        );

        $this->assertCount(3, $page1);
        $this->assertSame('River North Studio', $page1[0]['title']);
        $this->assertSame('Downtown Loft', $page1[1]['title']);
        $this->assertSame('Hollywood Studio', $page1[2]['title']);

        // Page 2: next 3
        $page2 = $this->ztdQuery(
            "SELECT id, title, price
             FROM mi_pl_properties
             WHERE status = 'active'
             ORDER BY price
             LIMIT 3 OFFSET 3"
        );

        $this->assertCount(3, $page2);
        $this->assertSame('Lincoln Park House', $page2[0]['title']);

        // Page 4: should have remaining listings (9 active total, page 4 = offset 9 -> empty)
        $page4 = $this->ztdQuery(
            "SELECT id, title, price
             FROM mi_pl_properties
             WHERE status = 'active'
             ORDER BY price
             LIMIT 3 OFFSET 9"
        );
        $this->assertCount(0, $page4);
    }

    /**
     * COUNT active listings grouped by city, ordered by count descending.
     */
    public function testListingCountsByCity(): void
    {
        $rows = $this->ztdQuery(
            "SELECT city, COUNT(*) AS listing_count
             FROM mi_pl_properties
             WHERE status = 'active'
             GROUP BY city
             ORDER BY COUNT(*) DESC, city"
        );

        $this->assertCount(3, $rows);
        // New York and Los Angeles and Chicago each have 3 active
        $this->assertEquals(3, (int) $rows[0]['listing_count']);
        $this->assertEquals(3, (int) $rows[1]['listing_count']);
        $this->assertEquals(3, (int) $rows[2]['listing_count']);
    }

    /**
     * UPDATE listing price, verify the new price is visible.
     */
    public function testUpdateListingPrice(): void
    {
        // Before: Downtown Loft at $450,000
        $before = $this->ztdQuery("SELECT price FROM mi_pl_properties WHERE id = 1");
        $this->assertEquals(450000.00, (float) $before[0]['price']);

        // Reduce price
        $this->mysqli->query("UPDATE mi_pl_properties SET price = 425000.00 WHERE id = 1");
        $this->assertSame(1, $this->mysqli->lastAffectedRows());

        // After: new price visible
        $after = $this->ztdQuery("SELECT price FROM mi_pl_properties WHERE id = 1");
        $this->assertEquals(425000.00, (float) $after[0]['price']);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_pl_properties VALUES (11, 'Test Property', 'Boston', 300000.00, 1, 1, 500, 'active', '2026-03-01 09:00:00')");
        $this->mysqli->query("UPDATE mi_pl_properties SET price = 999999.00 WHERE id = 1");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_pl_properties");
        $this->assertEquals(11, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_pl_properties');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
