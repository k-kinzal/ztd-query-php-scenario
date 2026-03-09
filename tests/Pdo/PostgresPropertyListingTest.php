<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests a property listing search workflow through ZTD shadow store (PostgreSQL PDO).
 * Covers faceted search with multi-condition WHERE, BETWEEN for price ranges,
 * LIMIT/OFFSET pagination, GROUP BY counts, price updates, and physical isolation.
 * @spec SPEC-10.2.83
 */
class PostgresPropertyListingTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_pl_properties (
                id INTEGER PRIMARY KEY,
                title VARCHAR(255),
                city VARCHAR(100),
                price NUMERIC(12,2),
                bedrooms INTEGER,
                bathrooms INTEGER,
                sqft INTEGER,
                status VARCHAR(20),
                listed_at TIMESTAMP
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_pl_properties'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_pl_properties VALUES (1, 'Downtown Loft', 'New York', 450000.00, 1, 1, 750, 'active', '2026-02-01 09:00:00')");
        $this->pdo->exec("INSERT INTO pg_pl_properties VALUES (2, 'Brooklyn Brownstone', 'New York', 1200000.00, 4, 3, 2800, 'active', '2026-02-05 10:00:00')");
        $this->pdo->exec("INSERT INTO pg_pl_properties VALUES (3, 'Midtown Condo', 'New York', 850000.00, 2, 2, 1200, 'active', '2026-02-10 11:00:00')");
        $this->pdo->exec("INSERT INTO pg_pl_properties VALUES (4, 'Venice Beach House', 'Los Angeles', 1500000.00, 3, 2, 2000, 'active', '2026-02-12 09:00:00')");
        $this->pdo->exec("INSERT INTO pg_pl_properties VALUES (5, 'Silver Lake Bungalow', 'Los Angeles', 980000.00, 2, 1, 1400, 'active', '2026-02-15 10:00:00')");
        $this->pdo->exec("INSERT INTO pg_pl_properties VALUES (6, 'Hollywood Studio', 'Los Angeles', 520000.00, 1, 1, 600, 'active', '2026-02-18 11:00:00')");
        $this->pdo->exec("INSERT INTO pg_pl_properties VALUES (7, 'Lincoln Park House', 'Chicago', 680000.00, 3, 2, 1800, 'active', '2026-02-20 09:00:00')");
        $this->pdo->exec("INSERT INTO pg_pl_properties VALUES (8, 'Loop Penthouse', 'Chicago', 920000.00, 2, 2, 1500, 'active', '2026-02-22 10:00:00')");
        $this->pdo->exec("INSERT INTO pg_pl_properties VALUES (9, 'Wicker Park Flat', 'Chicago', 350000.00, 1, 1, 800, 'sold', '2026-01-15 09:00:00')");
        $this->pdo->exec("INSERT INTO pg_pl_properties VALUES (10, 'River North Studio', 'Chicago', 410000.00, 1, 1, 650, 'active', '2026-02-25 11:00:00')");
    }

    /**
     * Prepared: search properties by city.
     */
    public function testSearchByCity(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT id, title, price FROM pg_pl_properties WHERE city = ? AND status = 'active' ORDER BY id",
            ['Los Angeles']
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Venice Beach House', $rows[0]['title']);
        $this->assertSame('Silver Lake Bungalow', $rows[1]['title']);
        $this->assertSame('Hollywood Studio', $rows[2]['title']);

        $rows = $this->ztdPrepareAndExecute(
            "SELECT id, title, price FROM pg_pl_properties WHERE city = ? AND status = 'active' ORDER BY id",
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
             FROM pg_pl_properties
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
             FROM pg_pl_properties
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
        $page1 = $this->ztdQuery(
            "SELECT id, title, price
             FROM pg_pl_properties
             WHERE status = 'active'
             ORDER BY price
             LIMIT 3 OFFSET 0"
        );

        $this->assertCount(3, $page1);
        $this->assertSame('River North Studio', $page1[0]['title']);
        $this->assertSame('Downtown Loft', $page1[1]['title']);
        $this->assertSame('Hollywood Studio', $page1[2]['title']);

        $page2 = $this->ztdQuery(
            "SELECT id, title, price
             FROM pg_pl_properties
             WHERE status = 'active'
             ORDER BY price
             LIMIT 3 OFFSET 3"
        );

        $this->assertCount(3, $page2);
        $this->assertSame('Lincoln Park House', $page2[0]['title']);

        $page4 = $this->ztdQuery(
            "SELECT id, title, price
             FROM pg_pl_properties
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
             FROM pg_pl_properties
             WHERE status = 'active'
             GROUP BY city
             ORDER BY COUNT(*) DESC, city"
        );

        $this->assertCount(3, $rows);
        $this->assertEquals(3, (int) $rows[0]['listing_count']);
        $this->assertEquals(3, (int) $rows[1]['listing_count']);
        $this->assertEquals(3, (int) $rows[2]['listing_count']);
    }

    /**
     * UPDATE listing price, verify the new price is visible.
     */
    public function testUpdateListingPrice(): void
    {
        $before = $this->ztdQuery("SELECT price FROM pg_pl_properties WHERE id = 1");
        $this->assertEquals(450000.00, (float) $before[0]['price']);

        $affected = $this->pdo->exec("UPDATE pg_pl_properties SET price = 425000.00 WHERE id = 1");
        $this->assertSame(1, $affected);

        $after = $this->ztdQuery("SELECT price FROM pg_pl_properties WHERE id = 1");
        $this->assertEquals(425000.00, (float) $after[0]['price']);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_pl_properties VALUES (11, 'Test Property', 'Boston', 300000.00, 1, 1, 500, 'active', '2026-03-01 09:00:00')");
        $this->pdo->exec("UPDATE pg_pl_properties SET price = 999999.00 WHERE id = 1");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_pl_properties");
        $this->assertEquals(11, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM pg_pl_properties')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
