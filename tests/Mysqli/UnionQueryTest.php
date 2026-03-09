<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests UNION and UNION ALL queries with shadow data from multiple tables.
 * Verifies deduplication, filtering, ordering with LIMIT, and DML visibility
 * across UNION results in the ZTD shadow store (MySQLi).
 * @spec SPEC-10.2.92
 */
class UnionQueryTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_uq_active_products (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                price DECIMAL(10,2),
                source VARCHAR(20)
            )',
            'CREATE TABLE mi_uq_archived_products (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                price DECIMAL(10,2),
                archived_date DATETIME
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_uq_archived_products', 'mi_uq_active_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 4 active products
        $this->mysqli->query("INSERT INTO mi_uq_active_products VALUES (1, 'Alpha',   29.99, 'web')");
        $this->mysqli->query("INSERT INTO mi_uq_active_products VALUES (2, 'Bravo',   59.99, 'store')");
        $this->mysqli->query("INSERT INTO mi_uq_active_products VALUES (3, 'Charlie', 89.99, 'web')");
        $this->mysqli->query("INSERT INTO mi_uq_active_products VALUES (4, 'Delta',   14.99, 'store')");

        // 3 archived products
        $this->mysqli->query("INSERT INTO mi_uq_archived_products VALUES (1, 'Echo',    45.00, '2025-01-15 00:00:00')");
        $this->mysqli->query("INSERT INTO mi_uq_archived_products VALUES (2, 'Foxtrot', 72.50, '2025-02-20 00:00:00')");
        $this->mysqli->query("INSERT INTO mi_uq_archived_products VALUES (3, 'Golf',    19.99, '2025-03-10 00:00:00')");
    }

    /**
     * @spec SPEC-10.2.92
     */
    public function testUnionAllCombined(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, price, 'active' AS status FROM mi_uq_active_products
             UNION ALL
             SELECT name, price, 'archived' AS status FROM mi_uq_archived_products
             ORDER BY name"
        );

        $this->assertCount(7, $rows);
        $this->assertSame('Alpha', $rows[0]['name']);
        $this->assertSame('Golf', $rows[6]['name']);
    }

    /**
     * @spec SPEC-10.2.92
     */
    public function testUnionDistinct(): void
    {
        // Insert a product that exists in both tables with same name/price
        $this->mysqli->query("INSERT INTO mi_uq_active_products VALUES (5, 'Echo', 45.00, 'web')");

        // UNION (without ALL) should deduplicate rows with same name and price
        $rows = $this->ztdQuery(
            "SELECT name, price FROM mi_uq_active_products
             UNION
             SELECT name, price FROM mi_uq_archived_products
             ORDER BY name"
        );

        // 4 active + 3 archived = 7 distinct name/price, but Echo/45.00 appears in both -> 7
        $this->assertCount(7, $rows);

        // Verify Echo appears exactly once
        $echoRows = array_filter($rows, fn($r) => $r['name'] === 'Echo');
        $this->assertCount(1, $echoRows);
    }

    /**
     * @spec SPEC-10.2.92
     */
    public function testUnionWithWhereFilter(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, price FROM mi_uq_active_products WHERE price > 50
             UNION ALL
             SELECT name, price FROM mi_uq_archived_products WHERE price > 50
             ORDER BY price DESC"
        );

        // Active > 50: Bravo (59.99), Charlie (89.99) = 2
        // Archived > 50: Foxtrot (72.50) = 1
        $this->assertCount(3, $rows);
        $this->assertSame('Charlie', $rows[0]['name']);
        $this->assertSame('89.99', $rows[0]['price']);
    }

    /**
     * @spec SPEC-10.2.92
     */
    public function testUnionWithOrderByLimit(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, price FROM mi_uq_active_products
             UNION ALL
             SELECT name, price FROM mi_uq_archived_products
             ORDER BY price DESC
             LIMIT 3"
        );

        $this->assertCount(3, $rows);
        // Top 3 by price: Charlie (89.99), Foxtrot (72.50), Bravo (59.99)
        $this->assertSame('Charlie', $rows[0]['name']);
        $this->assertSame('89.99', $rows[0]['price']);
        $this->assertSame('Foxtrot', $rows[1]['name']);
        $this->assertSame('72.50', $rows[1]['price']);
        $this->assertSame('Bravo', $rows[2]['name']);
        $this->assertSame('59.99', $rows[2]['price']);
    }

    /**
     * @spec SPEC-10.2.92
     */
    public function testInsertThenUnion(): void
    {
        $this->mysqli->query("INSERT INTO mi_uq_active_products VALUES (5, 'Hotel', 99.99, 'web')");

        $rows = $this->ztdQuery(
            "SELECT name, price FROM mi_uq_active_products
             UNION ALL
             SELECT name, price FROM mi_uq_archived_products
             ORDER BY name"
        );

        $this->assertCount(8, $rows);
        $hotelRows = array_filter($rows, fn($r) => $r['name'] === 'Hotel');
        $this->assertCount(1, $hotelRows);
    }

    /**
     * @spec SPEC-10.2.92
     */
    public function testDeleteAndUnion(): void
    {
        $this->mysqli->query("DELETE FROM mi_uq_archived_products WHERE id = 1");
        $this->assertSame(1, $this->mysqli->lastAffectedRows());

        $rows = $this->ztdQuery(
            "SELECT name, price FROM mi_uq_active_products
             UNION ALL
             SELECT name, price FROM mi_uq_archived_products
             ORDER BY name"
        );

        // 4 active + 2 archived = 6
        $this->assertCount(6, $rows);
        $echoRows = array_filter($rows, fn($r) => $r['name'] === 'Echo');
        $this->assertCount(0, $echoRows);
    }

    /**
     * @spec SPEC-10.2.92
     */
    public function testPhysicalIsolation(): void
    {
        // Mutate through ZTD
        $this->mysqli->query("INSERT INTO mi_uq_active_products VALUES (5, 'Hotel', 99.99, 'web')");
        $this->mysqli->query("DELETE FROM mi_uq_archived_products WHERE id = 3");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_uq_active_products");
        $this->assertSame(5, (int) $rows[0]['cnt']);
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_uq_archived_products");
        $this->assertSame(2, (int) $rows[0]['cnt']);

        // Physical tables untouched
        $this->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_uq_active_products');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_uq_archived_products');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
