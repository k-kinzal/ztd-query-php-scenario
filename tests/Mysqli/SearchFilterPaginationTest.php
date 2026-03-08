<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests parameterized search with multiple filter conditions and pagination,
 * simulating common REST API query patterns through ZTD shadow store.
 * @spec SPEC-3.2
 */
class SearchFilterPaginationTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_sfp_products (
            id INT PRIMARY KEY,
            name VARCHAR(255),
            category VARCHAR(50),
            price DECIMAL(10,2),
            stock INT,
            active TINYINT(1)
        )';
    }

    protected function getTableNames(): array
    {
        return ['mi_sfp_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_sfp_products VALUES (1, 'Wireless Mouse', 'electronics', 29.99, 150, 1)");
        $this->mysqli->query("INSERT INTO mi_sfp_products VALUES (2, 'Wireless Keyboard', 'electronics', 49.99, 80, 1)");
        $this->mysqli->query("INSERT INTO mi_sfp_products VALUES (3, 'USB Cable', 'accessories', 9.99, 500, 1)");
        $this->mysqli->query("INSERT INTO mi_sfp_products VALUES (4, 'Monitor Stand', 'accessories', 39.99, 30, 1)");
        $this->mysqli->query("INSERT INTO mi_sfp_products VALUES (5, 'Laptop Bag', 'accessories', 59.99, 0, 0)");
        $this->mysqli->query("INSERT INTO mi_sfp_products VALUES (6, 'Wireless Headphones', 'electronics', 89.99, 45, 1)");
        $this->mysqli->query("INSERT INTO mi_sfp_products VALUES (7, 'Desk Lamp', 'furniture', 24.99, 60, 1)");
        $this->mysqli->query("INSERT INTO mi_sfp_products VALUES (8, 'Standing Desk', 'furniture', 299.99, 10, 1)");
        $this->mysqli->query("INSERT INTO mi_sfp_products VALUES (9, 'Mouse Pad', 'accessories', 14.99, 200, 1)");
        $this->mysqli->query("INSERT INTO mi_sfp_products VALUES (10, 'Webcam', 'electronics', 69.99, 25, 1)");
    }

    /**
     * LIKE search with prepared statement.
     */
    public function testSearchByNamePattern(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT id, name FROM mi_sfp_products WHERE name LIKE ? ORDER BY name",
            ['%Wireless%']
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Wireless Headphones', $rows[0]['name']);
        $this->assertSame('Wireless Keyboard', $rows[1]['name']);
        $this->assertSame('Wireless Mouse', $rows[2]['name']);
    }

    /**
     * Category filter with price range.
     */
    public function testCategoryAndPriceRangeFilter(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT id, name, price FROM mi_sfp_products
             WHERE category = ? AND price BETWEEN ? AND ?
             ORDER BY price",
            ['electronics', 30.00, 100.00]
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Wireless Keyboard', $rows[0]['name']);
        $this->assertSame('Webcam', $rows[1]['name']);
        $this->assertSame('Wireless Headphones', $rows[2]['name']);
    }

    /**
     * Active + in-stock filter (common product listing pattern).
     */
    public function testActiveInStockFilter(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT id, name, stock FROM mi_sfp_products
             WHERE active = ? AND stock > ?
             ORDER BY stock DESC",
            [1, 0]
        );

        $this->assertCount(9, $rows); // 10 total - 1 inactive (Laptop Bag)
        $this->assertSame('USB Cable', $rows[0]['name']); // stock=500
    }

    /**
     * Combined search: LIKE + category + price + active.
     */
    public function testMultiFilterSearch(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT id, name, price FROM mi_sfp_products
             WHERE name LIKE ?
               AND category = ?
               AND price < ?
               AND active = ?
             ORDER BY price DESC",
            ['%Mouse%', 'accessories', 20.00, 1]
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Mouse Pad', $rows[0]['name']);
    }

    /**
     * Count + paginated results using LIMIT/OFFSET.
     */
    public function testPaginatedResults(): void
    {
        // Page 1: first 3 results
        $rows = $this->ztdQuery(
            "SELECT id, name, price FROM mi_sfp_products
             WHERE active = 1
             ORDER BY price ASC
             LIMIT 3 OFFSET 0"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('USB Cable', $rows[0]['name']);     // 9.99
        $this->assertSame('Mouse Pad', $rows[1]['name']);     // 14.99
        $this->assertSame('Desk Lamp', $rows[2]['name']);     // 24.99

        // Page 2
        $rows = $this->ztdQuery(
            "SELECT id, name, price FROM mi_sfp_products
             WHERE active = 1
             ORDER BY price ASC
             LIMIT 3 OFFSET 3"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Wireless Mouse', $rows[0]['name']);    // 29.99
        $this->assertSame('Monitor Stand', $rows[1]['name']);     // 39.99
        $this->assertSame('Wireless Keyboard', $rows[2]['name']); // 49.99
    }

    /**
     * Search results change after INSERT/UPDATE.
     */
    public function testSearchAfterMutations(): void
    {
        // Initial search
        $rows = $this->ztdPrepareAndExecute(
            "SELECT COUNT(*) AS cnt FROM mi_sfp_products WHERE category = ?",
            ['electronics']
        );
        $this->assertSame(4, (int) $rows[0]['cnt']);

        // Add a new electronics product
        $this->mysqli->query(
            "INSERT INTO mi_sfp_products VALUES (11, 'Bluetooth Speaker', 'electronics', 44.99, 75, 1)"
        );

        $rows = $this->ztdPrepareAndExecute(
            "SELECT COUNT(*) AS cnt FROM mi_sfp_products WHERE category = ?",
            ['electronics']
        );
        $this->assertSame(5, (int) $rows[0]['cnt']);

        // Change category of existing product
        $this->mysqli->query(
            "UPDATE mi_sfp_products SET category = 'audio' WHERE id = 6"
        );

        $rows = $this->ztdPrepareAndExecute(
            "SELECT COUNT(*) AS cnt FROM mi_sfp_products WHERE category = ?",
            ['electronics']
        );
        $this->assertSame(4, (int) $rows[0]['cnt']);
    }

    /**
     * Category summary with aggregate filters.
     */
    public function testCategorySummaryWithHaving(): void
    {
        $rows = $this->ztdQuery(
            "SELECT category,
                    COUNT(*) AS product_count,
                    AVG(price) AS avg_price,
                    SUM(stock) AS total_stock
             FROM mi_sfp_products
             WHERE active = 1
             GROUP BY category
             HAVING COUNT(*) >= 2
             ORDER BY avg_price DESC"
        );

        $this->assertCount(3, $rows); // electronics(4), accessories(2 active), furniture(2)
    }

    /**
     * Physical isolation of search results.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query(
            "INSERT INTO mi_sfp_products VALUES (11, 'New Product', 'test', 1.00, 1, 1)"
        );
        $this->mysqli->query("UPDATE mi_sfp_products SET price = 999.99 WHERE id = 1");

        $rows = $this->ztdQuery("SELECT price FROM mi_sfp_products WHERE id = 1");
        $this->assertEqualsWithDelta(999.99, (float) $rows[0]['price'], 0.01);

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_sfp_products');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
