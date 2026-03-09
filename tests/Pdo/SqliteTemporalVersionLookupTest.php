<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests point-in-time lookup using effective date ranges on versioned data,
 * a common pattern for pricing, configuration, and employee records (SQLite PDO).
 * @spec SPEC-10.2.122
 */
class SqliteTemporalVersionLookupTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_tv_products (
                id INTEGER PRIMARY KEY,
                name TEXT
            )',
            'CREATE TABLE sl_tv_product_prices (
                id INTEGER PRIMARY KEY,
                product_id INTEGER,
                price REAL,
                effective_from TEXT,
                effective_to TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_tv_product_prices', 'sl_tv_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Products
        $this->pdo->exec("INSERT INTO sl_tv_products VALUES (1, 'Widget')");
        $this->pdo->exec("INSERT INTO sl_tv_products VALUES (2, 'Gadget')");
        $this->pdo->exec("INSERT INTO sl_tv_products VALUES (3, 'Gizmo')");

        // Price history
        $this->pdo->exec("INSERT INTO sl_tv_product_prices VALUES (1, 1, 9.99,  '2025-01-01', '2025-06-30')");
        $this->pdo->exec("INSERT INTO sl_tv_product_prices VALUES (2, 1, 12.99, '2025-07-01', '2026-01-31')");
        $this->pdo->exec("INSERT INTO sl_tv_product_prices VALUES (3, 1, 14.99, '2026-02-01', NULL)");
        $this->pdo->exec("INSERT INTO sl_tv_product_prices VALUES (4, 2, 24.99, '2025-01-01', '2025-12-31')");
        $this->pdo->exec("INSERT INTO sl_tv_product_prices VALUES (5, 2, 29.99, '2026-01-01', NULL)");
        $this->pdo->exec("INSERT INTO sl_tv_product_prices VALUES (6, 3, 4.99,  '2025-01-01', '2025-03-31')");
        $this->pdo->exec("INSERT INTO sl_tv_product_prices VALUES (7, 3, 5.99,  '2025-04-01', '2025-12-31')");
        $this->pdo->exec("INSERT INTO sl_tv_product_prices VALUES (8, 3, 6.99,  '2026-01-01', NULL)");
    }

    /**
     * Join products with prices WHERE effective_to IS NULL to get current prices.
     */
    public function testCurrentPrices(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.name, pp.price
             FROM sl_tv_products p
             JOIN sl_tv_product_prices pp ON pp.product_id = p.id
             WHERE pp.effective_to IS NULL
             ORDER BY p.id"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Widget', $rows[0]['name']);
        $this->assertEqualsWithDelta(14.99, (float) $rows[0]['price'], 0.001);
        $this->assertSame('Gadget', $rows[1]['name']);
        $this->assertEqualsWithDelta(29.99, (float) $rows[1]['price'], 0.001);
        $this->assertSame('Gizmo', $rows[2]['name']);
        $this->assertEqualsWithDelta(6.99, (float) $rows[2]['price'], 0.001);
    }

    /**
     * Lookup price at a specific date using effective date range comparison.
     */
    public function testPriceAtDate(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.name, pp.price
             FROM sl_tv_products p
             JOIN sl_tv_product_prices pp ON pp.product_id = p.id
             WHERE pp.effective_from <= '2025-09-15'
               AND (pp.effective_to >= '2025-09-15' OR pp.effective_to IS NULL)
             ORDER BY p.id"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Widget', $rows[0]['name']);
        $this->assertEqualsWithDelta(12.99, (float) $rows[0]['price'], 0.001);
        $this->assertSame('Gadget', $rows[1]['name']);
        $this->assertEqualsWithDelta(24.99, (float) $rows[1]['price'], 0.001);
        $this->assertSame('Gizmo', $rows[2]['name']);
        $this->assertEqualsWithDelta(5.99, (float) $rows[2]['price'], 0.001);
    }

    /**
     * All prices for Widget ordered by effective_from.
     */
    public function testPriceHistory(): void
    {
        $rows = $this->ztdQuery(
            "SELECT pp.price, pp.effective_from, pp.effective_to
             FROM sl_tv_product_prices pp
             WHERE pp.product_id = 1
             ORDER BY pp.effective_from"
        );

        $this->assertCount(3, $rows);
        $this->assertEqualsWithDelta(9.99, (float) $rows[0]['price'], 0.001);
        $this->assertSame('2025-01-01', $rows[0]['effective_from']);
        $this->assertSame('2025-06-30', $rows[0]['effective_to']);
        $this->assertEqualsWithDelta(12.99, (float) $rows[1]['price'], 0.001);
        $this->assertSame('2025-07-01', $rows[1]['effective_from']);
        $this->assertSame('2026-01-31', $rows[1]['effective_to']);
        $this->assertEqualsWithDelta(14.99, (float) $rows[2]['price'], 0.001);
        $this->assertSame('2026-02-01', $rows[2]['effective_from']);
        $this->assertNull($rows[2]['effective_to']);
    }

    /**
     * COUNT price versions per product using GROUP BY.
     */
    public function testPriceChangeCount(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.name, COUNT(*) AS version_count
             FROM sl_tv_products p
             JOIN sl_tv_product_prices pp ON pp.product_id = p.id
             GROUP BY p.id, p.name
             ORDER BY p.id"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Widget', $rows[0]['name']);
        $this->assertEqualsWithDelta(3, (int) $rows[0]['version_count'], 0);
        $this->assertSame('Gadget', $rows[1]['name']);
        $this->assertEqualsWithDelta(2, (int) $rows[1]['version_count'], 0);
        $this->assertSame('Gizmo', $rows[2]['name']);
        $this->assertEqualsWithDelta(3, (int) $rows[2]['version_count'], 0);
    }

    /**
     * Compare current vs first price: ((current - first) / first * 100), rounded to 2 decimal places.
     */
    public function testPriceIncreasePct(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.name,
                    ROUND((curr.price - first.price) / first.price * 100, 2) AS increase_pct
             FROM sl_tv_products p
             JOIN sl_tv_product_prices curr ON curr.product_id = p.id AND curr.effective_to IS NULL
             JOIN sl_tv_product_prices first ON first.product_id = p.id
               AND first.effective_from = (
                   SELECT MIN(pp2.effective_from)
                   FROM sl_tv_product_prices pp2
                   WHERE pp2.product_id = p.id
               )
             ORDER BY p.id"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Widget', $rows[0]['name']);
        $this->assertEqualsWithDelta(50.05, (float) $rows[0]['increase_pct'], 0.01);
        $this->assertSame('Gadget', $rows[1]['name']);
        $this->assertEqualsWithDelta(20.01, (float) $rows[1]['increase_pct'], 0.01);
        $this->assertSame('Gizmo', $rows[2]['name']);
        $this->assertEqualsWithDelta(40.08, (float) $rows[2]['increase_pct'], 0.01);
    }

    /**
     * INSERT new price, UPDATE old current price's effective_to, verify new current price.
     */
    public function testUpdateCurrentPrice(): void
    {
        // Close out current Widget price
        $this->pdo->exec(
            "UPDATE sl_tv_product_prices SET effective_to = '2026-03-08' WHERE id = 3"
        );

        // Insert new current price for Widget
        $this->pdo->exec(
            "INSERT INTO sl_tv_product_prices VALUES (9, 1, 16.99, '2026-03-09', NULL)"
        );

        // Verify new current price
        $rows = $this->ztdQuery(
            "SELECT p.name, pp.price
             FROM sl_tv_products p
             JOIN sl_tv_product_prices pp ON pp.product_id = p.id
             WHERE pp.effective_to IS NULL AND p.id = 1"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Widget', $rows[0]['name']);
        $this->assertEqualsWithDelta(16.99, (float) $rows[0]['price'], 0.001);

        // Verify Widget now has 4 price versions
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt FROM sl_tv_product_prices WHERE product_id = 1"
        );
        $this->assertEqualsWithDelta(4, (int) $rows[0]['cnt'], 0);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO sl_tv_product_prices VALUES (9, 1, 16.99, '2026-03-09', NULL)");
        $this->pdo->exec("UPDATE sl_tv_products SET name = 'Widget Pro' WHERE id = 1");

        // ZTD sees changes
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_tv_product_prices");
        $this->assertEquals(9, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT name FROM sl_tv_products WHERE id = 1");
        $this->assertSame('Widget Pro', $rows[0]['name']);

        // Physical tables untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_tv_product_prices")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
