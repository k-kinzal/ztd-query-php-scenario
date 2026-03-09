<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests a dynamic pricing scenario: correlated MAX subquery for current effective price,
 * CASE for discount tier calculation, JOIN for competitor price comparison,
 * and GROUP BY with HAVING for price anomaly detection (PostgreSQL PDO).
 * @spec SPEC-10.2.143
 */
class PostgresDynamicPricingTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_dp_products (
                id SERIAL PRIMARY KEY,
                name TEXT,
                category TEXT
            )',
            'CREATE TABLE pg_dp_price_history (
                id SERIAL PRIMARY KEY,
                product_id INTEGER,
                price NUMERIC(10,2),
                effective_date TEXT
            )',
            'CREATE TABLE pg_dp_competitor_prices (
                id SERIAL PRIMARY KEY,
                product_id INTEGER,
                competitor_name TEXT,
                price NUMERIC(10,2),
                observed_date TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_dp_competitor_prices', 'pg_dp_price_history', 'pg_dp_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Products
        $this->pdo->exec("INSERT INTO pg_dp_products VALUES (1, 'Widget A', 'Electronics')");
        $this->pdo->exec("INSERT INTO pg_dp_products VALUES (2, 'Gadget B', 'Electronics')");
        $this->pdo->exec("INSERT INTO pg_dp_products VALUES (3, 'Tool C', 'Hardware')");

        // Price history
        $this->pdo->exec("INSERT INTO pg_dp_price_history VALUES (1, 1, 29.99, '2025-01-01')");
        $this->pdo->exec("INSERT INTO pg_dp_price_history VALUES (2, 1, 24.99, '2025-03-01')");
        $this->pdo->exec("INSERT INTO pg_dp_price_history VALUES (3, 1, 27.99, '2025-06-01')");
        $this->pdo->exec("INSERT INTO pg_dp_price_history VALUES (4, 2, 49.99, '2025-01-01')");
        $this->pdo->exec("INSERT INTO pg_dp_price_history VALUES (5, 2, 44.99, '2025-04-01')");
        $this->pdo->exec("INSERT INTO pg_dp_price_history VALUES (6, 2, 39.99, '2025-07-01')");
        $this->pdo->exec("INSERT INTO pg_dp_price_history VALUES (7, 3, 19.99, '2025-01-01')");
        $this->pdo->exec("INSERT INTO pg_dp_price_history VALUES (8, 3, 22.99, '2025-05-01')");

        // Competitor prices
        $this->pdo->exec("INSERT INTO pg_dp_competitor_prices VALUES (1, 1, 'CompetitorX', 26.99, '2025-06-15')");
        $this->pdo->exec("INSERT INTO pg_dp_competitor_prices VALUES (2, 1, 'CompetitorY', 28.99, '2025-06-15')");
        $this->pdo->exec("INSERT INTO pg_dp_competitor_prices VALUES (3, 2, 'CompetitorX', 42.99, '2025-07-15')");
        $this->pdo->exec("INSERT INTO pg_dp_competitor_prices VALUES (4, 2, 'CompetitorY', 37.99, '2025-07-15')");
        $this->pdo->exec("INSERT INTO pg_dp_competitor_prices VALUES (5, 3, 'CompetitorX', 21.99, '2025-05-15')");
        $this->pdo->exec("INSERT INTO pg_dp_competitor_prices VALUES (6, 3, 'CompetitorY', 24.99, '2025-05-15')");
    }

    /**
     * Correlated MAX subquery to get the latest price for each product.
     * Widget A: 27.99 (2025-06-01), Gadget B: 39.99 (2025-07-01), Tool C: 22.99 (2025-05-01).
     */
    public function testCurrentPricePerProduct(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.name, ph.price, ph.effective_date
             FROM pg_dp_products p
             JOIN pg_dp_price_history ph ON ph.product_id = p.id
                 AND ph.effective_date = (
                     SELECT MAX(ph2.effective_date)
                     FROM pg_dp_price_history ph2
                     WHERE ph2.product_id = p.id
                 )
             ORDER BY p.name"
        );

        $this->assertCount(3, $rows);

        // Gadget B: 39.99 (2025-07-01)
        $this->assertSame('Gadget B', $rows[0]['name']);
        $this->assertEqualsWithDelta(39.99, (float) $rows[0]['price'], 0.001);
        $this->assertSame('2025-07-01', $rows[0]['effective_date']);

        // Tool C: 22.99 (2025-05-01)
        $this->assertSame('Tool C', $rows[1]['name']);
        $this->assertEqualsWithDelta(22.99, (float) $rows[1]['price'], 0.001);
        $this->assertSame('2025-05-01', $rows[1]['effective_date']);

        // Widget A: 27.99 (2025-06-01)
        $this->assertSame('Widget A', $rows[2]['name']);
        $this->assertEqualsWithDelta(27.99, (float) $rows[2]['price'], 0.001);
        $this->assertSame('2025-06-01', $rows[2]['effective_date']);
    }

    /**
     * For Widget A (product_id=1), show all price changes ordered by effective_date.
     * 3 rows: 29.99 (Jan), 24.99 (Mar), 27.99 (Jun).
     */
    public function testPriceChangeHistory(): void
    {
        $rows = $this->ztdQuery(
            "SELECT ph.price, ph.effective_date
             FROM pg_dp_price_history ph
             WHERE ph.product_id = 1
             ORDER BY ph.effective_date"
        );

        $this->assertCount(3, $rows);

        $this->assertEqualsWithDelta(29.99, (float) $rows[0]['price'], 0.001);
        $this->assertSame('2025-01-01', $rows[0]['effective_date']);

        $this->assertEqualsWithDelta(24.99, (float) $rows[1]['price'], 0.001);
        $this->assertSame('2025-03-01', $rows[1]['effective_date']);

        $this->assertEqualsWithDelta(27.99, (float) $rows[2]['price'], 0.001);
        $this->assertSame('2025-06-01', $rows[2]['effective_date']);
    }

    /**
     * CASE to classify current price into tiers: < 25 = 'Budget', 25-39.99 = 'Standard', >= 40 = 'Premium'.
     * Widget A: 27.99 = Standard, Gadget B: 39.99 = Standard, Tool C: 22.99 = Budget.
     */
    public function testDiscountTierClassification(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.name, ph.price,
                    CASE
                        WHEN ph.price < 25 THEN 'Budget'
                        WHEN ph.price < 40 THEN 'Standard'
                        ELSE 'Premium'
                    END AS tier
             FROM pg_dp_products p
             JOIN pg_dp_price_history ph ON ph.product_id = p.id
                 AND ph.effective_date = (
                     SELECT MAX(ph2.effective_date)
                     FROM pg_dp_price_history ph2
                     WHERE ph2.product_id = p.id
                 )
             ORDER BY p.name"
        );

        $this->assertCount(3, $rows);

        // Gadget B: 39.99 = Standard
        $this->assertSame('Gadget B', $rows[0]['name']);
        $this->assertEqualsWithDelta(39.99, (float) $rows[0]['price'], 0.001);
        $this->assertSame('Standard', $rows[0]['tier']);

        // Tool C: 22.99 = Budget
        $this->assertSame('Tool C', $rows[1]['name']);
        $this->assertEqualsWithDelta(22.99, (float) $rows[1]['price'], 0.001);
        $this->assertSame('Budget', $rows[1]['tier']);

        // Widget A: 27.99 = Standard
        $this->assertSame('Widget A', $rows[2]['name']);
        $this->assertEqualsWithDelta(27.99, (float) $rows[2]['price'], 0.001);
        $this->assertSame('Standard', $rows[2]['tier']);
    }

    /**
     * JOIN products with latest own price and competitor_prices.
     * Calculate difference = ROUND(own price - competitor price, 2).
     * Order by product name, competitor name.
     */
    public function testCompetitorComparison(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.name, comp.competitor_name, ph.price AS own_price,
                    comp.price AS competitor_price,
                    ROUND(ph.price - comp.price, 2) AS difference
             FROM pg_dp_products p
             JOIN pg_dp_price_history ph ON ph.product_id = p.id
                 AND ph.effective_date = (
                     SELECT MAX(ph2.effective_date)
                     FROM pg_dp_price_history ph2
                     WHERE ph2.product_id = p.id
                 )
             JOIN pg_dp_competitor_prices comp ON comp.product_id = p.id
             ORDER BY p.name, comp.competitor_name"
        );

        $this->assertCount(6, $rows);

        // Gadget B vs CompetitorX: 39.99 - 42.99 = -3.00
        $this->assertSame('Gadget B', $rows[0]['name']);
        $this->assertSame('CompetitorX', $rows[0]['competitor_name']);
        $this->assertEqualsWithDelta(39.99, (float) $rows[0]['own_price'], 0.001);
        $this->assertEqualsWithDelta(42.99, (float) $rows[0]['competitor_price'], 0.001);
        $this->assertEqualsWithDelta(-3.00, (float) $rows[0]['difference'], 0.01);

        // Gadget B vs CompetitorY: 39.99 - 37.99 = 2.00
        $this->assertSame('Gadget B', $rows[1]['name']);
        $this->assertSame('CompetitorY', $rows[1]['competitor_name']);
        $this->assertEqualsWithDelta(2.00, (float) $rows[1]['difference'], 0.01);

        // Tool C vs CompetitorX: 22.99 - 21.99 = 1.00
        $this->assertSame('Tool C', $rows[2]['name']);
        $this->assertSame('CompetitorX', $rows[2]['competitor_name']);
        $this->assertEqualsWithDelta(1.00, (float) $rows[2]['difference'], 0.01);

        // Tool C vs CompetitorY: 22.99 - 24.99 = -2.00
        $this->assertSame('Tool C', $rows[3]['name']);
        $this->assertSame('CompetitorY', $rows[3]['competitor_name']);
        $this->assertEqualsWithDelta(-2.00, (float) $rows[3]['difference'], 0.01);

        // Widget A vs CompetitorX: 27.99 - 26.99 = 1.00
        $this->assertSame('Widget A', $rows[4]['name']);
        $this->assertSame('CompetitorX', $rows[4]['competitor_name']);
        $this->assertEqualsWithDelta(1.00, (float) $rows[4]['difference'], 0.01);

        // Widget A vs CompetitorY: 27.99 - 28.99 = -1.00
        $this->assertSame('Widget A', $rows[5]['name']);
        $this->assertSame('CompetitorY', $rows[5]['competitor_name']);
        $this->assertEqualsWithDelta(-1.00, (float) $rows[5]['difference'], 0.01);
    }

    /**
     * GROUP BY category, show MIN and MAX of current prices using a derived table.
     * Electronics: min=27.99, max=39.99. Hardware: min=22.99, max=22.99.
     */
    public function testCategoryPriceRange(): void
    {
        $rows = $this->ztdQuery(
            "SELECT category, MIN(current_price) AS min_price, MAX(current_price) AS max_price
             FROM (
                 SELECT p.category, ph.price AS current_price
                 FROM pg_dp_products p
                 JOIN pg_dp_price_history ph ON ph.product_id = p.id
                     AND ph.effective_date = (
                         SELECT MAX(ph2.effective_date)
                         FROM pg_dp_price_history ph2
                         WHERE ph2.product_id = p.id
                     )
             ) AS sub
             GROUP BY category
             ORDER BY category"
        );

        $this->assertCount(2, $rows);

        // Electronics: min=27.99, max=39.99
        $this->assertSame('Electronics', $rows[0]['category']);
        $this->assertEqualsWithDelta(27.99, (float) $rows[0]['min_price'], 0.001);
        $this->assertEqualsWithDelta(39.99, (float) $rows[0]['max_price'], 0.001);

        // Hardware: min=22.99, max=22.99
        $this->assertSame('Hardware', $rows[1]['category']);
        $this->assertEqualsWithDelta(22.99, (float) $rows[1]['min_price'], 0.001);
        $this->assertEqualsWithDelta(22.99, (float) $rows[1]['max_price'], 0.001);
    }

    /**
     * Physical isolation: insert a new price_history entry, verify ZTD count = 9,
     * verify physical table empty.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_dp_price_history VALUES (9, 1, 25.99, '2025-08-01')");

        // ZTD sees the new entry
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_dp_price_history");
        $this->assertEquals(9, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_dp_price_history")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
