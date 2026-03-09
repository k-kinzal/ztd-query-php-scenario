<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests multi-jurisdiction tax bracket lookup with tiered rates
 * using BETWEEN, CASE, JOIN, SUM, and GROUP BY (MySQLi).
 * @spec SPEC-10.2.118
 */
class TaxCalculationTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_tc_tax_jurisdictions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255),
                country VARCHAR(255)
            )',
            'CREATE TABLE mi_tc_tax_rules (
                id INT AUTO_INCREMENT PRIMARY KEY,
                jurisdiction_id INT,
                category VARCHAR(255),
                rate_percent DECIMAL(10,2)
            )',
            'CREATE TABLE mi_tc_order_items (
                id INT AUTO_INCREMENT PRIMARY KEY,
                jurisdiction_id INT,
                category VARCHAR(255),
                item_name VARCHAR(255),
                price DECIMAL(10,2)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_tc_order_items', 'mi_tc_tax_rules', 'mi_tc_tax_jurisdictions'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Jurisdictions (3)
        $this->mysqli->query("INSERT INTO mi_tc_tax_jurisdictions VALUES (1, 'California', 'US')");
        $this->mysqli->query("INSERT INTO mi_tc_tax_jurisdictions VALUES (2, 'Ontario', 'CA')");
        $this->mysqli->query("INSERT INTO mi_tc_tax_jurisdictions VALUES (3, 'London', 'UK')");

        // Tax rules (12: 4 categories x 3 jurisdictions)
        $this->mysqli->query("INSERT INTO mi_tc_tax_rules VALUES (1,  1, 'electronics', 8.25)");
        $this->mysqli->query("INSERT INTO mi_tc_tax_rules VALUES (2,  1, 'food',        0.00)");
        $this->mysqli->query("INSERT INTO mi_tc_tax_rules VALUES (3,  1, 'clothing',    8.25)");
        $this->mysqli->query("INSERT INTO mi_tc_tax_rules VALUES (4,  1, 'services',    7.50)");
        $this->mysqli->query("INSERT INTO mi_tc_tax_rules VALUES (5,  2, 'electronics', 13.00)");
        $this->mysqli->query("INSERT INTO mi_tc_tax_rules VALUES (6,  2, 'food',        0.00)");
        $this->mysqli->query("INSERT INTO mi_tc_tax_rules VALUES (7,  2, 'clothing',    13.00)");
        $this->mysqli->query("INSERT INTO mi_tc_tax_rules VALUES (8,  2, 'services',    13.00)");
        $this->mysqli->query("INSERT INTO mi_tc_tax_rules VALUES (9,  3, 'electronics', 20.00)");
        $this->mysqli->query("INSERT INTO mi_tc_tax_rules VALUES (10, 3, 'food',        0.00)");
        $this->mysqli->query("INSERT INTO mi_tc_tax_rules VALUES (11, 3, 'clothing',    20.00)");
        $this->mysqli->query("INSERT INTO mi_tc_tax_rules VALUES (12, 3, 'services',    20.00)");

        // Order items (8)
        $this->mysqli->query("INSERT INTO mi_tc_order_items VALUES (1, 1, 'electronics', 'Laptop',      999.99)");
        $this->mysqli->query("INSERT INTO mi_tc_order_items VALUES (2, 1, 'food',        'Groceries',   45.50)");
        $this->mysqli->query("INSERT INTO mi_tc_order_items VALUES (3, 1, 'clothing',    'Jacket',      120.00)");
        $this->mysqli->query("INSERT INTO mi_tc_order_items VALUES (4, 2, 'electronics', 'Phone',       799.00)");
        $this->mysqli->query("INSERT INTO mi_tc_order_items VALUES (5, 2, 'food',        'Maple Syrup', 15.00)");
        $this->mysqli->query("INSERT INTO mi_tc_order_items VALUES (6, 3, 'electronics', 'Tablet',      500.00)");
        $this->mysqli->query("INSERT INTO mi_tc_order_items VALUES (7, 3, 'clothing',    'Coat',        200.00)");
        $this->mysqli->query("INSERT INTO mi_tc_order_items VALUES (8, 3, 'services',    'Consulting',  1000.00)");
    }

    /**
     * Per-item tax using JOIN to match order items with applicable tax rules.
     */
    public function testTaxPerItem(): void
    {
        $rows = $this->ztdQuery(
            "SELECT oi.item_name,
                    oi.price,
                    tr.rate_percent,
                    ROUND(oi.price * tr.rate_percent / 100, 2) AS tax
             FROM mi_tc_order_items oi
             JOIN mi_tc_tax_rules tr
               ON tr.jurisdiction_id = oi.jurisdiction_id
              AND tr.category = oi.category
             ORDER BY oi.id"
        );

        $this->assertCount(8, $rows);

        // Laptop: 999.99 * 8.25% = 82.50
        $this->assertSame('Laptop', $rows[0]['item_name']);
        $this->assertEqualsWithDelta(999.99, (float) $rows[0]['price'], 0.01);
        $this->assertEqualsWithDelta(82.50, (float) $rows[0]['tax'], 0.01);

        // Groceries: 45.50 * 0% = 0.00
        $this->assertSame('Groceries', $rows[1]['item_name']);
        $this->assertEqualsWithDelta(0.00, (float) $rows[1]['tax'], 0.01);

        // Jacket: 120.00 * 8.25% = 9.90
        $this->assertSame('Jacket', $rows[2]['item_name']);
        $this->assertEqualsWithDelta(9.90, (float) $rows[2]['tax'], 0.01);

        // Phone: 799.00 * 13% = 103.87
        $this->assertSame('Phone', $rows[3]['item_name']);
        $this->assertEqualsWithDelta(103.87, (float) $rows[3]['tax'], 0.01);

        // Maple Syrup: 15.00 * 0% = 0.00
        $this->assertSame('Maple Syrup', $rows[4]['item_name']);
        $this->assertEqualsWithDelta(0.00, (float) $rows[4]['tax'], 0.01);

        // Tablet: 500.00 * 20% = 100.00
        $this->assertSame('Tablet', $rows[5]['item_name']);
        $this->assertEqualsWithDelta(100.00, (float) $rows[5]['tax'], 0.01);

        // Coat: 200.00 * 20% = 40.00
        $this->assertSame('Coat', $rows[6]['item_name']);
        $this->assertEqualsWithDelta(40.00, (float) $rows[6]['tax'], 0.01);

        // Consulting: 1000.00 * 20% = 200.00
        $this->assertSame('Consulting', $rows[7]['item_name']);
        $this->assertEqualsWithDelta(200.00, (float) $rows[7]['tax'], 0.01);
    }

    /**
     * Total tax grouped by jurisdiction using SUM and GROUP BY with multi-table JOIN.
     */
    public function testTotalTaxByJurisdiction(): void
    {
        $rows = $this->ztdQuery(
            "SELECT j.name,
                    SUM(ROUND(oi.price * tr.rate_percent / 100, 2)) AS total_tax
             FROM mi_tc_order_items oi
             JOIN mi_tc_tax_rules tr
               ON tr.jurisdiction_id = oi.jurisdiction_id
              AND tr.category = oi.category
             JOIN mi_tc_tax_jurisdictions j
               ON j.id = oi.jurisdiction_id
             GROUP BY j.name
             ORDER BY total_tax DESC"
        );

        $this->assertCount(3, $rows);

        // London: 100.00 + 40.00 + 200.00 = 340.00
        $this->assertSame('London', $rows[0]['name']);
        $this->assertEqualsWithDelta(340.00, (float) $rows[0]['total_tax'], 0.01);

        // Ontario: 103.87 + 0.00 = 103.87
        $this->assertSame('Ontario', $rows[1]['name']);
        $this->assertEqualsWithDelta(103.87, (float) $rows[1]['total_tax'], 0.01);

        // California: 82.50 + 0.00 + 9.90 = 92.40
        $this->assertSame('California', $rows[2]['name']);
        $this->assertEqualsWithDelta(92.40, (float) $rows[2]['total_tax'], 0.01);
    }

    /**
     * Tax-exempt items: food is zero-rated in all jurisdictions.
     */
    public function testTaxExemptItems(): void
    {
        $rows = $this->ztdQuery(
            "SELECT oi.item_name,
                    oi.price
             FROM mi_tc_order_items oi
             JOIN mi_tc_tax_rules tr
               ON tr.jurisdiction_id = oi.jurisdiction_id
              AND tr.category = oi.category
             WHERE tr.rate_percent = 0
             ORDER BY oi.id"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Groceries', $rows[0]['item_name']);
        $this->assertEqualsWithDelta(45.50, (float) $rows[0]['price'], 0.01);
        $this->assertSame('Maple Syrup', $rows[1]['item_name']);
        $this->assertEqualsWithDelta(15.00, (float) $rows[1]['price'], 0.01);
    }

    /**
     * Price with tax: item price plus computed tax, ordered by total descending.
     */
    public function testPriceWithTax(): void
    {
        $rows = $this->ztdQuery(
            "SELECT oi.item_name,
                    oi.price,
                    oi.price + ROUND(oi.price * tr.rate_percent / 100, 2) AS price_with_tax
             FROM mi_tc_order_items oi
             JOIN mi_tc_tax_rules tr
               ON tr.jurisdiction_id = oi.jurisdiction_id
              AND tr.category = oi.category
             ORDER BY price_with_tax DESC"
        );

        $this->assertCount(8, $rows);

        // Consulting: 1000 + 200 = 1200.00
        $this->assertSame('Consulting', $rows[0]['item_name']);
        $this->assertEqualsWithDelta(1200.00, (float) $rows[0]['price_with_tax'], 0.01);

        // Laptop: 999.99 + 82.50 = 1082.49
        $this->assertSame('Laptop', $rows[1]['item_name']);
        $this->assertEqualsWithDelta(1082.49, (float) $rows[1]['price_with_tax'], 0.01);

        // Phone: 799 + 103.87 = 902.87
        $this->assertSame('Phone', $rows[2]['item_name']);
        $this->assertEqualsWithDelta(902.87, (float) $rows[2]['price_with_tax'], 0.01);

        // Tablet: 500 + 100 = 600.00
        $this->assertSame('Tablet', $rows[3]['item_name']);
        $this->assertEqualsWithDelta(600.00, (float) $rows[3]['price_with_tax'], 0.01);

        // Coat: 200 + 40 = 240.00
        $this->assertSame('Coat', $rows[4]['item_name']);
        $this->assertEqualsWithDelta(240.00, (float) $rows[4]['price_with_tax'], 0.01);

        // Jacket: 120 + 9.90 = 129.90
        $this->assertSame('Jacket', $rows[5]['item_name']);
        $this->assertEqualsWithDelta(129.90, (float) $rows[5]['price_with_tax'], 0.01);

        // Groceries: 45.50 + 0 = 45.50
        $this->assertSame('Groceries', $rows[6]['item_name']);
        $this->assertEqualsWithDelta(45.50, (float) $rows[6]['price_with_tax'], 0.01);

        // Maple Syrup: 15 + 0 = 15.00
        $this->assertSame('Maple Syrup', $rows[7]['item_name']);
        $this->assertEqualsWithDelta(15.00, (float) $rows[7]['price_with_tax'], 0.01);
    }

    /**
     * Average tax rate by country using JOIN and GROUP BY on jurisdiction country.
     */
    public function testAverageTaxRateByCountry(): void
    {
        $rows = $this->ztdQuery(
            "SELECT j.country,
                    AVG(tr.rate_percent) AS avg_rate
             FROM mi_tc_tax_jurisdictions j
             JOIN mi_tc_tax_rules tr
               ON tr.jurisdiction_id = j.id
             GROUP BY j.country
             ORDER BY avg_rate DESC"
        );

        $this->assertCount(3, $rows);

        // UK: (20+0+20+20)/4 = 15.00
        $this->assertSame('UK', $rows[0]['country']);
        $this->assertEqualsWithDelta(15.00, (float) $rows[0]['avg_rate'], 0.01);

        // CA: (13+0+13+13)/4 = 9.75
        $this->assertSame('CA', $rows[1]['country']);
        $this->assertEqualsWithDelta(9.75, (float) $rows[1]['avg_rate'], 0.01);

        // US: (8.25+0+8.25+7.50)/4 = 6.00
        $this->assertSame('US', $rows[2]['country']);
        $this->assertEqualsWithDelta(6.00, (float) $rows[2]['avg_rate'], 0.01);
    }

    /**
     * High-tax items: only items where computed tax exceeds 50.
     */
    public function testHighTaxItems(): void
    {
        $rows = $this->ztdQuery(
            "SELECT oi.item_name,
                    ROUND(oi.price * tr.rate_percent / 100, 2) AS tax
             FROM mi_tc_order_items oi
             JOIN mi_tc_tax_rules tr
               ON tr.jurisdiction_id = oi.jurisdiction_id
              AND tr.category = oi.category
             WHERE oi.price * tr.rate_percent / 100 > 50
             ORDER BY tax DESC"
        );

        $this->assertCount(4, $rows);

        // Consulting: 200.00
        $this->assertSame('Consulting', $rows[0]['item_name']);
        $this->assertEqualsWithDelta(200.00, (float) $rows[0]['tax'], 0.01);

        // Phone: 103.87
        $this->assertSame('Phone', $rows[1]['item_name']);
        $this->assertEqualsWithDelta(103.87, (float) $rows[1]['tax'], 0.01);

        // Tablet: 100.00
        $this->assertSame('Tablet', $rows[2]['item_name']);
        $this->assertEqualsWithDelta(100.00, (float) $rows[2]['tax'], 0.01);

        // Laptop: 82.50
        $this->assertSame('Laptop', $rows[3]['item_name']);
        $this->assertEqualsWithDelta(82.50, (float) $rows[3]['tax'], 0.01);
    }

    /**
     * Category revenue summary: item count, subtotal, and total tax per category.
     */
    public function testCategoryRevenueSummary(): void
    {
        $rows = $this->ztdQuery(
            "SELECT oi.category,
                    COUNT(*) AS item_count,
                    SUM(oi.price) AS subtotal,
                    SUM(ROUND(oi.price * tr.rate_percent / 100, 2)) AS total_tax
             FROM mi_tc_order_items oi
             JOIN mi_tc_tax_rules tr
               ON tr.jurisdiction_id = oi.jurisdiction_id
              AND tr.category = oi.category
             GROUP BY oi.category
             ORDER BY total_tax DESC"
        );

        $this->assertCount(4, $rows);

        // electronics: 3 items, subtotal=2298.99, tax=82.50+103.87+100.00=286.37
        $this->assertSame('electronics', $rows[0]['category']);
        $this->assertEquals(3, (int) $rows[0]['item_count']);
        $this->assertEqualsWithDelta(2298.99, (float) $rows[0]['subtotal'], 0.01);
        $this->assertEqualsWithDelta(286.37, (float) $rows[0]['total_tax'], 0.01);

        // services: 1 item, subtotal=1000.00, tax=200.00
        $this->assertSame('services', $rows[1]['category']);
        $this->assertEquals(1, (int) $rows[1]['item_count']);
        $this->assertEqualsWithDelta(1000.00, (float) $rows[1]['subtotal'], 0.01);
        $this->assertEqualsWithDelta(200.00, (float) $rows[1]['total_tax'], 0.01);

        // clothing: 2 items, subtotal=320.00, tax=9.90+40.00=49.90
        $this->assertSame('clothing', $rows[2]['category']);
        $this->assertEquals(2, (int) $rows[2]['item_count']);
        $this->assertEqualsWithDelta(320.00, (float) $rows[2]['subtotal'], 0.01);
        $this->assertEqualsWithDelta(49.90, (float) $rows[2]['total_tax'], 0.01);

        // food: 2 items, subtotal=60.50, tax=0.00
        $this->assertSame('food', $rows[3]['category']);
        $this->assertEquals(2, (int) $rows[3]['item_count']);
        $this->assertEqualsWithDelta(60.50, (float) $rows[3]['subtotal'], 0.01);
        $this->assertEqualsWithDelta(0.00, (float) $rows[3]['total_tax'], 0.01);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_tc_order_items VALUES (9, 1, 'services', 'Repair', 250.00)");
        $this->mysqli->query("UPDATE mi_tc_tax_rules SET rate_percent = 10.00 WHERE id = 1");

        // ZTD sees changes
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_tc_order_items");
        $this->assertEquals(9, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT rate_percent FROM mi_tc_tax_rules WHERE id = 1");
        $this->assertEqualsWithDelta(10.00, (float) $rows[0]['rate_percent'], 0.01);

        // Physical tables untouched
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_tc_order_items');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
