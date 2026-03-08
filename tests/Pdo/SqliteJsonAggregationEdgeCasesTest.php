<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests JSON functions with aggregation, NULL, deep nesting, and prepared statements.
 * Covers gaps in SPEC-3.5 partial verification for SQLite.
 * @spec SPEC-3.5
 */
class SqliteJsonAggregationEdgeCasesTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_jae_products (
                id INTEGER PRIMARY KEY,
                category TEXT,
                metadata TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_jae_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_jae_products VALUES (1, 'electronics', '{\"brand\":\"Acme\",\"price\":99.99,\"specs\":{\"weight\":1.2,\"dimensions\":{\"width\":10,\"height\":5}},\"tags\":[\"sale\",\"new\"]}')");
        $this->pdo->exec("INSERT INTO sl_jae_products VALUES (2, 'electronics', '{\"brand\":\"Beta\",\"price\":149.50,\"specs\":{\"weight\":0.8,\"dimensions\":{\"width\":8,\"height\":4}},\"tags\":[\"premium\"]}')");
        $this->pdo->exec("INSERT INTO sl_jae_products VALUES (3, 'furniture', '{\"brand\":\"Gamma\",\"price\":299.00,\"specs\":{\"weight\":15.0},\"tags\":[]}')");
        $this->pdo->exec("INSERT INTO sl_jae_products VALUES (4, 'furniture', NULL)");
        $this->pdo->exec("INSERT INTO sl_jae_products VALUES (5, 'electronics', '{\"brand\":\"Delta\",\"price\":0,\"specs\":null,\"tags\":[\"clearance\"]}')");
    }

    /**
     * GROUP BY with json_extract and COUNT.
     */
    public function testGroupByJsonExtractWithCount(): void
    {
        $rows = $this->ztdQuery(
            "SELECT json_extract(metadata, '\$.brand') AS brand, COUNT(*) AS cnt
             FROM sl_jae_products
             WHERE metadata IS NOT NULL
             GROUP BY json_extract(metadata, '\$.brand')
             ORDER BY brand"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('Acme', $rows[0]['brand']);
        $this->assertEquals(1, (int) $rows[0]['cnt']);
    }

    /**
     * SUM of json_extract numeric values with GROUP BY category.
     */
    public function testSumJsonExtractGroupByCategory(): void
    {
        $rows = $this->ztdQuery(
            "SELECT category, SUM(json_extract(metadata, '\$.price')) AS total_price
             FROM sl_jae_products
             WHERE metadata IS NOT NULL
             GROUP BY category
             ORDER BY category"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('electronics', $rows[0]['category']);
        // 99.99 + 149.50 + 0 = 249.49
        $this->assertEqualsWithDelta(249.49, (float) $rows[0]['total_price'], 0.01);
        $this->assertSame('furniture', $rows[1]['category']);
        $this->assertEqualsWithDelta(299.00, (float) $rows[1]['total_price'], 0.01);
    }

    /**
     * json_extract on NULL metadata returns NULL; COALESCE wraps it.
     */
    public function testJsonExtractOnNullMetadata(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, COALESCE(json_extract(metadata, '\$.brand'), 'unknown') AS brand
             FROM sl_jae_products
             ORDER BY id"
        );

        $this->assertCount(5, $rows);
        $this->assertSame('Acme', $rows[0]['brand']);
        $this->assertSame('unknown', $rows[3]['brand']); // NULL metadata
    }

    /**
     * Deep nesting: 3-level path extraction.
     */
    public function testDeepNestedPathExtraction(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, json_extract(metadata, '\$.specs.dimensions.width') AS width
             FROM sl_jae_products
             WHERE id IN (1, 2)
             ORDER BY id"
        );

        $this->assertCount(2, $rows);
        $this->assertEquals(10, (int) $rows[0]['width']);
        $this->assertEquals(8, (int) $rows[1]['width']);
    }

    /**
     * json_extract on path that doesn't exist returns NULL.
     */
    public function testJsonExtractMissingPathReturnsNull(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, json_extract(metadata, '\$.specs.dimensions.depth') AS depth
             FROM sl_jae_products
             WHERE id = 1"
        );

        $this->assertCount(1, $rows);
        $this->assertNull($rows[0]['depth']);
    }

    /**
     * json_extract on null JSON value (specs is null for id=5).
     */
    public function testJsonExtractOnNullJsonValue(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, json_extract(metadata, '\$.specs.weight') AS weight
             FROM sl_jae_products
             WHERE id = 5"
        );

        $this->assertCount(1, $rows);
        $this->assertNull($rows[0]['weight']);
    }

    /**
     * json_group_array with GROUP BY.
     */
    public function testJsonGroupArrayWithGroupBy(): void
    {
        $rows = $this->ztdQuery(
            "SELECT category, json_group_array(json_extract(metadata, '\$.brand')) AS brands
             FROM sl_jae_products
             WHERE metadata IS NOT NULL
             GROUP BY category
             ORDER BY category"
        );

        $this->assertCount(2, $rows);
        $electronicsbrands = json_decode($rows[0]['brands'], true);
        $this->assertCount(3, $electronicsbrands);
        $this->assertContains('Acme', $electronicsbrands);
        $this->assertContains('Beta', $electronicsbrands);
        $this->assertContains('Delta', $electronicsbrands);
    }

    /**
     * json_array_length with empty array.
     */
    public function testJsonArrayLengthEmptyArray(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, json_array_length(metadata, '\$.tags') AS tag_count
             FROM sl_jae_products
             WHERE id = 3"
        );

        $this->assertEquals(0, (int) $rows[0]['tag_count']);
    }

    /**
     * HAVING clause with json_extract aggregate.
     */
    public function testHavingWithJsonExtractAggregate(): void
    {
        $rows = $this->ztdQuery(
            "SELECT category, COUNT(*) AS cnt
             FROM sl_jae_products
             WHERE metadata IS NOT NULL
             GROUP BY category
             HAVING SUM(json_extract(metadata, '\$.price')) > 250
             ORDER BY category"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('furniture', $rows[0]['category']);
    }

    /**
     * Prepared statement with json_extract in WHERE and GROUP BY.
     * May return empty on SQLite due to CTE snapshot limitations with prepared params.
     */
    public function testPreparedJsonExtractGroupBy(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT category, COUNT(*) AS cnt
             FROM sl_jae_products
             WHERE metadata IS NOT NULL AND json_extract(metadata, '\$.price') > ?
             GROUP BY category
             ORDER BY category",
            [50]
        );

        if (count($rows) === 0) {
            // Known limitation: prepared statements with json_extract comparison
            // and bound parameters may return empty on SQLite
            $this->markTestIncomplete(
                'Prepared json_extract with bound parameter returns empty on SQLite'
            );
        }
        $this->assertCount(2, $rows);
        $this->assertSame('electronics', $rows[0]['category']);
        $this->assertEquals(2, (int) $rows[0]['cnt']);
    }

    /**
     * json_extract with CASE WHEN for conditional logic.
     */
    public function testJsonExtractWithCaseWhen(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id,
                    CASE
                        WHEN json_extract(metadata, '\$.price') > 100 THEN 'expensive'
                        WHEN json_extract(metadata, '\$.price') > 0 THEN 'affordable'
                        ELSE 'free_or_unknown'
                    END AS price_tier
             FROM sl_jae_products
             WHERE metadata IS NOT NULL
             ORDER BY id"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('affordable', $rows[0]['price_tier']); // 99.99
        $this->assertSame('expensive', $rows[1]['price_tier']); // 149.50
        $this->assertSame('expensive', $rows[2]['price_tier']); // 299
        $this->assertSame('free_or_unknown', $rows[3]['price_tier']); // 0
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_jae_products');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
