<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests JSONB functions with aggregation, NULL, deep nesting on PostgreSQL.
 * @spec SPEC-3.5
 */
class PostgresJsonbAggregationEdgeCasesTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE pg_jae_products (
                id SERIAL PRIMARY KEY,
                category VARCHAR(50),
                metadata JSONB
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_jae_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_jae_products VALUES (1, 'electronics', '{\"brand\":\"Acme\",\"price\":99.99,\"specs\":{\"weight\":1.2,\"dimensions\":{\"width\":10,\"height\":5}},\"tags\":[\"sale\",\"new\"]}')");
        $this->pdo->exec("INSERT INTO pg_jae_products VALUES (2, 'electronics', '{\"brand\":\"Beta\",\"price\":149.50,\"specs\":{\"weight\":0.8,\"dimensions\":{\"width\":8,\"height\":4}},\"tags\":[\"premium\"]}')");
        $this->pdo->exec("INSERT INTO pg_jae_products VALUES (3, 'furniture', '{\"brand\":\"Gamma\",\"price\":299.00,\"specs\":{\"weight\":15.0},\"tags\":[]}')");
        $this->pdo->exec("INSERT INTO pg_jae_products VALUES (4, 'furniture', NULL)");
        $this->pdo->exec("INSERT INTO pg_jae_products VALUES (5, 'electronics', '{\"brand\":\"Delta\",\"price\":0,\"specs\":null,\"tags\":[\"clearance\"]}')");
    }

    /**
     * GROUP BY with JSONB extraction and COUNT.
     */
    public function testGroupByJsonbExtractWithCount(): void
    {
        $rows = $this->ztdQuery(
            "SELECT metadata->>'brand' AS brand, COUNT(*) AS cnt
             FROM pg_jae_products
             WHERE metadata IS NOT NULL
             GROUP BY metadata->>'brand'
             ORDER BY brand"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('Acme', $rows[0]['brand']);
        $this->assertEquals(1, (int) $rows[0]['cnt']);
    }

    /**
     * SUM of JSONB numeric values with GROUP BY.
     */
    public function testSumJsonbExtractGroupByCategory(): void
    {
        $rows = $this->ztdQuery(
            "SELECT category, SUM((metadata->>'price')::numeric) AS total_price
             FROM pg_jae_products
             WHERE metadata IS NOT NULL
             GROUP BY category
             ORDER BY category"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('electronics', $rows[0]['category']);
        $this->assertEqualsWithDelta(249.49, (float) $rows[0]['total_price'], 0.01);
    }

    /**
     * COALESCE with NULL metadata.
     */
    public function testCoalesceWithNullMetadata(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, COALESCE(metadata->>'brand', 'unknown') AS brand
             FROM pg_jae_products
             ORDER BY id"
        );

        $this->assertSame('unknown', $rows[3]['brand']);
    }

    /**
     * Deep nested path via #>> operator.
     */
    public function testDeepNestedPathExtraction(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, (metadata #>> '{specs,dimensions,width}')::int AS width
             FROM pg_jae_products
             WHERE id IN (1, 2)
             ORDER BY id"
        );

        $this->assertCount(2, $rows);
        $this->assertEquals(10, (int) $rows[0]['width']);
        $this->assertEquals(8, (int) $rows[1]['width']);
    }

    /**
     * jsonb_agg with GROUP BY.
     */
    public function testJsonbAggWithGroupBy(): void
    {
        $rows = $this->ztdQuery(
            "SELECT category, jsonb_agg(metadata->>'brand') AS brands
             FROM pg_jae_products
             WHERE metadata IS NOT NULL
             GROUP BY category
             ORDER BY category"
        );

        $this->assertCount(2, $rows);
        $electronicsbrands = json_decode($rows[0]['brands'], true);
        $this->assertCount(3, $electronicsbrands);
        $this->assertContains('Acme', $electronicsbrands);
    }

    /**
     * @> containment operator.
     */
    public function testContainmentOperator(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id FROM pg_jae_products
             WHERE metadata @> '{\"brand\": \"Acme\"}'
             ORDER BY id"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(1, (int) $rows[0]['id']);
    }

    /**
     * HAVING with JSONB aggregate.
     */
    public function testHavingWithJsonbAggregate(): void
    {
        $rows = $this->ztdQuery(
            "SELECT category, COUNT(*) AS cnt
             FROM pg_jae_products
             WHERE metadata IS NOT NULL
             GROUP BY category
             HAVING SUM((metadata->>'price')::numeric) > 250"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('furniture', $rows[0]['category']);
    }

    /**
     * jsonb_extract_path_text for missing path.
     */
    public function testJsonbExtractPathTextMissingPath(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, jsonb_extract_path_text(metadata, 'specs', 'dimensions', 'depth') AS depth
             FROM pg_jae_products
             WHERE id = 1"
        );

        $this->assertCount(1, $rows);
        $this->assertNull($rows[0]['depth']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_jae_products');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
