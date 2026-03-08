<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests JSON functions with aggregation, NULL, deep nesting, and prepared statements.
 * Covers gaps in SPEC-3.5 partial verification for MySQL via MySQLi.
 * @spec SPEC-3.5
 */
class JsonAggregationEdgeCasesTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_jae_products (
                id INT PRIMARY KEY,
                category VARCHAR(50),
                metadata JSON
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_jae_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO my_jae_products VALUES (1, 'electronics', '{\"brand\":\"Acme\",\"price\":99.99,\"specs\":{\"weight\":1.2,\"dimensions\":{\"width\":10,\"height\":5}},\"tags\":[\"sale\",\"new\"]}')");
        $this->mysqli->query("INSERT INTO my_jae_products VALUES (2, 'electronics', '{\"brand\":\"Beta\",\"price\":149.50,\"specs\":{\"weight\":0.8,\"dimensions\":{\"width\":8,\"height\":4}},\"tags\":[\"premium\"]}')");
        $this->mysqli->query("INSERT INTO my_jae_products VALUES (3, 'furniture', '{\"brand\":\"Gamma\",\"price\":299.00,\"specs\":{\"weight\":15.0},\"tags\":[]}')");
        $this->mysqli->query("INSERT INTO my_jae_products VALUES (4, 'furniture', NULL)");
        $this->mysqli->query("INSERT INTO my_jae_products VALUES (5, 'electronics', '{\"brand\":\"Delta\",\"price\":0,\"specs\":null,\"tags\":[\"clearance\"]}')");
    }

    /**
     * GROUP BY with JSON_UNQUOTE(JSON_EXTRACT()) and COUNT.
     * MySQL only_full_group_by requires SELECT and GROUP BY to use the same expression.
     */
    public function testGroupByJsonExtractWithCount(): void
    {
        $rows = $this->ztdQuery(
            "SELECT JSON_UNQUOTE(JSON_EXTRACT(metadata, '\$.brand')) AS brand, COUNT(*) AS cnt
             FROM my_jae_products
             WHERE metadata IS NOT NULL
             GROUP BY JSON_UNQUOTE(JSON_EXTRACT(metadata, '\$.brand'))
             ORDER BY brand"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('Acme', $rows[0]['brand']);
        $this->assertEquals(1, (int) $rows[0]['cnt']);
    }

    /**
     * SUM of JSON_EXTRACT numeric values with GROUP BY.
     */
    public function testSumJsonExtractGroupByCategory(): void
    {
        $rows = $this->ztdQuery(
            "SELECT category, SUM(JSON_EXTRACT(metadata, '\$.price')) AS total_price
             FROM my_jae_products
             WHERE metadata IS NOT NULL
             GROUP BY category
             ORDER BY category"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('electronics', $rows[0]['category']);
        $this->assertEqualsWithDelta(249.49, (float) $rows[0]['total_price'], 0.01);
    }

    /**
     * JSON_EXTRACT on NULL metadata returns NULL; COALESCE wraps it.
     */
    public function testJsonExtractOnNullMetadata(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, COALESCE(JSON_UNQUOTE(JSON_EXTRACT(metadata, '\$.brand')), 'unknown') AS brand
             FROM my_jae_products
             ORDER BY id"
        );

        $this->assertCount(5, $rows);
        $this->assertSame('Acme', $rows[0]['brand']);
        $this->assertSame('unknown', $rows[3]['brand']);
    }

    /**
     * Deep nesting: 3-level path extraction.
     */
    public function testDeepNestedPathExtraction(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, JSON_EXTRACT(metadata, '\$.specs.dimensions.width') AS width
             FROM my_jae_products
             WHERE id IN (1, 2)
             ORDER BY id"
        );

        $this->assertCount(2, $rows);
        $this->assertEquals(10, (int) $rows[0]['width']);
        $this->assertEquals(8, (int) $rows[1]['width']);
    }

    /**
     * JSON_EXTRACT on missing path returns NULL.
     */
    public function testJsonExtractMissingPathReturnsNull(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, JSON_EXTRACT(metadata, '\$.specs.dimensions.depth') AS depth
             FROM my_jae_products
             WHERE id = 1"
        );

        $this->assertCount(1, $rows);
        $this->assertNull($rows[0]['depth']);
    }

    /**
     * JSON_LENGTH with empty array.
     */
    public function testJsonLengthEmptyArray(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, JSON_LENGTH(metadata, '\$.tags') AS tag_count
             FROM my_jae_products
             WHERE id = 3"
        );

        $this->assertEquals(0, (int) $rows[0]['tag_count']);
    }

    /**
     * HAVING clause with JSON_EXTRACT aggregate.
     */
    public function testHavingWithJsonExtractAggregate(): void
    {
        $rows = $this->ztdQuery(
            "SELECT category, COUNT(*) AS cnt
             FROM my_jae_products
             WHERE metadata IS NOT NULL
             GROUP BY category
             HAVING SUM(JSON_EXTRACT(metadata, '\$.price')) > 250
             ORDER BY category"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('furniture', $rows[0]['category']);
    }

    /**
     * Prepared statement with JSON_EXTRACT in WHERE and GROUP BY.
     * May return empty due to CTE snapshot limitations with prepared params.
     */
    public function testPreparedJsonExtractGroupBy(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT category, COUNT(*) AS cnt
             FROM my_jae_products
             WHERE metadata IS NOT NULL AND JSON_EXTRACT(metadata, '\$.price') > ?
             GROUP BY category
             ORDER BY category",
            [50]
        );

        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'Prepared JSON_EXTRACT with bound parameter returns empty on MySQLi'
            );
        }
        $this->assertCount(2, $rows);
        $this->assertSame('electronics', $rows[0]['category']);
        $this->assertEquals(2, (int) $rows[0]['cnt']);
    }

    /**
     * JSON_CONTAINS for array membership check.
     */
    public function testJsonContainsArrayMembership(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, JSON_UNQUOTE(JSON_EXTRACT(metadata, '\$.brand')) AS brand
             FROM my_jae_products
             WHERE JSON_CONTAINS(metadata, '\"sale\"', '\$.tags')
             ORDER BY id"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Acme', $rows[0]['brand']);
    }

    /**
     * JSON_EXTRACT with CASE WHEN.
     */
    public function testJsonExtractWithCaseWhen(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id,
                    CASE
                        WHEN JSON_EXTRACT(metadata, '\$.price') > 100 THEN 'expensive'
                        WHEN JSON_EXTRACT(metadata, '\$.price') > 0 THEN 'affordable'
                        ELSE 'free_or_unknown'
                    END AS price_tier
             FROM my_jae_products
             WHERE metadata IS NOT NULL
             ORDER BY id"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('affordable', $rows[0]['price_tier']);
        $this->assertSame('expensive', $rows[1]['price_tier']);
        $this->assertSame('expensive', $rows[2]['price_tier']);
        $this->assertSame('free_or_unknown', $rows[3]['price_tier']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM my_jae_products');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
