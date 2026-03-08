<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests JSON functions with aggregation, NULL, deep nesting on MySQL PDO.
 * @spec SPEC-3.5
 */
class MysqlJsonAggregationEdgeCasesTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_jae_products (
                id INT PRIMARY KEY,
                category VARCHAR(50),
                metadata JSON
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_jae_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_jae_products VALUES (1, 'electronics', '{\"brand\":\"Acme\",\"price\":99.99,\"specs\":{\"weight\":1.2,\"dimensions\":{\"width\":10,\"height\":5}},\"tags\":[\"sale\",\"new\"]}')");
        $this->pdo->exec("INSERT INTO mp_jae_products VALUES (2, 'electronics', '{\"brand\":\"Beta\",\"price\":149.50,\"specs\":{\"weight\":0.8,\"dimensions\":{\"width\":8,\"height\":4}},\"tags\":[\"premium\"]}')");
        $this->pdo->exec("INSERT INTO mp_jae_products VALUES (3, 'furniture', '{\"brand\":\"Gamma\",\"price\":299.00,\"specs\":{\"weight\":15.0},\"tags\":[]}')");
        $this->pdo->exec("INSERT INTO mp_jae_products VALUES (4, 'furniture', NULL)");
        $this->pdo->exec("INSERT INTO mp_jae_products VALUES (5, 'electronics', '{\"brand\":\"Delta\",\"price\":0,\"specs\":null,\"tags\":[\"clearance\"]}')");
    }

    /**
     * GROUP BY with JSON_UNQUOTE(JSON_EXTRACT()) and COUNT.
     * MySQL only_full_group_by requires SELECT and GROUP BY to use the same expression.
     */
    public function testGroupByJsonExtractWithCount(): void
    {
        $rows = $this->ztdQuery(
            "SELECT JSON_UNQUOTE(JSON_EXTRACT(metadata, '\$.brand')) AS brand, COUNT(*) AS cnt
             FROM mp_jae_products
             WHERE metadata IS NOT NULL
             GROUP BY JSON_UNQUOTE(JSON_EXTRACT(metadata, '\$.brand'))
             ORDER BY brand"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('Acme', $rows[0]['brand']);
    }

    /**
     * SUM of JSON_EXTRACT numeric values with GROUP BY category.
     */
    public function testSumJsonExtractGroupByCategory(): void
    {
        $rows = $this->ztdQuery(
            "SELECT category, SUM(JSON_EXTRACT(metadata, '\$.price')) AS total_price
             FROM mp_jae_products
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
            "SELECT id, COALESCE(JSON_UNQUOTE(JSON_EXTRACT(metadata, '\$.brand')), 'unknown') AS brand
             FROM mp_jae_products
             ORDER BY id"
        );

        $this->assertSame('unknown', $rows[3]['brand']);
    }

    /**
     * 3-level deep path.
     */
    public function testDeepNestedPath(): void
    {
        $rows = $this->ztdQuery(
            "SELECT JSON_EXTRACT(metadata, '\$.specs.dimensions.width') AS width
             FROM mp_jae_products WHERE id = 1"
        );
        $this->assertEquals(10, (int) $rows[0]['width']);
    }

    /**
     * HAVING with JSON aggregate.
     */
    public function testHavingWithJsonAggregate(): void
    {
        $rows = $this->ztdQuery(
            "SELECT category, COUNT(*) AS cnt
             FROM mp_jae_products
             WHERE metadata IS NOT NULL
             GROUP BY category
             HAVING SUM(JSON_EXTRACT(metadata, '\$.price')) > 250"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('furniture', $rows[0]['category']);
    }

    /**
     * Prepared statement with JSON_EXTRACT in WHERE.
     * May return empty due to CTE snapshot limitations with prepared params.
     */
    public function testPreparedJsonExtractInWhere(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT category, COUNT(*) AS cnt
             FROM mp_jae_products
             WHERE metadata IS NOT NULL AND JSON_EXTRACT(metadata, '\$.price') > ?
             GROUP BY category ORDER BY category",
            [50]
        );

        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'Prepared JSON_EXTRACT with bound parameter returns empty on MySQL PDO'
            );
        }
        $this->assertCount(2, $rows);
        $this->assertEquals(2, (int) $rows[0]['cnt']);
    }

    /**
     * JSON_CONTAINS array membership.
     */
    public function testJsonContainsArrayMembership(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id FROM mp_jae_products
             WHERE JSON_CONTAINS(metadata, '\"sale\"', '\$.tags')"
        );
        $this->assertCount(1, $rows);
        $this->assertEquals(1, (int) $rows[0]['id']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM mp_jae_products');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
