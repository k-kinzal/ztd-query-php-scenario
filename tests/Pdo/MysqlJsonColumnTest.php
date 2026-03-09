<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests MySQL JSON column operations through CTE shadow store.
 *
 * MySQL's native JSON type is widely used for semi-structured data.
 * The CTE rewriter must preserve JSON type or at least produce values
 * that JSON_EXTRACT(), ->, ->>, JSON_CONTAINS(), JSON_SEARCH(),
 * and JSON_LENGTH() can operate on from the shadow CTE.
 *
 * @spec SPEC-3.5
 */
class MysqlJsonColumnTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE my_jc_products (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                attributes JSON,
                tags JSON
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_jc_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_jc_products VALUES (1, 'Laptop', '{\"brand\": \"Acme\", \"specs\": {\"ram\": 16, \"storage\": 512}, \"price\": 999.99}', '[\"electronics\", \"portable\", \"computer\"]')");
        $this->pdo->exec("INSERT INTO my_jc_products VALUES (2, 'Phone', '{\"brand\": \"Beta\", \"specs\": {\"ram\": 8, \"storage\": 256}, \"price\": 599.99}', '[\"electronics\", \"mobile\"]')");
        $this->pdo->exec("INSERT INTO my_jc_products VALUES (3, 'Desk', '{\"brand\": \"Gamma\", \"material\": \"wood\", \"price\": 299.99}', '[\"furniture\", \"office\"]')");
        $this->pdo->exec("INSERT INTO my_jc_products VALUES (4, 'Chair', '{\"brand\": \"Gamma\", \"material\": \"metal\", \"price\": 199.99}', '[\"furniture\", \"office\", \"ergonomic\"]')");
    }

    /**
     * JSON_EXTRACT() returns a value from a JSON path.
     */
    public function testJsonExtract(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, JSON_EXTRACT(attributes, '$.brand') AS brand
             FROM my_jc_products ORDER BY id"
        );

        $this->assertCount(4, $rows);
        // JSON_EXTRACT returns quoted strings on MySQL
        $this->assertSame('"Acme"', $rows[0]['brand']);
        $this->assertSame('"Beta"', $rows[1]['brand']);
    }

    /**
     * JSON_UNQUOTE(JSON_EXTRACT()) returns unquoted text — common pattern.
     */
    public function testJsonUnquoteExtract(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, JSON_UNQUOTE(JSON_EXTRACT(attributes, '$.brand')) AS brand
             FROM my_jc_products ORDER BY id"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('Acme', $rows[0]['brand']);
        $this->assertSame('Gamma', $rows[2]['brand']);
    }

    /**
     * ->> operator (MySQL 5.7.13+) extracts and unquotes.
     */
    public function testJsonUnquoteArrowOperator(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, attributes->>'$.brand' AS brand
             FROM my_jc_products ORDER BY id"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('Acme', $rows[0]['brand']);
        $this->assertSame('Gamma', $rows[3]['brand']);
    }

    /**
     * -> operator extracts JSON value.
     */
    public function testJsonArrowOperator(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, attributes->'$.specs' AS specs
             FROM my_jc_products WHERE id = 1"
        );

        $this->assertCount(1, $rows);
        $decoded = json_decode($rows[0]['specs'], true);
        $this->assertSame(16, $decoded['ram']);
    }

    /**
     * Nested path extraction with ->> operator.
     */
    public function testJsonNestedPathExtraction(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, attributes->>'$.specs.ram' AS ram
             FROM my_jc_products WHERE id = 1"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(16, (int) $rows[0]['ram']);
    }

    /**
     * JSON_EXTRACT in WHERE clause.
     */
    public function testJsonExtractInWhere(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name FROM my_jc_products
             WHERE JSON_UNQUOTE(JSON_EXTRACT(attributes, '$.brand')) = 'Gamma'
             ORDER BY id"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Desk', $rows[0]['name']);
        $this->assertSame('Chair', $rows[1]['name']);
    }

    /**
     * ->> operator in WHERE clause.
     */
    public function testJsonArrowInWhere(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name FROM my_jc_products
             WHERE attributes->>'$.brand' = 'Gamma'
             ORDER BY id"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Desk', $rows[0]['name']);
        $this->assertSame('Chair', $rows[1]['name']);
    }

    /**
     * JSON_CONTAINS() checks if a JSON document contains a value.
     */
    public function testJsonContains(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name FROM my_jc_products
             WHERE JSON_CONTAINS(tags, '\"electronics\"')
             ORDER BY id"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Laptop', $rows[0]['name']);
        $this->assertSame('Phone', $rows[1]['name']);
    }

    /**
     * JSON_LENGTH() returns the number of elements.
     */
    public function testJsonLength(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, JSON_LENGTH(tags) AS tag_count
             FROM my_jc_products ORDER BY id"
        );

        $this->assertCount(4, $rows);
        $this->assertEquals(3, (int) $rows[0]['tag_count']); // Laptop: 3 tags
        $this->assertEquals(2, (int) $rows[1]['tag_count']); // Phone: 2 tags
        $this->assertEquals(3, (int) $rows[3]['tag_count']); // Chair: 3 tags
    }

    /**
     * JSON_SEARCH() finds a string within a JSON value.
     */
    public function testJsonSearch(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name FROM my_jc_products
             WHERE JSON_SEARCH(tags, 'one', 'ergonomic') IS NOT NULL
             ORDER BY id"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Chair', $rows[0]['name']);
    }

    /**
     * GROUP BY on JSON extracted value.
     */
    public function testGroupByJsonExtract(): void
    {
        $rows = $this->ztdQuery(
            "SELECT attributes->>'$.brand' AS brand, COUNT(*) AS cnt
             FROM my_jc_products
             GROUP BY attributes->>'$.brand'
             ORDER BY cnt DESC, brand"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Gamma', $rows[0]['brand']);
        $this->assertEquals(2, (int) $rows[0]['cnt']);
    }

    /**
     * JSON_SET() in UPDATE.
     */
    public function testUpdateWithJsonSet(): void
    {
        $this->pdo->exec("UPDATE my_jc_products SET attributes = JSON_SET(attributes, '$.price', 1099.99) WHERE id = 1");

        $rows = $this->ztdQuery(
            "SELECT attributes->>'$.price' AS price FROM my_jc_products WHERE id = 1"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(1099.99, (float) $rows[0]['price']);
    }

    /**
     * Prepared statement with JSON extraction in WHERE.
     */
    public function testPreparedJsonWhere(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT name FROM my_jc_products WHERE attributes->>'$.brand' = ? ORDER BY id",
            ['Gamma']
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Desk', $rows[0]['name']);
        $this->assertSame('Chair', $rows[1]['name']);
    }

    /**
     * ORDER BY JSON extracted numeric value.
     */
    public function testOrderByJsonNumeric(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, CAST(attributes->>'$.price' AS DECIMAL(10,2)) AS price
             FROM my_jc_products
             ORDER BY CAST(attributes->>'$.price' AS DECIMAL(10,2)) ASC"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('Chair', $rows[0]['name']);
        $this->assertSame('Desk', $rows[1]['name']);
        $this->assertSame('Phone', $rows[2]['name']);
        $this->assertSame('Laptop', $rows[3]['name']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM my_jc_products")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
