<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL JSONB column operations through CTE shadow store.
 *
 * JSONB is PostgreSQL's binary JSON type, commonly used for semi-structured data.
 * The CTE rewriter must preserve JSONB type in CAST expressions for operators
 * like ->, ->>, @>, <@, ?, ?|, ?& and functions like jsonb_extract_path(),
 * jsonb_array_length() to work correctly.
 *
 * Related risk: SPEC-11.PG-ARRAY-TYPE shows CastRenderer already fails for
 * INTEGER[] — JSONB may have similar issues if cast as TEXT instead of JSONB.
 *
 * @spec SPEC-3.5
 */
class PostgresJsonbColumnTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE pg_jb_products (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                attributes JSONB,
                tags JSONB
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_jb_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_jb_products (id, name, attributes, tags) VALUES (1, 'Laptop', '{\"brand\": \"Acme\", \"specs\": {\"ram\": 16, \"storage\": 512}, \"price\": 999.99}', '[\"electronics\", \"portable\", \"computer\"]')");
        $this->pdo->exec("INSERT INTO pg_jb_products (id, name, attributes, tags) VALUES (2, 'Phone', '{\"brand\": \"Beta\", \"specs\": {\"ram\": 8, \"storage\": 256}, \"price\": 599.99}', '[\"electronics\", \"mobile\"]')");
        $this->pdo->exec("INSERT INTO pg_jb_products (id, name, attributes, tags) VALUES (3, 'Desk', '{\"brand\": \"Gamma\", \"material\": \"wood\", \"price\": 299.99}', '[\"furniture\", \"office\"]')");
        $this->pdo->exec("INSERT INTO pg_jb_products (id, name, attributes, tags) VALUES (4, 'Chair', '{\"brand\": \"Gamma\", \"material\": \"metal\", \"price\": 199.99}', '[\"furniture\", \"office\", \"ergonomic\"]')");
    }

    /**
     * JSONB ->> operator extracts text value.
     */
    public function testJsonbTextExtractOperator(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, attributes->>'brand' AS brand
             FROM pg_jb_products
             ORDER BY id"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('Acme', $rows[0]['brand']);
        $this->assertSame('Beta', $rows[1]['brand']);
        $this->assertSame('Gamma', $rows[2]['brand']);
        $this->assertSame('Gamma', $rows[3]['brand']);
    }

    /**
     * JSONB -> operator extracts JSON value (returns JSONB).
     */
    public function testJsonbJsonExtractOperator(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, attributes->'specs' AS specs
             FROM pg_jb_products
             WHERE id = 1"
        );

        $this->assertCount(1, $rows);
        $decoded = json_decode($rows[0]['specs'], true);
        $this->assertSame(16, $decoded['ram']);
        $this->assertSame(512, $decoded['storage']);
    }

    /**
     * Nested JSONB path extraction: ->'key'->>'nested_key'.
     */
    public function testJsonbNestedPathExtraction(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, attributes->'specs'->>'ram' AS ram
             FROM pg_jb_products
             WHERE id = 1"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('16', $rows[0]['ram']);
    }

    /**
     * JSONB ->> in WHERE clause.
     */
    public function testJsonbExtractInWhere(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name FROM pg_jb_products
             WHERE attributes->>'brand' = 'Gamma'
             ORDER BY id"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Desk', $rows[0]['name']);
        $this->assertSame('Chair', $rows[1]['name']);
    }

    /**
     * JSONB @> containment operator.
     * This is the most common JSONB query pattern for checking if a document
     * contains a given sub-document.
     */
    public function testJsonbContainmentOperator(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name FROM pg_jb_products
             WHERE attributes @> '{\"brand\": \"Gamma\"}'
             ORDER BY id"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Desk', $rows[0]['name']);
        $this->assertSame('Chair', $rows[1]['name']);
    }

    /**
     * JSONB ? operator checks key existence.
     *
     * The CTE rewriter treats ? as a parameter placeholder and converts it to $1,
     * producing invalid SQL: WHERE attributes $1 'material'.
     * This is a ZTD bug: the PgSqlParser does not distinguish between the JSONB ?
     * operator and the prepared-statement ? placeholder.
     *
     * Workaround: use jsonb_exists(col, 'key') function instead of the ? operator.
     */
    public function testJsonbKeyExistsOperator(): void
    {
        // ? operator fails — CTE rewriter converts ? to $1
        $threw = false;
        try {
            $this->ztdQuery(
                "SELECT name FROM pg_jb_products
                 WHERE attributes ? 'material'
                 ORDER BY id"
            );
        } catch (\PDOException $e) {
            $threw = true;
            $this->assertStringContainsString('syntax error', $e->getMessage());
        }
        $this->assertTrue($threw, 'Expected PDOException from ? operator conflict');

        // Workaround: jsonb_exists() function
        $rows = $this->ztdQuery(
            "SELECT name FROM pg_jb_products
             WHERE jsonb_exists(attributes, 'material')
             ORDER BY id"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Desk', $rows[0]['name']);
        $this->assertSame('Chair', $rows[1]['name']);
    }

    /**
     * jsonb_extract_path_text() function — functional equivalent of #>>.
     */
    public function testJsonbExtractPathText(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, jsonb_extract_path_text(attributes, 'specs', 'ram') AS ram
             FROM pg_jb_products
             WHERE id = 1"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('16', $rows[0]['ram']);
    }

    /**
     * jsonb_array_length() on a JSONB array column.
     */
    public function testJsonbArrayLength(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, jsonb_array_length(tags) AS tag_count
             FROM pg_jb_products
             ORDER BY id"
        );

        $this->assertCount(4, $rows);
        $this->assertEquals(3, (int) $rows[0]['tag_count']); // Laptop: 3 tags
        $this->assertEquals(2, (int) $rows[1]['tag_count']); // Phone: 2 tags
        $this->assertEquals(2, (int) $rows[2]['tag_count']); // Desk: 2 tags
        $this->assertEquals(3, (int) $rows[3]['tag_count']); // Chair: 3 tags
    }

    /**
     * JSONB array element access with -> integer index.
     */
    public function testJsonbArrayElementAccess(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, tags->>0 AS first_tag
             FROM pg_jb_products
             ORDER BY id"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('electronics', $rows[0]['first_tag']);
        $this->assertSame('electronics', $rows[1]['first_tag']);
        $this->assertSame('furniture', $rows[2]['first_tag']);
        $this->assertSame('furniture', $rows[3]['first_tag']);
    }

    /**
     * JSONB in GROUP BY with aggregate.
     */
    public function testJsonbGroupByWithAggregate(): void
    {
        $rows = $this->ztdQuery(
            "SELECT attributes->>'brand' AS brand, COUNT(*) AS cnt
             FROM pg_jb_products
             GROUP BY attributes->>'brand'
             ORDER BY cnt DESC, brand"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Gamma', $rows[0]['brand']);
        $this->assertEquals(2, (int) $rows[0]['cnt']);
    }

    /**
     * JSONB cast to numeric for comparison.
     */
    public function testJsonbNumericCast(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, (attributes->>'price')::numeric AS price
             FROM pg_jb_products
             WHERE (attributes->>'price')::numeric > 500
             ORDER BY (attributes->>'price')::numeric DESC"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Laptop', $rows[0]['name']);
        $this->assertSame('Phone', $rows[1]['name']);
    }

    /**
     * UPDATE JSONB column with jsonb_set().
     */
    public function testUpdateJsonbWithJsonbSet(): void
    {
        $this->pdo->exec("UPDATE pg_jb_products SET attributes = jsonb_set(attributes, '{price}', '1099.99') WHERE id = 1");

        $rows = $this->ztdQuery(
            "SELECT (attributes->>'price')::numeric AS price FROM pg_jb_products WHERE id = 1"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(1099.99, (float) $rows[0]['price']);
    }

    /**
     * Prepared statement with JSONB ->> in WHERE.
     */
    public function testPreparedJsonbWhere(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT name FROM pg_jb_products WHERE attributes->>'brand' = ? ORDER BY id",
            ['Gamma']
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Desk', $rows[0]['name']);
        $this->assertSame('Chair', $rows[1]['name']);
    }

    /**
     * JSONB @> containment with prepared parameter.
     */
    public function testPreparedJsonbContainment(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT name FROM pg_jb_products WHERE attributes @> ?::jsonb ORDER BY id",
            ['{"material": "wood"}']
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Desk', $rows[0]['name']);
    }

    /**
     * ORDER BY JSONB extracted numeric value.
     */
    public function testOrderByJsonbNumeric(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, (attributes->>'price')::numeric AS price
             FROM pg_jb_products
             ORDER BY (attributes->>'price')::numeric ASC"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('Chair', $rows[0]['name']);
        $this->assertSame('Desk', $rows[1]['name']);
        $this->assertSame('Phone', $rows[2]['name']);
        $this->assertSame('Laptop', $rows[3]['name']);
    }

    /**
     * Physical isolation — no data in physical table.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) FROM pg_jb_products")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['count']);
    }
}
