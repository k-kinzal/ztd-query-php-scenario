<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests MySQL JSON column operations through MySQLi CTE shadow store.
 *
 * Mirrors MysqlJsonColumnTest (PDO) to verify consistent behavior across
 * MySQLi and PDO adapters for JSON operations.
 *
 * @spec SPEC-3.5
 */
class JsonColumnTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE mi_jc_products (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                attributes JSON,
                tags JSON
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_jc_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_jc_products VALUES (1, 'Laptop', '{\"brand\": \"Acme\", \"specs\": {\"ram\": 16, \"storage\": 512}, \"price\": 999.99}', '[\"electronics\", \"portable\", \"computer\"]')");
        $this->mysqli->query("INSERT INTO mi_jc_products VALUES (2, 'Phone', '{\"brand\": \"Beta\", \"specs\": {\"ram\": 8, \"storage\": 256}, \"price\": 599.99}', '[\"electronics\", \"mobile\"]')");
        $this->mysqli->query("INSERT INTO mi_jc_products VALUES (3, 'Desk', '{\"brand\": \"Gamma\", \"material\": \"wood\", \"price\": 299.99}', '[\"furniture\", \"office\"]')");
        $this->mysqli->query("INSERT INTO mi_jc_products VALUES (4, 'Chair', '{\"brand\": \"Gamma\", \"material\": \"metal\", \"price\": 199.99}', '[\"furniture\", \"office\", \"ergonomic\"]')");
    }

    public function testJsonUnquoteExtract(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, JSON_UNQUOTE(JSON_EXTRACT(attributes, '$.brand')) AS brand
             FROM mi_jc_products ORDER BY id"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('Acme', $rows[0]['brand']);
        $this->assertSame('Gamma', $rows[2]['brand']);
    }

    public function testJsonArrowOperator(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, attributes->>'$.brand' AS brand
             FROM mi_jc_products ORDER BY id"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('Acme', $rows[0]['brand']);
        $this->assertSame('Gamma', $rows[3]['brand']);
    }

    public function testJsonExtractInWhere(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name FROM mi_jc_products
             WHERE attributes->>'$.brand' = 'Gamma'
             ORDER BY id"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Desk', $rows[0]['name']);
        $this->assertSame('Chair', $rows[1]['name']);
    }

    public function testJsonContains(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name FROM mi_jc_products
             WHERE JSON_CONTAINS(tags, '\"electronics\"')
             ORDER BY id"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Laptop', $rows[0]['name']);
        $this->assertSame('Phone', $rows[1]['name']);
    }

    public function testJsonLength(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, JSON_LENGTH(tags) AS tag_count
             FROM mi_jc_products ORDER BY id"
        );

        $this->assertCount(4, $rows);
        $this->assertEquals(3, (int) $rows[0]['tag_count']);
        $this->assertEquals(2, (int) $rows[1]['tag_count']);
    }

    public function testGroupByJsonExtract(): void
    {
        $rows = $this->ztdQuery(
            "SELECT attributes->>'$.brand' AS brand, COUNT(*) AS cnt
             FROM mi_jc_products
             GROUP BY attributes->>'$.brand'
             ORDER BY cnt DESC, brand"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Gamma', $rows[0]['brand']);
        $this->assertEquals(2, (int) $rows[0]['cnt']);
    }

    public function testUpdateWithJsonSet(): void
    {
        $this->mysqli->query("UPDATE mi_jc_products SET attributes = JSON_SET(attributes, '$.price', 1099.99) WHERE id = 1");

        $rows = $this->ztdQuery(
            "SELECT attributes->>'$.price' AS price FROM mi_jc_products WHERE id = 1"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(1099.99, (float) $rows[0]['price']);
    }

    public function testPreparedJsonWhere(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT name FROM mi_jc_products WHERE attributes->>'$.brand' = ? ORDER BY id",
            ['Gamma']
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Desk', $rows[0]['name']);
        $this->assertSame('Chair', $rows[1]['name']);
    }

    public function testPhysicalIsolation(): void
    {
        $this->disableZtd();
        $result = (new \mysqli(
            \Tests\Support\MySQLContainer::getHost(),
            'root', 'root', 'test',
            \Tests\Support\MySQLContainer::getPort(),
        ))->query("SELECT COUNT(*) AS cnt FROM mi_jc_products");
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
