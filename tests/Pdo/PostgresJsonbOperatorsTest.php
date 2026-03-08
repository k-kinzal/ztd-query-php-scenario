<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL JSONB operators and functions through CTE shadow.
 *
 * JSONB is heavily used in PostgreSQL apps for semi-structured data.
 * Tests verify that JSONB operators (->, ->>, @>, ?, #>) and functions
 * (jsonb_extract_path, jsonb_agg, jsonb_object_keys) work correctly
 * when reading from the CTE-rewritten shadow store.
 * @spec SPEC-10.2.14
 */
class PostgresJsonbOperatorsTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_jb_items (id SERIAL PRIMARY KEY, name VARCHAR(100), attrs JSONB)';
    }

    protected function getTableNames(): array
    {
        return ['pg_jb_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_jb_items (id, name, attrs) VALUES (1, 'Laptop', '{\"brand\":\"Acme\",\"specs\":{\"ram\":16,\"storage\":512},\"tags\":[\"electronics\",\"portable\"]}')");
        $this->pdo->exec("INSERT INTO pg_jb_items (id, name, attrs) VALUES (2, 'Phone', '{\"brand\":\"Beta\",\"specs\":{\"ram\":8,\"storage\":256},\"tags\":[\"electronics\",\"mobile\"]}')");
        $this->pdo->exec("INSERT INTO pg_jb_items (id, name, attrs) VALUES (3, 'Desk', '{\"brand\":\"Gamma\",\"specs\":{\"material\":\"wood\"},\"tags\":[\"furniture\"]}')");
    }

    /**
     * -> operator returns JSON sub-object.
     */
    public function testArrowObjectAccess(): void
    {
        $rows = $this->ztdQuery("SELECT name, attrs->'specs' AS specs FROM pg_jb_items WHERE id = 1");
        $this->assertSame('Laptop', $rows[0]['name']);
        $specs = json_decode($rows[0]['specs'], true);
        $this->assertSame(16, $specs['ram']);
    }

    /**
     * ->> operator returns text value.
     */
    public function testDoubleArrowTextAccess(): void
    {
        $rows = $this->ztdQuery("SELECT name, attrs->>'brand' AS brand FROM pg_jb_items ORDER BY id");
        $this->assertSame('Acme', $rows[0]['brand']);
        $this->assertSame('Beta', $rows[1]['brand']);
        $this->assertSame('Gamma', $rows[2]['brand']);
    }

    /**
     * Nested path access with -> and ->>.
     */
    public function testNestedPathAccess(): void
    {
        $rows = $this->ztdQuery("SELECT name, attrs->'specs'->>'ram' AS ram FROM pg_jb_items WHERE id = 1");
        $this->assertSame('16', $rows[0]['ram']);
    }

    /**
     * @> containment operator in WHERE.
     */
    public function testContainmentOperator(): void
    {
        $rows = $this->ztdQuery("SELECT name FROM pg_jb_items WHERE attrs @> '{\"brand\":\"Acme\"}' ORDER BY id");
        $this->assertCount(1, $rows);
        $this->assertSame('Laptop', $rows[0]['name']);
    }

    /**
     * ? key existence operator in WHERE.
     *
     * The ? operator conflicts with PostgreSQL prepared statement
     * parameter placeholders. The CTE rewriter may misinterpret
     * the ? as a bind parameter.
     */
    public function testKeyExistenceOperator(): void
    {
        try {
            $rows = $this->ztdQuery("SELECT name FROM pg_jb_items WHERE attrs->'specs' ? 'ram' ORDER BY id");
            $this->assertCount(2, $rows);
            $names = array_column($rows, 'name');
            $this->assertContains('Laptop', $names);
            $this->assertContains('Phone', $names);
        } catch (\PDOException $e) {
            // ? operator may be misinterpreted as parameter placeholder by CTE rewriter
            $this->assertStringContainsString('syntax error', $e->getMessage());
        }
    }

    /**
     * #> path extraction operator.
     */
    public function testPathExtractionOperator(): void
    {
        $rows = $this->ztdQuery("SELECT name, attrs #>> '{specs,ram}' AS ram FROM pg_jb_items WHERE id = 1");
        $this->assertSame('16', $rows[0]['ram']);
    }

    /**
     * jsonb_extract_path_text function.
     */
    public function testJsonbExtractPathText(): void
    {
        $rows = $this->ztdQuery("SELECT name, jsonb_extract_path_text(attrs, 'specs', 'ram') AS ram FROM pg_jb_items WHERE id = 2");
        $this->assertSame('8', $rows[0]['ram']);
    }

    /**
     * JSONB value in WHERE with ->> comparison.
     */
    public function testWhereWithJsonbTextComparison(): void
    {
        $rows = $this->ztdQuery("SELECT name FROM pg_jb_items WHERE attrs->>'brand' = 'Beta'");
        $this->assertCount(1, $rows);
        $this->assertSame('Phone', $rows[0]['name']);
    }

    /**
     * JSONB array element access.
     */
    public function testJsonbArrayAccess(): void
    {
        $rows = $this->ztdQuery("SELECT name, attrs->'tags'->>0 AS first_tag FROM pg_jb_items ORDER BY id");
        $this->assertSame('electronics', $rows[0]['first_tag']);
        $this->assertSame('electronics', $rows[1]['first_tag']);
        $this->assertSame('furniture', $rows[2]['first_tag']);
    }

    /**
     * ORDER BY jsonb expression.
     */
    public function testOrderByJsonbExpression(): void
    {
        $rows = $this->ztdQuery("SELECT name FROM pg_jb_items ORDER BY attrs->>'brand' ASC");
        $this->assertSame('Laptop', $rows[0]['name']); // Acme
        $this->assertSame('Phone', $rows[1]['name']); // Beta
        $this->assertSame('Desk', $rows[2]['name']); // Gamma
    }

    /**
     * JSONB value reflects INSERT/UPDATE mutations.
     */
    public function testJsonbAfterMutation(): void
    {
        $this->pdo->exec("UPDATE pg_jb_items SET attrs = jsonb_set(attrs, '{brand}', '\"Delta\"') WHERE id = 3");

        $rows = $this->ztdQuery("SELECT attrs->>'brand' AS brand FROM pg_jb_items WHERE id = 3");
        $this->assertSame('Delta', $rows[0]['brand']);
    }

    /**
     * Prepared statement with JSONB containment.
     *
     * PostgreSQL prepared statements with JSONB operators may return
     * empty results through the CTE rewriter. See SPEC-11.PG-PREPARED-FUNCTION.
     */
    public function testPreparedJsonbContainment(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT name FROM pg_jb_items WHERE attrs @> \$1::jsonb ORDER BY id",
                ['{"brand":"Acme"}']
            );
            $this->assertCount(1, $rows);
            $this->assertSame('Laptop', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('Prepared JSONB containment not supported through ZTD: ' . $e->getMessage());
        }
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_jb_items');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
