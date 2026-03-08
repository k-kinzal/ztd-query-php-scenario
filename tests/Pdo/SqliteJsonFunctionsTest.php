<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests SQLite JSON1 extension functions through CTE shadow.
 *
 * SQLite's json1 extension (built-in since 3.38.0) is commonly used for
 * semi-structured data. Tests verify json_extract(), json_set(),
 * json_type(), json_each(), and json_group_array() work correctly
 * when reading from the CTE-rewritten shadow store.
 * @spec SPEC-10.2.16
 */
class SqliteJsonFunctionsTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_jf_items (id INTEGER PRIMARY KEY, name TEXT, data TEXT)';
    }

    protected function getTableNames(): array
    {
        return ['sl_jf_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_jf_items (id, name, data) VALUES (1, 'Laptop', '{\"brand\":\"Acme\",\"specs\":{\"ram\":16,\"storage\":512},\"tags\":[\"electronics\",\"portable\"]}')");
        $this->pdo->exec("INSERT INTO sl_jf_items (id, name, data) VALUES (2, 'Phone', '{\"brand\":\"Beta\",\"specs\":{\"ram\":8,\"storage\":256},\"tags\":[\"electronics\",\"mobile\"]}')");
        $this->pdo->exec("INSERT INTO sl_jf_items (id, name, data) VALUES (3, 'Desk', '{\"brand\":\"Gamma\",\"material\":\"wood\"}')");
    }

    /**
     * json_extract() returns a value from a JSON path.
     */
    public function testJsonExtract(): void
    {
        $rows = $this->ztdQuery("SELECT name, json_extract(data, '\$.brand') AS brand FROM sl_jf_items ORDER BY id");
        $this->assertSame('Acme', $rows[0]['brand']);
        $this->assertSame('Beta', $rows[1]['brand']);
        $this->assertSame('Gamma', $rows[2]['brand']);
    }

    /**
     * json_extract() with nested path.
     */
    public function testJsonExtractNestedPath(): void
    {
        $rows = $this->ztdQuery("SELECT name, json_extract(data, '\$.specs.ram') AS ram FROM sl_jf_items WHERE id = 1");
        $this->assertEquals(16, $rows[0]['ram']);
    }

    /**
     * -> and ->> operators (SQLite 3.38.0+).
     */
    public function testArrowOperators(): void
    {
        $rows = $this->ztdQuery("SELECT name, data->>'brand' AS brand FROM sl_jf_items ORDER BY id");
        $this->assertSame('Acme', $rows[0]['brand']);
        $this->assertSame('Beta', $rows[1]['brand']);
    }

    /**
     * json_extract() in WHERE clause.
     */
    public function testJsonExtractInWhere(): void
    {
        $rows = $this->ztdQuery("SELECT name FROM sl_jf_items WHERE json_extract(data, '\$.brand') = 'Beta'");
        $this->assertCount(1, $rows);
        $this->assertSame('Phone', $rows[0]['name']);
    }

    /**
     * json_type() returns the type of a JSON value.
     */
    public function testJsonType(): void
    {
        $rows = $this->ztdQuery("SELECT json_type(data, '\$.brand') AS t FROM sl_jf_items WHERE id = 1");
        $this->assertSame('text', $rows[0]['t']);
    }

    /**
     * json_array_length() on a JSON array.
     */
    public function testJsonArrayLength(): void
    {
        $rows = $this->ztdQuery("SELECT name, json_array_length(data, '\$.tags') AS tag_count FROM sl_jf_items WHERE id = 1");
        $this->assertEquals(2, $rows[0]['tag_count']);
    }

    /**
     * json_extract() for array element access.
     */
    public function testJsonArrayElementAccess(): void
    {
        $rows = $this->ztdQuery("SELECT json_extract(data, '\$.tags[0]') AS first_tag FROM sl_jf_items WHERE id = 1");
        $this->assertSame('electronics', $rows[0]['first_tag']);
    }

    /**
     * json_group_array() aggregate function.
     */
    public function testJsonGroupArray(): void
    {
        $rows = $this->ztdQuery("SELECT json_group_array(name) AS names FROM sl_jf_items");
        $names = json_decode($rows[0]['names'], true);
        $this->assertCount(3, $names);
        $this->assertContains('Laptop', $names);
    }

    /**
     * ORDER BY json_extract expression.
     */
    public function testOrderByJsonExtract(): void
    {
        $rows = $this->ztdQuery("SELECT name FROM sl_jf_items ORDER BY json_extract(data, '\$.brand') ASC");
        $this->assertSame('Laptop', $rows[0]['name']); // Acme
        $this->assertSame('Phone', $rows[1]['name']); // Beta
        $this->assertSame('Desk', $rows[2]['name']); // Gamma
    }

    /**
     * json_set() in UPDATE modifies JSON value.
     */
    public function testJsonSetInUpdate(): void
    {
        $this->pdo->exec("UPDATE sl_jf_items SET data = json_set(data, '\$.brand', 'Delta') WHERE id = 3");

        $rows = $this->ztdQuery("SELECT json_extract(data, '\$.brand') AS brand FROM sl_jf_items WHERE id = 3");
        $this->assertSame('Delta', $rows[0]['brand']);
    }

    /**
     * Prepared statement with json_extract in WHERE.
     */
    public function testPreparedJsonExtract(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT name FROM sl_jf_items WHERE json_extract(data, '\$.brand') = ?",
            ['Acme']
        );
        $this->assertCount(1, $rows);
        $this->assertSame('Laptop', $rows[0]['name']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_jf_items');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
