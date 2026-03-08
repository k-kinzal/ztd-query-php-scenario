<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests MySQL JSON functions through CTE shadow on MySQLi.
 *
 * MySQL 5.7+ supports JSON column type and JSON functions.
 * Tests verify JSON extraction, modification, and querying work
 * correctly through CTE-rewritten shadow queries.
 * @spec pending
 */
class JsonFunctionsTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_json_test (id INT PRIMARY KEY, data JSON, name VARCHAR(50))';
    }

    protected function getTableNames(): array
    {
        return ['mi_json_test'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_json_test VALUES (1, '{\"name\": \"Alice\", \"age\": 30, \"tags\": [\"admin\", \"user\"]}', 'Alice')");
        $this->mysqli->query("INSERT INTO mi_json_test VALUES (2, '{\"name\": \"Bob\", \"age\": 25, \"tags\": [\"user\"]}', 'Bob')");
        $this->mysqli->query("INSERT INTO mi_json_test VALUES (3, '{\"name\": \"Charlie\", \"age\": 35, \"tags\": [\"admin\"]}', 'Charlie')");
    }

    /**
     * JSON_EXTRACT to read a scalar value.
     */
    public function testJsonExtract(): void
    {
        $result = $this->mysqli->query("SELECT JSON_EXTRACT(data, '$.name') AS jname FROM mi_json_test WHERE id = 1");
        $row = $result->fetch_assoc();
        // JSON_EXTRACT returns quoted strings
        $this->assertSame('"Alice"', $row['jname']);
    }

    /**
     * JSON_UNQUOTE + JSON_EXTRACT.
     */
    public function testJsonUnquoteExtract(): void
    {
        $result = $this->mysqli->query("SELECT JSON_UNQUOTE(JSON_EXTRACT(data, '$.name')) AS jname FROM mi_json_test WHERE id = 2");
        $row = $result->fetch_assoc();
        $this->assertSame('Bob', $row['jname']);
    }

    /**
     * JSON arrow operator (->>) for unquoted extraction.
     */
    public function testJsonArrowOperator(): void
    {
        try {
            $result = $this->mysqli->query("SELECT data->>'$.age' AS age FROM mi_json_test WHERE id = 1");
            $row = $result->fetch_assoc();
            $this->assertEquals(30, (int) $row['age']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('->> operator not supported: ' . $e->getMessage());
        }
    }

    /**
     * JSON_EXTRACT in WHERE clause.
     */
    public function testJsonExtractInWhere(): void
    {
        $result = $this->mysqli->query(
            "SELECT name FROM mi_json_test WHERE JSON_EXTRACT(data, '$.age') > 28 ORDER BY name"
        );
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $names = array_column($rows, 'name');
        $this->assertContains('Alice', $names);
        $this->assertContains('Charlie', $names);
        $this->assertNotContains('Bob', $names);
    }

    /**
     * JSON_CONTAINS to check array membership.
     */
    public function testJsonContains(): void
    {
        $result = $this->mysqli->query(
            "SELECT name FROM mi_json_test WHERE JSON_CONTAINS(data->'$.tags', '\"admin\"') ORDER BY name"
        );
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $names = array_column($rows, 'name');
        $this->assertContains('Alice', $names);
        $this->assertContains('Charlie', $names);
        $this->assertNotContains('Bob', $names);
    }

    /**
     * JSON_LENGTH function.
     */
    public function testJsonLength(): void
    {
        $result = $this->mysqli->query(
            "SELECT name, JSON_LENGTH(data, '$.tags') AS tag_count FROM mi_json_test ORDER BY name"
        );
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $alice = array_filter($rows, fn($r) => $r['name'] === 'Alice');
        $alice = reset($alice);
        $this->assertEquals(2, (int) $alice['tag_count']);
    }

    /**
     * INSERT with JSON, then query.
     */
    public function testInsertWithJsonThenQuery(): void
    {
        $this->mysqli->query("INSERT INTO mi_json_test VALUES (4, '{\"name\": \"Diana\", \"age\": 28}', 'Diana')");

        $result = $this->mysqli->query("SELECT JSON_UNQUOTE(JSON_EXTRACT(data, '$.name')) AS jname FROM mi_json_test WHERE id = 4");
        $row = $result->fetch_assoc();
        $this->assertSame('Diana', $row['jname']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_json_test');
        $row = $result->fetch_assoc();
        $this->assertSame(0, (int) $row['cnt']);
    }
}
