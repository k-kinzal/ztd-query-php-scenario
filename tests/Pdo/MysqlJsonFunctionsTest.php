<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests MySQL JSON functions through CTE shadow on PDO.
 *
 * MySQL 5.7+ supports JSON column type and JSON functions.
 * Tests verify JSON extraction, modification, and querying work
 * correctly through CTE-rewritten shadow queries.
 * @spec pending
 */
class MysqlJsonFunctionsTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pdo_json_test (id INT PRIMARY KEY, data JSON, name VARCHAR(50))';
    }

    protected function getTableNames(): array
    {
        return ['pdo_json_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pdo_json_test VALUES (1, '{\"name\": \"Alice\", \"age\": 30, \"tags\": [\"admin\", \"user\"]}', 'Alice')");
        $this->pdo->exec("INSERT INTO pdo_json_test VALUES (2, '{\"name\": \"Bob\", \"age\": 25, \"tags\": [\"user\"]}', 'Bob')");
        $this->pdo->exec("INSERT INTO pdo_json_test VALUES (3, '{\"name\": \"Charlie\", \"age\": 35, \"tags\": [\"admin\"]}', 'Charlie')");
    }

    /**
     * JSON_EXTRACT to read a scalar value.
     */
    public function testJsonExtract(): void
    {
        $stmt = $this->pdo->query("SELECT JSON_EXTRACT(data, '$.name') AS jname FROM pdo_json_test WHERE id = 1");
        $value = $stmt->fetchColumn();
        // JSON_EXTRACT returns quoted strings
        $this->assertSame('"Alice"', $value);
    }

    /**
     * JSON_UNQUOTE + JSON_EXTRACT (shorthand: ->>).
     */
    public function testJsonUnquoteExtract(): void
    {
        $stmt = $this->pdo->query("SELECT JSON_UNQUOTE(JSON_EXTRACT(data, '$.name')) AS jname FROM pdo_json_test WHERE id = 2");
        $this->assertSame('Bob', $stmt->fetchColumn());
    }

    /**
     * JSON arrow operator (->>) for unquoted extraction.
     */
    public function testJsonArrowOperator(): void
    {
        try {
            $stmt = $this->pdo->query("SELECT data->>'$.age' AS age FROM pdo_json_test WHERE id = 1");
            $this->assertEquals(30, (int) $stmt->fetchColumn());
        } catch (\Throwable $e) {
            // ->> may not be available on older MySQL versions
            $this->markTestSkipped('->> operator not supported: ' . $e->getMessage());
        }
    }

    /**
     * JSON_EXTRACT in WHERE clause.
     */
    public function testJsonExtractInWhere(): void
    {
        $stmt = $this->pdo->query(
            "SELECT name FROM pdo_json_test WHERE JSON_EXTRACT(data, '$.age') > 28 ORDER BY name"
        );
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertContains('Alice', $rows);
        $this->assertContains('Charlie', $rows);
        $this->assertNotContains('Bob', $rows);
    }

    /**
     * JSON_CONTAINS to check array membership.
     */
    public function testJsonContains(): void
    {
        $stmt = $this->pdo->query(
            "SELECT name FROM pdo_json_test WHERE JSON_CONTAINS(data->'$.tags', '\"admin\"') ORDER BY name"
        );
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertContains('Alice', $rows);
        $this->assertContains('Charlie', $rows);
        $this->assertNotContains('Bob', $rows);
    }

    /**
     * JSON_LENGTH function.
     */
    public function testJsonLength(): void
    {
        $stmt = $this->pdo->query(
            "SELECT name, JSON_LENGTH(data, '$.tags') AS tag_count FROM pdo_json_test ORDER BY name"
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // Alice has 2 tags, Bob has 1, Charlie has 1
        $alice = array_filter($rows, fn($r) => $r['name'] === 'Alice');
        $alice = reset($alice);
        $this->assertEquals(2, (int) $alice['tag_count']);
    }

    /**
     * INSERT with JSON, then query.
     */
    public function testInsertWithJsonThenQuery(): void
    {
        $this->pdo->exec("INSERT INTO pdo_json_test VALUES (4, '{\"name\": \"Diana\", \"age\": 28}', 'Diana')");

        $stmt = $this->pdo->query("SELECT JSON_UNQUOTE(JSON_EXTRACT(data, '$.name')) FROM pdo_json_test WHERE id = 4");
        $this->assertSame('Diana', $stmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_json_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
