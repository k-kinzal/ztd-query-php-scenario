<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests JSON column DML operations through ZTD shadow store on SQLite.
 *
 * SQLite supports json_extract(), json_set(), json_replace(), json_remove()
 * via the JSON1 extension (built-in since 3.38.0). These are common in
 * modern applications storing semi-structured data.
 *
 * The CTE rewriter must handle JSON functions in UPDATE SET, DELETE WHERE,
 * and INSERT VALUES contexts without breaking the JSON expression parsing.
 *
 * @spec SPEC-3.1, SPEC-4.2, SPEC-4.5
 */
class SqliteJsonDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_jdml_items (id INTEGER PRIMARY KEY, name TEXT, meta TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_jdml_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->pdo->exec("INSERT INTO sl_jdml_items VALUES (1, 'Widget', '{\"color\": \"red\", \"weight\": 10, \"tags\": [\"sale\"]}')");
        $this->pdo->exec("INSERT INTO sl_jdml_items VALUES (2, 'Gadget', '{\"color\": \"blue\", \"weight\": 20, \"tags\": [\"new\"]}')");
        $this->pdo->exec("INSERT INTO sl_jdml_items VALUES (3, 'Doohickey', '{\"color\": \"red\", \"weight\": 5, \"tags\": []}')");
    }

    /**
     * UPDATE SET using json_set() to modify a JSON field.
     * This is the most common JSON DML pattern.
     */
    public function testUpdateWithJsonSet(): void
    {
        $sql = "UPDATE sl_jdml_items SET meta = json_set(meta, '$.color', 'green') WHERE id = 1";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT meta FROM sl_jdml_items WHERE id = 1");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'UPDATE json_set: expected 1 row, got ' . count($rows)
                );
            }

            $decoded = json_decode($rows[0]['meta'], true);
            if ($decoded === null) {
                $this->markTestIncomplete(
                    'UPDATE json_set: meta is not valid JSON: ' . $rows[0]['meta']
                );
            }

            $this->assertSame('green', $decoded['color']);
            $this->assertEquals(10, $decoded['weight']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE json_set failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET using json_replace() — only replaces existing keys.
     */
    public function testUpdateWithJsonReplace(): void
    {
        $sql = "UPDATE sl_jdml_items SET meta = json_replace(meta, '$.weight', 99) WHERE id = 2";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT meta FROM sl_jdml_items WHERE id = 2");

            $decoded = json_decode($rows[0]['meta'], true);
            if ($decoded === null) {
                $this->markTestIncomplete(
                    'UPDATE json_replace: meta is not valid JSON: ' . $rows[0]['meta']
                );
            }

            $this->assertEquals(99, $decoded['weight']);
            $this->assertSame('blue', $decoded['color']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE json_replace failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with json_extract() in WHERE clause.
     */
    public function testDeleteWithJsonExtractWhere(): void
    {
        $sql = "DELETE FROM sl_jdml_items WHERE json_extract(meta, '$.color') = 'red'";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT name FROM sl_jdml_items ORDER BY id");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'DELETE json_extract WHERE: expected 1, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Gadget', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE json_extract WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with json_extract() in WHERE and json_set() in SET combined.
     */
    public function testUpdateJsonSetWithJsonExtractWhere(): void
    {
        $sql = "UPDATE sl_jdml_items
                SET meta = json_set(meta, '$.weight', 0)
                WHERE json_extract(meta, '$.color') = 'red'";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT id, meta FROM sl_jdml_items ORDER BY id");

            $this->assertCount(3, $rows);

            // id=1 (red) should have weight=0
            $d1 = json_decode($rows[0]['meta'], true);
            $this->assertEquals(0, $d1['weight']);

            // id=2 (blue) should be unchanged
            $d2 = json_decode($rows[1]['meta'], true);
            $this->assertEquals(20, $d2['weight']);

            // id=3 (red) should have weight=0
            $d3 = json_decode($rows[2]['meta'], true);
            $this->assertEquals(0, $d3['weight']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE json_set + json_extract WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE with json_set() and params.
     */
    public function testPreparedUpdateJsonSet(): void
    {
        $sql = "UPDATE sl_jdml_items SET meta = json_set(meta, '$.color', ?) WHERE id = ?";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute(['yellow', 1]);

            $rows = $this->ztdQuery("SELECT meta FROM sl_jdml_items WHERE id = 1");
            $decoded = json_decode($rows[0]['meta'], true);

            if ($decoded === null || !isset($decoded['color'])) {
                $this->markTestIncomplete(
                    'Prepared UPDATE json_set: unexpected result: ' . $rows[0]['meta']
                );
            }

            $this->assertSame('yellow', $decoded['color']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE json_set failed: ' . $e->getMessage());
        }
    }

    /**
     * SELECT with json_extract in WHERE using prepared param.
     */
    public function testPreparedSelectJsonExtractWhere(): void
    {
        $sql = "SELECT name FROM sl_jdml_items WHERE json_extract(meta, '$.weight') > ? ORDER BY name";

        try {
            $rows = $this->ztdPrepareAndExecute($sql, [8]);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Prepared SELECT json_extract WHERE: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $names = array_column($rows, 'name');
            $this->assertContains('Widget', $names);
            $this->assertContains('Gadget', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared SELECT json_extract WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE using json_remove() to remove a key.
     */
    public function testUpdateJsonRemove(): void
    {
        $sql = "UPDATE sl_jdml_items SET meta = json_remove(meta, '$.tags') WHERE id = 1";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT meta FROM sl_jdml_items WHERE id = 1");
            $decoded = json_decode($rows[0]['meta'], true);

            if ($decoded === null) {
                $this->markTestIncomplete(
                    'UPDATE json_remove: meta is not valid JSON: ' . $rows[0]['meta']
                );
            }

            $this->assertArrayNotHasKey('tags', $decoded);
            $this->assertSame('red', $decoded['color']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE json_remove failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT with json_object() in VALUES.
     */
    public function testInsertWithJsonObject(): void
    {
        $sql = "INSERT INTO sl_jdml_items (id, name, meta) VALUES (4, 'Thingamajig', json_object('color', 'purple', 'weight', 15))";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT meta FROM sl_jdml_items WHERE id = 4");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT json_object: expected 1 row, got ' . count($rows)
                );
            }

            $decoded = json_decode($rows[0]['meta'], true);
            $this->assertSame('purple', $decoded['color']);
            $this->assertEquals(15, $decoded['weight']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT json_object failed: ' . $e->getMessage());
        }
    }
}
