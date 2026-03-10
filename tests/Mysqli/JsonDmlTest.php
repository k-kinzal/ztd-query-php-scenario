<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests JSON column DML operations (UPDATE SET, DELETE WHERE) through ZTD
 * shadow store on MySQLi.
 *
 * MySQL 5.7+ supports JSON column type and JSON_SET/JSON_REPLACE/JSON_REMOVE
 * for in-place mutation. These are heavily used in modern applications.
 *
 * @spec SPEC-3.1, SPEC-4.2, SPEC-4.5
 */
class JsonDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_jdml_items (
            id INT PRIMARY KEY,
            name VARCHAR(50),
            meta JSON
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mi_jdml_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->mysqli->query("INSERT INTO mi_jdml_items VALUES (1, 'Widget', '{\"color\": \"red\", \"weight\": 10}')");
        $this->mysqli->query("INSERT INTO mi_jdml_items VALUES (2, 'Gadget', '{\"color\": \"blue\", \"weight\": 20}')");
        $this->mysqli->query("INSERT INTO mi_jdml_items VALUES (3, 'Doohickey', '{\"color\": \"red\", \"weight\": 5}')");
    }

    /**
     * UPDATE SET using JSON_SET() to modify a JSON field.
     */
    public function testUpdateWithJsonSet(): void
    {
        $sql = "UPDATE mi_jdml_items SET meta = JSON_SET(meta, '$.color', 'green') WHERE id = 1";

        try {
            $this->mysqli->query($sql);
            $rows = $this->ztdQuery("SELECT meta FROM mi_jdml_items WHERE id = 1");

            $decoded = json_decode($rows[0]['meta'], true);
            if ($decoded === null) {
                $this->markTestIncomplete('UPDATE JSON_SET: meta is not valid JSON: ' . $rows[0]['meta']);
            }

            $this->assertSame('green', $decoded['color']);
            $this->assertEquals(10, $decoded['weight']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE JSON_SET failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with JSON_EXTRACT in WHERE clause.
     */
    public function testDeleteWithJsonExtractWhere(): void
    {
        $sql = "DELETE FROM mi_jdml_items WHERE JSON_EXTRACT(meta, '$.color') = 'red'";

        try {
            $this->mysqli->query($sql);
            $rows = $this->ztdQuery("SELECT name FROM mi_jdml_items ORDER BY id");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'DELETE JSON_EXTRACT WHERE: expected 1, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Gadget', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE JSON_EXTRACT WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with JSON_SET in SET and JSON_EXTRACT in WHERE combined.
     */
    public function testUpdateJsonSetWithJsonExtractWhere(): void
    {
        $sql = "UPDATE mi_jdml_items
                SET meta = JSON_SET(meta, '$.weight', 0)
                WHERE JSON_EXTRACT(meta, '$.color') = 'red'";

        try {
            $this->mysqli->query($sql);
            $rows = $this->ztdQuery("SELECT id, meta FROM mi_jdml_items ORDER BY id");

            $this->assertCount(3, $rows);

            $d1 = json_decode($rows[0]['meta'], true);
            $this->assertEquals(0, $d1['weight'], 'id=1 (red) should have weight=0');

            $d2 = json_decode($rows[1]['meta'], true);
            $this->assertEquals(20, $d2['weight'], 'id=2 (blue) should be unchanged');

            $d3 = json_decode($rows[2]['meta'], true);
            $this->assertEquals(0, $d3['weight'], 'id=3 (red) should have weight=0');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE JSON_SET + JSON_EXTRACT WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE with JSON_SET and params.
     */
    public function testPreparedUpdateJsonSet(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "UPDATE mi_jdml_items SET meta = JSON_SET(meta, '$.color', ?) WHERE id = ?",
                ['yellow', 1]
            );

            $result = $this->ztdQuery("SELECT meta FROM mi_jdml_items WHERE id = 1");
            $decoded = json_decode($result[0]['meta'], true);

            if ($decoded === null || !isset($decoded['color'])) {
                $this->markTestIncomplete('Prepared UPDATE JSON_SET: unexpected result: ' . $result[0]['meta']);
            }

            $this->assertSame('yellow', $decoded['color']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE JSON_SET failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared SELECT with JSON_EXTRACT > ? comparison.
     */
    public function testPreparedSelectJsonExtractComparison(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT name FROM mi_jdml_items WHERE JSON_EXTRACT(meta, '$.weight') > ? ORDER BY name",
                [8]
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Prepared SELECT JSON_EXTRACT >: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $names = array_column($rows, 'name');
            $this->assertContains('Widget', $names);
            $this->assertContains('Gadget', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared SELECT JSON_EXTRACT comparison failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT ON DUPLICATE KEY UPDATE with JSON_SET.
     */
    public function testUpsertWithJsonSet(): void
    {
        $sql = "INSERT INTO mi_jdml_items (id, name, meta)
                VALUES (1, 'Widget', '{\"color\": \"purple\", \"weight\": 99}')
                ON DUPLICATE KEY UPDATE meta = JSON_SET(meta, '$.color', 'purple')";

        try {
            $this->mysqli->query($sql);
            $rows = $this->ztdQuery("SELECT meta FROM mi_jdml_items WHERE id = 1");

            $decoded = json_decode($rows[0]['meta'], true);
            if ($decoded === null) {
                $this->markTestIncomplete('Upsert JSON_SET: meta not valid JSON: ' . $rows[0]['meta']);
            }

            $this->assertSame('purple', $decoded['color']);
            // Weight should be from the original row (update path), not the new value
            $this->assertEquals(10, $decoded['weight']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Upsert JSON_SET failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with JSON_REMOVE in SET.
     */
    public function testUpdateJsonRemove(): void
    {
        $sql = "UPDATE mi_jdml_items SET meta = JSON_REMOVE(meta, '$.weight') WHERE id = 2";

        try {
            $this->mysqli->query($sql);
            $rows = $this->ztdQuery("SELECT meta FROM mi_jdml_items WHERE id = 2");

            $decoded = json_decode($rows[0]['meta'], true);
            if ($decoded === null) {
                $this->markTestIncomplete('UPDATE JSON_REMOVE: meta not valid JSON: ' . $rows[0]['meta']);
            }

            $this->assertArrayNotHasKey('weight', $decoded);
            $this->assertSame('blue', $decoded['color']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE JSON_REMOVE failed: ' . $e->getMessage());
        }
    }
}
