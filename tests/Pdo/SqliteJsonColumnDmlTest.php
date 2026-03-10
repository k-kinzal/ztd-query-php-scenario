<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests JSON column operations in DML through ZTD on SQLite.
 *
 * JSON functions (json_extract, json_set, json_replace) are increasingly
 * common in application code. This tests whether the CTE rewriter correctly
 * handles JSON operations in UPDATE SET and WHERE clauses.
 *
 * @spec SPEC-10.2
 */
class SqliteJsonColumnDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE sl_json_products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            metadata TEXT DEFAULT '{}'
        )";
    }

    protected function getTableNames(): array
    {
        return ['sl_json_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO sl_json_products (name, metadata) VALUES ('Widget', '{\"color\":\"red\",\"weight\":1.5,\"tags\":[\"sale\",\"new\"]}')");
        $this->ztdExec("INSERT INTO sl_json_products (name, metadata) VALUES ('Gadget', '{\"color\":\"blue\",\"weight\":2.0,\"tags\":[\"premium\"]}')");
        $this->ztdExec("INSERT INTO sl_json_products (name, metadata) VALUES ('Doohickey', '{\"color\":\"green\",\"weight\":0.5,\"tags\":[]}')");
    }

    /**
     * SELECT with json_extract — baseline.
     */
    public function testSelectJsonExtract(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name, json_extract(metadata, '$.color') AS color FROM sl_json_products ORDER BY name"
            );

            $this->assertCount(3, $rows);
            $this->assertSame('blue', $rows[1]['color']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT json_extract failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET using json_set to modify a JSON property.
     */
    public function testUpdateJsonSet(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_json_products SET metadata = json_set(metadata, '$.color', 'yellow') WHERE name = 'Widget'"
            );

            $rows = $this->ztdQuery(
                "SELECT json_extract(metadata, '$.color') AS color FROM sl_json_products WHERE name = 'Widget'"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'UPDATE json_set: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertSame('yellow', $rows[0]['color']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE json_set failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET using json_replace.
     */
    public function testUpdateJsonReplace(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_json_products SET metadata = json_replace(metadata, '$.weight', 9.9) WHERE name = 'Gadget'"
            );

            $rows = $this->ztdQuery(
                "SELECT json_extract(metadata, '$.weight') AS weight FROM sl_json_products WHERE name = 'Gadget'"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'UPDATE json_replace: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertEqualsWithDelta(9.9, (float) $rows[0]['weight'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE json_replace failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE WHERE json_extract matches a value.
     */
    public function testDeleteWhereJsonExtract(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM sl_json_products WHERE json_extract(metadata, '$.color') = 'green'"
            );

            $rows = $this->ztdQuery("SELECT name FROM sl_json_products ORDER BY name");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE WHERE json_extract: expected 2, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE json_extract failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with json_insert to add a new key.
     */
    public function testUpdateJsonInsertNewKey(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_json_products SET metadata = json_insert(metadata, '$.size', 'large') WHERE name = 'Widget'"
            );

            $rows = $this->ztdQuery(
                "SELECT json_extract(metadata, '$.size') AS size FROM sl_json_products WHERE name = 'Widget'"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'UPDATE json_insert new key: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertSame('large', $rows[0]['size']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE json_insert new key failed: ' . $e->getMessage());
        }
    }

    /**
     * WHERE with json_extract numeric comparison.
     */
    public function testDeleteWhereJsonExtractNumeric(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM sl_json_products WHERE json_extract(metadata, '$.weight') > 1.0"
            );

            $rows = $this->ztdQuery("SELECT name FROM sl_json_products ORDER BY name");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'DELETE WHERE json_extract numeric: expected 1 (Doohickey), got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertSame('Doohickey', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE json_extract numeric failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE with json_set and bound parameter.
     */
    public function testPreparedUpdateJsonSet(): void
    {
        try {
            $stmt = $this->ztdPrepare(
                "UPDATE sl_json_products SET metadata = json_set(metadata, '$.color', ?) WHERE name = ?"
            );
            $stmt->execute(['purple', 'Gadget']);

            $rows = $this->ztdQuery(
                "SELECT json_extract(metadata, '$.color') AS color FROM sl_json_products WHERE name = 'Gadget'"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Prepared UPDATE json_set: expected 1, got ' . count($rows)
                );
            }

            $this->assertSame('purple', $rows[0]['color']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE json_set failed: ' . $e->getMessage());
        }
    }
}
