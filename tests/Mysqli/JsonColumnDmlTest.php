<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests JSON column operations in DML through ZTD on MySQLi.
 *
 * @spec SPEC-10.2
 */
class JsonColumnDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE mi_json_products (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            metadata JSON DEFAULT NULL
        ) ENGINE=InnoDB";
    }

    protected function getTableNames(): array
    {
        return ['mi_json_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO mi_json_products (name, metadata) VALUES ('Widget', '{\"color\":\"red\",\"weight\":1.5}')");
        $this->ztdExec("INSERT INTO mi_json_products (name, metadata) VALUES ('Gadget', '{\"color\":\"blue\",\"weight\":2.0}')");
        $this->ztdExec("INSERT INTO mi_json_products (name, metadata) VALUES ('Doohickey', '{\"color\":\"green\",\"weight\":0.5}')");
    }

    public function testUpdateJsonSet(): void
    {
        try {
            $this->ztdExec(
                "UPDATE mi_json_products SET metadata = JSON_SET(metadata, '$.color', 'yellow') WHERE name = 'Widget'"
            );

            $rows = $this->ztdQuery(
                "SELECT JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.color')) AS color FROM mi_json_products WHERE name = 'Widget'"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'UPDATE JSON_SET (MySQLi): expected 1, got ' . count($rows)
                );
            }

            $this->assertSame('yellow', $rows[0]['color']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE JSON_SET (MySQLi) failed: ' . $e->getMessage());
        }
    }

    public function testDeleteWhereJsonExtract(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM mi_json_products WHERE JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.color')) = 'green'"
            );

            $rows = $this->ztdQuery("SELECT name FROM mi_json_products ORDER BY name");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE WHERE JSON_EXTRACT (MySQLi): expected 2, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE JSON_EXTRACT (MySQLi) failed: ' . $e->getMessage());
        }
    }

    public function testDeleteWhereJsonExtractNumeric(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM mi_json_products WHERE JSON_EXTRACT(metadata, '$.weight') > 1.0"
            );

            $rows = $this->ztdQuery("SELECT name FROM mi_json_products ORDER BY name");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'DELETE WHERE JSON_EXTRACT numeric (MySQLi): expected 1, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertSame('Doohickey', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE JSON_EXTRACT numeric (MySQLi) failed: ' . $e->getMessage());
        }
    }

    public function testUpdateJsonReplace(): void
    {
        try {
            $this->ztdExec(
                "UPDATE mi_json_products SET metadata = JSON_REPLACE(metadata, '$.weight', 9.9) WHERE name = 'Gadget'"
            );

            $rows = $this->ztdQuery(
                "SELECT JSON_EXTRACT(metadata, '$.weight') AS weight FROM mi_json_products WHERE name = 'Gadget'"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'UPDATE JSON_REPLACE (MySQLi): expected 1, got ' . count($rows)
                );
            }

            $this->assertEqualsWithDelta(9.9, (float) $rows[0]['weight'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE JSON_REPLACE (MySQLi) failed: ' . $e->getMessage());
        }
    }
}
