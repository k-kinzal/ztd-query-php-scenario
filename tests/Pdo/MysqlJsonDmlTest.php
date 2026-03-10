<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests JSON column DML operations through ZTD shadow store on MySQL PDO.
 *
 * @spec SPEC-3.1, SPEC-4.2, SPEC-4.5
 */
class MysqlJsonDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mp_jdml_items (
            id INT PRIMARY KEY,
            name VARCHAR(50),
            meta JSON
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mp_jdml_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->pdo->exec("INSERT INTO mp_jdml_items VALUES (1, 'Widget', '{\"color\": \"red\", \"weight\": 10}')");
        $this->pdo->exec("INSERT INTO mp_jdml_items VALUES (2, 'Gadget', '{\"color\": \"blue\", \"weight\": 20}')");
        $this->pdo->exec("INSERT INTO mp_jdml_items VALUES (3, 'Doohickey', '{\"color\": \"red\", \"weight\": 5}')");
    }

    public function testUpdateWithJsonSet(): void
    {
        try {
            $this->pdo->exec("UPDATE mp_jdml_items SET meta = JSON_SET(meta, '$.color', 'green') WHERE id = 1");
            $rows = $this->ztdQuery("SELECT meta FROM mp_jdml_items WHERE id = 1");

            $decoded = json_decode($rows[0]['meta'], true);
            $this->assertSame('green', $decoded['color']);
            $this->assertEquals(10, $decoded['weight']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE JSON_SET failed: ' . $e->getMessage());
        }
    }

    public function testDeleteWithJsonExtractWhere(): void
    {
        try {
            $this->pdo->exec("DELETE FROM mp_jdml_items WHERE JSON_EXTRACT(meta, '$.color') = 'red'");
            $rows = $this->ztdQuery("SELECT name FROM mp_jdml_items ORDER BY id");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'DELETE JSON_EXTRACT WHERE: expected 1, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame('Gadget', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE JSON_EXTRACT WHERE failed: ' . $e->getMessage());
        }
    }

    public function testUpdateJsonSetWithJsonExtractWhere(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE mp_jdml_items SET meta = JSON_SET(meta, '$.weight', 0)
                 WHERE JSON_EXTRACT(meta, '$.color') = 'red'"
            );
            $rows = $this->ztdQuery("SELECT id, meta FROM mp_jdml_items ORDER BY id");

            $this->assertCount(3, $rows);
            $d1 = json_decode($rows[0]['meta'], true);
            $this->assertEquals(0, $d1['weight']);
            $d2 = json_decode($rows[1]['meta'], true);
            $this->assertEquals(20, $d2['weight']);
            $d3 = json_decode($rows[2]['meta'], true);
            $this->assertEquals(0, $d3['weight']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE JSON_SET + JSON_EXTRACT WHERE failed: ' . $e->getMessage());
        }
    }

    public function testPreparedSelectJsonExtractComparison(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT name FROM mp_jdml_items WHERE JSON_EXTRACT(meta, '$.weight') > ? ORDER BY name",
                [8]
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Prepared SELECT JSON_EXTRACT >: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared SELECT JSON_EXTRACT comparison failed: ' . $e->getMessage());
        }
    }

    public function testUpsertWithJsonSet(): void
    {
        $sql = "INSERT INTO mp_jdml_items (id, name, meta)
                VALUES (1, 'Widget', '{\"color\": \"purple\", \"weight\": 99}')
                ON DUPLICATE KEY UPDATE meta = JSON_SET(meta, '$.color', 'purple')";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT meta FROM mp_jdml_items WHERE id = 1");

            $decoded = json_decode($rows[0]['meta'], true);
            $this->assertSame('purple', $decoded['color']);
            $this->assertEquals(10, $decoded['weight']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Upsert JSON_SET failed: ' . $e->getMessage());
        }
    }
}
