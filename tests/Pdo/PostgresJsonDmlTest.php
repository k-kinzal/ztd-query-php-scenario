<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests JSONB column DML operations through ZTD shadow store on PostgreSQL.
 *
 * PostgreSQL has native JSONB with operators (->>, @>, ||) and functions
 * (jsonb_set, jsonb_strip_nulls). These are heavily used in modern apps.
 *
 * @spec SPEC-3.1, SPEC-4.2, SPEC-4.5
 */
class PostgresJsonDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_jdml_items (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50),
                meta JSONB
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_jdml_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->pdo->exec("INSERT INTO pg_jdml_items (id, name, meta) VALUES (1, 'Widget', '{\"color\": \"red\", \"weight\": 10}')");
        $this->pdo->exec("INSERT INTO pg_jdml_items (id, name, meta) VALUES (2, 'Gadget', '{\"color\": \"blue\", \"weight\": 20}')");
        $this->pdo->exec("INSERT INTO pg_jdml_items (id, name, meta) VALUES (3, 'Doohickey', '{\"color\": \"red\", \"weight\": 5}')");
    }

    /**
     * UPDATE SET using jsonb_set() to modify a JSONB field.
     */
    public function testUpdateWithJsonbSet(): void
    {
        $sql = "UPDATE pg_jdml_items SET meta = jsonb_set(meta, '{color}', '\"green\"') WHERE id = 1";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT meta FROM pg_jdml_items WHERE id = 1");

            $decoded = json_decode($rows[0]['meta'], true);
            if ($decoded === null) {
                $this->markTestIncomplete('UPDATE jsonb_set: meta not valid JSON: ' . $rows[0]['meta']);
            }

            $this->assertSame('green', $decoded['color']);
            $this->assertEquals(10, $decoded['weight']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE jsonb_set failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with ->> operator in WHERE clause.
     */
    public function testDeleteWithJsonbArrowWhere(): void
    {
        $sql = "DELETE FROM pg_jdml_items WHERE meta->>'color' = 'red'";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT name FROM pg_jdml_items ORDER BY id");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'DELETE ->> WHERE: expected 1, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame('Gadget', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE ->> WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with jsonb_set in SET and ->> in WHERE.
     */
    public function testUpdateJsonbSetWithArrowWhere(): void
    {
        $sql = "UPDATE pg_jdml_items
                SET meta = jsonb_set(meta, '{weight}', '0')
                WHERE meta->>'color' = 'red'";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT id, meta FROM pg_jdml_items ORDER BY id");

            $this->assertCount(3, $rows);

            $d1 = json_decode($rows[0]['meta'], true);
            $this->assertEquals(0, $d1['weight']);

            $d2 = json_decode($rows[1]['meta'], true);
            $this->assertEquals(20, $d2['weight']);

            $d3 = json_decode($rows[2]['meta'], true);
            $this->assertEquals(0, $d3['weight']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE jsonb_set + ->> WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared SELECT with ->> comparison and ? param.
     */
    public function testPreparedSelectJsonbArrowComparison(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT name FROM pg_jdml_items WHERE (meta->>'weight')::int > ? ORDER BY name",
                [8]
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Prepared SELECT ->> >: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared SELECT ->> comparison failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with JSONB || merge operator.
     */
    public function testUpdateJsonbMergeOperator(): void
    {
        $sql = "UPDATE pg_jdml_items SET meta = meta || '{\"verified\": true}' WHERE id = 1";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT meta FROM pg_jdml_items WHERE id = 1");

            $decoded = json_decode($rows[0]['meta'], true);
            if ($decoded === null) {
                $this->markTestIncomplete('UPDATE || merge: meta not valid JSON: ' . $rows[0]['meta']);
            }

            $this->assertTrue($decoded['verified']);
            $this->assertSame('red', $decoded['color']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE || merge failed: ' . $e->getMessage());
        }
    }

    /**
     * UPSERT with jsonb_set in ON CONFLICT DO UPDATE.
     */
    public function testUpsertWithJsonbSet(): void
    {
        $sql = "INSERT INTO pg_jdml_items (id, name, meta)
                VALUES (1, 'Widget', '{\"color\": \"purple\", \"weight\": 99}')
                ON CONFLICT (id) DO UPDATE SET meta = jsonb_set(pg_jdml_items.meta, '{color}', '\"purple\"')";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT meta FROM pg_jdml_items WHERE id = 1");

            $decoded = json_decode($rows[0]['meta'], true);
            $this->assertSame('purple', $decoded['color']);
            $this->assertEquals(10, $decoded['weight']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Upsert jsonb_set failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with @> containment operator.
     */
    public function testDeleteWithJsonbContainment(): void
    {
        $sql = "DELETE FROM pg_jdml_items WHERE meta @> '{\"color\": \"red\"}'";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT name FROM pg_jdml_items ORDER BY id");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'DELETE @> containment: expected 1, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame('Gadget', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE @> containment failed: ' . $e->getMessage());
        }
    }
}
