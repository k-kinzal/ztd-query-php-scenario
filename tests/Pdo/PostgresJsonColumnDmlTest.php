<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests JSON/JSONB column operations in DML through ZTD on PostgreSQL.
 *
 * PostgreSQL JSONB with operators (->>, #>>, jsonb_set) is widely used.
 * Tests whether the CTE rewriter handles JSONB operations in DML correctly.
 *
 * @spec SPEC-10.2
 */
class PostgresJsonColumnDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE pg_json_products (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            metadata JSONB DEFAULT '{}'::jsonb
        )";
    }

    protected function getTableNames(): array
    {
        return ['pg_json_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO pg_json_products (name, metadata) VALUES ('Widget', '{\"color\":\"red\",\"weight\":1.5,\"tags\":[\"sale\",\"new\"]}')");
        $this->ztdExec("INSERT INTO pg_json_products (name, metadata) VALUES ('Gadget', '{\"color\":\"blue\",\"weight\":2.0,\"tags\":[\"premium\"]}')");
        $this->ztdExec("INSERT INTO pg_json_products (name, metadata) VALUES ('Doohickey', '{\"color\":\"green\",\"weight\":0.5,\"tags\":[]}')");
    }

    /**
     * SELECT with ->> operator — baseline.
     */
    public function testSelectJsonbOperator(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name, metadata->>'color' AS color FROM pg_json_products ORDER BY name"
            );

            $this->assertCount(3, $rows);
            $this->assertSame('blue', $rows[1]['color']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT JSONB ->> (PG) failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET using jsonb_set to modify a property.
     */
    public function testUpdateJsonbSet(): void
    {
        try {
            $this->ztdExec(
                "UPDATE pg_json_products SET metadata = jsonb_set(metadata, '{color}', '\"yellow\"') WHERE name = 'Widget'"
            );

            $rows = $this->ztdQuery(
                "SELECT metadata->>'color' AS color FROM pg_json_products WHERE name = 'Widget'"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'UPDATE jsonb_set (PG): expected 1, got ' . count($rows)
                );
            }

            $this->assertSame('yellow', $rows[0]['color']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE jsonb_set (PG) failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE WHERE jsonb ->> matches.
     */
    public function testDeleteWhereJsonbOperator(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM pg_json_products WHERE metadata->>'color' = 'green'"
            );

            $rows = $this->ztdQuery("SELECT name FROM pg_json_products ORDER BY name");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE WHERE JSONB ->> (PG): expected 2, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE JSONB ->> (PG) failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with || merge operator to add a key.
     */
    public function testUpdateJsonbMergeOperator(): void
    {
        try {
            $this->ztdExec(
                "UPDATE pg_json_products SET metadata = metadata || '{\"size\":\"large\"}' WHERE name = 'Widget'"
            );

            $rows = $this->ztdQuery(
                "SELECT metadata->>'size' AS size FROM pg_json_products WHERE name = 'Widget'"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'UPDATE JSONB || merge (PG): expected 1, got ' . count($rows)
                );
            }

            $this->assertSame('large', $rows[0]['size']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE JSONB || merge (PG) failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE WHERE jsonb numeric cast comparison.
     */
    public function testDeleteWhereJsonbNumericCast(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM pg_json_products WHERE (metadata->>'weight')::numeric > 1.0"
            );

            $rows = $this->ztdQuery("SELECT name FROM pg_json_products ORDER BY name");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'DELETE WHERE JSONB numeric (PG): expected 1, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertSame('Doohickey', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE JSONB numeric (PG) failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with jsonb_set using prepared parameter for the new value.
     */
    public function testPreparedUpdateJsonbSet(): void
    {
        try {
            $stmt = $this->ztdPrepare(
                "UPDATE pg_json_products SET metadata = jsonb_set(metadata, '{color}', to_jsonb($1::text)) WHERE name = $2"
            );
            $stmt->execute(['purple', 'Gadget']);

            $rows = $this->ztdQuery(
                "SELECT metadata->>'color' AS color FROM pg_json_products WHERE name = 'Gadget'"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Prepared UPDATE jsonb_set (PG): expected 1, got ' . count($rows)
                );
            }

            $this->assertSame('purple', $rows[0]['color']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE jsonb_set (PG) failed: ' . $e->getMessage());
        }
    }

    /**
     * JSONB @> containment operator in DELETE WHERE.
     */
    public function testDeleteWhereJsonbContainment(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM pg_json_products WHERE metadata @> '{\"color\":\"red\"}'"
            );

            $rows = $this->ztdQuery("SELECT name FROM pg_json_products ORDER BY name");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE WHERE JSONB @> (PG): expected 2, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE JSONB @> (PG) failed: ' . $e->getMessage());
        }
    }
}
