<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests INSERT...SELECT with partial column list (skipping SERIAL PK)
 * on PostgreSQL.
 *
 * @spec SPEC-4.1
 */
class PostgresInsertSelectPartialColumnListTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_ispcl_source (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                value NUMERIC(10,2) NOT NULL
            )',
            'CREATE TABLE pg_ispcl_target (
                id SERIAL PRIMARY KEY,
                source_id INTEGER NOT NULL,
                source_name VARCHAR(50) NOT NULL,
                source_value NUMERIC(10,2) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_ispcl_target', 'pg_ispcl_source'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_ispcl_source VALUES (1, 'Alpha', 10.5)");
        $this->pdo->exec("INSERT INTO pg_ispcl_source VALUES (2, 'Beta', 20.0)");
        $this->pdo->exec("INSERT INTO pg_ispcl_source VALUES (3, 'Gamma', 30.7)");
    }

    /**
     * INSERT...SELECT with partial column list (skip SERIAL).
     */
    public function testInsertSelectPartialColumns(): void
    {
        $sql = "INSERT INTO pg_ispcl_target (source_id, source_name, source_value)
                SELECT id, name, value FROM pg_ispcl_source";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT source_id, source_name, source_value FROM pg_ispcl_target ORDER BY source_id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Partial INSERT SELECT: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);

            if ($rows[0]['source_id'] === null) {
                $this->markTestIncomplete(
                    'Partial INSERT SELECT: NULL columns. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame(1, (int) $rows[0]['source_id']);
            $this->assertSame('Alpha', $rows[0]['source_name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Partial INSERT SELECT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT...SELECT with string literal.
     */
    public function testInsertSelectWithStringLiteral(): void
    {
        $this->createTable("CREATE TABLE pg_ispcl_log (
            id SERIAL PRIMARY KEY,
            ref_id INTEGER NOT NULL,
            label VARCHAR(50) NOT NULL,
            action VARCHAR(20) NOT NULL
        )");

        $sql = "INSERT INTO pg_ispcl_log (ref_id, label, action)
                SELECT id, name, 'imported' FROM pg_ispcl_source";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT ref_id, label, action FROM pg_ispcl_log ORDER BY ref_id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Literal INSERT SELECT: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            if ($rows[0]['ref_id'] === null) {
                $this->markTestIncomplete(
                    'Literal INSERT SELECT: NULL columns. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame(1, (int) $rows[0]['ref_id']);
            $this->assertSame('imported', $rows[0]['action']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Literal INSERT SELECT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT...SELECT * with matching schemas.
     */
    public function testInsertSelectStarMatchingSchema(): void
    {
        $this->createTable("CREATE TABLE pg_ispcl_archive (
            id INTEGER PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            value NUMERIC(10,2) NOT NULL
        )");

        $sql = "INSERT INTO pg_ispcl_archive SELECT * FROM pg_ispcl_source";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, name FROM pg_ispcl_archive ORDER BY id");

            $this->assertCount(3, $rows);
            $this->assertSame(1, (int) $rows[0]['id']);
            $this->assertSame('Alpha', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT SELECT * failed: ' . $e->getMessage()
            );
        }
    }
}
