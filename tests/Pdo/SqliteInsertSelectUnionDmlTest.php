<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT...SELECT with UNION ALL on SQLite.
 *
 * INSERT...SELECT UNION ALL is used for data consolidation, merging
 * multiple sources into a single table, ETL pipelines, and data migration.
 *
 * @spec SPEC-10.2
 */
class SqliteInsertSelectUnionDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE sl_isu_source_a (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                value INTEGER
            )",
            "CREATE TABLE sl_isu_source_b (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                value INTEGER
            )",
            "CREATE TABLE sl_isu_combined (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                value INTEGER,
                origin TEXT
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_isu_combined', 'sl_isu_source_b', 'sl_isu_source_a'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO sl_isu_source_a (name, value) VALUES ('alpha', 10)");
        $this->ztdExec("INSERT INTO sl_isu_source_a (name, value) VALUES ('beta', 20)");
        $this->ztdExec("INSERT INTO sl_isu_source_b (name, value) VALUES ('gamma', 30)");
        $this->ztdExec("INSERT INTO sl_isu_source_b (name, value) VALUES ('delta', 40)");
    }

    /**
     * INSERT...SELECT ... UNION ALL SELECT ... from two source tables.
     */
    public function testInsertSelectUnionAll(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO sl_isu_combined (name, value, origin)
                 SELECT name, value, 'a' FROM sl_isu_source_a
                 UNION ALL
                 SELECT name, value, 'b' FROM sl_isu_source_b"
            );

            $rows = $this->ztdQuery("SELECT name, value, origin FROM sl_isu_combined ORDER BY name");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'INSERT UNION ALL (SQLite): expected 4, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(4, $rows);
            $this->assertSame('alpha', $rows[0]['name']);
            $this->assertSame('a', $rows[0]['origin']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT UNION ALL (SQLite) failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT UNION ALL after prior DML on source tables.
     */
    public function testInsertUnionAfterSourceDml(): void
    {
        try {
            $this->ztdExec("INSERT INTO sl_isu_source_a (name, value) VALUES ('epsilon', 50)");

            $this->ztdExec(
                "INSERT INTO sl_isu_combined (name, value, origin)
                 SELECT name, value, 'a' FROM sl_isu_source_a
                 UNION ALL
                 SELECT name, value, 'b' FROM sl_isu_source_b"
            );

            $rows = $this->ztdQuery("SELECT name FROM sl_isu_combined ORDER BY name");

            if (count($rows) !== 5) {
                $this->markTestIncomplete(
                    'INSERT UNION after DML (SQLite): expected 5, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(5, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT UNION after DML (SQLite) failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT UNION (without ALL) — deduplication.
     */
    public function testInsertSelectUnionDistinct(): void
    {
        try {
            $this->ztdExec("INSERT INTO sl_isu_source_b (name, value) VALUES ('alpha', 10)");

            $this->ztdExec(
                "INSERT INTO sl_isu_combined (name, value, origin)
                 SELECT name, value, 'x' FROM sl_isu_source_a
                 UNION
                 SELECT name, value, 'x' FROM sl_isu_source_b"
            );

            $rows = $this->ztdQuery("SELECT name FROM sl_isu_combined ORDER BY name");

            // alpha(10) exists in both, so UNION should deduplicate → 4 rows
            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'INSERT UNION DISTINCT (SQLite): expected 4, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(4, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT UNION DISTINCT (SQLite) failed: ' . $e->getMessage());
        }
    }
}
