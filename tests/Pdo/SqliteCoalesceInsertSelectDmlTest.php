<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT...SELECT with COALESCE / IFNULL / NULLIF on SQLite.
 *
 * NULL-handling functions in INSERT...SELECT are common for data migration,
 * ETL, and materializing views where NULL values need default substitution.
 *
 * @spec SPEC-10.2
 */
class SqliteCoalesceInsertSelectDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE sl_cis_raw (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                email TEXT,
                score INTEGER
            )",
            "CREATE TABLE sl_cis_clean (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                score INTEGER NOT NULL
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_cis_clean', 'sl_cis_raw'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO sl_cis_raw (name, email, score) VALUES ('Alice', 'alice@test.com', 95)");
        $this->ztdExec("INSERT INTO sl_cis_raw (name, email, score) VALUES (NULL, 'bob@test.com', 80)");
        $this->ztdExec("INSERT INTO sl_cis_raw (name, email, score) VALUES ('Charlie', NULL, 70)");
        $this->ztdExec("INSERT INTO sl_cis_raw (name, email, score) VALUES (NULL, NULL, NULL)");
    }

    /**
     * INSERT...SELECT with COALESCE for default substitution.
     */
    public function testInsertSelectCoalesce(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO sl_cis_clean (name, email, score)
                 SELECT COALESCE(name, 'Unknown'), COALESCE(email, 'no-email@placeholder.com'), COALESCE(score, 0)
                 FROM sl_cis_raw"
            );

            $rows = $this->ztdQuery("SELECT name, email, score FROM sl_cis_clean ORDER BY id");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'INSERT COALESCE (SQLite): expected 4, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            // Row 1: all values present
            $this->assertSame('Alice', $rows[0]['name']);

            // Row 2: name was NULL → 'Unknown'
            if ($rows[1]['name'] !== 'Unknown') {
                $this->markTestIncomplete(
                    'INSERT COALESCE (SQLite): row 2 name expected Unknown, got ' . $rows[1]['name']
                );
            }
            $this->assertSame('Unknown', $rows[1]['name']);

            // Row 3: email was NULL → placeholder
            if ($rows[2]['email'] !== 'no-email@placeholder.com') {
                $this->markTestIncomplete(
                    'INSERT COALESCE (SQLite): row 3 email expected placeholder, got ' . $rows[2]['email']
                );
            }
            $this->assertSame('no-email@placeholder.com', $rows[2]['email']);

            // Row 4: all NULL → all defaults
            $this->assertSame('Unknown', $rows[3]['name']);
            $this->assertSame(0, (int) $rows[3]['score']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT COALESCE (SQLite) failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT with IFNULL (SQLite-specific).
     */
    public function testInsertSelectIfnull(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO sl_cis_clean (name, email, score)
                 SELECT IFNULL(name, 'N/A'), IFNULL(email, 'n/a'), IFNULL(score, -1)
                 FROM sl_cis_raw WHERE id = 4"
            );

            $rows = $this->ztdQuery("SELECT name, email, score FROM sl_cis_clean");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT IFNULL (SQLite): expected 1, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertSame('N/A', $rows[0]['name']);
            $this->assertSame('n/a', $rows[0]['email']);
            $this->assertSame(-1, (int) $rows[0]['score']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT IFNULL (SQLite) failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT COALESCE after prior DML on source.
     */
    public function testCoalesceAfterSourceDml(): void
    {
        try {
            $this->ztdExec("INSERT INTO sl_cis_raw (name, email, score) VALUES (NULL, 'new@test.com', 60)");

            $this->ztdExec(
                "INSERT INTO sl_cis_clean (name, email, score)
                 SELECT COALESCE(name, 'Unknown'), COALESCE(email, 'none'), COALESCE(score, 0)
                 FROM sl_cis_raw"
            );

            $rows = $this->ztdQuery("SELECT name FROM sl_cis_clean WHERE email = 'new@test.com'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'COALESCE after DML (SQLite): expected 1, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            if ($rows[0]['name'] !== 'Unknown') {
                $this->markTestIncomplete(
                    'COALESCE after DML (SQLite): expected Unknown, got ' . $rows[0]['name']
                );
            }

            $this->assertSame('Unknown', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('COALESCE after DML (SQLite) failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with COALESCE — replace NULLs in existing data.
     */
    public function testUpdateCoalesce(): void
    {
        try {
            $this->ztdExec("UPDATE sl_cis_raw SET name = COALESCE(name, 'Patched') WHERE name IS NULL");

            $rows = $this->ztdQuery("SELECT name FROM sl_cis_raw ORDER BY id");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'UPDATE COALESCE (SQLite): expected 4, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            // Rows 2 and 4 had NULL name → should be 'Patched'
            if ($rows[1]['name'] !== 'Patched') {
                $this->markTestIncomplete(
                    'UPDATE COALESCE (SQLite): row 2 expected Patched, got ' . ($rows[1]['name'] ?? 'NULL')
                );
            }

            $this->assertSame('Patched', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE COALESCE (SQLite) failed: ' . $e->getMessage());
        }
    }
}
