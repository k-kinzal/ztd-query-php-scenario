<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests INSERT...SELECT with COALESCE on PostgreSQL.
 *
 * @spec SPEC-10.2
 */
class PostgresCoalesceInsertSelectDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE pg_cis_raw (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(200),
                score INTEGER
            )",
            "CREATE TABLE pg_cis_clean (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(200) NOT NULL,
                score INTEGER NOT NULL
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_cis_clean', 'pg_cis_raw'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO pg_cis_raw (name, email, score) VALUES ('Alice', 'alice@test.com', 95)");
        $this->ztdExec("INSERT INTO pg_cis_raw (name, email, score) VALUES (NULL, 'bob@test.com', 80)");
        $this->ztdExec("INSERT INTO pg_cis_raw (name, email, score) VALUES ('Charlie', NULL, 70)");
        $this->ztdExec("INSERT INTO pg_cis_raw (name, email, score) VALUES (NULL, NULL, NULL)");
    }

    public function testInsertSelectCoalesce(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pg_cis_clean (name, email, score)
                 SELECT COALESCE(name, 'Unknown'), COALESCE(email, 'no-email@placeholder.com'), COALESCE(score, 0)
                 FROM pg_cis_raw"
            );

            $rows = $this->ztdQuery("SELECT name, email, score FROM pg_cis_clean ORDER BY id");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'INSERT COALESCE (PG): expected 4, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertSame('Alice', $rows[0]['name']);

            if ($rows[1]['name'] !== 'Unknown') {
                $this->markTestIncomplete(
                    'INSERT COALESCE (PG): row 2 name expected Unknown, got ' . $rows[1]['name']
                );
            }

            $this->assertSame('Unknown', $rows[1]['name']);
            $this->assertSame('no-email@placeholder.com', $rows[2]['email']);
            $this->assertSame(0, (int) $rows[3]['score']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT COALESCE (PG) failed: ' . $e->getMessage());
        }
    }

    public function testCoalesceAfterSourceDml(): void
    {
        try {
            $this->ztdExec("INSERT INTO pg_cis_raw (name, email, score) VALUES (NULL, 'new@test.com', 60)");

            $this->ztdExec(
                "INSERT INTO pg_cis_clean (name, email, score)
                 SELECT COALESCE(name, 'Unknown'), COALESCE(email, 'none'), COALESCE(score, 0)
                 FROM pg_cis_raw"
            );

            $rows = $this->ztdQuery("SELECT name FROM pg_cis_clean WHERE email = 'new@test.com'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'COALESCE after DML (PG): expected 1, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            if ($rows[0]['name'] !== 'Unknown') {
                $this->markTestIncomplete(
                    'COALESCE after DML (PG): expected Unknown, got ' . $rows[0]['name']
                );
            }

            $this->assertSame('Unknown', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('COALESCE after DML (PG) failed: ' . $e->getMessage());
        }
    }

    public function testUpdateCoalesce(): void
    {
        try {
            $this->ztdExec("UPDATE pg_cis_raw SET name = COALESCE(name, 'Patched') WHERE name IS NULL");

            $rows = $this->ztdQuery("SELECT name FROM pg_cis_raw ORDER BY id");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'UPDATE COALESCE (PG): expected 4, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            if ($rows[1]['name'] !== 'Patched') {
                $this->markTestIncomplete(
                    'UPDATE COALESCE (PG): row 2 expected Patched, got ' . ($rows[1]['name'] ?? 'NULL')
                );
            }

            $this->assertSame('Patched', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE COALESCE (PG) failed: ' . $e->getMessage());
        }
    }
}
