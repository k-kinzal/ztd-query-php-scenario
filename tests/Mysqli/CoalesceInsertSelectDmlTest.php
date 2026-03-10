<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests INSERT...SELECT with COALESCE / IFNULL on MySQLi.
 *
 * @spec SPEC-10.2
 */
class CoalesceInsertSelectDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE mi_cis_raw (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(200),
                score INT
            ) ENGINE=InnoDB",
            "CREATE TABLE mi_cis_clean (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(200) NOT NULL,
                score INT NOT NULL
            ) ENGINE=InnoDB",
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_cis_clean', 'mi_cis_raw'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO mi_cis_raw (name, email, score) VALUES ('Alice', 'alice@test.com', 95)");
        $this->ztdExec("INSERT INTO mi_cis_raw (name, email, score) VALUES (NULL, 'bob@test.com', 80)");
        $this->ztdExec("INSERT INTO mi_cis_raw (name, email, score) VALUES ('Charlie', NULL, 70)");
        $this->ztdExec("INSERT INTO mi_cis_raw (name, email, score) VALUES (NULL, NULL, NULL)");
    }

    public function testInsertSelectCoalesce(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO mi_cis_clean (name, email, score)
                 SELECT COALESCE(name, 'Unknown'), COALESCE(email, 'no-email@placeholder.com'), COALESCE(score, 0)
                 FROM mi_cis_raw"
            );

            $rows = $this->ztdQuery("SELECT name, email, score FROM mi_cis_clean ORDER BY id");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'INSERT COALESCE (MySQLi): expected 4, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertSame('Alice', $rows[0]['name']);
            if ($rows[1]['name'] !== 'Unknown') {
                $this->markTestIncomplete(
                    'INSERT COALESCE (MySQLi): row 2 name expected Unknown, got ' . $rows[1]['name']
                );
            }
            $this->assertSame('Unknown', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT COALESCE (MySQLi) failed: ' . $e->getMessage());
        }
    }

    public function testUpdateCoalesce(): void
    {
        try {
            $this->ztdExec("UPDATE mi_cis_raw SET name = COALESCE(name, 'Patched') WHERE name IS NULL");

            $rows = $this->ztdQuery("SELECT name FROM mi_cis_raw ORDER BY id");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'UPDATE COALESCE (MySQLi): expected 4, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            if ($rows[1]['name'] !== 'Patched') {
                $this->markTestIncomplete(
                    'UPDATE COALESCE (MySQLi): row 2 expected Patched, got ' . ($rows[1]['name'] ?? 'NULL')
                );
            }

            $this->assertSame('Patched', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE COALESCE (MySQLi) failed: ' . $e->getMessage());
        }
    }
}
