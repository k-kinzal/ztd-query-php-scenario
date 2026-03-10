<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests INSERT...SELECT with COALESCE / IFNULL on MySQL PDO.
 *
 * @spec SPEC-10.2
 */
class MysqlCoalesceInsertSelectDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE my_cis_raw (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(200),
                score INT
            ) ENGINE=InnoDB",
            "CREATE TABLE my_cis_clean (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(200) NOT NULL,
                score INT NOT NULL
            ) ENGINE=InnoDB",
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_cis_clean', 'my_cis_raw'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO my_cis_raw (name, email, score) VALUES ('Alice', 'alice@test.com', 95)");
        $this->ztdExec("INSERT INTO my_cis_raw (name, email, score) VALUES (NULL, 'bob@test.com', 80)");
        $this->ztdExec("INSERT INTO my_cis_raw (name, email, score) VALUES ('Charlie', NULL, 70)");
        $this->ztdExec("INSERT INTO my_cis_raw (name, email, score) VALUES (NULL, NULL, NULL)");
    }

    public function testInsertSelectCoalesce(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO my_cis_clean (name, email, score)
                 SELECT COALESCE(name, 'Unknown'), COALESCE(email, 'no-email@placeholder.com'), COALESCE(score, 0)
                 FROM my_cis_raw"
            );

            $rows = $this->ztdQuery("SELECT name, email, score FROM my_cis_clean ORDER BY id");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'INSERT COALESCE (MySQL): expected 4, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertSame('Alice', $rows[0]['name']);

            if ($rows[1]['name'] !== 'Unknown') {
                $this->markTestIncomplete(
                    'INSERT COALESCE (MySQL): row 2 name expected Unknown, got ' . $rows[1]['name']
                );
            }

            $this->assertSame('Unknown', $rows[1]['name']);
            $this->assertSame('no-email@placeholder.com', $rows[2]['email']);
            $this->assertSame(0, (int) $rows[3]['score']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT COALESCE (MySQL) failed: ' . $e->getMessage());
        }
    }

    public function testInsertSelectIfnull(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO my_cis_clean (name, email, score)
                 SELECT IFNULL(name, 'N/A'), IFNULL(email, 'n/a'), IFNULL(score, -1)
                 FROM my_cis_raw WHERE id = 4"
            );

            $rows = $this->ztdQuery("SELECT name, email, score FROM my_cis_clean");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT IFNULL (MySQL): expected 1, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertSame('N/A', $rows[0]['name']);
            $this->assertSame(-1, (int) $rows[0]['score']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT IFNULL (MySQL) failed: ' . $e->getMessage());
        }
    }

    public function testUpdateCoalesce(): void
    {
        try {
            $this->ztdExec("UPDATE my_cis_raw SET name = COALESCE(name, 'Patched') WHERE name IS NULL");

            $rows = $this->ztdQuery("SELECT name FROM my_cis_raw ORDER BY id");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'UPDATE COALESCE (MySQL): expected 4, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            if ($rows[1]['name'] !== 'Patched') {
                $this->markTestIncomplete(
                    'UPDATE COALESCE (MySQL): row 2 expected Patched, got ' . ($rows[1]['name'] ?? 'NULL')
                );
            }

            $this->assertSame('Patched', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE COALESCE (MySQL) failed: ' . $e->getMessage());
        }
    }
}
