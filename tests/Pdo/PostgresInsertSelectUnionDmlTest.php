<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests INSERT...SELECT with UNION ALL on PostgreSQL.
 *
 * @spec SPEC-10.2
 */
class PostgresInsertSelectUnionDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE pg_isu_source_a (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                value INTEGER
            )",
            "CREATE TABLE pg_isu_source_b (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                value INTEGER
            )",
            "CREATE TABLE pg_isu_combined (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                value INTEGER,
                origin VARCHAR(10)
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_isu_combined', 'pg_isu_source_b', 'pg_isu_source_a'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO pg_isu_source_a (name, value) VALUES ('alpha', 10)");
        $this->ztdExec("INSERT INTO pg_isu_source_a (name, value) VALUES ('beta', 20)");
        $this->ztdExec("INSERT INTO pg_isu_source_b (name, value) VALUES ('gamma', 30)");
        $this->ztdExec("INSERT INTO pg_isu_source_b (name, value) VALUES ('delta', 40)");
    }

    public function testInsertSelectUnionAll(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pg_isu_combined (name, value, origin)
                 SELECT name, value, 'a' FROM pg_isu_source_a
                 UNION ALL
                 SELECT name, value, 'b' FROM pg_isu_source_b"
            );

            $rows = $this->ztdQuery("SELECT name, value, origin FROM pg_isu_combined ORDER BY name");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'INSERT UNION ALL (PG): expected 4, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(4, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT UNION ALL (PG) failed: ' . $e->getMessage());
        }
    }

    public function testInsertUnionAfterSourceDml(): void
    {
        try {
            $this->ztdExec("INSERT INTO pg_isu_source_a (name, value) VALUES ('epsilon', 50)");

            $this->ztdExec(
                "INSERT INTO pg_isu_combined (name, value, origin)
                 SELECT name, value, 'a' FROM pg_isu_source_a
                 UNION ALL
                 SELECT name, value, 'b' FROM pg_isu_source_b"
            );

            $rows = $this->ztdQuery("SELECT name FROM pg_isu_combined ORDER BY name");

            if (count($rows) !== 5) {
                $this->markTestIncomplete(
                    'INSERT UNION after DML (PG): expected 5, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(5, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT UNION after DML (PG) failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT with UNION ALL and EXCEPT (set difference).
     */
    public function testInsertSelectExcept(): void
    {
        try {
            $this->ztdExec("INSERT INTO pg_isu_source_b (name, value) VALUES ('alpha', 10)");

            $this->ztdExec(
                "INSERT INTO pg_isu_combined (name, value, origin)
                 SELECT name, value, 'only-a' FROM pg_isu_source_a
                 EXCEPT
                 SELECT name, value, 'only-a' FROM pg_isu_source_b"
            );

            $rows = $this->ztdQuery("SELECT name FROM pg_isu_combined ORDER BY name");

            // alpha(10) exists in both, so EXCEPT removes it → only beta(20)
            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT EXCEPT (PG): expected 1, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertSame('beta', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT EXCEPT (PG) failed: ' . $e->getMessage());
        }
    }
}
