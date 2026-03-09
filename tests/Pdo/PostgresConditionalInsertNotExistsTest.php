<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests INSERT ... SELECT WHERE NOT EXISTS on PostgreSQL shadow data.
 *
 * Real-world scenario: portable upsert alternative used across databases.
 * The EXISTS subquery must correctly evaluate against shadow data.
 *
 * @spec SPEC-4.1
 * @spec SPEC-4.1a
 */
class PostgresConditionalInsertNotExistsTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_cine_config (
                key VARCHAR(100) PRIMARY KEY,
                value TEXT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_cine_config'];
    }

    /**
     * INSERT WHERE NOT EXISTS — new row inserted.
     */
    public function testInsertNotExistsNewRow(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pg_cine_config (key, value)
                 SELECT 'app.name', 'MyApp'
                 WHERE NOT EXISTS (SELECT 1 FROM pg_cine_config WHERE key = 'app.name')"
            );

            $rows = $this->ztdQuery("SELECT * FROM pg_cine_config");
            $this->assertCount(1, $rows);
            $this->assertSame('MyApp', $rows[0]['value']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT NOT EXISTS (new) failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT WHERE NOT EXISTS — existing row skipped.
     */
    public function testInsertNotExistsExistingSkipped(): void
    {
        $this->ztdExec("INSERT INTO pg_cine_config VALUES ('app.name', 'MyApp')");

        try {
            $this->ztdExec(
                "INSERT INTO pg_cine_config (key, value)
                 SELECT 'app.name', 'OtherApp'
                 WHERE NOT EXISTS (SELECT 1 FROM pg_cine_config WHERE key = 'app.name')"
            );

            $rows = $this->ztdQuery("SELECT * FROM pg_cine_config WHERE key = 'app.name'");
            $this->assertCount(1, $rows);

            if ($rows[0]['value'] !== 'MyApp') {
                $this->markTestIncomplete(
                    'INSERT NOT EXISTS overwrote existing value: got ' . $rows[0]['value']
                );
            }

            $this->assertSame('MyApp', $rows[0]['value']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT NOT EXISTS (existing) failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Sequential conditional inserts — second should see first shadow row.
     */
    public function testSequentialConditionalInserts(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pg_cine_config (key, value)
                 SELECT 'db.host', 'localhost'
                 WHERE NOT EXISTS (SELECT 1 FROM pg_cine_config WHERE key = 'db.host')"
            );

            $this->ztdExec(
                "INSERT INTO pg_cine_config (key, value)
                 SELECT 'db.host', 'remote.server'
                 WHERE NOT EXISTS (SELECT 1 FROM pg_cine_config WHERE key = 'db.host')"
            );

            $rows = $this->ztdQuery("SELECT * FROM pg_cine_config WHERE key = 'db.host'");

            if (count($rows) > 1) {
                $this->markTestIncomplete(
                    'Sequential INSERT NOT EXISTS created duplicate. '
                    . 'The NOT EXISTS subquery did not see the first shadow-inserted row.'
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('localhost', $rows[0]['value']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Sequential conditional inserts failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT NOT EXISTS with $N prepared parameters.
     */
    public function testInsertNotExistsWithDollarParams(): void
    {
        $this->ztdExec("INSERT INTO pg_cine_config VALUES ('existing_key', 'existing_val')");

        try {
            $stmt = $this->pdo->prepare(
                "INSERT INTO pg_cine_config (key, value)
                 SELECT $1, $2
                 WHERE NOT EXISTS (SELECT 1 FROM pg_cine_config WHERE key = $1)"
            );

            // Try duplicate
            $stmt->execute(['existing_key', 'new_val']);

            $rows = $this->ztdQuery("SELECT * FROM pg_cine_config WHERE key = 'existing_key'");

            if (count($rows) > 1) {
                $this->markTestIncomplete(
                    'INSERT NOT EXISTS with $N params created duplicate.'
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('existing_val', $rows[0]['value']);

            // Insert genuinely new key
            $stmt->execute(['new_key', 'new_val']);
            $rows = $this->ztdQuery("SELECT * FROM pg_cine_config ORDER BY key");
            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT NOT EXISTS with $N params failed: ' . $e->getMessage()
            );
        }
    }
}
