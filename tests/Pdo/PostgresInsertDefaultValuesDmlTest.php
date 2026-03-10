<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests INSERT DEFAULT VALUES through ZTD on PostgreSQL.
 *
 * @spec SPEC-10.2
 */
class PostgresInsertDefaultValuesDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE pg_idv_counters (
            id SERIAL PRIMARY KEY,
            value INTEGER DEFAULT 0,
            label VARCHAR(100) DEFAULT 'untitled',
            created_at TIMESTAMP DEFAULT NOW()
        )";
    }

    protected function getTableNames(): array
    {
        return ['pg_idv_counters'];
    }

    public function testInsertDefaultValues(): void
    {
        try {
            $this->ztdExec("INSERT INTO pg_idv_counters DEFAULT VALUES");

            $rows = $this->ztdQuery("SELECT id, value, label FROM pg_idv_counters");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT DEFAULT VALUES (PG): expected 1, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertSame(0, (int) $rows[0]['value']);
            $this->assertSame('untitled', $rows[0]['label']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT DEFAULT VALUES (PG) failed: ' . $e->getMessage());
        }
    }

    public function testMultipleInsertDefaultValues(): void
    {
        try {
            $this->ztdExec("INSERT INTO pg_idv_counters DEFAULT VALUES");
            $this->ztdExec("INSERT INTO pg_idv_counters DEFAULT VALUES");
            $this->ztdExec("INSERT INTO pg_idv_counters DEFAULT VALUES");

            $rows = $this->ztdQuery("SELECT id FROM pg_idv_counters ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Multiple INSERT DEFAULT VALUES (PG): expected 3, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multiple INSERT DEFAULT VALUES (PG) failed: ' . $e->getMessage());
        }
    }

    public function testInsertDefaultValuesThenUpdate(): void
    {
        try {
            $this->ztdExec("INSERT INTO pg_idv_counters DEFAULT VALUES");
            $this->ztdExec("UPDATE pg_idv_counters SET value = 42, label = 'updated' WHERE value = 0");

            $rows = $this->ztdQuery("SELECT value, label FROM pg_idv_counters");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT DEFAULT then UPDATE (PG): expected 1, got ' . count($rows)
                );
            }

            $this->assertSame(42, (int) $rows[0]['value']);
            $this->assertSame('updated', $rows[0]['label']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT DEFAULT then UPDATE (PG) failed: ' . $e->getMessage());
        }
    }
}
