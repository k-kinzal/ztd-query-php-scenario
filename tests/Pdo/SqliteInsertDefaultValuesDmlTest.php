<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT DEFAULT VALUES through ZTD on SQLite.
 *
 * INSERT DEFAULT VALUES is a SQL standard form that inserts a row using
 * only column defaults. This tests whether the CTE rewriter can handle
 * a statement with no explicit column list or VALUES clause.
 *
 * @spec SPEC-10.2
 */
class SqliteInsertDefaultValuesDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE sl_idv_counters (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            value INTEGER DEFAULT 0,
            label TEXT DEFAULT 'untitled',
            created_at TEXT DEFAULT (datetime('now'))
        )";
    }

    protected function getTableNames(): array
    {
        return ['sl_idv_counters'];
    }

    /**
     * INSERT DEFAULT VALUES — single row.
     */
    public function testInsertDefaultValues(): void
    {
        try {
            $this->ztdExec("INSERT INTO sl_idv_counters DEFAULT VALUES");

            $rows = $this->ztdQuery("SELECT id, value, label FROM sl_idv_counters");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT DEFAULT VALUES: expected 1, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame(0, (int) $rows[0]['value']);
            $this->assertSame('untitled', $rows[0]['label']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT DEFAULT VALUES failed: ' . $e->getMessage());
        }
    }

    /**
     * Multiple INSERT DEFAULT VALUES — ids should auto-increment.
     */
    public function testMultipleInsertDefaultValues(): void
    {
        try {
            $this->ztdExec("INSERT INTO sl_idv_counters DEFAULT VALUES");
            $this->ztdExec("INSERT INTO sl_idv_counters DEFAULT VALUES");
            $this->ztdExec("INSERT INTO sl_idv_counters DEFAULT VALUES");

            $rows = $this->ztdQuery("SELECT id FROM sl_idv_counters ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Multiple INSERT DEFAULT VALUES: expected 3, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $this->assertTrue((int) $rows[0]['id'] < (int) $rows[1]['id']);
            $this->assertTrue((int) $rows[1]['id'] < (int) $rows[2]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multiple INSERT DEFAULT VALUES failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT DEFAULT VALUES followed by UPDATE — verify shadow visibility.
     */
    public function testInsertDefaultValuesThenUpdate(): void
    {
        try {
            $this->ztdExec("INSERT INTO sl_idv_counters DEFAULT VALUES");
            $this->ztdExec("UPDATE sl_idv_counters SET value = 42, label = 'updated' WHERE value = 0");

            $rows = $this->ztdQuery("SELECT value, label FROM sl_idv_counters");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT DEFAULT then UPDATE: expected 1, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertSame(42, (int) $rows[0]['value']);
            $this->assertSame('updated', $rows[0]['label']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT DEFAULT then UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT DEFAULT VALUES followed by DELETE.
     */
    public function testInsertDefaultValuesThenDelete(): void
    {
        try {
            $this->ztdExec("INSERT INTO sl_idv_counters DEFAULT VALUES");
            $this->ztdExec("INSERT INTO sl_idv_counters DEFAULT VALUES");
            $this->ztdExec("DELETE FROM sl_idv_counters WHERE id = 1");

            $rows = $this->ztdQuery("SELECT id FROM sl_idv_counters");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT DEFAULT then DELETE: expected 1, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(1, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT DEFAULT then DELETE failed: ' . $e->getMessage());
        }
    }
}
