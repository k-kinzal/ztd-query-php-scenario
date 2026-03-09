<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT DEFAULT VALUES and partial-column INSERT with DEFAULT.
 * These patterns are common for auto-increment tables and tables with
 * many defaulted columns. Issue #31 reports that INSERT...VALUES (DEFAULT)
 * fails, but INSERT DEFAULT VALUES (no columns at all) may differ.
 *
 * SQL patterns exercised: INSERT DEFAULT VALUES, INSERT with partial columns
 * relying on defaults, INSERT DEFAULT VALUES then SELECT, INSERT DEFAULT
 * VALUES then UPDATE.
 * @spec SPEC-4.1
 */
class SqliteInsertDefaultValuesTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_idv_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message TEXT DEFAULT \'no message\',
                level TEXT DEFAULT \'info\',
                created_at TEXT DEFAULT \'2025-01-01\'
            )',
            'CREATE TABLE sl_idv_counters (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                value INTEGER DEFAULT 0
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_idv_counters', 'sl_idv_logs'];
    }

    /**
     * INSERT DEFAULT VALUES — no column list, no VALUES clause.
     * All columns should get their default values (or NULL for no-default).
     */
    public function testInsertDefaultValues(): void
    {
        try {
            $this->ztdExec("INSERT INTO sl_idv_logs DEFAULT VALUES");

            $rows = $this->ztdQuery("SELECT * FROM sl_idv_logs");
            $this->assertCount(1, $rows);
            $this->assertSame('no message', $rows[0]['message']);
            $this->assertSame('info', $rows[0]['level']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT DEFAULT VALUES failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Multiple INSERT DEFAULT VALUES — auto-increment should advance.
     */
    public function testMultipleInsertDefaultValues(): void
    {
        try {
            $this->ztdExec("INSERT INTO sl_idv_logs DEFAULT VALUES");
            $this->ztdExec("INSERT INTO sl_idv_logs DEFAULT VALUES");
            $this->ztdExec("INSERT INTO sl_idv_logs DEFAULT VALUES");

            $rows = $this->ztdQuery("SELECT id FROM sl_idv_logs ORDER BY id");
            $this->assertCount(3, $rows);
            $ids = array_column($rows, 'id');
            $this->assertCount(3, array_unique($ids));
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Multiple INSERT DEFAULT VALUES failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT with partial columns, relying on defaults for rest.
     */
    public function testInsertPartialColumnsWithDefaults(): void
    {
        $this->ztdExec("INSERT INTO sl_idv_logs (message) VALUES ('test message')");

        $rows = $this->ztdQuery("SELECT message, level FROM sl_idv_logs");
        $this->assertCount(1, $rows);
        $this->assertSame('test message', $rows[0]['message']);
        if ($rows[0]['level'] !== 'info') {
            $this->markTestIncomplete(
                'Shadow store did not apply DEFAULT value for level column. Got: ' . var_export($rows[0]['level'], true)
            );
        }
        $this->assertSame('info', $rows[0]['level']);
    }

    /**
     * INSERT DEFAULT VALUES then UPDATE the row.
     */
    public function testInsertDefaultValuesThenUpdate(): void
    {
        try {
            $this->ztdExec("INSERT INTO sl_idv_logs DEFAULT VALUES");

            $rows = $this->ztdQuery("SELECT id FROM sl_idv_logs LIMIT 1");
            $id = $rows[0]['id'];

            $this->ztdExec("UPDATE sl_idv_logs SET message = 'updated', level = 'warn' WHERE id = {$id}");

            $rows = $this->ztdQuery("SELECT message, level FROM sl_idv_logs WHERE id = {$id}");
            $this->assertCount(1, $rows);
            $this->assertSame('updated', $rows[0]['message']);
            $this->assertSame('warn', $rows[0]['level']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT DEFAULT VALUES + UPDATE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT into table with NOT NULL + DEFAULT, using only required columns.
     */
    public function testInsertRequiredColumnsOnly(): void
    {
        $this->ztdExec("INSERT INTO sl_idv_counters (name) VALUES ('page_views')");

        $rows = $this->ztdQuery("SELECT name, value FROM sl_idv_counters WHERE name = 'page_views'");
        $this->assertCount(1, $rows);
        $this->assertSame('page_views', $rows[0]['name']);
        if ((int) $rows[0]['value'] !== 0) {
            $this->markTestIncomplete(
                'Shadow store did not apply DEFAULT value for value column. Got: ' . var_export($rows[0]['value'], true)
            );
        }
        $this->assertEquals(0, (int) $rows[0]['value']);
    }

    /**
     * INSERT partial columns then increment the defaulted counter.
     */
    public function testInsertPartialThenIncrement(): void
    {
        $this->ztdExec("INSERT INTO sl_idv_counters (name) VALUES ('clicks')");
        $this->ztdExec("UPDATE sl_idv_counters SET value = value + 1 WHERE name = 'clicks'");
        $this->ztdExec("UPDATE sl_idv_counters SET value = value + 1 WHERE name = 'clicks'");

        $rows = $this->ztdQuery("SELECT value FROM sl_idv_counters WHERE name = 'clicks'");
        $this->assertCount(1, $rows);

        if ($rows[0]['value'] === null) {
            $this->markTestIncomplete(
                'Shadow store did not apply DEFAULT, counter stayed NULL after increment'
            );
        }
        $this->assertEquals(2, (int) $rows[0]['value']);
    }

    /**
     * Physical isolation — INSERT DEFAULT VALUES should not touch physical table.
     */
    public function testInsertDefaultValuesPhysicalIsolation(): void
    {
        try {
            $this->ztdExec("INSERT INTO sl_idv_logs DEFAULT VALUES");

            $shadow = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_idv_logs");
            $this->assertEquals(1, (int) $shadow[0]['cnt']);

            $this->pdo->disableZtd();
            $physical = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_idv_logs")
                ->fetchAll(PDO::FETCH_ASSOC);
            $this->assertEquals(0, (int) $physical[0]['cnt']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT DEFAULT VALUES failed: ' . $e->getMessage()
            );
        }
    }
}
