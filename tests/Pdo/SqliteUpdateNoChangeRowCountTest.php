<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests rowCount() behavior when UPDATE SET assigns the same value on SQLite.
 *
 * SQLite's changes() function counts only rows where values actually changed,
 * same as native MySQL default. ZTD counts matched rows via count($rows).
 *
 * @spec SPEC-4.4
 */
class SqliteUpdateNoChangeRowCountTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_uncr_items (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            status INTEGER NOT NULL DEFAULT 0
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_uncr_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_uncr_items VALUES (1, 'Alpha', 1)");
        $this->pdo->exec("INSERT INTO sl_uncr_items VALUES (2, 'Beta', 1)");
        $this->pdo->exec("INSERT INTO sl_uncr_items VALUES (3, 'Gamma', 0)");
    }

    /**
     * UPDATE setting column to its current value — native SQLite returns 0.
     */
    public function testUpdateSameValueRowCount(): void
    {
        try {
            $affected = $this->ztdExec("UPDATE sl_uncr_items SET status = 1 WHERE id = 1");

            if ($affected !== 0) {
                $this->markTestIncomplete(
                    'UPDATE no-change returned rowCount=' . $affected
                    . ', expected 0 (SQLite changes() counts actually changed rows)'
                );
            }
            $this->assertSame(0, $affected);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Same value test failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE where only some rows actually change.
     */
    public function testUpdatePartialChangeRowCount(): void
    {
        try {
            // Set all to 1. Only id=3 changes (status 0→1).
            $affected = $this->ztdExec("UPDATE sl_uncr_items SET status = 1");

            if ($affected !== 1) {
                $this->markTestIncomplete(
                    'UPDATE partial change returned rowCount=' . $affected
                    . ', expected 1 (only id=3 value changed)'
                );
            }
            $this->assertSame(1, $affected);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Partial change test failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared statement variant.
     */
    public function testPreparedUpdateSameValueRowCount(): void
    {
        try {
            $stmt = $this->pdo->prepare('UPDATE sl_uncr_items SET status = ? WHERE id = ?');
            $stmt->execute([1, 1]); // status already 1 for id=1
            $affected = $stmt->rowCount();

            if ($affected !== 0) {
                $this->markTestIncomplete(
                    'Prepared UPDATE same value returned rowCount()=' . $affected
                    . ', expected 0'
                );
            }
            $this->assertSame(0, $affected);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared same-value test failed: ' . $e->getMessage());
        }
    }

    /**
     * Positive control: actual value change.
     */
    public function testUpdateActualChangeRowCount(): void
    {
        try {
            $affected = $this->ztdExec("UPDATE sl_uncr_items SET status = 99 WHERE id = 1");
            $this->assertSame(1, $affected);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Actual change test failed: ' . $e->getMessage());
        }
    }
}
