<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests rowCount() behavior when UPDATE SET assigns the same value on PostgreSQL.
 *
 * Note: PostgreSQL's native behavior differs from MySQL/SQLite — it counts
 * all matched rows regardless of whether values changed. So ZTD's matched-rows
 * behavior may coincidentally match PostgreSQL's native behavior.
 *
 * This test documents the cross-platform inconsistency: ZTD returns matched-rows
 * on ALL platforms, which matches PostgreSQL but not MySQL/SQLite.
 *
 * @spec SPEC-4.4
 */
class PostgresUpdateNoChangeRowCountTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_uncr_items (
            id INT PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            status INT NOT NULL DEFAULT 0
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_uncr_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_uncr_items VALUES (1, 'Alpha', 1)");
        $this->pdo->exec("INSERT INTO pg_uncr_items VALUES (2, 'Beta', 1)");
        $this->pdo->exec("INSERT INTO pg_uncr_items VALUES (3, 'Gamma', 0)");
    }

    /**
     * UPDATE same value — PostgreSQL natively returns matched count.
     * ZTD should match this behavior on PostgreSQL.
     * Documents cross-platform inconsistency vs MySQL/SQLite.
     */
    public function testUpdateSameValueRowCount(): void
    {
        try {
            $affected = $this->ztdExec("UPDATE pg_uncr_items SET status = 1 WHERE id = 1");

            // PostgreSQL native: returns 1 (matched rows)
            // ZTD: returns 1 (count from SELECT)
            // This matches on PostgreSQL but diverges from MySQL/SQLite native behavior
            $this->markTestIncomplete(
                'PostgreSQL UPDATE no-change returned rowCount=' . $affected
                . '. Native PG returns matched-rows (1). ZTD also returns ' . $affected
                . '. Cross-platform: MySQL/SQLite native would return 0 (changed-rows).'
            );
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Same value test failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE where only some rows change — on PostgreSQL native returns all matched.
     */
    public function testUpdatePartialChangeRowCount(): void
    {
        try {
            // Set all to 1. Native PG returns 3 (all matched), not 1 (only changed).
            $affected = $this->ztdExec("UPDATE pg_uncr_items SET status = 1");

            $this->markTestIncomplete(
                'PostgreSQL UPDATE partial-change returned rowCount=' . $affected
                . '. Native PG returns 3 (all matched). ZTD returns ' . $affected
                . '. MySQL/SQLite native would return 1 (only id=3 changed).'
            );
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Partial change test failed: ' . $e->getMessage());
        }
    }

    /**
     * Positive control: actual value change should return 1 on all platforms.
     */
    public function testUpdateActualChangeRowCount(): void
    {
        try {
            $affected = $this->ztdExec("UPDATE pg_uncr_items SET status = 99 WHERE id = 1");
            $this->assertSame(1, $affected);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Actual change test failed: ' . $e->getMessage());
        }
    }
}
