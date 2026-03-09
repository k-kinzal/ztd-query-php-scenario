<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests UPDATE SET with SQL expressions containing FROM keyword in function
 * syntax (TRIM). SQLite parser regex does not use FROM as a SET clause
 * terminator, so these should work. Control test for PostgreSQL comparison.
 * Note: SQLite TRIM supports TRIM(name, ' ') syntax, and SUBSTR(x, start, len)
 * rather than SUBSTRING(x FROM n FOR m).
 *
 * SQL patterns exercised: UPDATE SET TRIM(...FROM...).
 * @spec SPEC-10.2.184
 */
class SqliteUpdateSetFromKeywordTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_ufk_items (
            id INTEGER PRIMARY KEY,
            name TEXT,
            code TEXT,
            created_date TEXT,
            status TEXT
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_ufk_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_ufk_items VALUES (1, '  Alice  ', 'ABC-001-XYZ', '2025-03-10', 'active')");
        $this->pdo->exec("INSERT INTO sl_ufk_items VALUES (2, '  Bob  ', 'DEF-002-UVW', '2025-06-15', 'active')");
        $this->pdo->exec("INSERT INTO sl_ufk_items VALUES (3, ' Carol ', 'GHI-003-RST', '2024-12-01', 'inactive')");
    }

    /**
     * SQLite supports standard TRIM(BOTH ' ' FROM name) syntax (since 3.39.0+).
     * Unlike PostgreSQL, the SQLite parser does not stop SET extraction at FROM.
     */
    public function testUpdateSetTrimBothFrom(): void
    {
        $this->ztdExec("UPDATE sl_ufk_items SET name = TRIM(name) WHERE id = 1");

        $rows = $this->ztdQuery("SELECT name FROM sl_ufk_items WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    /**
     * SQLite SUBSTR function uses comma syntax: SUBSTR(x, start, len).
     * This avoids the FROM keyword entirely — no parser conflict.
     */
    public function testUpdateSetSubstrComma(): void
    {
        $this->ztdExec("UPDATE sl_ufk_items SET code = SUBSTR(code, 1, 3) WHERE id = 1");

        $rows = $this->ztdQuery("SELECT code FROM sl_ufk_items WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertSame('ABC', $rows[0]['code']);
    }

    /**
     * Multiple SET assignments including function call.
     */
    public function testUpdateMultipleAssignmentsWithTrim(): void
    {
        $this->ztdExec("UPDATE sl_ufk_items SET name = TRIM(name), status = 'trimmed' WHERE id = 1");

        $rows = $this->ztdQuery("SELECT name, status FROM sl_ufk_items WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('trimmed', $rows[0]['status']);
    }

    /**
     * UPDATE with REPLACE function in SET (uses similar syntax, no FROM).
     */
    public function testUpdateSetReplace(): void
    {
        $this->ztdExec("UPDATE sl_ufk_items SET code = REPLACE(code, '-', '_') WHERE id = 1");

        $rows = $this->ztdQuery("SELECT code FROM sl_ufk_items WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertSame('ABC_001_XYZ', $rows[0]['code']);
    }
}
