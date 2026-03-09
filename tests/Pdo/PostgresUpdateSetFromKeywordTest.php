<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests UPDATE SET with SQL expressions that contain the FROM keyword as part
 * of function syntax (TRIM, SUBSTRING, EXTRACT). The PgSqlParser uses a regex
 * that stops SET clause extraction at the first FROM keyword, which conflicts
 * with standard SQL functions: TRIM(x FROM y), SUBSTRING(x FROM n FOR m),
 * EXTRACT(field FROM source).
 *
 * SQL patterns exercised: UPDATE SET TRIM(...FROM...), UPDATE SET SUBSTRING(...FROM...),
 * UPDATE SET with multiple assignments where one uses FROM-syntax function.
 * @spec SPEC-10.2.184
 */
class PostgresUpdateSetFromKeywordTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_ufk_items (
            id INT PRIMARY KEY,
            name VARCHAR(100),
            code VARCHAR(30),
            created_date VARCHAR(20),
            status VARCHAR(20)
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_ufk_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_ufk_items VALUES (1, '  Alice  ', 'ABC-001-XYZ', '2025-03-10', 'active')");
        $this->pdo->exec("INSERT INTO pg_ufk_items VALUES (2, '  Bob  ', 'DEF-002-UVW', '2025-06-15', 'active')");
        $this->pdo->exec("INSERT INTO pg_ufk_items VALUES (3, ' Carol ', 'GHI-003-RST', '2024-12-01', 'inactive')");
    }

    /**
     * UPDATE SET col = TRIM(BOTH ' ' FROM col).
     * Standard SQL TRIM syntax uses FROM as function keyword.
     * PgSqlParser::extractUpdateSets regex stops at first FROM.
     */
    public function testUpdateSetTrimBothFrom(): void
    {
        $this->ztdExec("UPDATE pg_ufk_items SET name = TRIM(BOTH ' ' FROM name) WHERE id = 1");

        $rows = $this->ztdQuery("SELECT name FROM pg_ufk_items WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    /**
     * UPDATE SET col = TRIM(LEADING FROM col) — TRIM without explicit character.
     */
    public function testUpdateSetTrimLeadingFrom(): void
    {
        $this->ztdExec("UPDATE pg_ufk_items SET name = TRIM(LEADING ' ' FROM name) WHERE id = 2");

        $rows = $this->ztdQuery("SELECT name FROM pg_ufk_items WHERE id = 2");
        $this->assertCount(1, $rows);
        $this->assertSame('Bob  ', $rows[0]['name']);
    }

    /**
     * UPDATE SET col = SUBSTRING(col FROM n FOR m).
     * Standard SQL SUBSTRING syntax uses FROM as position keyword.
     */
    public function testUpdateSetSubstringFrom(): void
    {
        $this->ztdExec("UPDATE pg_ufk_items SET code = SUBSTRING(code FROM 1 FOR 3) WHERE id = 1");

        $rows = $this->ztdQuery("SELECT code FROM pg_ufk_items WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertSame('ABC', $rows[0]['code']);
    }

    /**
     * UPDATE SET col = EXTRACT(YEAR FROM col).
     * EXTRACT uses FROM to separate field and source.
     * Related to SPEC-11.PG-EXTRACT but that issue documents CTE casting;
     * this tests whether the SET clause itself is parsed correctly.
     */
    public function testUpdateSetExtractFrom(): void
    {
        $this->ztdExec("UPDATE pg_ufk_items SET code = CAST(EXTRACT(YEAR FROM CAST(created_date AS DATE)) AS VARCHAR) WHERE id = 1");

        $rows = $this->ztdQuery("SELECT code FROM pg_ufk_items WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertSame('2025', $rows[0]['code']);
    }

    /**
     * Multiple SET assignments where one uses TRIM(... FROM ...).
     * Tests that subsequent assignments aren't lost.
     */
    public function testUpdateMultipleAssignmentsWithTrimFrom(): void
    {
        $this->ztdExec("UPDATE pg_ufk_items SET name = TRIM(BOTH ' ' FROM name), status = 'trimmed' WHERE id = 1");

        $rows = $this->ztdQuery("SELECT name, status FROM pg_ufk_items WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('trimmed', $rows[0]['status']);
    }

    /**
     * TRIM without FROM syntax (simple form) should work regardless.
     * This is the control test.
     */
    public function testUpdateSetTrimSimpleForm(): void
    {
        $this->ztdExec("UPDATE pg_ufk_items SET name = TRIM(name) WHERE id = 1");

        $rows = $this->ztdQuery("SELECT name FROM pg_ufk_items WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    /**
     * SELECT with TRIM(... FROM ...) should work (not an UPDATE parsing issue).
     * Control test: the FROM keyword in TRIM should be harmless in SELECT context.
     */
    public function testSelectTrimFromWorksCorrectly(): void
    {
        $rows = $this->ztdQuery("SELECT TRIM(BOTH ' ' FROM name) AS trimmed FROM pg_ufk_items WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['trimmed']);
    }
}
