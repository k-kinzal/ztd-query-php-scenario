<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests UPDATE SET with SQL expressions containing FROM keyword in function
 * syntax (TRIM, SUBSTRING). MySQL uses phpMyAdmin SQL parser (not regex),
 * so these should work correctly. Control test for PostgreSQL comparison.
 *
 * SQL patterns exercised: UPDATE SET TRIM(...FROM...), UPDATE SET SUBSTRING(...FROM...).
 * @spec SPEC-10.2.184
 */
class UpdateSetFromKeywordTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_ufk_items (
            id INT PRIMARY KEY,
            name VARCHAR(100),
            code VARCHAR(30),
            created_date VARCHAR(20),
            status VARCHAR(20)
        )';
    }

    protected function getTableNames(): array
    {
        return ['mi_ufk_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_ufk_items VALUES (1, '  Alice  ', 'ABC-001-XYZ', '2025-03-10', 'active')");
        $this->mysqli->query("INSERT INTO mi_ufk_items VALUES (2, '  Bob  ', 'DEF-002-UVW', '2025-06-15', 'active')");
        $this->mysqli->query("INSERT INTO mi_ufk_items VALUES (3, ' Carol ', 'GHI-003-RST', '2024-12-01', 'inactive')");
    }

    public function testUpdateSetTrimBothFrom(): void
    {
        $this->ztdExec("UPDATE mi_ufk_items SET name = TRIM(BOTH ' ' FROM name) WHERE id = 1");

        $rows = $this->ztdQuery("SELECT name FROM mi_ufk_items WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public function testUpdateSetSubstringFrom(): void
    {
        $this->ztdExec("UPDATE mi_ufk_items SET code = SUBSTRING(code FROM 1 FOR 3) WHERE id = 1");

        $rows = $this->ztdQuery("SELECT code FROM mi_ufk_items WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertSame('ABC', $rows[0]['code']);
    }

    public function testUpdateMultipleAssignmentsWithTrimFrom(): void
    {
        $this->ztdExec("UPDATE mi_ufk_items SET name = TRIM(BOTH ' ' FROM name), status = 'trimmed' WHERE id = 1");

        $rows = $this->ztdQuery("SELECT name, status FROM mi_ufk_items WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('trimmed', $rows[0]['status']);
    }

    public function testUpdateSetTrimSimpleForm(): void
    {
        $this->ztdExec("UPDATE mi_ufk_items SET name = TRIM(name) WHERE id = 1");

        $rows = $this->ztdQuery("SELECT name FROM mi_ufk_items WHERE id = 1");
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }
}
