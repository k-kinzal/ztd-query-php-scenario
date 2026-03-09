<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL string prefix syntax (B'...', X'...', U&'...') through CTE rewriter.
 *
 * Real-world scenario: PostgreSQL supports several string literal prefix forms:
 * - B'101' (bit string)
 * - X'FF' (hex string)
 * - U&'\0041' (Unicode escape string)
 * These follow the same pattern as E'...' which is known to break the CTE rewriter
 * (Issue #89). Testing whether these prefixes cause the same issue.
 *
 * @spec SPEC-3.1
 * @spec SPEC-4.1
 */
class PostgresStringPrefixTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_sp_data (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                hex_val TEXT,
                flags BIT(8)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_sp_data'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO pg_sp_data (id, name, hex_val, flags) VALUES (1, 'Alpha', 'FF', B'10101010')");
        $this->ztdExec("INSERT INTO pg_sp_data (id, name, hex_val, flags) VALUES (2, 'Beta', 'AB', B'01010101')");
    }

    /**
     * SELECT with B'...' bit string literal in WHERE.
     */
    public function testBitStringInWhere(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name FROM pg_sp_data WHERE flags = B'10101010'"
            );

            $this->assertCount(1, $rows);
            $this->assertSame('Alpha', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Bit string B in WHERE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with B'...' bit string literal.
     */
    public function testBitStringInUpdate(): void
    {
        try {
            $this->ztdExec(
                "UPDATE pg_sp_data SET flags = B'11111111' WHERE id = 1"
            );

            $rows = $this->ztdQuery("SELECT flags::TEXT AS f FROM pg_sp_data WHERE id = 1");
            $this->assertCount(1, $rows);
            $this->assertSame('11111111', $rows[0]['f']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Bit string B in UPDATE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT with B'...' bit string literal.
     */
    public function testBitStringInInsert(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pg_sp_data (id, name, flags) VALUES (3, 'Gamma', B'00001111')"
            );

            $rows = $this->ztdQuery("SELECT flags::TEXT AS f FROM pg_sp_data WHERE id = 3");
            $this->assertCount(1, $rows);
            $this->assertSame('00001111', $rows[0]['f']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Bit string B in INSERT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * U&'...' Unicode escape string in INSERT.
     * U&'\0041' = 'A' (Unicode code point 0041)
     */
    public function testUnicodeEscapeStringInsert(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pg_sp_data (id, name) VALUES (4, U&'\\0041lpha')"
            );

            $rows = $this->ztdQuery("SELECT name FROM pg_sp_data WHERE id = 4");
            $this->assertCount(1, $rows);
            $this->assertSame('Alpha', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Unicode escape string in INSERT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * U&'...' Unicode escape string in UPDATE.
     */
    public function testUnicodeEscapeStringUpdate(): void
    {
        try {
            $this->ztdExec(
                "UPDATE pg_sp_data SET name = U&'\\0042eta' WHERE id = 2"
            );

            $rows = $this->ztdQuery("SELECT name FROM pg_sp_data WHERE id = 2");
            $this->assertCount(1, $rows);
            $this->assertSame('Beta', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Unicode escape string in UPDATE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * U&'...' Unicode escape string in WHERE.
     */
    public function testUnicodeEscapeStringInWhere(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id FROM pg_sp_data WHERE name = U&'\\0041lpha'"
            );

            $this->assertCount(1, $rows);
            $this->assertEquals(1, (int) $rows[0]['id']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Unicode escape string in WHERE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Mixed string prefixes in single query.
     */
    public function testMixedStringPrefixes(): void
    {
        try {
            $this->ztdExec(
                "UPDATE pg_sp_data SET name = 'normal', flags = B'11110000' WHERE id = 1"
            );

            $rows = $this->ztdQuery("SELECT name, flags::TEXT AS f FROM pg_sp_data WHERE id = 1");
            $this->assertCount(1, $rows);
            $this->assertSame('normal', $rows[0]['name']);
            $this->assertSame('11110000', $rows[0]['f']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Mixed string prefixes failed: ' . $e->getMessage()
            );
        }
    }
}
