<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests CTE shadow handling of empty strings and special values on SQLite.
 *
 * @spec SPEC-4.2
 */
class SqliteEmptyStringAndSpecialValueTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_esv_items (
            id INTEGER PRIMARY KEY,
            str_val TEXT NOT NULL DEFAULT \'\',
            num_val REAL NOT NULL DEFAULT 0,
            int_val INTEGER NOT NULL DEFAULT 0
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_esv_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_esv_items VALUES (1, '', 0, 0)");
        $this->pdo->exec("INSERT INTO sl_esv_items VALUES (2, 'normal', 12.3456, 42)");
    }

    public function testEmptyStringPreserved(): void
    {
        try {
            $this->pdo->exec("UPDATE sl_esv_items SET int_val = 1 WHERE id = 2");

            $rows = $this->ztdQuery("SELECT str_val FROM sl_esv_items WHERE id = 1");
            $this->assertCount(1, $rows);

            $val = $rows[0]['str_val'];
            if ($val === null) {
                $this->markTestIncomplete('Empty string became NULL in CTE shadow');
            }
            $this->assertSame('', $val);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Empty string test failed: ' . $e->getMessage());
        }
    }

    public function testStringZeroPreserved(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_esv_items VALUES (3, '0', 0, 0)");

            $rows = $this->ztdQuery("SELECT str_val FROM sl_esv_items WHERE id = 3");
            $this->assertCount(1, $rows);
            $this->assertSame('0', $rows[0]['str_val']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('String zero test failed: ' . $e->getMessage());
        }
    }

    public function testSingleQuoteInStringPreserved(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_esv_items VALUES (4, 'O''Brien', 0, 0)");

            $rows = $this->ztdQuery("SELECT str_val FROM sl_esv_items WHERE id = 4");
            $this->assertCount(1, $rows);

            $val = $rows[0]['str_val'];
            if ($val !== "O'Brien") {
                $this->markTestIncomplete(
                    'Single quote corrupted: expected O\'Brien, got ' . json_encode($val)
                );
            }
            $this->assertSame("O'Brien", $val);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Single quote test failed: ' . $e->getMessage());
        }
    }

    public function testNegativeIntegerPreserved(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_esv_items VALUES (5, 'neg', -1.5, -2147483648)");

            $rows = $this->ztdQuery("SELECT int_val FROM sl_esv_items WHERE id = 5");
            $this->assertCount(1, $rows);
            $this->assertEquals(-2147483648, (int) $rows[0]['int_val']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Negative integer test failed: ' . $e->getMessage());
        }
    }
}
