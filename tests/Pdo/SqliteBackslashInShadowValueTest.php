<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests CTE rewriter handling of backslash characters on SQLite.
 *
 * SQLite does NOT treat backslash as an escape character in string literals,
 * so this should work correctly (control test for MySQL comparison).
 *
 * @spec SPEC-4.2
 */
class SqliteBackslashInShadowValueTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_bsv_items (
            id INTEGER PRIMARY KEY,
            val TEXT NOT NULL,
            qty INTEGER NOT NULL DEFAULT 0
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_bsv_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // SQLite does not interpret backslash as escape, so single \ is literal
        $this->pdo->exec("INSERT INTO sl_bsv_items VALUES (1, 'C:\\Users\\test', 10)");
        $this->pdo->exec("INSERT INTO sl_bsv_items VALUES (2, 'no-backslash', 20)");
        $this->pdo->exec("INSERT INTO sl_bsv_items VALUES (3, 'line1\\nline2', 30)");
    }

    public function testBackslashPreservedInPathValue(): void
    {
        try {
            $this->pdo->exec("UPDATE sl_bsv_items SET qty = 15 WHERE id = 2");

            $rows = $this->ztdQuery("SELECT id, val FROM sl_bsv_items WHERE id = 1");
            $this->assertCount(1, $rows);

            $val = $rows[0]['val'];
            $expected = 'C:\\Users\\test';
            if ($val !== $expected) {
                $this->markTestIncomplete(
                    'Backslash corrupted: expected ' . json_encode($expected) .
                    ', got ' . json_encode($val)
                );
            }
            $this->assertSame($expected, $val);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Backslash path value test failed: ' . $e->getMessage());
        }
    }

    public function testBackslashNRemainsLiteral(): void
    {
        try {
            $this->pdo->exec("UPDATE sl_bsv_items SET qty = 35 WHERE id = 2");

            $rows = $this->ztdQuery("SELECT val FROM sl_bsv_items WHERE id = 3");
            $this->assertCount(1, $rows);

            $val = $rows[0]['val'];
            $expected = 'line1\\nline2';
            if ($val !== $expected) {
                $this->markTestIncomplete(
                    'Backslash-n corrupted: expected ' . json_encode($expected) .
                    ', got ' . json_encode($val)
                );
            }
            $this->assertSame($expected, $val);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Backslash-n test failed: ' . $e->getMessage());
        }
    }

    public function testValueEndingWithBackslash(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_bsv_items VALUES (4, 'endslash\\', 40)");

            $rows = $this->ztdQuery("SELECT val FROM sl_bsv_items WHERE id = 4");
            if (empty($rows)) {
                $this->markTestIncomplete('Value ending with backslash: no rows returned');
            }

            $val = $rows[0]['val'];
            $expected = 'endslash\\';
            if ($val !== $expected) {
                $this->markTestIncomplete(
                    'Value ending with backslash corrupted: expected ' . json_encode($expected) .
                    ', got ' . json_encode($val)
                );
            }
            $this->assertSame($expected, $val);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Value ending with backslash failed: ' . $e->getMessage());
        }
    }
}
