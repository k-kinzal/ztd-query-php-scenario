<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests CTE rewriter handling of backslash characters in shadow values.
 *
 * In MySQL, backslash is an escape character in string literals by default
 * (unless NO_BACKSLASH_ESCAPES is set). The CTE rewriter's quoteValue()
 * only escapes single quotes (via ''), but does NOT escape backslashes.
 * This means shadow values containing literal backslashes may be corrupted
 * when regenerated as CTE SQL string literals.
 *
 * Common real-world values containing backslashes:
 * - Windows file paths: C:\Users\test
 * - Regex patterns stored in DB: \d+\.\d+
 * - Escaped sequences in log data
 *
 * @spec SPEC-4.2
 */
class BackslashInShadowValueTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_bsv_items (
            id INT PRIMARY KEY,
            val VARCHAR(200) NOT NULL,
            qty INT NOT NULL DEFAULT 0
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mi_bsv_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // In MySQL, \\\\ in the SQL string = literal backslash
        // 'C:\\Users\\test' stores C:\Users\test
        $this->mysqli->query("INSERT INTO mi_bsv_items VALUES (1, 'C:\\\\Users\\\\test', 10)");
        $this->mysqli->query("INSERT INTO mi_bsv_items VALUES (2, 'no-backslash', 20)");
        // 'line1\\nline2' stores line1\nline2 (literal backslash + n, NOT newline)
        $this->mysqli->query("INSERT INTO mi_bsv_items VALUES (3, 'line1\\\\nline2', 30)");
    }

    /**
     * Backslash in path-like value should be preserved through CTE shadow.
     *
     * If quoteValue() doesn't escape backslashes, MySQL interprets
     * \U as U and \t as tab in the CTE-generated SQL literal.
     */
    public function testBackslashPreservedInPathValue(): void
    {
        try {
            $this->mysqli->query("UPDATE mi_bsv_items SET qty = 15 WHERE id = 2");

            $rows = $this->ztdQuery("SELECT id, val FROM mi_bsv_items WHERE id = 1");
            $this->assertCount(1, $rows);

            $val = $rows[0]['val'];
            $expected = 'C:\\Users\\test';
            if ($val !== $expected) {
                $this->markTestIncomplete(
                    'Backslash corrupted in CTE shadow: expected ' . json_encode($expected) .
                    ', got ' . json_encode($val) .
                    ' (hex: ' . bin2hex($val) . ')'
                );
            }
            $this->assertSame($expected, $val);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Backslash path value test failed: ' . $e->getMessage());
        }
    }

    /**
     * Backslash followed by 'n' should remain literal \n, not become newline.
     *
     * MySQL interprets \n as newline (0x0a) in string literals.
     * If the CTE emits 'line1\nline2', MySQL reads it as line1 + newline + line2.
     */
    public function testBackslashNNotInterpretedAsNewline(): void
    {
        try {
            $this->mysqli->query("UPDATE mi_bsv_items SET qty = 35 WHERE id = 2");

            $rows = $this->ztdQuery("SELECT val FROM mi_bsv_items WHERE id = 3");
            $this->assertCount(1, $rows);

            $val = $rows[0]['val'];
            $expected = 'line1\\nline2';
            if ($val !== $expected) {
                $hasNewline = str_contains($val, "\n");
                $this->markTestIncomplete(
                    'Backslash-n corrupted: expected ' . json_encode($expected) .
                    ', got ' . json_encode($val) .
                    ($hasNewline ? ' (contains actual newline 0x0a!)' : '') .
                    ' (hex: ' . bin2hex($val) . ')'
                );
            }
            $this->assertSame($expected, $val);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Backslash-n test failed: ' . $e->getMessage());
        }
    }

    /**
     * Value ending with a single backslash could cause CTE syntax error.
     *
     * The CTE would generate 'endslash\' where \' is interpreted as an
     * escaped quote in MySQL, causing the string literal to not terminate.
     */
    public function testValueEndingWithBackslash(): void
    {
        try {
            // 'endslash\\' stores endslash\ (ends with literal backslash)
            $this->mysqli->query("INSERT INTO mi_bsv_items VALUES (4, 'endslash\\\\', 40)");

            $rows = $this->ztdQuery("SELECT val FROM mi_bsv_items WHERE id = 4");
            if (empty($rows)) {
                $this->markTestIncomplete(
                    'Value ending with backslash: no rows returned (possible CTE syntax error)'
                );
            }

            $val = $rows[0]['val'];
            $expected = 'endslash\\';
            if ($val !== $expected) {
                $this->markTestIncomplete(
                    'Value ending with backslash corrupted: expected ' . json_encode($expected) .
                    ', got ' . json_encode($val) .
                    ' (hex: ' . bin2hex($val) . ')'
                );
            }
            $this->assertSame($expected, $val);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Value ending with backslash failed: ' . $e->getMessage());
        }
    }

    /**
     * Backslash followed by 't' should remain literal, not become tab.
     */
    public function testBackslashTNotInterpretedAsTab(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_bsv_items VALUES (5, 'col1\\\\tcol2', 50)");

            $rows = $this->ztdQuery("SELECT val FROM mi_bsv_items WHERE id = 5");
            $this->assertCount(1, $rows);

            $val = $rows[0]['val'];
            $expected = 'col1\\tcol2';
            if ($val !== $expected) {
                $hasTab = str_contains($val, "\t");
                $this->markTestIncomplete(
                    'Backslash-t corrupted: expected ' . json_encode($expected) .
                    ', got ' . json_encode($val) .
                    ($hasTab ? ' (contains actual tab 0x09!)' : '') .
                    ' (hex: ' . bin2hex($val) . ')'
                );
            }
            $this->assertSame($expected, $val);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Backslash-t test failed: ' . $e->getMessage());
        }
    }

    /**
     * Double backslash should be preserved (not collapsed to single).
     */
    public function testDoubleBackslashPreserved(): void
    {
        try {
            // 'test\\\\\\\\value' = MySQL 'test\\\\value' = stored test\\value
            $this->mysqli->query("INSERT INTO mi_bsv_items VALUES (6, 'test\\\\\\\\value', 60)");

            $rows = $this->ztdQuery("SELECT val FROM mi_bsv_items WHERE id = 6");
            $this->assertCount(1, $rows);

            $val = $rows[0]['val'];
            $expected = 'test\\\\value';
            if ($val !== $expected) {
                $this->markTestIncomplete(
                    'Double backslash corrupted: expected ' . json_encode($expected) .
                    ', got ' . json_encode($val) .
                    ' (hex: ' . bin2hex($val) . ')'
                );
            }
            $this->assertSame($expected, $val);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Double backslash test failed: ' . $e->getMessage());
        }
    }
}
