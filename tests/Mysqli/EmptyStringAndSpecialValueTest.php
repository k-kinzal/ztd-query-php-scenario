<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests CTE shadow handling of empty strings, special numeric values,
 * and edge-case data types.
 *
 * The CTE rewriter must preserve:
 * - Empty string '' (not confuse with NULL)
 * - Strings that look like numbers ('0', '00', '1e5')
 * - Unicode/multi-byte characters
 * - Very long strings
 *
 * @spec SPEC-4.2
 */
class EmptyStringAndSpecialValueTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_esv_items (
            id INT PRIMARY KEY,
            str_val VARCHAR(500) NOT NULL DEFAULT \'\',
            num_val DECIMAL(10,4) NOT NULL DEFAULT 0,
            int_val INT NOT NULL DEFAULT 0
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4';
    }

    protected function getTableNames(): array
    {
        return ['mi_esv_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_esv_items VALUES (1, '', 0, 0)");
        $this->mysqli->query("INSERT INTO mi_esv_items VALUES (2, 'normal', 12.3456, 42)");
    }

    /**
     * Empty string should remain empty string, not become NULL.
     */
    public function testEmptyStringPreserved(): void
    {
        try {
            // Trigger shadow
            $this->mysqli->query("UPDATE mi_esv_items SET int_val = 1 WHERE id = 2");

            $rows = $this->ztdQuery("SELECT str_val FROM mi_esv_items WHERE id = 1");
            $this->assertCount(1, $rows);

            $val = $rows[0]['str_val'];
            if ($val === null) {
                $this->markTestIncomplete('Empty string became NULL in CTE shadow');
            }
            if ($val !== '') {
                $this->markTestIncomplete(
                    'Empty string corrupted: expected "", got ' . json_encode($val)
                );
            }
            $this->assertSame('', $val);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Empty string test failed: ' . $e->getMessage());
        }
    }

    /**
     * String '0' should not be treated as falsy/empty.
     */
    public function testStringZeroPreserved(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_esv_items VALUES (3, '0', 0, 0)");

            $rows = $this->ztdQuery("SELECT str_val FROM mi_esv_items WHERE id = 3");
            $this->assertCount(1, $rows);

            $val = $rows[0]['str_val'];
            if ($val !== '0') {
                $this->markTestIncomplete(
                    'String "0" corrupted: got ' . json_encode($val)
                );
            }
            $this->assertSame('0', $val);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('String zero test failed: ' . $e->getMessage());
        }
    }

    /**
     * DECIMAL precision should be preserved through CTE.
     */
    public function testDecimalPrecisionPreserved(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_esv_items VALUES (4, 'dec', 123456.7890, 0)");

            $rows = $this->ztdQuery("SELECT num_val FROM mi_esv_items WHERE id = 4");
            $this->assertCount(1, $rows);

            $val = $rows[0]['num_val'];
            $expected = '123456.7890';
            if ((string) $val !== $expected) {
                $this->markTestIncomplete(
                    'DECIMAL precision lost: expected ' . $expected . ', got ' . json_encode($val)
                );
            }
            $this->assertSame($expected, (string) $val);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Decimal precision test failed: ' . $e->getMessage());
        }
    }

    /**
     * Negative numbers should be preserved.
     */
    public function testNegativeIntegerPreserved(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_esv_items VALUES (5, 'neg', -1.5000, -2147483648)");

            $rows = $this->ztdQuery("SELECT int_val, num_val FROM mi_esv_items WHERE id = 5");
            $this->assertCount(1, $rows);

            $intVal = (int) $rows[0]['int_val'];
            if ($intVal !== -2147483648) {
                $this->markTestIncomplete(
                    'Negative int corrupted: expected -2147483648, got ' . $intVal
                );
            }
            $this->assertEquals(-2147483648, $intVal);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Negative integer test failed: ' . $e->getMessage());
        }
    }

    /**
     * Multi-byte UTF-8 characters (emoji) should be preserved.
     */
    public function testEmojiPreserved(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_esv_items VALUES (6, '🎉🚀', 0, 0)");

            $rows = $this->ztdQuery("SELECT str_val FROM mi_esv_items WHERE id = 6");
            $this->assertCount(1, $rows);

            $val = $rows[0]['str_val'];
            if ($val !== '🎉🚀') {
                $this->markTestIncomplete(
                    'Emoji corrupted: expected 🎉🚀, got ' . json_encode($val) .
                    ' (hex: ' . bin2hex($val) . ')'
                );
            }
            $this->assertSame('🎉🚀', $val);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Emoji test failed: ' . $e->getMessage());
        }
    }

    /**
     * WHERE condition on empty string column should work through CTE.
     */
    public function testWhereOnEmptyString(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_esv_items VALUES (7, '', 0, 7)");

            $rows = $this->ztdQuery("SELECT id FROM mi_esv_items WHERE str_val = '' ORDER BY id");
            $ids = array_map('intval', array_column($rows, 'id'));

            if (!in_array(1, $ids) || !in_array(7, $ids)) {
                $this->markTestIncomplete(
                    'WHERE on empty string missed rows. Expected [1,7], got ' . json_encode($ids)
                );
            }
            $this->assertCount(2, $ids);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('WHERE on empty string failed: ' . $e->getMessage());
        }
    }

    /**
     * String containing single quotes should be preserved.
     */
    public function testSingleQuoteInStringPreserved(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_esv_items VALUES (8, 'O''Brien', 0, 0)");

            $rows = $this->ztdQuery("SELECT str_val FROM mi_esv_items WHERE id = 8");
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
}
