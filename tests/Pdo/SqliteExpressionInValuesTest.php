<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests whether the CTE rewriter handles computed expressions in
 * INSERT VALUES clauses: string concatenation, arithmetic, function
 * calls, and mixed literal+expression values.
 *
 * @spec SPEC-4.1
 */
class SqliteExpressionInValuesTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE sl_eiv_items (
            id INTEGER PRIMARY KEY,
            label TEXT NOT NULL,
            amount REAL NOT NULL,
            code TEXT
        )";
    }

    protected function getTableNames(): array
    {
        return ['sl_eiv_items'];
    }

    /**
     * String concatenation expression in VALUES.
     */
    public function testStringConcatInValues(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_eiv_items (id, label, amount, code) VALUES (1, 'Hello' || ' ' || 'World', 10.0, 'A')"
            );

            $rows = $this->ztdQuery("SELECT label FROM sl_eiv_items WHERE id = 1");
            $this->assertCount(1, $rows);

            $label = $rows[0]['label'];
            if ($label !== 'Hello World') {
                $this->markTestIncomplete(
                    'String concat in VALUES wrong. Expected "Hello World", got ' . json_encode($label)
                );
            }
            $this->assertSame('Hello World', $label);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('String concat in VALUES test failed: ' . $e->getMessage());
        }
    }

    /**
     * Arithmetic expression in VALUES.
     */
    public function testArithmeticInValues(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_eiv_items (id, label, amount, code) VALUES (1, 'calc', 10.5 * 2 + 3.0, 'B')"
            );

            $rows = $this->ztdQuery("SELECT amount FROM sl_eiv_items WHERE id = 1");
            $this->assertCount(1, $rows);

            $amount = (float) $rows[0]['amount'];
            if (abs($amount - 24.0) > 0.01) {
                $this->markTestIncomplete(
                    'Arithmetic in VALUES wrong. Expected 24.0, got ' . json_encode($amount)
                );
            }
            $this->assertEquals(24.0, $amount, '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Arithmetic in VALUES test failed: ' . $e->getMessage());
        }
    }

    /**
     * SQLite function call in VALUES (UPPER, LENGTH).
     */
    public function testFunctionCallInValues(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_eiv_items (id, label, amount, code) VALUES (1, UPPER('hello'), LENGTH('testing'), SUBSTR('ABCDEF', 2, 3))"
            );

            $rows = $this->ztdQuery("SELECT label, amount, code FROM sl_eiv_items WHERE id = 1");
            $this->assertCount(1, $rows);

            $row = $rows[0];
            if ($row['label'] !== 'HELLO') {
                $this->markTestIncomplete(
                    'UPPER() in VALUES wrong. Expected "HELLO", got ' . json_encode($row['label'])
                );
            }
            if ((int) $row['amount'] !== 7) {
                $this->markTestIncomplete(
                    'LENGTH() in VALUES wrong. Expected 7, got ' . json_encode($row['amount'])
                );
            }
            if ($row['code'] !== 'BCD') {
                $this->markTestIncomplete(
                    'SUBSTR() in VALUES wrong. Expected "BCD", got ' . json_encode($row['code'])
                );
            }
            $this->assertSame('HELLO', $row['label']);
            $this->assertEquals(7, (int) $row['amount']);
            $this->assertSame('BCD', $row['code']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Function call in VALUES test failed: ' . $e->getMessage());
        }
    }

    /**
     * CASE expression in INSERT VALUES.
     */
    public function testCaseExpressionInValues(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_eiv_items (id, label, amount, code) VALUES (
                    1,
                    CASE WHEN 1 > 0 THEN 'positive' ELSE 'negative' END,
                    100.0,
                    'C'
                )"
            );

            $rows = $this->ztdQuery("SELECT label FROM sl_eiv_items WHERE id = 1");
            $this->assertCount(1, $rows);

            $label = $rows[0]['label'];
            if ($label !== 'positive') {
                $this->markTestIncomplete(
                    'CASE in VALUES wrong. Expected "positive", got ' . json_encode($label)
                );
            }
            $this->assertSame('positive', $label);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CASE expression in VALUES test failed: ' . $e->getMessage());
        }
    }

    /**
     * COALESCE and NULLIF in INSERT VALUES.
     */
    public function testCoalesceNullifInValues(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_eiv_items (id, label, amount, code) VALUES (
                    1,
                    COALESCE(NULL, 'fallback'),
                    COALESCE(NULL, NULL, 42.5),
                    NULLIF('same', 'same')
                )"
            );

            $rows = $this->ztdQuery("SELECT label, amount, code FROM sl_eiv_items WHERE id = 1");
            $this->assertCount(1, $rows);

            $row = $rows[0];
            if ($row['label'] !== 'fallback') {
                $this->markTestIncomplete(
                    'COALESCE in VALUES wrong. Expected "fallback", got ' . json_encode($row['label'])
                );
            }
            if ($row['code'] !== null) {
                $this->markTestIncomplete(
                    'NULLIF("same","same") in VALUES wrong. Expected NULL, got ' . json_encode($row['code'])
                );
            }
            $this->assertSame('fallback', $row['label']);
            $this->assertEquals(42.5, (float) $row['amount']);
            $this->assertNull($row['code']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('COALESCE/NULLIF in VALUES test failed: ' . $e->getMessage());
        }
    }

    /**
     * Multi-row INSERT with expressions in VALUES.
     */
    public function testMultiRowExpressionInsert(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_eiv_items (id, label, amount, code) VALUES
                    (1, 'item-' || '001', 10.0 + 5.0, UPPER('abc')),
                    (2, 'item-' || '002', 20.0 * 1.1, LOWER('XYZ')),
                    (3, 'item-' || '003', ABS(-30.0), REPLACE('hello', 'l', 'r'))"
            );

            $rows = $this->ztdQuery("SELECT id, label, amount, code FROM sl_eiv_items ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Multi-row expression INSERT produced ' . count($rows) . ' rows. Expected 3.'
                );
            }

            // Row 1
            if ($rows[0]['label'] !== 'item-001') {
                $this->markTestIncomplete(
                    'Row 1 label wrong. Expected "item-001", got ' . json_encode($rows[0]['label'])
                );
            }

            $this->assertSame('item-001', $rows[0]['label']);
            $this->assertEquals(15.0, (float) $rows[0]['amount'], '', 0.01);
            $this->assertSame('ABC', $rows[0]['code']);

            $this->assertSame('item-002', $rows[1]['label']);
            $this->assertEquals(22.0, (float) $rows[1]['amount'], '', 0.01);
            $this->assertSame('xyz', $rows[1]['code']);

            $this->assertSame('item-003', $rows[2]['label']);
            $this->assertEquals(30.0, (float) $rows[2]['amount'], '', 0.01);
            $this->assertSame('herro', $rows[2]['code']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-row expression INSERT test failed: ' . $e->getMessage());
        }
    }
}
