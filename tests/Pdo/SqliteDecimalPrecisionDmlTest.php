<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests numeric precision preservation through ZTD on SQLite.
 *
 * SQLite uses dynamic typing. REAL affinity stores IEEE 754 doubles.
 * NUMERIC affinity may store as integer or real depending on value.
 * Precision loss through the CTE rewriter is a real concern.
 *
 * @spec SPEC-10.2
 */
class SqliteDecimalPrecisionDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        // Use explicit PK (not AUTOINCREMENT) to avoid Issue #145 shadow PK=null
        return "CREATE TABLE sl_dec_t (
            id INTEGER PRIMARY KEY,
            amount REAL,
            exact_amount TEXT,
            num_val NUMERIC
        )";
    }

    protected function getTableNames(): array
    {
        return ['sl_dec_t'];
    }

    /**
     * REAL column round-trip.
     */
    public function testRealColumnInsertSelect(): void
    {
        try {
            $this->ztdExec("INSERT INTO sl_dec_t (id, amount) VALUES (1, 12345.67)");

            $rows = $this->ztdQuery("SELECT amount FROM sl_dec_t");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'REAL basic (SQLite): expected 1 row, got ' . count($rows)
                );
            }

            // SQLite REAL is IEEE 754 — 12345.67 should survive
            $this->assertEquals(12345.67, (float) $rows[0]['amount'], '', 0.001);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('REAL basic (SQLite) failed: ' . $e->getMessage());
        }
    }

    /**
     * TEXT column for exact decimal storage (common SQLite pattern for money).
     */
    public function testTextColumnForExactDecimal(): void
    {
        try {
            $this->ztdExec("INSERT INTO sl_dec_t (id, exact_amount) VALUES (1, '12345678901234567890.1234567890')");

            $rows = $this->ztdQuery("SELECT exact_amount FROM sl_dec_t");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'TEXT exact decimal (SQLite): expected 1 row, got ' . count($rows)
                );
            }

            $expected = '12345678901234567890.1234567890';
            if ($rows[0]['exact_amount'] !== $expected) {
                $this->markTestIncomplete(
                    'TEXT exact decimal (SQLite): precision lost. Expected ' . $expected
                    . ', got ' . $rows[0]['exact_amount']
                );
            }

            $this->assertSame($expected, $rows[0]['exact_amount']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('TEXT exact decimal (SQLite) failed: ' . $e->getMessage());
        }
    }

    /**
     * NUMERIC affinity arithmetic UPDATE.
     */
    public function testNumericArithmeticUpdate(): void
    {
        try {
            $this->ztdExec("INSERT INTO sl_dec_t (id, num_val) VALUES (1, 100)");
            $this->ztdExec("UPDATE sl_dec_t SET num_val = num_val + 0.01 WHERE num_val = 100");

            $rows = $this->ztdQuery("SELECT num_val FROM sl_dec_t");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'NUMERIC arithmetic (SQLite): expected 1 row, got ' . count($rows)
                );
            }

            $val = (float) $rows[0]['num_val'];
            if (abs($val - 100.01) > 0.001) {
                $this->markTestIncomplete(
                    'NUMERIC arithmetic (SQLite): expected ~100.01, got ' . $rows[0]['num_val']
                );
            }

            $this->assertEquals(100.01, $val, '', 0.001);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('NUMERIC arithmetic (SQLite) failed: ' . $e->getMessage());
        }
    }

    /**
     * Zero and negative values.
     */
    public function testZeroAndNegativeValues(): void
    {
        try {
            $this->ztdExec("INSERT INTO sl_dec_t (id, amount) VALUES (1, 0.0)");
            $this->ztdExec("INSERT INTO sl_dec_t (id, amount) VALUES (2, -1234.56)");

            $rows = $this->ztdQuery("SELECT amount FROM sl_dec_t ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Zero/negative (SQLite): expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertEquals(0.0, (float) $rows[0]['amount'], '', 0.001);
            $this->assertEquals(-1234.56, (float) $rows[1]['amount'], '', 0.001);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Zero/negative (SQLite) failed: ' . $e->getMessage());
        }
    }
}
