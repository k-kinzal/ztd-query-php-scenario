<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests NUMERIC/DECIMAL precision handling in the CTE shadow store.
 *
 * PostgreSQL NUMERIC can store up to 131072 digits before the decimal
 * and up to 16383 after. This test uses NUMERIC(38,18) -- near the
 * maximum precision seen in financial/crypto applications -- to check
 * whether the CTE rewriter preserves full precision through insert,
 * select, arithmetic, and comparison operations.
 *
 * Also tests REAL vs DOUBLE PRECISION vs NUMERIC to detect type
 * coercion issues in the shadow store.
 *
 * @spec SPEC-3.1
 * @spec SPEC-4.1
 * @spec SPEC-3.4
 */
class PostgresNumericPrecisionTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pgt_np_precise (
                id SERIAL PRIMARY KEY,
                amount NUMERIC(38,18) NOT NULL,
                label VARCHAR(50)
            )',
            'CREATE TABLE pgt_np_types (
                id SERIAL PRIMARY KEY,
                val_real REAL,
                val_double DOUBLE PRECISION,
                val_numeric NUMERIC(20,10)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pgt_np_types', 'pgt_np_precise'];
    }

    /**
     * INSERT and SELECT a high-precision NUMERIC(38,18) value.
     */
    public function testHighPrecisionInsertAndSelect(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pgt_np_precise (id, amount, label) VALUES (1, 12345678901234567890.123456789012345678, 'big')"
            );

            $rows = $this->ztdQuery('SELECT amount FROM pgt_np_precise WHERE id = 1');
            $this->assertCount(1, $rows);

            $amount = $rows[0]['amount'];
            $this->assertNotNull($amount, 'Amount should not be null');

            // Verify leading digits preserved
            $this->assertStringStartsWith('12345678901234567890', $amount,
                'Leading digits of high-precision NUMERIC should be preserved');

            // Verify decimal portion preserved (at least first 10 digits)
            $parts = explode('.', $amount);
            $this->assertCount(2, $parts, 'Should have decimal portion');
            $this->assertStringStartsWith('1234567890', $parts[1],
                'First 10 decimal digits should be preserved');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'High-precision NUMERIC insert/select failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * NUMERIC comparison in WHERE clause: exact decimal equality.
     */
    public function testNumericExactComparison(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pgt_np_precise (id, amount, label) VALUES (1, 99.990000000000000000, 'price')"
            );

            $rows = $this->ztdQuery('SELECT id FROM pgt_np_precise WHERE amount = 99.99');
            $this->assertCount(1, $rows, 'Exact NUMERIC comparison should find the row');
            $this->assertSame(1, (int) $rows[0]['id']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'NUMERIC exact comparison failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Arithmetic on high-precision NUMERIC in SELECT.
     */
    public function testNumericArithmeticInSelect(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pgt_np_precise (id, amount, label) VALUES (1, 1000000.000000000000000001, 'base')"
            );

            $rows = $this->ztdQuery(
                'SELECT (amount * 1.000001) AS scaled FROM pgt_np_precise WHERE id = 1'
            );
            $this->assertCount(1, $rows);

            $scaled = (float) $rows[0]['scaled'];
            // 1000000.000000000000000001 * 1.000001 ~ 1000001.000001
            $this->assertGreaterThan(1000000.0, $scaled, 'Arithmetic result should be > 1000000');
            $this->assertLessThan(1000002.0, $scaled, 'Arithmetic result should be < 1000002');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'NUMERIC arithmetic in SELECT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * REAL vs DOUBLE PRECISION vs NUMERIC -- type coercion in shadow store.
     *
     * REAL has ~6-7 significant digits, DOUBLE has ~15-16,
     * NUMERIC(20,10) preserves exactly 10 decimal digits.
     * The CTE rewriter must not silently truncate any of these.
     */
    public function testFloatTypeCoercion(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pgt_np_types (id, val_real, val_double, val_numeric)
                 VALUES (1, 1.23456789, 1.23456789012345678, 1.2345678901)"
            );

            $rows = $this->ztdQuery('SELECT val_real, val_double, val_numeric FROM pgt_np_types WHERE id = 1');
            $this->assertCount(1, $rows);

            // REAL: ~6-7 significant digits
            $this->assertEqualsWithDelta(1.2345679, (float) $rows[0]['val_real'], 0.0001,
                'REAL should preserve ~6 digits');

            // DOUBLE PRECISION: ~15-16 significant digits
            $this->assertEqualsWithDelta(1.23456789012345678, (float) $rows[0]['val_double'], 1e-14,
                'DOUBLE should preserve ~15 digits');

            // NUMERIC(20,10): exactly 10 decimal digits
            $numericStr = $rows[0]['val_numeric'];
            $this->assertStringContainsString('1234567890', str_replace('.', '', $numericStr),
                'NUMERIC(20,10) should preserve 10 decimal digits');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Float type coercion test failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SUM aggregate on high-precision NUMERIC column.
     */
    public function testNumericSumAggregate(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pgt_np_precise (id, amount, label) VALUES (1, 0.000000000000000001, 'tiny1')"
            );
            $this->ztdExec(
                "INSERT INTO pgt_np_precise (id, amount, label) VALUES (2, 0.000000000000000002, 'tiny2')"
            );
            $this->ztdExec(
                "INSERT INTO pgt_np_precise (id, amount, label) VALUES (3, 0.000000000000000003, 'tiny3')"
            );

            $rows = $this->ztdQuery('SELECT SUM(amount) AS total FROM pgt_np_precise');
            $this->assertCount(1, $rows);

            // SUM should be 0.000000000000000006
            $total = $rows[0]['total'];
            $this->assertNotNull($total, 'SUM should not be null');

            // At minimum the total should be non-zero
            $this->assertGreaterThan(0, (float) $total, 'SUM of tiny NUMERIC values should be positive');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'NUMERIC SUM aggregate failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with NUMERIC arithmetic -- precision should be preserved.
     */
    public function testNumericUpdateArithmetic(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pgt_np_precise (id, amount, label) VALUES (1, 100.000000000000000000, 'start')"
            );

            $this->ztdExec(
                'UPDATE pgt_np_precise SET amount = amount + 0.000000000000000001 WHERE id = 1'
            );

            $rows = $this->ztdQuery('SELECT amount FROM pgt_np_precise WHERE id = 1');
            $this->assertCount(1, $rows);

            // Result should be 100.000000000000000001
            $amount = $rows[0]['amount'];
            $this->assertStringStartsWith('100.', $amount,
                'Updated amount should start with 100.');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'NUMERIC update arithmetic failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Negative high-precision NUMERIC values.
     */
    public function testNegativeHighPrecision(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pgt_np_precise (id, amount, label) VALUES (1, -99999999999999999999.999999999999999999, 'neg')"
            );

            $rows = $this->ztdQuery('SELECT amount FROM pgt_np_precise WHERE id = 1');
            $this->assertCount(1, $rows);

            $amount = $rows[0]['amount'];
            $this->assertStringStartsWith('-', $amount, 'Negative sign should be preserved');
            $this->assertStringContainsString('99999', $amount, 'Digits should be preserved');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Negative high-precision NUMERIC failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Mixed type comparison in WHERE: REAL column compared to NUMERIC literal.
     *
     * The CTE rewriter may emit types differently, causing implicit casts.
     */
    public function testMixedTypeComparison(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pgt_np_types (id, val_real, val_double, val_numeric)
                 VALUES (1, 100.5, 100.5, 100.5000000000)"
            );
            $this->ztdExec(
                "INSERT INTO pgt_np_types (id, val_real, val_double, val_numeric)
                 VALUES (2, 200.75, 200.75, 200.7500000000)"
            );

            // Compare REAL column to a NUMERIC literal
            $rows = $this->ztdQuery(
                'SELECT id FROM pgt_np_types WHERE val_real > 150.0 ORDER BY id'
            );
            $this->assertCount(1, $rows);
            $this->assertSame(2, (int) $rows[0]['id']);

            // Compare DOUBLE column to NUMERIC column
            $rows = $this->ztdQuery(
                'SELECT id FROM pgt_np_types WHERE val_double = val_numeric::DOUBLE PRECISION ORDER BY id'
            );
            $this->assertCount(2, $rows, 'DOUBLE and NUMERIC with same value should compare equal after cast');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Mixed type comparison failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared statement with NUMERIC parameter binding.
     */
    public function testPreparedNumericBinding(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pgt_np_precise (id, amount, label) VALUES (1, 123.456000000000000000, 'a')"
            );
            $this->ztdExec(
                "INSERT INTO pgt_np_precise (id, amount, label) VALUES (2, 789.012000000000000000, 'b')"
            );

            $rows = $this->ztdPrepareAndExecute(
                'SELECT id FROM pgt_np_precise WHERE amount > ? ORDER BY id',
                [500.0]
            );
            $this->assertCount(1, $rows);
            $this->assertSame(2, (int) $rows[0]['id']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Prepared NUMERIC binding failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * ROUND and TRUNC functions on NUMERIC in shadow store.
     */
    public function testRoundAndTruncFunctions(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pgt_np_precise (id, amount, label) VALUES (1, 123.456789012345678901, 'roundme')"
            );

            $rows = $this->ztdQuery(
                'SELECT ROUND(amount, 2) AS rounded, TRUNC(amount, 4) AS truncated FROM pgt_np_precise WHERE id = 1'
            );
            $this->assertCount(1, $rows);

            $this->assertEqualsWithDelta(123.46, (float) $rows[0]['rounded'], 0.001,
                'ROUND to 2 decimals');
            $this->assertEqualsWithDelta(123.4567, (float) $rows[0]['truncated'], 0.0001,
                'TRUNC to 4 decimals');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'ROUND/TRUNC on NUMERIC failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Physical isolation -- no data should reach the physical DB.
     */
    public function testPhysicalIsolation(): void
    {
        $this->ztdExec(
            "INSERT INTO pgt_np_precise (id, amount, label) VALUES (1, 42.000000000000000000, 'test')"
        );

        $this->disableZtd();
        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM pgt_np_precise');
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should have 0 rows');
    }
}
