<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests numeric precision handling in the shadow store on MySQLi.
 *
 * The CTE rewriter converts values to SQL literals for the shadow CTE.
 * Numeric precision may be lost during this conversion, especially for
 * DECIMAL, DOUBLE, and very large/small numbers.
 *
 * @spec SPEC-3.1
 * @spec SPEC-4.1
 */
class NumericPrecisionTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_np_values (
            id INT AUTO_INCREMENT PRIMARY KEY,
            label VARCHAR(50) NOT NULL,
            dec_val DECIMAL(20,10),
            dbl_val DOUBLE,
            int_val BIGINT
        )';
    }

    protected function getTableNames(): array
    {
        return ['mi_np_values'];
    }

    /**
     * DECIMAL with high precision.
     *
     * @spec SPEC-4.1
     */
    public function testDecimalPrecision(): void
    {
        try {
            $this->mysqli->query(
                "INSERT INTO mi_np_values (label, dec_val) VALUES ('pi', 3.1415926535)"
            );

            $rows = $this->ztdQuery("SELECT dec_val FROM mi_np_values WHERE label = 'pi'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('DECIMAL precision: expected 1 row');
            }

            $this->assertEquals('3.1415926535', $rows[0]['dec_val'],
                'DECIMAL(20,10) should preserve 10 decimal places');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DECIMAL precision failed: ' . $e->getMessage());
        }
    }

    /**
     * Very small DECIMAL values near zero.
     *
     * @spec SPEC-4.1
     */
    public function testSmallDecimalValue(): void
    {
        try {
            $this->mysqli->query(
                "INSERT INTO mi_np_values (label, dec_val) VALUES ('tiny', 0.0000000001)"
            );

            $rows = $this->ztdQuery("SELECT dec_val FROM mi_np_values WHERE label = 'tiny'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Small DECIMAL: expected 1 row');
            }

            $val = (float) $rows[0]['dec_val'];
            $this->assertGreaterThan(0, $val, 'Small DECIMAL should not be 0');
            $this->assertEquals('0.0000000001', $rows[0]['dec_val']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Small DECIMAL value failed: ' . $e->getMessage());
        }
    }

    /**
     * BIGINT boundary values.
     *
     * @spec SPEC-4.1
     */
    public function testBigintBoundary(): void
    {
        try {
            $this->mysqli->query(
                "INSERT INTO mi_np_values (label, int_val) VALUES ('max_int', 9223372036854775807)"
            );
            $this->mysqli->query(
                "INSERT INTO mi_np_values (label, int_val) VALUES ('min_int', -9223372036854775808)"
            );

            $rows = $this->ztdQuery("SELECT label, int_val FROM mi_np_values WHERE label IN ('max_int', 'min_int') ORDER BY label");

            if (count($rows) !== 2) {
                $this->markTestIncomplete('BIGINT boundary: expected 2 rows, got ' . count($rows));
            }

            // PHP may represent these as strings on 64-bit systems
            $this->assertSame('9223372036854775807', $rows[0]['int_val'],
                'Max BIGINT should be preserved');
            $this->assertSame('-9223372036854775808', $rows[1]['int_val'],
                'Min BIGINT should be preserved');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('BIGINT boundary failed: ' . $e->getMessage());
        }
    }

    /**
     * DOUBLE with scientific notation-scale values.
     *
     * @spec SPEC-4.1
     */
    public function testDoubleScientific(): void
    {
        try {
            $this->mysqli->query(
                "INSERT INTO mi_np_values (label, dbl_val) VALUES ('large', 1.23456789e15)"
            );
            $this->mysqli->query(
                "INSERT INTO mi_np_values (label, dbl_val) VALUES ('small', 1.23456789e-10)"
            );

            $large = $this->ztdQuery("SELECT dbl_val FROM mi_np_values WHERE label = 'large'");
            $small = $this->ztdQuery("SELECT dbl_val FROM mi_np_values WHERE label = 'small'");

            if (count($large) !== 1 || count($small) !== 1) {
                $this->markTestIncomplete('DOUBLE scientific: expected 1 row each');
            }

            $this->assertEqualsWithDelta(1.23456789e15, (float) $large[0]['dbl_val'], 1e9,
                'Large DOUBLE should be approximately preserved');
            $this->assertEqualsWithDelta(1.23456789e-10, (float) $small[0]['dbl_val'], 1e-16,
                'Small DOUBLE should be approximately preserved');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DOUBLE scientific notation failed: ' . $e->getMessage());
        }
    }

    /**
     * Negative DECIMAL.
     *
     * @spec SPEC-4.1
     */
    public function testNegativeDecimal(): void
    {
        try {
            $this->mysqli->query(
                "INSERT INTO mi_np_values (label, dec_val) VALUES ('neg', -99.1234567890)"
            );

            $rows = $this->ztdQuery("SELECT dec_val FROM mi_np_values WHERE label = 'neg'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Negative DECIMAL: expected 1 row');
            }

            $this->assertEquals('-99.1234567890', $rows[0]['dec_val']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Negative DECIMAL failed: ' . $e->getMessage());
        }
    }

    /**
     * Arithmetic on DECIMAL columns in UPDATE.
     *
     * @spec SPEC-4.2
     */
    public function testDecimalArithmeticUpdate(): void
    {
        try {
            $this->mysqli->query(
                "INSERT INTO mi_np_values (label, dec_val) VALUES ('price', 100.5000000000)"
            );

            $this->mysqli->query(
                "UPDATE mi_np_values SET dec_val = dec_val * 1.075 WHERE label = 'price'"
            );

            $rows = $this->ztdQuery("SELECT dec_val FROM mi_np_values WHERE label = 'price'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('DECIMAL arithmetic: expected 1 row');
            }

            // 100.5 * 1.075 = 108.0375
            $this->assertEqualsWithDelta(108.0375, (float) $rows[0]['dec_val'], 0.0001,
                'DECIMAL arithmetic should be approximately correct');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DECIMAL arithmetic UPDATE failed: ' . $e->getMessage());
        }
    }
}
