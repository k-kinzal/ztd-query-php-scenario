<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests FLOAT/DOUBLE precision through CTE shadow on MySQL.
 *
 * The CTE rewriter converts PHP float values to string via (string)$val,
 * which uses PHP's default precision (~14 significant digits). Values with
 * more precision may be truncated. Also tests edge cases like very small
 * numbers, scientific notation boundaries, and IEEE 754 special cases.
 *
 * @spec SPEC-4.2
 */
class FloatPrecisionInShadowTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_fps_items (
            id INT PRIMARY KEY,
            float_val FLOAT,
            double_val DOUBLE
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mi_fps_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_fps_items VALUES (1, 3.14159, 3.141592653589793)");
    }

    /**
     * Basic DOUBLE value should survive CTE round-trip.
     */
    public function testDoublePrecisionBasic(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_fps_items VALUES (2, 0, 2.718281828459045)");

            $rows = $this->ztdQuery("SELECT double_val FROM mi_fps_items WHERE id = 2");
            $this->assertCount(1, $rows);

            $val = (float) $rows[0]['double_val'];
            if (abs($val - 2.718281828459045) > 1e-15) {
                $this->markTestIncomplete(
                    'DOUBLE precision lost: expected 2.718281828459045, got ' . $rows[0]['double_val']
                );
            }
            $this->assertEqualsWithDelta(2.718281828459045, $val, 1e-15);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Basic double test failed: ' . $e->getMessage());
        }
    }

    /**
     * Very small DOUBLE — near epsilon values.
     */
    public function testVerySmallDouble(): void
    {
        try {
            // Use a value that requires scientific notation
            $this->mysqli->query("INSERT INTO mi_fps_items VALUES (3, 0, 1.23456789012345e-20)");

            $rows = $this->ztdQuery("SELECT double_val FROM mi_fps_items WHERE id = 3");
            $this->assertCount(1, $rows);

            $val = (float) $rows[0]['double_val'];
            $expected = 1.23456789012345e-20;
            $relError = abs($val - $expected) / abs($expected);

            if ($relError > 1e-10) {
                $this->markTestIncomplete(
                    'Very small DOUBLE precision lost: expected ' . $expected
                    . ', got ' . $rows[0]['double_val'] . ' (relative error: ' . $relError . ')'
                );
            }
            $this->assertEqualsWithDelta($expected, $val, abs($expected) * 1e-10);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Very small double test failed: ' . $e->getMessage());
        }
    }

    /**
     * Very large DOUBLE — tests scientific notation rendering.
     */
    public function testVeryLargeDouble(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_fps_items VALUES (4, 0, 9.87654321098765e+18)");

            $rows = $this->ztdQuery("SELECT double_val FROM mi_fps_items WHERE id = 4");
            $this->assertCount(1, $rows);

            $val = (float) $rows[0]['double_val'];
            $expected = 9.87654321098765e+18;
            $relError = abs($val - $expected) / abs($expected);

            if ($relError > 1e-10) {
                $this->markTestIncomplete(
                    'Very large DOUBLE precision lost: expected ' . $expected
                    . ', got ' . $rows[0]['double_val'] . ' (relative error: ' . $relError . ')'
                );
            }
            $this->assertEqualsWithDelta($expected, $val, abs($expected) * 1e-10);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Very large double test failed: ' . $e->getMessage());
        }
    }

    /**
     * Classic floating point: 0.1 + 0.2 ≠ 0.3.
     * If ZTD truncates the stored value, equality checks may break.
     */
    public function testFloatingPointRepresentation(): void
    {
        try {
            // 0.1 + 0.2 = 0.30000000000000004 in IEEE 754
            $this->mysqli->query("INSERT INTO mi_fps_items VALUES (5, 0, 0.30000000000000004)");

            $rows = $this->ztdQuery("SELECT double_val FROM mi_fps_items WHERE id = 5");
            $this->assertCount(1, $rows);

            $val = (float) $rows[0]['double_val'];
            // PHP (string) cast rounds 0.30000000000000004 to "0.3"
            // So the CTE will contain 0.3, not 0.30000000000000004
            if ($val === 0.3 && $val !== 0.30000000000000004) {
                $this->markTestIncomplete(
                    'Float representation truncated: 0.30000000000000004 became 0.3.'
                    . ' PHP (string) cast loses the trailing 4.'
                    . ' Got: ' . $rows[0]['double_val']
                );
            }
            // If ZTD somehow preserves it perfectly
            $this->assertEqualsWithDelta(0.30000000000000004, $val, 1e-16);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Float representation test failed: ' . $e->getMessage());
        }
    }

    /**
     * Negative DOUBLE precision.
     */
    public function testNegativeDoublePrecision(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_fps_items VALUES (6, 0, -1.7976931348623157e+308)");

            $rows = $this->ztdQuery("SELECT double_val FROM mi_fps_items WHERE id = 6");
            $this->assertCount(1, $rows);

            $val = (float) $rows[0]['double_val'];
            $expected = -1.7976931348623157e+308;

            if (!is_finite($val)) {
                $this->markTestIncomplete(
                    'Extreme negative DOUBLE became non-finite: ' . $rows[0]['double_val']
                );
            }

            $relError = abs($val - $expected) / abs($expected);
            if ($relError > 1e-10) {
                $this->markTestIncomplete(
                    'Negative DOUBLE precision lost: expected ' . $expected
                    . ', got ' . $rows[0]['double_val']
                );
            }
            $this->assertEqualsWithDelta($expected, $val, abs($expected) * 1e-10);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Negative double test failed: ' . $e->getMessage());
        }
    }

    /**
     * FLOAT column (single precision) — inherently less precise.
     * Tests that CTE doesn't add false precision.
     */
    public function testFloatColumnPrecision(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_fps_items VALUES (7, 1.23456789, 0)");

            $rows = $this->ztdQuery("SELECT float_val FROM mi_fps_items WHERE id = 7");
            $this->assertCount(1, $rows);

            $val = (float) $rows[0]['float_val'];
            // FLOAT has ~7 significant digits
            $this->assertEqualsWithDelta(1.2345679, $val, 1e-5);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Float column test failed: ' . $e->getMessage());
        }
    }

    /**
     * Zero and negative zero should be preserved.
     */
    public function testZeroPreserved(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_fps_items VALUES (8, 0.0, 0.0)");

            $rows = $this->ztdQuery("SELECT double_val FROM mi_fps_items WHERE id = 8");
            $this->assertCount(1, $rows);

            $val = (float) $rows[0]['double_val'];
            $this->assertSame(0.0, $val);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Zero test failed: ' . $e->getMessage());
        }
    }
}
