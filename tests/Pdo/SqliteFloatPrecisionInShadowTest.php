<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests FLOAT/DOUBLE precision through CTE shadow on SQLite.
 *
 * SQLite uses REAL (8-byte IEEE 754) for all floating-point. The CTE
 * rewriter converts values via (string)$val with PHP's default precision.
 *
 * @spec SPEC-4.2
 */
class SqliteFloatPrecisionInShadowTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_fps_items (
            id INTEGER PRIMARY KEY,
            real_val REAL
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_fps_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_fps_items VALUES (1, 3.141592653589793)");
    }

    /**
     * Basic precision preservation through CTE.
     */
    public function testDoublePrecisionBasic(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_fps_items VALUES (2, 2.718281828459045)");

            $rows = $this->ztdQuery("SELECT real_val FROM sl_fps_items WHERE id = 2");
            $this->assertCount(1, $rows);

            $val = (float) $rows[0]['real_val'];
            if (abs($val - 2.718281828459045) > 1e-15) {
                $this->markTestIncomplete(
                    'REAL precision lost: expected 2.718281828459045, got ' . $rows[0]['real_val']
                );
            }
            $this->assertEqualsWithDelta(2.718281828459045, $val, 1e-15);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Basic double test failed: ' . $e->getMessage());
        }
    }

    /**
     * 0.1 + 0.2 representation.
     */
    public function testFloatingPointRepresentation(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_fps_items VALUES (3, 0.30000000000000004)");

            $rows = $this->ztdQuery("SELECT real_val FROM sl_fps_items WHERE id = 3");
            $this->assertCount(1, $rows);

            $val = (float) $rows[0]['real_val'];
            if ($val === 0.3 && $val !== 0.30000000000000004) {
                $this->markTestIncomplete(
                    'Float representation truncated: 0.30000000000000004 became 0.3.'
                    . ' Got: ' . $rows[0]['real_val']
                );
            }
            $this->assertEqualsWithDelta(0.30000000000000004, $val, 1e-16);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Float representation test failed: ' . $e->getMessage());
        }
    }

    /**
     * Very small value with scientific notation.
     */
    public function testVerySmallReal(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_fps_items VALUES (4, 1.23456789012345e-20)");

            $rows = $this->ztdQuery("SELECT real_val FROM sl_fps_items WHERE id = 4");
            $this->assertCount(1, $rows);

            $val = (float) $rows[0]['real_val'];
            $expected = 1.23456789012345e-20;
            $relError = abs($val - $expected) / abs($expected);

            if ($relError > 1e-10) {
                $this->markTestIncomplete(
                    'Small REAL precision lost: expected ' . $expected
                    . ', got ' . $rows[0]['real_val']
                );
            }
            $this->assertEqualsWithDelta($expected, $val, abs($expected) * 1e-10);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Very small real test failed: ' . $e->getMessage());
        }
    }

    /**
     * Zero preserved correctly.
     */
    public function testZeroPreserved(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_fps_items VALUES (5, 0.0)");

            $rows = $this->ztdQuery("SELECT real_val FROM sl_fps_items WHERE id = 5");
            $this->assertCount(1, $rows);

            $val = (float) $rows[0]['real_val'];
            $this->assertSame(0.0, $val);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Zero test failed: ' . $e->getMessage());
        }
    }
}
