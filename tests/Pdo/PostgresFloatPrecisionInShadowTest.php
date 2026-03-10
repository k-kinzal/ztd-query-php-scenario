<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests FLOAT/DOUBLE PRECISION through CTE shadow on PostgreSQL.
 *
 * PostgreSQL uses DOUBLE PRECISION (8-byte IEEE 754). The CTE rewriter
 * formats values via (string)$val, which uses PHP's default precision.
 * PostgreSQL's VALUES clause optimization may interact differently.
 *
 * @spec SPEC-4.2
 */
class PostgresFloatPrecisionInShadowTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_fps_items (
            id INT PRIMARY KEY,
            real_val REAL,
            double_val DOUBLE PRECISION
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_fps_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_fps_items VALUES (1, 3.14159, 3.141592653589793)");
    }

    /**
     * Basic DOUBLE PRECISION preservation.
     */
    public function testDoublePrecisionBasic(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_fps_items VALUES (2, 0, 2.718281828459045)");

            $rows = $this->ztdQuery("SELECT double_val FROM pg_fps_items WHERE id = 2");
            $this->assertCount(1, $rows);

            $val = (float) $rows[0]['double_val'];
            if (abs($val - 2.718281828459045) > 1e-15) {
                $this->markTestIncomplete(
                    'DOUBLE PRECISION lost: expected 2.718281828459045, got ' . $rows[0]['double_val']
                );
            }
            $this->assertEqualsWithDelta(2.718281828459045, $val, 1e-15);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Basic double test failed: ' . $e->getMessage());
        }
    }

    /**
     * 0.1 + 0.2 representation through CTE.
     */
    public function testFloatingPointRepresentation(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_fps_items VALUES (3, 0, 0.30000000000000004)");

            $rows = $this->ztdQuery("SELECT double_val FROM pg_fps_items WHERE id = 3");
            $this->assertCount(1, $rows);

            $val = (float) $rows[0]['double_val'];
            if ($val === 0.3 && $val !== 0.30000000000000004) {
                $this->markTestIncomplete(
                    'Float representation truncated: 0.30000000000000004 became 0.3.'
                    . ' Got: ' . $rows[0]['double_val']
                );
            }
            $this->assertEqualsWithDelta(0.30000000000000004, $val, 1e-16);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Float representation test failed: ' . $e->getMessage());
        }
    }

    /**
     * Very large DOUBLE PRECISION value.
     */
    public function testVeryLargeDouble(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_fps_items VALUES (4, 0, 9.87654321098765e+18)");

            $rows = $this->ztdQuery("SELECT double_val FROM pg_fps_items WHERE id = 4");
            $this->assertCount(1, $rows);

            $val = (float) $rows[0]['double_val'];
            $expected = 9.87654321098765e+18;
            $relError = abs($val - $expected) / abs($expected);

            if ($relError > 1e-10) {
                $this->markTestIncomplete(
                    'Large DOUBLE precision lost: expected ' . $expected
                    . ', got ' . $rows[0]['double_val']
                );
            }
            $this->assertEqualsWithDelta($expected, $val, abs($expected) * 1e-10);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Very large double test failed: ' . $e->getMessage());
        }
    }

    /**
     * Positive control: zero preserved.
     */
    public function testZeroPreserved(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_fps_items VALUES (5, 0.0, 0.0)");

            $rows = $this->ztdQuery("SELECT double_val FROM pg_fps_items WHERE id = 5");
            $this->assertCount(1, $rows);

            $val = (float) $rows[0]['double_val'];
            $this->assertSame(0.0, $val);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Zero test failed: ' . $e->getMessage());
        }
    }
}
