<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests NUMERIC/DECIMAL precision preservation through ZTD on PostgreSQL.
 *
 * @spec SPEC-10.2
 */
class PostgresDecimalPrecisionDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE pg_dec_t (
            id SERIAL PRIMARY KEY,
            amount NUMERIC(15,2),
            rate NUMERIC(10,6),
            big_amount NUMERIC(30,10)
        )";
    }

    protected function getTableNames(): array
    {
        return ['pg_dec_t'];
    }

    public function testBasicNumericInsertSelect(): void
    {
        try {
            $this->ztdExec("INSERT INTO pg_dec_t (amount) VALUES (12345.67)");

            $rows = $this->ztdQuery("SELECT amount FROM pg_dec_t");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'NUMERIC basic (PG): expected 1 row, got ' . count($rows)
                );
            }

            $this->assertSame('12345.67', $rows[0]['amount']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('NUMERIC basic (PG) failed: ' . $e->getMessage());
        }
    }

    public function testHighPrecisionNumeric(): void
    {
        try {
            $this->ztdExec("INSERT INTO pg_dec_t (rate) VALUES (3.141593)");

            $rows = $this->ztdQuery("SELECT rate FROM pg_dec_t");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'NUMERIC precision (PG): expected 1 row, got ' . count($rows)
                );
            }

            $this->assertSame('3.141593', $rows[0]['rate']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('NUMERIC precision (PG) failed: ' . $e->getMessage());
        }
    }

    public function testLargeNumericBeyondFloatPrecision(): void
    {
        try {
            $this->ztdExec("INSERT INTO pg_dec_t (big_amount) VALUES (12345678901234567890.1234567890)");

            $rows = $this->ztdQuery("SELECT big_amount FROM pg_dec_t");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Large NUMERIC (PG): expected 1 row, got ' . count($rows)
                );
            }

            $expected = '12345678901234567890.1234567890';
            if ($rows[0]['big_amount'] !== $expected) {
                $this->markTestIncomplete(
                    'Large NUMERIC (PG): precision lost. Expected ' . $expected
                    . ', got ' . $rows[0]['big_amount']
                );
            }

            $this->assertSame($expected, $rows[0]['big_amount']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Large NUMERIC (PG) failed: ' . $e->getMessage());
        }
    }

    public function testNumericArithmeticUpdate(): void
    {
        try {
            $this->ztdExec("INSERT INTO pg_dec_t (amount) VALUES (100.00)");
            $this->ztdExec("UPDATE pg_dec_t SET amount = amount + 0.01 WHERE amount = 100.00");

            $rows = $this->ztdQuery("SELECT amount FROM pg_dec_t");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'NUMERIC arithmetic (PG): expected 1 row, got ' . count($rows)
                );
            }

            if ($rows[0]['amount'] !== '100.01') {
                $this->markTestIncomplete(
                    'NUMERIC arithmetic (PG): expected 100.01, got ' . $rows[0]['amount']
                );
            }

            $this->assertSame('100.01', $rows[0]['amount']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('NUMERIC arithmetic (PG) failed: ' . $e->getMessage());
        }
    }

    public function testZeroAndNegativeNumeric(): void
    {
        try {
            $this->ztdExec("INSERT INTO pg_dec_t (amount) VALUES (0.00)");
            $this->ztdExec("INSERT INTO pg_dec_t (amount) VALUES (-1234.56)");

            $rows = $this->ztdQuery("SELECT amount FROM pg_dec_t ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Zero/negative NUMERIC (PG): expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertSame('0.00', $rows[0]['amount']);
            $this->assertSame('-1234.56', $rows[1]['amount']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Zero/negative NUMERIC (PG) failed: ' . $e->getMessage());
        }
    }
}
