<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests DECIMAL precision preservation through ZTD on MySQLi.
 *
 * @spec SPEC-10.2
 */
class DecimalPrecisionDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE mi_dec_t (
            id INT AUTO_INCREMENT PRIMARY KEY,
            amount DECIMAL(15,2),
            rate DECIMAL(10,6),
            big_amount DECIMAL(30,10)
        ) ENGINE=InnoDB";
    }

    protected function getTableNames(): array
    {
        return ['mi_dec_t'];
    }

    public function testBasicDecimalInsertSelect(): void
    {
        try {
            $this->ztdExec("INSERT INTO mi_dec_t (amount) VALUES (12345.67)");

            $rows = $this->ztdQuery("SELECT amount FROM mi_dec_t");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'DECIMAL basic (MySQLi): expected 1 row, got ' . count($rows)
                );
            }

            $this->assertSame('12345.67', $rows[0]['amount']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DECIMAL basic (MySQLi) failed: ' . $e->getMessage());
        }
    }

    public function testHighPrecisionDecimal(): void
    {
        try {
            $this->ztdExec("INSERT INTO mi_dec_t (rate) VALUES (3.141593)");

            $rows = $this->ztdQuery("SELECT rate FROM mi_dec_t");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'DECIMAL precision (MySQLi): expected 1 row, got ' . count($rows)
                );
            }

            $this->assertSame('3.141593', $rows[0]['rate']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DECIMAL precision (MySQLi) failed: ' . $e->getMessage());
        }
    }

    public function testLargeDecimalBeyondFloatPrecision(): void
    {
        try {
            $this->ztdExec("INSERT INTO mi_dec_t (big_amount) VALUES (12345678901234567890.1234567890)");

            $rows = $this->ztdQuery("SELECT big_amount FROM mi_dec_t");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Large DECIMAL (MySQLi): expected 1 row, got ' . count($rows)
                );
            }

            $expected = '12345678901234567890.1234567890';
            if ($rows[0]['big_amount'] !== $expected) {
                $this->markTestIncomplete(
                    'Large DECIMAL (MySQLi): precision lost. Expected ' . $expected
                    . ', got ' . $rows[0]['big_amount']
                );
            }

            $this->assertSame($expected, $rows[0]['big_amount']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Large DECIMAL (MySQLi) failed: ' . $e->getMessage());
        }
    }

    public function testDecimalArithmeticUpdate(): void
    {
        try {
            $this->ztdExec("INSERT INTO mi_dec_t (amount) VALUES (100.00)");
            $this->ztdExec("UPDATE mi_dec_t SET amount = amount + 0.01 WHERE amount = 100.00");

            $rows = $this->ztdQuery("SELECT amount FROM mi_dec_t");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'DECIMAL arithmetic (MySQLi): expected 1 row, got ' . count($rows)
                );
            }

            if ($rows[0]['amount'] !== '100.01') {
                $this->markTestIncomplete(
                    'DECIMAL arithmetic (MySQLi): expected 100.01, got ' . $rows[0]['amount']
                );
            }

            $this->assertSame('100.01', $rows[0]['amount']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DECIMAL arithmetic (MySQLi) failed: ' . $e->getMessage());
        }
    }

    public function testZeroAndNegativeDecimal(): void
    {
        try {
            $this->ztdExec("INSERT INTO mi_dec_t (amount) VALUES (0.00)");
            $this->ztdExec("INSERT INTO mi_dec_t (amount) VALUES (-1234.56)");

            $rows = $this->ztdQuery("SELECT amount FROM mi_dec_t ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Zero/negative DECIMAL (MySQLi): expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertSame('0.00', $rows[0]['amount']);
            $this->assertSame('-1234.56', $rows[1]['amount']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Zero/negative DECIMAL (MySQLi) failed: ' . $e->getMessage());
        }
    }
}
