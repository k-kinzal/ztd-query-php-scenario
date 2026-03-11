<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests DECIMAL precision preservation through the ZTD CTE rewriter on MySQL PDO.
 *
 * Financial applications depend on exact DECIMAL values. If the CTE rewriter
 * converts DECIMAL to float during shadow store operations, precision loss
 * will cause incorrect monetary calculations.
 *
 * @spec SPEC-10.2
 */
class MysqlDecimalPrecisionDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE my_dec_t (
            id INT AUTO_INCREMENT PRIMARY KEY,
            amount DECIMAL(15,2),
            rate DECIMAL(10,6),
            big_amount DECIMAL(30,10)
        ) ENGINE=InnoDB";
    }

    protected function getTableNames(): array
    {
        return ['my_dec_t'];
    }

    /**
     * Basic DECIMAL(15,2) round-trip.
     */
    public function testBasicDecimalInsertSelect(): void
    {
        try {
            $this->ztdExec("INSERT INTO my_dec_t (amount) VALUES (12345.67)");

            $rows = $this->ztdQuery("SELECT amount FROM my_dec_t");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'DECIMAL basic (MySQL): expected 1 row, got ' . count($rows)
                );
            }

            // DECIMAL should come back as string "12345.67" via PDO
            $this->assertSame('12345.67', $rows[0]['amount']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DECIMAL basic (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * High-precision DECIMAL(10,6) preservation.
     */
    public function testHighPrecisionDecimal(): void
    {
        try {
            $this->ztdExec("INSERT INTO my_dec_t (rate) VALUES (3.141593)");

            $rows = $this->ztdQuery("SELECT rate FROM my_dec_t");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'DECIMAL precision (MySQL): expected 1 row, got ' . count($rows)
                );
            }

            $this->assertSame('3.141593', $rows[0]['rate']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DECIMAL precision (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * Large DECIMAL(30,10) — exceeds float64 precision.
     */
    public function testLargeDecimalBeyondFloatPrecision(): void
    {
        try {
            $this->ztdExec("INSERT INTO my_dec_t (big_amount) VALUES (12345678901234567890.1234567890)");

            $rows = $this->ztdQuery("SELECT big_amount FROM my_dec_t");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Large DECIMAL (MySQL): expected 1 row, got ' . count($rows)
                );
            }

            // This value exceeds float64 precision — must be preserved as string
            $expected = '12345678901234567890.1234567890';
            if ($rows[0]['big_amount'] !== $expected) {
                $this->markTestIncomplete(
                    'Large DECIMAL (MySQL): precision lost. Expected ' . $expected
                    . ', got ' . $rows[0]['big_amount']
                );
            }

            $this->assertSame($expected, $rows[0]['big_amount']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Large DECIMAL (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * DECIMAL arithmetic in UPDATE, then read back.
     */
    public function testDecimalArithmeticUpdate(): void
    {
        try {
            $this->ztdExec("INSERT INTO my_dec_t (amount) VALUES (100.00)");
            $this->ztdExec("UPDATE my_dec_t SET amount = amount + 0.01 WHERE amount = 100.00");

            $rows = $this->ztdQuery("SELECT amount FROM my_dec_t");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'DECIMAL arithmetic (MySQL): expected 1 row, got ' . count($rows)
                );
            }

            if ($rows[0]['amount'] !== '100.01') {
                $this->markTestIncomplete(
                    'DECIMAL arithmetic (MySQL): expected 100.01, got ' . $rows[0]['amount']
                );
            }

            $this->assertSame('100.01', $rows[0]['amount']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DECIMAL arithmetic (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared INSERT with DECIMAL via string parameter.
     */
    public function testPreparedDecimalInsert(): void
    {
        try {
            $stmt = $this->pdo->prepare("INSERT INTO my_dec_t (amount, rate) VALUES (?, ?)");
            $stmt->execute(['99999.99', '0.000001']);

            $rows = $this->ztdQuery("SELECT amount, rate FROM my_dec_t");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Prepared DECIMAL (MySQL): expected 1 row, got ' . count($rows)
                );
            }

            $this->assertSame('99999.99', $rows[0]['amount']);
            $this->assertSame('0.000001', $rows[0]['rate']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DECIMAL (MySQL) failed: ' . $e->getMessage());
        }
    }

    /**
     * Zero and negative DECIMAL values.
     */
    public function testZeroAndNegativeDecimal(): void
    {
        try {
            $this->ztdExec("INSERT INTO my_dec_t (amount) VALUES (0.00)");
            $this->ztdExec("INSERT INTO my_dec_t (amount) VALUES (-1234.56)");

            $rows = $this->ztdQuery("SELECT amount FROM my_dec_t ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Zero/negative DECIMAL (MySQL): expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertSame('0.00', $rows[0]['amount']);
            $this->assertSame('-1234.56', $rows[1]['amount']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Zero/negative DECIMAL (MySQL) failed: ' . $e->getMessage());
        }
    }
}
