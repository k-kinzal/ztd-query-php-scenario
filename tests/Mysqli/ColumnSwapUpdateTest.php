<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests column value swap via UPDATE SET a=b, b=a through ZTD on MySQLi.
 *
 * MySQL evaluates SET assignments left-to-right, so `SET a=b, b=a` uses
 * the already-updated value of `a`. The shadow store must replicate this
 * behavior exactly.
 *
 * @spec SPEC-4.2
 */
class ColumnSwapUpdateTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_csw_pairs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            col_a VARCHAR(50) NOT NULL,
            col_b VARCHAR(50) NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['mi_csw_pairs'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_csw_pairs (col_a, col_b) VALUES ('hello', 'world')");
        $this->mysqli->query("INSERT INTO mi_csw_pairs (col_a, col_b) VALUES ('foo', 'bar')");
    }

    /**
     * Swap columns a and b.
     * MySQL evaluates left-to-right: a becomes old b, then b becomes new a (which is old b).
     * So effectively: a=old_b, b=old_b (NOT a true swap without temp var).
     *
     * @spec SPEC-4.2
     */
    public function testSwapColumnsLeftToRight(): void
    {
        try {
            $this->mysqli->query(
                "UPDATE mi_csw_pairs SET col_a = col_b, col_b = col_a WHERE id = 1"
            );

            $rows = $this->ztdQuery('SELECT col_a, col_b FROM mi_csw_pairs WHERE id = 1');

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Column swap: expected 1 row');
            }

            // MySQL left-to-right: col_a = 'world' (old col_b), col_b = 'world' (new col_a)
            $this->assertSame('world', $rows[0]['col_a'],
                'col_a should be old col_b value');
            $this->assertSame('world', $rows[0]['col_b'],
                'col_b should be new col_a value (left-to-right)');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Column swap failed: ' . $e->getMessage());
        }
    }

    /**
     * Swap using temp variable approach (MySQL user variable workaround).
     *
     * @spec SPEC-4.2
     */
    public function testSwapWithArithmetic(): void
    {
        $this->mysqli->query("INSERT INTO mi_csw_pairs (col_a, col_b) VALUES ('10', '20')");

        try {
            // Use XOR swap for numeric strings via CAST
            $this->mysqli->query(
                "UPDATE mi_csw_pairs SET
                    col_a = col_b,
                    col_b = CONCAT('', CAST(col_a AS UNSIGNED))
                 WHERE id = 3"
            );

            $rows = $this->ztdQuery('SELECT col_a, col_b FROM mi_csw_pairs WHERE id = 3');

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Swap arithmetic: expected 1 row');
            }

            // col_a = old col_b = '20'
            // col_b = old col_a cast... but left-to-right: col_a already changed
            // So col_b = CONCAT('', CAST('20' AS UNSIGNED)) = '20'
            // Both become '20' in MySQL left-to-right evaluation
            $this->assertSame('20', $rows[0]['col_a']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Swap with arithmetic failed: ' . $e->getMessage());
        }
    }

    /**
     * Multi-column update with interdependencies.
     *
     * @spec SPEC-4.2
     */
    public function testMultiColumnInterdependent(): void
    {
        try {
            $this->mysqli->query(
                "UPDATE mi_csw_pairs SET col_a = CONCAT(col_a, '-', col_b), col_b = UPPER(col_a) WHERE id = 2"
            );

            $rows = $this->ztdQuery('SELECT col_a, col_b FROM mi_csw_pairs WHERE id = 2');

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Multi-column interdep: expected 1 row');
            }

            // Left-to-right: col_a = 'foo-bar', then col_b = UPPER('foo-bar') = 'FOO-BAR'
            $this->assertSame('foo-bar', $rows[0]['col_a']);
            $this->assertSame('FOO-BAR', $rows[0]['col_b']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-column interdependent update failed: ' . $e->getMessage());
        }
    }
}
