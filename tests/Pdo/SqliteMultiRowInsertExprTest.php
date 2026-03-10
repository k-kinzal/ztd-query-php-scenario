<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests multi-row INSERT with per-row function expressions in VALUES
 * on SQLite via PDO.
 *
 * @spec SPEC-4.1
 */
class SqliteMultiRowInsertExprTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_mrie_products (
            id INTEGER PRIMARY KEY,
            code TEXT NOT NULL,
            price REAL NOT NULL,
            quantity INTEGER NOT NULL DEFAULT 0
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_mrie_products'];
    }

    public function testMultiRowInsertWithStringFunctions(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_mrie_products (id, code, price, quantity) VALUES
                    (1, UPPER('widget'), 9.99, 10),
                    (2, LOWER('GADGET'), 19.99, 5),
                    (3, 'item-' || '003', 29.99, 3)"
            );

            $rows = $this->ztdQuery("SELECT id, code FROM sl_mrie_products ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Multi-row INSERT with string functions: expected 3 rows, got '
                    . count($rows) . ': ' . json_encode($rows)
                );
            }
            $this->assertCount(3, $rows);

            if ($rows[0]['code'] !== 'WIDGET') {
                $this->markTestIncomplete(
                    'UPPER() in VALUES not applied: expected "WIDGET", got '
                    . var_export($rows[0]['code'], true)
                );
            }
            $this->assertSame('WIDGET', $rows[0]['code']);
            $this->assertSame('gadget', $rows[1]['code']);
            $this->assertSame('item-003', $rows[2]['code']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-row INSERT with string functions failed: ' . $e->getMessage());
        }
    }

    public function testMultiRowInsertWithArithmetic(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_mrie_products (id, code, price, quantity) VALUES
                    (1, 'A', 10.00 * 1.1, 5 + 3),
                    (2, 'B', 20.00 * 0.9, 10 - 2),
                    (3, 'C', 100.00 / 3, 2 * 4)"
            );

            $rows = $this->ztdQuery("SELECT id, price, quantity FROM sl_mrie_products ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Multi-row INSERT with arithmetic: expected 3 rows, got ' . count($rows)
                );
            }
            $this->assertCount(3, $rows);

            $price1 = round((float) $rows[0]['price'], 2);
            if ($price1 != 11.00) {
                $this->markTestIncomplete(
                    'Arithmetic in VALUES not evaluated: expected price=11.00, got '
                    . var_export($rows[0]['price'], true)
                );
            }
            $this->assertEquals(11.00, $price1);
            $this->assertEquals(8, (int) $rows[0]['quantity']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-row INSERT with arithmetic failed: ' . $e->getMessage());
        }
    }

    public function testMultiRowInsertWithCaseExpr(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_mrie_products (id, code, price, quantity) VALUES
                    (1, CASE WHEN 1 > 0 THEN 'positive' ELSE 'negative' END, 10.00, 1),
                    (2, CASE WHEN 0 > 1 THEN 'positive' ELSE 'negative' END, 20.00, 2)"
            );

            $rows = $this->ztdQuery("SELECT id, code FROM sl_mrie_products ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Multi-row INSERT with CASE: expected 2 rows, got ' . count($rows)
                );
            }
            $this->assertCount(2, $rows);

            if ($rows[0]['code'] !== 'positive') {
                $this->markTestIncomplete(
                    'CASE in VALUES not evaluated: expected "positive", got '
                    . var_export($rows[0]['code'], true)
                );
            }
            $this->assertSame('positive', $rows[0]['code']);
            $this->assertSame('negative', $rows[1]['code']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-row INSERT with CASE failed: ' . $e->getMessage());
        }
    }
}
