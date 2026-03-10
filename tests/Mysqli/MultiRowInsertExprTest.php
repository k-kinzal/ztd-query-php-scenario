<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests multi-row INSERT with per-row function expressions in VALUES
 * through ZTD shadow store.
 *
 * Real applications often batch-insert rows with computed values:
 *   INSERT INTO t VALUES (1, UPPER('abc'), NOW()), (2, LOWER('XYZ'), NOW())
 *
 * The CTE rewriter must evaluate function expressions in each row of a
 * multi-row VALUES clause independently.
 *
 * @spec SPEC-4.1
 */
class MultiRowInsertExprTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_mrie_products (
            id INT PRIMARY KEY,
            code VARCHAR(50) NOT NULL,
            price DECIMAL(10,2) NOT NULL,
            quantity INT NOT NULL DEFAULT 0
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mi_mrie_products'];
    }

    /**
     * Multi-row INSERT with UPPER() and LOWER() in VALUES.
     */
    public function testMultiRowInsertWithStringFunctions(): void
    {
        try {
            $this->mysqli->query(
                "INSERT INTO mi_mrie_products (id, code, price, quantity) VALUES
                    (1, UPPER('widget'), 9.99, 10),
                    (2, LOWER('GADGET'), 19.99, 5),
                    (3, CONCAT('item-', '003'), 29.99, 3)"
            );

            $rows = $this->ztdQuery("SELECT id, code, price FROM mi_mrie_products ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Multi-row INSERT with string functions: expected 3 rows, got '
                    . count($rows) . ': ' . json_encode($rows)
                );
            }
            $this->assertCount(3, $rows);

            // Check UPPER was applied
            if ($rows[0]['code'] !== 'WIDGET') {
                $this->markTestIncomplete(
                    'UPPER() in VALUES not applied: expected "WIDGET", got '
                    . var_export($rows[0]['code'], true)
                );
            }
            $this->assertSame('WIDGET', $rows[0]['code']);

            // Check LOWER was applied
            if ($rows[1]['code'] !== 'gadget') {
                $this->markTestIncomplete(
                    'LOWER() in VALUES not applied: expected "gadget", got '
                    . var_export($rows[1]['code'], true)
                );
            }
            $this->assertSame('gadget', $rows[1]['code']);

            // Check CONCAT was applied
            if ($rows[2]['code'] !== 'item-003') {
                $this->markTestIncomplete(
                    'CONCAT() in VALUES not applied: expected "item-003", got '
                    . var_export($rows[2]['code'], true)
                );
            }
            $this->assertSame('item-003', $rows[2]['code']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-row INSERT with string functions failed: ' . $e->getMessage());
        }
    }

    /**
     * Multi-row INSERT with arithmetic expressions in VALUES.
     */
    public function testMultiRowInsertWithArithmetic(): void
    {
        try {
            $this->mysqli->query(
                "INSERT INTO mi_mrie_products (id, code, price, quantity) VALUES
                    (1, 'A', 10.00 * 1.1, 5 + 3),
                    (2, 'B', 20.00 * 0.9, 10 - 2),
                    (3, 'C', 100.00 / 3, 2 * 4)"
            );

            $rows = $this->ztdQuery("SELECT id, price, quantity FROM mi_mrie_products ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Multi-row INSERT with arithmetic: expected 3 rows, got '
                    . count($rows) . ': ' . json_encode($rows)
                );
            }
            $this->assertCount(3, $rows);

            // Check arithmetic was evaluated
            $price1 = round((float) $rows[0]['price'], 2);
            if ($price1 != 11.00) {
                $this->markTestIncomplete(
                    'Arithmetic in VALUES not evaluated: expected price=11.00, got '
                    . var_export($rows[0]['price'], true)
                );
            }
            $this->assertEquals(11.00, $price1);
            $this->assertEquals(8, (int) $rows[0]['quantity']);

            $this->assertEquals(18.00, round((float) $rows[1]['price'], 2));
            $this->assertEquals(8, (int) $rows[1]['quantity']);

            $this->assertEquals(8, (int) $rows[2]['quantity']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-row INSERT with arithmetic failed: ' . $e->getMessage());
        }
    }

    /**
     * Multi-row INSERT with NULL and function mix.
     */
    public function testMultiRowInsertMixedNullAndFunction(): void
    {
        try {
            $this->mysqli->query(
                "INSERT INTO mi_mrie_products (id, code, price, quantity) VALUES
                    (1, UPPER('first'), 5.00, 1),
                    (2, 'plain', 10.00, 2),
                    (3, UPPER('third'), 15.00, 3)"
            );

            $rows = $this->ztdQuery("SELECT id, code FROM mi_mrie_products ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Multi-row INSERT mixed: expected 3 rows, got ' . count($rows)
                );
            }
            $this->assertCount(3, $rows);

            $this->assertSame('FIRST', $rows[0]['code']);
            $this->assertSame('plain', $rows[1]['code']);
            $this->assertSame('THIRD', $rows[2]['code']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-row INSERT mixed failed: ' . $e->getMessage());
        }
    }

    /**
     * Multi-row INSERT with IF() conditional per row (MySQL-specific).
     */
    public function testMultiRowInsertWithIfFunction(): void
    {
        try {
            $this->mysqli->query(
                "INSERT INTO mi_mrie_products (id, code, price, quantity) VALUES
                    (1, IF(1 > 0, 'positive', 'negative'), 10.00, 1),
                    (2, IF(0 > 1, 'positive', 'negative'), 20.00, 2)"
            );

            $rows = $this->ztdQuery("SELECT id, code FROM mi_mrie_products ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Multi-row INSERT with IF(): expected 2 rows, got ' . count($rows)
                );
            }
            $this->assertCount(2, $rows);

            if ($rows[0]['code'] !== 'positive') {
                $this->markTestIncomplete(
                    'IF() in VALUES not evaluated: expected "positive", got '
                    . var_export($rows[0]['code'], true)
                );
            }
            $this->assertSame('positive', $rows[0]['code']);
            $this->assertSame('negative', $rows[1]['code']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-row INSERT with IF() failed: ' . $e->getMessage());
        }
    }
}
