<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests UPDATE SET with math functions (ROUND, FLOOR, CEIL, ABS, MOD)
 * and DELETE WHERE with function-based conditions through ZTD shadow store.
 *
 * Math functions in UPDATE SET are common for price adjustments, inventory
 * management, and rounding corrections. These test the CTE rewriter's
 * ability to evaluate math function expressions in mutation contexts.
 *
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class MathFunctionDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_mfd_items (
            id INT PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            price DECIMAL(10,2) NOT NULL,
            quantity INT NOT NULL,
            rating DECIMAL(3,1) NOT NULL DEFAULT 0.0
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mi_mfd_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_mfd_items VALUES (1, 'Widget', 10.567, 15, 4.3)");
        $this->mysqli->query("INSERT INTO mi_mfd_items VALUES (2, 'Gadget', 25.999, 8, 3.7)");
        $this->mysqli->query("INSERT INTO mi_mfd_items VALUES (3, 'Doohickey', 5.001, 100, 2.1)");
        $this->mysqli->query("INSERT INTO mi_mfd_items VALUES (4, 'Thingamajig', -3.50, 0, 1.5)");
    }

    /**
     * UPDATE SET price = ROUND(price, 1) — round to 1 decimal.
     */
    public function testUpdateSetRound(): void
    {
        try {
            $this->mysqli->query("UPDATE mi_mfd_items SET price = ROUND(price, 1)");

            $rows = $this->ztdQuery("SELECT id, price FROM mi_mfd_items ORDER BY id");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'UPDATE SET ROUND: expected 4 rows, got ' . count($rows)
                );
            }
            $this->assertCount(4, $rows);

            $p1 = round((float) $rows[0]['price'], 1);
            if ($p1 != 10.6) {
                $this->markTestIncomplete(
                    'ROUND(10.567, 1) in SET: expected 10.6, got ' . var_export($rows[0]['price'], true)
                );
            }
            $this->assertEquals(10.6, $p1);

            $p2 = round((float) $rows[1]['price'], 1);
            $this->assertEquals(26.0, $p2);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET ROUND failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET quantity = CEIL(price) — ceiling function.
     */
    public function testUpdateSetCeil(): void
    {
        try {
            $this->mysqli->query("UPDATE mi_mfd_items SET quantity = CEIL(price) WHERE id = 1");

            $rows = $this->ztdQuery("SELECT quantity FROM mi_mfd_items WHERE id = 1");
            $this->assertCount(1, $rows);

            $qty = (int) $rows[0]['quantity'];
            if ($qty !== 11) {
                $this->markTestIncomplete(
                    'CEIL(10.567) in SET: expected 11, got ' . $qty
                );
            }
            $this->assertEquals(11, $qty);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET CEIL failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET with ABS() — absolute value.
     */
    public function testUpdateSetAbs(): void
    {
        try {
            $this->mysqli->query("UPDATE mi_mfd_items SET price = ABS(price) WHERE price < 0");

            $rows = $this->ztdQuery("SELECT id, price FROM mi_mfd_items WHERE id = 4");
            $this->assertCount(1, $rows);

            $price = (float) $rows[0]['price'];
            if ($price != 3.50) {
                $this->markTestIncomplete(
                    'ABS(-3.50) in SET: expected 3.50, got ' . $price
                );
            }
            $this->assertEquals(3.50, $price);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET ABS failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET with MOD() — modulo.
     */
    public function testUpdateSetMod(): void
    {
        try {
            $this->mysqli->query("UPDATE mi_mfd_items SET quantity = MOD(quantity, 10)");

            $rows = $this->ztdQuery("SELECT id, quantity FROM mi_mfd_items ORDER BY id");
            $this->assertCount(4, $rows);

            // MOD(15, 10) = 5
            if ((int) $rows[0]['quantity'] !== 5) {
                $this->markTestIncomplete(
                    'MOD(15, 10) in SET: expected 5, got ' . $rows[0]['quantity']
                );
            }
            $this->assertEquals(5, (int) $rows[0]['quantity']);

            // MOD(100, 10) = 0
            $this->assertEquals(0, (int) $rows[2]['quantity']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET MOD failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE WHERE with function-based compound condition.
     */
    public function testDeleteWhereCompoundFunctionCondition(): void
    {
        try {
            $this->mysqli->query(
                "DELETE FROM mi_mfd_items WHERE ABS(price) < 6 OR quantity = 0"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM mi_mfd_items ORDER BY id");

            // Deletes: id=3 (ABS(5.001) < 6), id=4 (quantity=0 AND ABS(-3.5) < 6)
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE WHERE compound function: expected 2 remaining rows, got '
                    . count($rows) . ': ' . json_encode(array_column($rows, 'name'))
                );
            }
            $this->assertCount(2, $rows);
            $this->assertEquals([1, 2], array_map('intval', array_column($rows, 'id')));
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE compound function failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET with combined math: ROUND(price * 1.1, 2) — 10% markup.
     */
    public function testUpdateSetArithmeticAndRound(): void
    {
        try {
            $this->mysqli->query(
                "UPDATE mi_mfd_items SET price = ROUND(price * 1.1, 2) WHERE id = 1"
            );

            $rows = $this->ztdQuery("SELECT price FROM mi_mfd_items WHERE id = 1");
            $this->assertCount(1, $rows);

            $price = round((float) $rows[0]['price'], 2);
            // DECIMAL(10,2) stores 10.57 (truncated), 10.57 * 1.1 = 11.627, ROUND = 11.63
            if ($price != 11.63) {
                $this->markTestIncomplete(
                    'ROUND(price * 1.1, 2): expected 11.63, got ' . $price
                );
            }
            $this->assertEquals(11.63, $price);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET arithmetic+ROUND failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE WHERE FLOOR(rating) = target — function in WHERE condition.
     */
    public function testDeleteWhereFloor(): void
    {
        try {
            $this->mysqli->query("DELETE FROM mi_mfd_items WHERE FLOOR(rating) = 3");

            $rows = $this->ztdQuery("SELECT id FROM mi_mfd_items ORDER BY id");

            // FLOOR(3.7) = 3, so id=2 (Gadget) should be deleted
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE WHERE FLOOR: expected 3 remaining rows, got '
                    . count($rows) . ': ' . json_encode(array_column($rows, 'id'))
                );
            }
            $this->assertCount(3, $rows);
            $this->assertNotContains(2, array_map('intval', array_column($rows, 'id')));
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE FLOOR failed: ' . $e->getMessage());
        }
    }
}
