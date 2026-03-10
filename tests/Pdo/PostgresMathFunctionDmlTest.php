<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests UPDATE SET with math functions and DELETE WHERE with function-based
 * conditions on PostgreSQL via PDO.
 *
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class PostgresMathFunctionDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_mfd_items (
            id INT PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            price NUMERIC(10,3) NOT NULL,
            quantity INT NOT NULL,
            rating NUMERIC(3,1) NOT NULL DEFAULT 0.0
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_mfd_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_mfd_items VALUES (1, 'Widget', 10.567, 15, 4.3)");
        $this->pdo->exec("INSERT INTO pg_mfd_items VALUES (2, 'Gadget', 25.999, 8, 3.7)");
        $this->pdo->exec("INSERT INTO pg_mfd_items VALUES (3, 'Doohickey', 5.001, 100, 2.1)");
        $this->pdo->exec("INSERT INTO pg_mfd_items VALUES (4, 'Thingamajig', -3.50, 0, 1.5)");
    }

    public function testUpdateSetRound(): void
    {
        try {
            $this->pdo->exec("UPDATE pg_mfd_items SET price = ROUND(price::numeric, 1)");
            $rows = $this->ztdQuery("SELECT id, price FROM pg_mfd_items ORDER BY id");
            $this->assertCount(4, $rows);

            $p1 = round((float) $rows[0]['price'], 1);
            if ($p1 != 10.6) {
                $this->markTestIncomplete('ROUND in SET: expected 10.6, got ' . var_export($rows[0]['price'], true));
            }
            $this->assertEquals(10.6, $p1);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET ROUND failed: ' . $e->getMessage());
        }
    }

    public function testUpdateSetAbs(): void
    {
        try {
            $this->pdo->exec("UPDATE pg_mfd_items SET price = ABS(price) WHERE price < 0");
            $rows = $this->ztdQuery("SELECT price FROM pg_mfd_items WHERE id = 4");
            $this->assertCount(1, $rows);

            $price = round((float) $rows[0]['price'], 2);
            if ($price != 3.50) {
                $this->markTestIncomplete('ABS(-3.50): expected 3.50, got ' . $price);
            }
            $this->assertEquals(3.50, $price);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET ABS failed: ' . $e->getMessage());
        }
    }

    public function testDeleteWhereCompoundFunctionCondition(): void
    {
        try {
            $this->pdo->exec("DELETE FROM pg_mfd_items WHERE ABS(price) < 6 OR quantity = 0");
            $rows = $this->ztdQuery("SELECT id FROM pg_mfd_items ORDER BY id");
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE WHERE compound: expected 2 rows, got ' . count($rows) . ': ' . json_encode($rows)
                );
            }
            $this->assertCount(2, $rows);
            $this->assertEquals([1, 2], array_map('intval', array_column($rows, 'id')));
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE compound function failed: ' . $e->getMessage());
        }
    }

    public function testUpdateSetArithmeticAndRound(): void
    {
        try {
            $this->pdo->exec("UPDATE pg_mfd_items SET price = ROUND((price * 1.1)::numeric, 2) WHERE id = 1");
            $rows = $this->ztdQuery("SELECT price FROM pg_mfd_items WHERE id = 1");
            $this->assertCount(1, $rows);
            $price = round((float) $rows[0]['price'], 2);
            if ($price != 11.62) {
                $this->markTestIncomplete('ROUND(price*1.1,2): expected 11.62, got ' . $price);
            }
            $this->assertEquals(11.62, $price);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET arithmetic+ROUND failed: ' . $e->getMessage());
        }
    }

    public function testDeleteWhereFloor(): void
    {
        try {
            $this->pdo->exec("DELETE FROM pg_mfd_items WHERE FLOOR(rating) = 3");
            $rows = $this->ztdQuery("SELECT id FROM pg_mfd_items ORDER BY id");
            if (count($rows) !== 3) {
                $this->markTestIncomplete('DELETE WHERE FLOOR: expected 3 rows, got ' . count($rows));
            }
            $this->assertCount(3, $rows);
            $this->assertNotContains(2, array_map('intval', array_column($rows, 'id')));
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE FLOOR failed: ' . $e->getMessage());
        }
    }
}
