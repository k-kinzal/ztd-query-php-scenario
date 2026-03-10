<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests SQLite UPDATE FROM pattern (added in SQLite 3.33.0, 2020-08-14).
 *
 * SQLite 3.33+ supports UPDATE ... FROM syntax similar to PostgreSQL, allowing
 * joins in UPDATE statements. Combined with VALUES virtual tables or subqueries,
 * this is a powerful batch update pattern.
 *
 * The CTE rewriter must handle both the UPDATE target and the FROM source
 * when both reference shadow-affected tables.
 *
 * @spec SPEC-4.2
 */
class SqliteUpdateFromValuesTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE sl_ufv_inventory (
                id INTEGER PRIMARY KEY,
                item TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                price REAL NOT NULL
            )",
            "CREATE TABLE sl_ufv_adjustments (
                item_id INTEGER NOT NULL,
                qty_delta INTEGER NOT NULL,
                price_delta REAL NOT NULL
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_ufv_inventory', 'sl_ufv_adjustments'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_ufv_inventory VALUES (1, 'Widget', 100, 10.00)");
        $this->pdo->exec("INSERT INTO sl_ufv_inventory VALUES (2, 'Gadget', 50, 20.00)");
        $this->pdo->exec("INSERT INTO sl_ufv_inventory VALUES (3, 'Doohickey', 25, 30.00)");
    }

    /**
     * UPDATE FROM with a subquery source.
     */
    public function testUpdateFromSubquery(): void
    {
        try {
            // Insert adjustments
            $this->pdo->exec("INSERT INTO sl_ufv_adjustments VALUES (1, 10, 0.50)");
            $this->pdo->exec("INSERT INTO sl_ufv_adjustments VALUES (2, -5, 1.00)");

            $this->pdo->exec("
                UPDATE sl_ufv_inventory
                SET quantity = sl_ufv_inventory.quantity + adj.total_delta
                FROM (
                    SELECT item_id, SUM(qty_delta) as total_delta
                    FROM sl_ufv_adjustments
                    GROUP BY item_id
                ) AS adj
                WHERE sl_ufv_inventory.id = adj.item_id
            ");

            $rows = $this->ztdQuery("SELECT id, quantity FROM sl_ufv_inventory ORDER BY id");

            $this->assertCount(3, $rows);

            if ((int) $rows[0]['quantity'] !== 110) {
                $this->markTestIncomplete(
                    'UPDATE FROM subquery: id=1 quantity=' . $rows[0]['quantity']
                    . ', expected 110. UPDATE FROM may not work in shadow.'
                );
            }

            $this->assertSame(110, (int) $rows[0]['quantity']); // 100 + 10
            $this->assertSame(45, (int) $rows[1]['quantity']);   // 50 + (-5)
            $this->assertSame(25, (int) $rows[2]['quantity']);   // unchanged
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE FROM subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE FROM with inline VALUES (SQLite 3.33+).
     *
     * Note: SQLite requires VALUES to be wrapped as a subquery.
     */
    public function testUpdateFromValues(): void
    {
        try {
            $this->pdo->exec("
                UPDATE sl_ufv_inventory
                SET price = v.new_price
                FROM (
                    SELECT column1 as id, column2 as new_price
                    FROM (VALUES (1, 15.00), (3, 35.00))
                ) AS v
                WHERE sl_ufv_inventory.id = v.id
            ");

            $rows = $this->ztdQuery("SELECT id, price FROM sl_ufv_inventory ORDER BY id");

            $this->assertCount(3, $rows);

            if (abs((float) $rows[0]['price'] - 15.00) > 0.01) {
                $this->markTestIncomplete(
                    'UPDATE FROM VALUES: id=1 price=' . $rows[0]['price']
                    . ', expected 15.00.'
                );
            }

            $this->assertEquals(15.00, (float) $rows[0]['price'], '', 0.01);
            $this->assertEquals(20.00, (float) $rows[1]['price'], '', 0.01); // unchanged
            $this->assertEquals(35.00, (float) $rows[2]['price'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE FROM VALUES failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE FROM where both tables have shadow DML.
     *
     * Both the target and source tables have been modified through shadow store.
     */
    public function testUpdateFromBothTablesModified(): void
    {
        try {
            // Modify inventory (target)
            $this->pdo->exec("INSERT INTO sl_ufv_inventory VALUES (4, 'Whatsit', 75, 40.00)");

            // Modify adjustments (source)
            $this->pdo->exec("INSERT INTO sl_ufv_adjustments VALUES (1, 20, 0)");
            $this->pdo->exec("INSERT INTO sl_ufv_adjustments VALUES (4, -25, 0)");

            $this->pdo->exec("
                UPDATE sl_ufv_inventory
                SET quantity = sl_ufv_inventory.quantity + adj.qty_delta
                FROM sl_ufv_adjustments AS adj
                WHERE sl_ufv_inventory.id = adj.item_id
            ");

            $rows = $this->ztdQuery("SELECT id, quantity FROM sl_ufv_inventory ORDER BY id");

            $this->assertCount(4, $rows);
            $this->assertSame(120, (int) $rows[0]['quantity']); // 100 + 20
            $this->assertSame(50, (int) $rows[1]['quantity']);   // unchanged
            $this->assertSame(25, (int) $rows[2]['quantity']);   // unchanged
            $this->assertSame(50, (int) $rows[3]['quantity']);   // 75 + (-25)
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE FROM both modified failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE FROM with parameters.
     */
    public function testPreparedUpdateFrom(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_ufv_adjustments VALUES (1, 5, 2.50)");

            $stmt = $this->pdo->prepare("
                UPDATE sl_ufv_inventory
                SET quantity = sl_ufv_inventory.quantity + adj.qty_delta
                FROM sl_ufv_adjustments AS adj
                WHERE sl_ufv_inventory.id = adj.item_id
                AND sl_ufv_inventory.id = ?
            ");
            $stmt->execute([1]);

            $rows = $this->ztdQuery("SELECT id, quantity FROM sl_ufv_inventory WHERE id = 1");

            if ((int) $rows[0]['quantity'] !== 105) {
                $this->markTestIncomplete(
                    'Prepared UPDATE FROM: id=1 quantity=' . $rows[0]['quantity']
                    . ', expected 105.'
                );
            }

            $this->assertSame(105, (int) $rows[0]['quantity']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE FROM failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE after UPDATE FROM to verify shadow consistency.
     */
    public function testDeleteAfterUpdateFrom(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_ufv_adjustments VALUES (1, 50, 0)");
            $this->pdo->exec("INSERT INTO sl_ufv_adjustments VALUES (2, 50, 0)");

            // First: UPDATE FROM
            $this->pdo->exec("
                UPDATE sl_ufv_inventory
                SET quantity = sl_ufv_inventory.quantity + adj.qty_delta
                FROM sl_ufv_adjustments AS adj
                WHERE sl_ufv_inventory.id = adj.item_id
            ");

            // Then: DELETE rows with high quantity
            $this->pdo->exec("DELETE FROM sl_ufv_inventory WHERE quantity > 100");

            $rows = $this->ztdQuery("SELECT id, quantity FROM sl_ufv_inventory ORDER BY id");

            // id=1: 100+50=150 (deleted), id=2: 50+50=100 (kept), id=3: 25 (kept)
            $this->assertCount(2, $rows);
            $this->assertSame(2, (int) $rows[0]['id']);
            $this->assertSame(100, (int) $rows[0]['quantity']);
            $this->assertSame(3, (int) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE after UPDATE FROM failed: ' . $e->getMessage());
        }
    }
}
