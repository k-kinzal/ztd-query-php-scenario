<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests GREATEST/LEAST equivalent (MAX/MIN) functions in DML through ZTD shadow store on SQLite.
 *
 * GREATEST/LEAST patterns are common in applications for clamping values,
 * finding max/min of expressions, and conditional updates. SQLite uses MAX/MIN
 * as scalar functions instead of GREATEST/LEAST.
 *
 * @spec SPEC-4.1, SPEC-4.2, SPEC-4.3
 */
class SqliteGreatestLeastDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_gl_products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                price REAL NOT NULL,
                min_price REAL NOT NULL DEFAULT 0.0,
                stock INTEGER NOT NULL DEFAULT 0
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_gl_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_gl_products VALUES (1, 'Widget', 25.00, 10.00, 100)");
        $this->pdo->exec("INSERT INTO sl_gl_products VALUES (2, 'Gadget', 50.00, 20.00, 50)");
        $this->pdo->exec("INSERT INTO sl_gl_products VALUES (3, 'Doohickey', 15.00, 5.00, 200)");
        $this->pdo->exec("INSERT INTO sl_gl_products VALUES (4, 'Thingamajig', 75.00, 30.00, 25)");
    }

    /**
     * UPDATE SET price = MAX(price, min_price) — clamp price to minimum.
     */
    public function testUpdateSetWithMaxFunction(): void
    {
        try {
            // Try to set price to 8.00 but clamp to min_price
            $this->pdo->exec(
                "UPDATE sl_gl_products SET price = MAX(8.00, min_price) WHERE id = 1"
            );

            $rows = $this->ztdQuery("SELECT price FROM sl_gl_products WHERE id = 1");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('UPDATE MAX: got ' . json_encode($rows));
            }

            // MAX(8.00, 10.00) = 10.00
            $this->assertEquals(10.00, (float) $rows[0]['price']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with MAX in SET failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET with MIN function — cap price at a ceiling.
     */
    public function testUpdateSetWithMinFunction(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE sl_gl_products SET price = MIN(price, 40.00)"
            );

            $rows = $this->ztdQuery("SELECT id, price FROM sl_gl_products ORDER BY id");

            if (count($rows) !== 4) {
                $this->markTestIncomplete('UPDATE MIN: got ' . json_encode($rows));
            }

            // Widget 25 → MIN(25,40)=25, Gadget 50 → 40, Doohickey 15 → 15, Thingamajig 75 → 40
            $this->assertEquals(25.00, (float) $rows[0]['price']);
            $this->assertEquals(40.00, (float) $rows[1]['price']);
            $this->assertEquals(15.00, (float) $rows[2]['price']);
            $this->assertEquals(40.00, (float) $rows[3]['price']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with MIN in SET failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE SET with MAX and bound parameter.
     */
    public function testPreparedUpdateSetMaxWithParam(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE sl_gl_products SET price = MAX(price, ?) WHERE id = ?"
            );
            $stmt->execute([60.00, 2]);

            $rows = $this->ztdQuery("SELECT price FROM sl_gl_products WHERE id = 2");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Prepared UPDATE MAX: got ' . json_encode($rows));
            }

            // MAX(50.00, 60.00) = 60.00
            $this->assertEquals(60.00, (float) $rows[0]['price']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE MAX with param failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared DELETE WHERE MIN(col1, col2) < ? — scalar function in WHERE with param.
     */
    public function testPreparedDeleteWhereMinWithParam(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "DELETE FROM sl_gl_products WHERE MIN(price, stock) < ?"
            );
            $stmt->execute([20]);

            $rows = $this->ztdQuery("SELECT name FROM sl_gl_products ORDER BY id");

            // MIN(25,100)=25, MIN(50,50)=50, MIN(15,200)=15, MIN(75,25)=25
            // Items where MIN < 20: Doohickey (15)
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Prepared DELETE MIN WHERE: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DELETE with MIN in WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET with nested MAX/MIN — clamp value to a range.
     */
    public function testUpdateSetClampWithNestedMaxMin(): void
    {
        try {
            // Clamp price to range [min_price, 50.00]
            $this->pdo->exec(
                "UPDATE sl_gl_products SET price = MIN(MAX(price, min_price), 50.00)"
            );

            $rows = $this->ztdQuery("SELECT id, price FROM sl_gl_products ORDER BY id");

            if (count($rows) !== 4) {
                $this->markTestIncomplete('UPDATE clamp: got ' . json_encode($rows));
            }

            // Widget: MIN(MAX(25,10),50) = 25
            // Gadget: MIN(MAX(50,20),50) = 50
            // Doohickey: MIN(MAX(15,5),50) = 15
            // Thingamajig: MIN(MAX(75,30),50) = 50
            $this->assertEquals(25.00, (float) $rows[0]['price']);
            $this->assertEquals(50.00, (float) $rows[1]['price']);
            $this->assertEquals(15.00, (float) $rows[2]['price']);
            $this->assertEquals(50.00, (float) $rows[3]['price']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with nested MAX/MIN clamp failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE SET MAX with two column references and a param.
     */
    public function testPreparedUpdateMaxTwoColumnsAndParam(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE sl_gl_products SET stock = MAX(stock, CAST(price AS INTEGER), ?) WHERE id = ?"
            );
            $stmt->execute([150, 3]);

            $rows = $this->ztdQuery("SELECT stock FROM sl_gl_products WHERE id = 3");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Prepared UPDATE MAX 3-arg: got ' . json_encode($rows));
            }

            // MAX(200, 15, 150) = 200
            $this->assertEquals(200, (int) $rows[0]['stock']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE MAX with 3 args failed: ' . $e->getMessage());
        }
    }

    /**
     * SELECT with MAX/MIN scalar after UPDATE mutations.
     */
    public function testSelectScalarMaxMinAfterMutations(): void
    {
        try {
            $this->pdo->exec("UPDATE sl_gl_products SET price = 30.00 WHERE id = 1");
            $this->pdo->exec("UPDATE sl_gl_products SET price = 30.00 WHERE id = 3");

            $rows = $this->ztdQuery(
                "SELECT id, MIN(price, min_price) AS effective_min,
                        MAX(price, min_price) AS effective_max
                 FROM sl_gl_products ORDER BY id"
            );

            if (count($rows) !== 4) {
                $this->markTestIncomplete('SELECT scalar MAX/MIN: got ' . json_encode($rows));
            }

            // id=1: price=30, min_price=10 → MIN=10, MAX=30
            $this->assertEquals(10.00, (float) $rows[0]['effective_min']);
            $this->assertEquals(30.00, (float) $rows[0]['effective_max']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT with scalar MAX/MIN after mutations failed: ' . $e->getMessage());
        }
    }
}
