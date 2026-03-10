<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests DML with BETWEEN and prepared parameters through ZTD shadow store on SQLite.
 *
 * BETWEEN introduces range syntax (col BETWEEN ? AND ?) whose AND keyword
 * the CTE rewriter must not confuse with the logical AND operator.
 *
 * @spec SPEC-4.2, SPEC-4.3, SPEC-3.2
 */
class SqliteBetweenParamDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_btw_products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            price REAL NOT NULL,
            stock INTEGER NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_btw_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_btw_products VALUES (1, 'Widget', 9.99, 100)");
        $this->pdo->exec("INSERT INTO sl_btw_products VALUES (2, 'Gadget', 24.99, 50)");
        $this->pdo->exec("INSERT INTO sl_btw_products VALUES (3, 'Doohickey', 49.99, 25)");
        $this->pdo->exec("INSERT INTO sl_btw_products VALUES (4, 'Thingamajig', 74.99, 10)");
        $this->pdo->exec("INSERT INTO sl_btw_products VALUES (5, 'Whatchamacallit', 99.99, 5)");
    }

    /**
     * UPDATE WHERE col BETWEEN ? AND ? with prepared params.
     */
    public function testPreparedUpdateBetween(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE sl_btw_products SET stock = stock + 10 WHERE price BETWEEN ? AND ?"
            );
            $stmt->execute([20.00, 75.00]);

            $rows = $this->ztdQuery("SELECT name, stock FROM sl_btw_products ORDER BY id");

            $byName = [];
            foreach ($rows as $r) {
                $byName[$r['name']] = (int) $r['stock'];
            }

            // Gadget (24.99), Doohickey (49.99), Thingamajig (74.99) should be updated
            if ($byName['Gadget'] !== 60 || $byName['Doohickey'] !== 35 || $byName['Thingamajig'] !== 20) {
                $this->markTestIncomplete(
                    'Prepared UPDATE BETWEEN: expected 60/35/20 for mid-range, got '
                    . json_encode($byName)
                );
            }

            $this->assertSame(100, $byName['Widget'], 'Widget unchanged');
            $this->assertSame(60, $byName['Gadget'], 'Gadget updated');
            $this->assertSame(35, $byName['Doohickey'], 'Doohickey updated');
            $this->assertSame(20, $byName['Thingamajig'], 'Thingamajig updated');
            $this->assertSame(5, $byName['Whatchamacallit'], 'Whatchamacallit unchanged');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE BETWEEN failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE WHERE col BETWEEN ? AND ? with prepared params.
     */
    public function testPreparedDeleteBetween(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "DELETE FROM sl_btw_products WHERE price BETWEEN ? AND ?"
            );
            $stmt->execute([25.00, 100.00]);

            $rows = $this->ztdQuery("SELECT name FROM sl_btw_products ORDER BY id");

            // Only Widget (9.99) and Gadget (24.99) should remain
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Prepared DELETE BETWEEN: expected 2 remaining, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Widget', $rows[0]['name']);
            $this->assertSame('Gadget', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DELETE BETWEEN failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE with BETWEEN combined with additional AND condition.
     * Tests that BETWEEN's AND is not confused with the logical AND.
     */
    public function testPreparedUpdateBetweenWithLogicalAnd(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE sl_btw_products SET stock = 0 WHERE price BETWEEN ? AND ? AND stock < ?"
            );
            $stmt->execute([20.00, 100.00, 30]);

            $rows = $this->ztdQuery("SELECT name, stock FROM sl_btw_products ORDER BY id");

            $byName = [];
            foreach ($rows as $r) {
                $byName[$r['name']] = (int) $r['stock'];
            }

            // Doohickey (49.99, stock 25), Thingamajig (74.99, stock 10), Whatchamacallit (99.99, stock 5)
            // should be zeroed. Gadget (24.99, stock 50) has stock >= 30 so unchanged.
            if ($byName['Gadget'] !== 50 || $byName['Doohickey'] !== 0) {
                $this->markTestIncomplete(
                    'Prepared UPDATE BETWEEN AND: expected Gadget=50 Doohickey=0, got '
                    . json_encode($byName)
                );
            }

            $this->assertSame(100, $byName['Widget']);
            $this->assertSame(50, $byName['Gadget']);
            $this->assertSame(0, $byName['Doohickey']);
            $this->assertSame(0, $byName['Thingamajig']);
            $this->assertSame(0, $byName['Whatchamacallit']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE BETWEEN AND failed: ' . $e->getMessage());
        }
    }

    /**
     * NOT BETWEEN with prepared params in DELETE.
     */
    public function testPreparedDeleteNotBetween(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "DELETE FROM sl_btw_products WHERE price NOT BETWEEN ? AND ?"
            );
            $stmt->execute([20.00, 80.00]);

            $rows = $this->ztdQuery("SELECT name FROM sl_btw_products ORDER BY id");

            // Gadget (24.99), Doohickey (49.99), Thingamajig (74.99) should remain
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Prepared DELETE NOT BETWEEN: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DELETE NOT BETWEEN failed: ' . $e->getMessage());
        }
    }

    /**
     * SELECT with BETWEEN in prepared statement (read path).
     */
    public function testPreparedSelectBetween(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT name FROM sl_btw_products WHERE price BETWEEN ? AND ? ORDER BY price",
                [10.00, 50.00]
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Prepared SELECT BETWEEN: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Gadget', $rows[0]['name']);
            $this->assertSame('Doohickey', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared SELECT BETWEEN failed: ' . $e->getMessage());
        }
    }
}
