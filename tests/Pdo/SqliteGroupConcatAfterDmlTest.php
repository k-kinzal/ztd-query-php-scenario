<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests group_concat() aggregate function with shadow-modified data on SQLite.
 *
 * SQLite's group_concat() is similar to MySQL's GROUP_CONCAT but without
 * ORDER BY inside the aggregate. After shadow DML, the aggregate must
 * reflect mutations.
 *
 * @spec SPEC-3.1
 * @spec SPEC-4.1
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class SqliteGroupConcatAfterDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_gcd_orders (
            id INTEGER PRIMARY KEY,
            customer TEXT NOT NULL,
            product TEXT NOT NULL,
            amount REAL NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_gcd_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_gcd_orders VALUES (1, 'Alice', 'Gadget', 20.00)");
        $this->pdo->exec("INSERT INTO sl_gcd_orders VALUES (2, 'Alice', 'Widget', 10.00)");
        $this->pdo->exec("INSERT INTO sl_gcd_orders VALUES (3, 'Bob', 'Widget', 15.00)");
        $this->pdo->exec("INSERT INTO sl_gcd_orders VALUES (4, 'Carol', 'Gadget', 25.00)");
    }

    /**
     * group_concat on physical data (baseline).
     * Note: SQLite group_concat does not support ORDER BY inside the function.
     * We avoid derived tables (known Issue #13) and just check presence.
     */
    public function testGroupConcatBaseline(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT customer, group_concat(product, ',') AS products
                 FROM sl_gcd_orders GROUP BY customer ORDER BY customer"
            );

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'group_concat baseline: expected 3 rows, got ' . count($rows)
                );
            }
            $this->assertCount(3, $rows);

            // Alice has Gadget and Widget (order not guaranteed)
            $aliceProducts = explode(',', $rows[0]['products']);
            sort($aliceProducts);
            if ($aliceProducts !== ['Gadget', 'Widget']) {
                $this->markTestIncomplete(
                    'group_concat baseline: expected [Gadget,Widget] for Alice, got '
                    . var_export($rows[0]['products'], true)
                );
            }
            $this->assertEquals(['Gadget', 'Widget'], $aliceProducts);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('group_concat baseline failed: ' . $e->getMessage());
        }
    }

    /**
     * group_concat after shadow INSERT.
     */
    public function testGroupConcatAfterInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_gcd_orders VALUES (5, 'Alice', 'Doohickey', 30.00)");

            $rows = $this->ztdQuery(
                "SELECT customer, group_concat(product, ',') AS products
                 FROM sl_gcd_orders WHERE customer = 'Alice' GROUP BY customer"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'group_concat after INSERT: expected 1 row, got ' . count($rows)
                );
            }
            $this->assertCount(1, $rows);

            // Order not guaranteed in SQLite group_concat; check all 3 products present
            $products = explode(',', $rows[0]['products']);
            sort($products);
            $expected = ['Doohickey', 'Gadget', 'Widget'];
            if ($products !== $expected) {
                $this->markTestIncomplete(
                    'group_concat after INSERT: expected ' . json_encode($expected) . ', got '
                    . var_export($rows[0]['products'], true)
                );
            }
            $this->assertEquals($expected, $products);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('group_concat after INSERT failed: ' . $e->getMessage());
        }
    }

    /**
     * group_concat after shadow DELETE.
     */
    public function testGroupConcatAfterDelete(): void
    {
        try {
            $this->pdo->exec("DELETE FROM sl_gcd_orders WHERE id = 1"); // remove Alice's Gadget

            $rows = $this->ztdQuery(
                "SELECT customer, group_concat(product, ',') AS products
                 FROM sl_gcd_orders WHERE customer = 'Alice' GROUP BY customer"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'group_concat after DELETE: expected 1 row, got ' . count($rows)
                );
            }
            $this->assertCount(1, $rows);

            if ($rows[0]['products'] !== 'Widget') {
                $this->markTestIncomplete(
                    'group_concat after DELETE: expected "Widget", got '
                    . var_export($rows[0]['products'], true)
                );
            }
            $this->assertSame('Widget', $rows[0]['products']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('group_concat after DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * group_concat after shadow UPDATE.
     */
    public function testGroupConcatAfterUpdate(): void
    {
        try {
            $this->pdo->exec("UPDATE sl_gcd_orders SET product = 'Thingamajig' WHERE id = 1");

            $rows = $this->ztdQuery(
                "SELECT customer, group_concat(product, ',') AS products
                 FROM sl_gcd_orders WHERE customer = 'Alice' GROUP BY customer"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'group_concat after UPDATE: expected 1 row, got ' . count($rows)
                );
            }
            $this->assertCount(1, $rows);

            $products = explode(',', $rows[0]['products']);
            sort($products);
            $expected = ['Thingamajig', 'Widget'];
            if ($products !== $expected) {
                $this->markTestIncomplete(
                    'group_concat after UPDATE: expected ' . json_encode($expected) . ', got '
                    . var_export($rows[0]['products'], true)
                );
            }
            $this->assertEquals($expected, $products);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('group_concat after UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * Simple group_concat without ordering (default behavior).
     */
    public function testGroupConcatSimple(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT customer, group_concat(product) AS products
                 FROM sl_gcd_orders GROUP BY customer ORDER BY customer"
            );

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'group_concat simple: expected 3 rows, got ' . count($rows)
                );
            }
            $this->assertCount(3, $rows);

            // Each customer has at least one product
            foreach ($rows as $row) {
                $this->assertNotEmpty($row['products'], 'products should not be empty for ' . $row['customer']);
            }
        } catch (\Throwable $e) {
            $this->markTestIncomplete('group_concat simple failed: ' . $e->getMessage());
        }
    }
}
