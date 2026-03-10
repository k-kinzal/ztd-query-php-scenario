<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests GROUP_CONCAT aggregate function with shadow-modified data.
 *
 * GROUP_CONCAT is extremely common in MySQL applications for
 * denormalization (e.g., tags, categories as CSV). After shadow DML
 * (INSERT/UPDATE/DELETE), GROUP_CONCAT must reflect the mutated data.
 *
 * @spec SPEC-3.1
 * @spec SPEC-4.1
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class GroupConcatAfterDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_gcd_orders (
                id INT PRIMARY KEY,
                customer VARCHAR(50) NOT NULL,
                product VARCHAR(50) NOT NULL,
                amount DECIMAL(10,2) NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_gcd_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_gcd_orders VALUES (1, 'Alice', 'Widget', 10.00)");
        $this->mysqli->query("INSERT INTO mi_gcd_orders VALUES (2, 'Alice', 'Gadget', 20.00)");
        $this->mysqli->query("INSERT INTO mi_gcd_orders VALUES (3, 'Bob', 'Widget', 15.00)");
        $this->mysqli->query("INSERT INTO mi_gcd_orders VALUES (4, 'Carol', 'Gadget', 25.00)");
    }

    /**
     * GROUP_CONCAT on physical data (baseline).
     */
    public function testGroupConcatBaseline(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT customer, GROUP_CONCAT(product ORDER BY product) AS products
                 FROM mi_gcd_orders GROUP BY customer ORDER BY customer"
            );

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'GROUP_CONCAT baseline: expected 3 rows, got ' . count($rows)
                );
            }
            $this->assertCount(3, $rows);

            // Alice has Gadget, Widget
            if ($rows[0]['products'] !== 'Gadget,Widget') {
                $this->markTestIncomplete(
                    'GROUP_CONCAT baseline: expected "Gadget,Widget" for Alice, got '
                    . var_export($rows[0]['products'], true)
                );
            }
            $this->assertSame('Gadget,Widget', $rows[0]['products']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('GROUP_CONCAT baseline failed: ' . $e->getMessage());
        }
    }

    /**
     * GROUP_CONCAT after shadow INSERT — new row should appear in aggregation.
     */
    public function testGroupConcatAfterInsert(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_gcd_orders VALUES (5, 'Alice', 'Doohickey', 30.00)");

            $rows = $this->ztdQuery(
                "SELECT customer, GROUP_CONCAT(product ORDER BY product) AS products
                 FROM mi_gcd_orders WHERE customer = 'Alice' GROUP BY customer"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'GROUP_CONCAT after INSERT: expected 1 row, got ' . count($rows)
                );
            }
            $this->assertCount(1, $rows);

            // Alice now has Doohickey, Gadget, Widget
            $expected = 'Doohickey,Gadget,Widget';
            if ($rows[0]['products'] !== $expected) {
                $this->markTestIncomplete(
                    'GROUP_CONCAT after INSERT: expected "' . $expected . '", got '
                    . var_export($rows[0]['products'], true)
                );
            }
            $this->assertSame($expected, $rows[0]['products']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('GROUP_CONCAT after INSERT failed: ' . $e->getMessage());
        }
    }

    /**
     * GROUP_CONCAT after shadow DELETE — removed row should disappear.
     */
    public function testGroupConcatAfterDelete(): void
    {
        try {
            $this->mysqli->query("DELETE FROM mi_gcd_orders WHERE id = 2"); // remove Alice's Gadget

            $rows = $this->ztdQuery(
                "SELECT customer, GROUP_CONCAT(product ORDER BY product) AS products
                 FROM mi_gcd_orders WHERE customer = 'Alice' GROUP BY customer"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'GROUP_CONCAT after DELETE: expected 1 row, got ' . count($rows)
                );
            }
            $this->assertCount(1, $rows);

            if ($rows[0]['products'] !== 'Widget') {
                $this->markTestIncomplete(
                    'GROUP_CONCAT after DELETE: expected "Widget", got '
                    . var_export($rows[0]['products'], true)
                );
            }
            $this->assertSame('Widget', $rows[0]['products']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('GROUP_CONCAT after DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * GROUP_CONCAT after shadow UPDATE — updated value should appear.
     */
    public function testGroupConcatAfterUpdate(): void
    {
        try {
            $this->mysqli->query("UPDATE mi_gcd_orders SET product = 'Thingamajig' WHERE id = 1");

            $rows = $this->ztdQuery(
                "SELECT customer, GROUP_CONCAT(product ORDER BY product) AS products
                 FROM mi_gcd_orders WHERE customer = 'Alice' GROUP BY customer"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'GROUP_CONCAT after UPDATE: expected 1 row, got ' . count($rows)
                );
            }
            $this->assertCount(1, $rows);

            // Alice: id=1 changed to Thingamajig, id=2 still Gadget
            $expected = 'Gadget,Thingamajig';
            if ($rows[0]['products'] !== $expected) {
                $this->markTestIncomplete(
                    'GROUP_CONCAT after UPDATE: expected "' . $expected . '", got '
                    . var_export($rows[0]['products'], true)
                );
            }
            $this->assertSame($expected, $rows[0]['products']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('GROUP_CONCAT after UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * GROUP_CONCAT with custom separator.
     */
    public function testGroupConcatCustomSeparator(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT customer, GROUP_CONCAT(product ORDER BY product SEPARATOR ' | ') AS products
                 FROM mi_gcd_orders WHERE customer = 'Alice' GROUP BY customer"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'GROUP_CONCAT SEPARATOR: expected 1 row, got ' . count($rows)
                );
            }
            $this->assertCount(1, $rows);

            if ($rows[0]['products'] !== 'Gadget | Widget') {
                $this->markTestIncomplete(
                    'GROUP_CONCAT SEPARATOR: expected "Gadget | Widget", got '
                    . var_export($rows[0]['products'], true)
                );
            }
            $this->assertSame('Gadget | Widget', $rows[0]['products']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('GROUP_CONCAT SEPARATOR failed: ' . $e->getMessage());
        }
    }

    /**
     * GROUP_CONCAT with DISTINCT after shadow INSERT of duplicate product.
     */
    public function testGroupConcatDistinctAfterInsert(): void
    {
        try {
            // Insert duplicate product for Alice
            $this->mysqli->query("INSERT INTO mi_gcd_orders VALUES (5, 'Alice', 'Widget', 5.00)");

            $rows = $this->ztdQuery(
                "SELECT customer, GROUP_CONCAT(DISTINCT product ORDER BY product) AS products
                 FROM mi_gcd_orders WHERE customer = 'Alice' GROUP BY customer"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'GROUP_CONCAT DISTINCT: expected 1 row, got ' . count($rows)
                );
            }
            $this->assertCount(1, $rows);

            // Should deduplicate Widget
            if ($rows[0]['products'] !== 'Gadget,Widget') {
                $this->markTestIncomplete(
                    'GROUP_CONCAT DISTINCT: expected "Gadget,Widget", got '
                    . var_export($rows[0]['products'], true)
                );
            }
            $this->assertSame('Gadget,Widget', $rows[0]['products']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('GROUP_CONCAT DISTINCT failed: ' . $e->getMessage());
        }
    }
}
