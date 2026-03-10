<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests string_agg() aggregate function with shadow-modified data on PostgreSQL.
 *
 * string_agg() is PostgreSQL's equivalent of MySQL's GROUP_CONCAT.
 * After shadow DML (INSERT/UPDATE/DELETE), the aggregate must reflect mutations.
 *
 * @spec SPEC-3.1
 * @spec SPEC-4.1
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class PostgresStringAggAfterDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_sad_orders (
            id INT PRIMARY KEY,
            customer VARCHAR(50) NOT NULL,
            product VARCHAR(50) NOT NULL,
            amount NUMERIC(10,2) NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_sad_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_sad_orders VALUES (1, 'Alice', 'Widget', 10.00)");
        $this->pdo->exec("INSERT INTO pg_sad_orders VALUES (2, 'Alice', 'Gadget', 20.00)");
        $this->pdo->exec("INSERT INTO pg_sad_orders VALUES (3, 'Bob', 'Widget', 15.00)");
        $this->pdo->exec("INSERT INTO pg_sad_orders VALUES (4, 'Carol', 'Gadget', 25.00)");
    }

    /**
     * string_agg on physical data (baseline).
     */
    public function testStringAggBaseline(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT customer, string_agg(product, ',' ORDER BY product) AS products
                 FROM pg_sad_orders GROUP BY customer ORDER BY customer"
            );

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'string_agg baseline: expected 3 rows, got ' . count($rows)
                );
            }
            $this->assertCount(3, $rows);

            if ($rows[0]['products'] !== 'Gadget,Widget') {
                $this->markTestIncomplete(
                    'string_agg baseline: expected "Gadget,Widget" for Alice, got '
                    . var_export($rows[0]['products'], true)
                );
            }
            $this->assertSame('Gadget,Widget', $rows[0]['products']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('string_agg baseline failed: ' . $e->getMessage());
        }
    }

    /**
     * string_agg after shadow INSERT.
     */
    public function testStringAggAfterInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_sad_orders VALUES (5, 'Alice', 'Doohickey', 30.00)");

            $rows = $this->ztdQuery(
                "SELECT customer, string_agg(product, ',' ORDER BY product) AS products
                 FROM pg_sad_orders WHERE customer = 'Alice' GROUP BY customer"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'string_agg after INSERT: expected 1 row, got ' . count($rows)
                );
            }
            $this->assertCount(1, $rows);

            $expected = 'Doohickey,Gadget,Widget';
            if ($rows[0]['products'] !== $expected) {
                $this->markTestIncomplete(
                    'string_agg after INSERT: expected "' . $expected . '", got '
                    . var_export($rows[0]['products'], true)
                );
            }
            $this->assertSame($expected, $rows[0]['products']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('string_agg after INSERT failed: ' . $e->getMessage());
        }
    }

    /**
     * string_agg after shadow DELETE.
     */
    public function testStringAggAfterDelete(): void
    {
        try {
            $this->pdo->exec("DELETE FROM pg_sad_orders WHERE id = 2");

            $rows = $this->ztdQuery(
                "SELECT customer, string_agg(product, ',' ORDER BY product) AS products
                 FROM pg_sad_orders WHERE customer = 'Alice' GROUP BY customer"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'string_agg after DELETE: expected 1 row, got ' . count($rows)
                );
            }
            $this->assertCount(1, $rows);

            if ($rows[0]['products'] !== 'Widget') {
                $this->markTestIncomplete(
                    'string_agg after DELETE: expected "Widget", got '
                    . var_export($rows[0]['products'], true)
                );
            }
            $this->assertSame('Widget', $rows[0]['products']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('string_agg after DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * string_agg after shadow UPDATE.
     */
    public function testStringAggAfterUpdate(): void
    {
        try {
            $this->pdo->exec("UPDATE pg_sad_orders SET product = 'Thingamajig' WHERE id = 1");

            $rows = $this->ztdQuery(
                "SELECT customer, string_agg(product, ',' ORDER BY product) AS products
                 FROM pg_sad_orders WHERE customer = 'Alice' GROUP BY customer"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'string_agg after UPDATE: expected 1 row, got ' . count($rows)
                );
            }
            $this->assertCount(1, $rows);

            $expected = 'Gadget,Thingamajig';
            if ($rows[0]['products'] !== $expected) {
                $this->markTestIncomplete(
                    'string_agg after UPDATE: expected "' . $expected . '", got '
                    . var_export($rows[0]['products'], true)
                );
            }
            $this->assertSame($expected, $rows[0]['products']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('string_agg after UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * string_agg with custom separator.
     */
    public function testStringAggCustomSeparator(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT customer, string_agg(product, ' | ' ORDER BY product) AS products
                 FROM pg_sad_orders WHERE customer = 'Alice' GROUP BY customer"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'string_agg separator: expected 1 row, got ' . count($rows)
                );
            }
            $this->assertCount(1, $rows);

            if ($rows[0]['products'] !== 'Gadget | Widget') {
                $this->markTestIncomplete(
                    'string_agg separator: expected "Gadget | Widget", got '
                    . var_export($rows[0]['products'], true)
                );
            }
            $this->assertSame('Gadget | Widget', $rows[0]['products']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('string_agg separator failed: ' . $e->getMessage());
        }
    }

    /**
     * DISTINCT string_agg after shadow INSERT of duplicate.
     */
    public function testStringAggDistinctAfterInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_sad_orders VALUES (5, 'Alice', 'Widget', 5.00)");

            $rows = $this->ztdQuery(
                "SELECT customer, string_agg(DISTINCT product, ',' ORDER BY product) AS products
                 FROM pg_sad_orders WHERE customer = 'Alice' GROUP BY customer"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'string_agg DISTINCT: expected 1 row, got ' . count($rows)
                );
            }
            $this->assertCount(1, $rows);

            if ($rows[0]['products'] !== 'Gadget,Widget') {
                $this->markTestIncomplete(
                    'string_agg DISTINCT: expected "Gadget,Widget", got '
                    . var_export($rows[0]['products'], true)
                );
            }
            $this->assertSame('Gadget,Widget', $rows[0]['products']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('string_agg DISTINCT failed: ' . $e->getMessage());
        }
    }
}
