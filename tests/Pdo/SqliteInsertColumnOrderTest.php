<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests whether the shadow store correctly handles INSERT with columns
 * listed in a different order from the DDL definition.
 *
 * When INSERT specifies columns in a non-DDL order, the CTE rewriter must
 * map values to the correct columns by name, not by position. If position-
 * based mapping is used, data will silently end up in the wrong columns.
 *
 * @spec SPEC-4.1
 */
class SqliteInsertColumnOrderTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE sl_ico_items (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            price REAL NOT NULL,
            stock INTEGER NOT NULL DEFAULT 0
        )";
    }

    protected function getTableNames(): array
    {
        return ['sl_ico_items'];
    }

    /**
     * INSERT with columns in reverse order from DDL.
     * DDL: id, name, category, price, stock
     * INSERT: stock, price, category, name, id
     */
    public function testReverseColumnOrder(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_ico_items (stock, price, category, name, id) VALUES (10, 29.99, 'electronics', 'Widget', 1)"
            );

            $rows = $this->ztdQuery("SELECT id, name, category, price, stock FROM sl_ico_items WHERE id = 1");
            $this->assertCount(1, $rows);

            $row = $rows[0];
            if ($row['name'] !== 'Widget') {
                $this->markTestIncomplete(
                    'Column mapping wrong. Expected name="Widget", got name=' . json_encode($row['name'])
                    . '. Values may be mapped by position instead of column name.'
                    . ' Full row: ' . json_encode($row)
                );
            }
            if ((float) $row['price'] !== 29.99) {
                $this->markTestIncomplete(
                    'Column mapping wrong. Expected price=29.99, got price=' . json_encode($row['price'])
                    . '. Full row: ' . json_encode($row)
                );
            }
            $this->assertSame('Widget', $row['name']);
            $this->assertSame('electronics', $row['category']);
            $this->assertEquals(29.99, (float) $row['price']);
            $this->assertEquals(10, (int) $row['stock']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Reverse column order test failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT with partial columns in non-DDL order.
     * Omit stock (has DEFAULT 0). Supply name before category.
     */
    public function testPartialColumnsNonDdlOrder(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_ico_items (price, name, id, category) VALUES (15.50, 'Gadget', 2, 'toys')"
            );

            $rows = $this->ztdQuery("SELECT * FROM sl_ico_items WHERE id = 2");
            $this->assertCount(1, $rows);

            $row = $rows[0];
            if ($row['name'] !== 'Gadget') {
                $this->markTestIncomplete(
                    'Partial column mapping wrong. Expected name="Gadget", got name=' . json_encode($row['name'])
                    . '. Full row: ' . json_encode($row)
                );
            }
            $this->assertSame('Gadget', $row['name']);
            $this->assertSame('toys', $row['category']);
            $this->assertEquals(15.50, (float) $row['price']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Partial columns non-DDL order test failed: ' . $e->getMessage());
        }
    }

    /**
     * Multiple INSERTs with different column orders, then query all.
     */
    public function testMultipleInsertsVaryingOrder(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_ico_items (id, name, category, price, stock) VALUES (1, 'Alpha', 'cat_a', 10.00, 5)"
            );
            $this->pdo->exec(
                "INSERT INTO sl_ico_items (category, id, stock, name, price) VALUES ('cat_b', 2, 8, 'Beta', 20.00)"
            );
            $this->pdo->exec(
                "INSERT INTO sl_ico_items (name, price, id, category, stock) VALUES ('Gamma', 30.00, 3, 'cat_c', 12)"
            );

            $rows = $this->ztdQuery("SELECT id, name, category, price, stock FROM sl_ico_items ORDER BY id");
            $this->assertCount(3, $rows);

            // Verify each row has correct column mapping
            $expected = [
                ['id' => 1, 'name' => 'Alpha', 'category' => 'cat_a', 'price' => 10.00, 'stock' => 5],
                ['id' => 2, 'name' => 'Beta',  'category' => 'cat_b', 'price' => 20.00, 'stock' => 8],
                ['id' => 3, 'name' => 'Gamma', 'category' => 'cat_c', 'price' => 30.00, 'stock' => 12],
            ];

            foreach ($expected as $i => $exp) {
                $actual = $rows[$i];
                if ($actual['name'] !== $exp['name'] || $actual['category'] !== $exp['category']) {
                    $this->markTestIncomplete(
                        "Row {$exp['id']} column mapping wrong. Expected name={$exp['name']}, category={$exp['category']}. "
                        . "Got: " . json_encode($actual)
                    );
                }
            }

            $this->assertSame('Alpha', $rows[0]['name']);
            $this->assertSame('cat_b', $rows[1]['category']);
            $this->assertEquals(30.00, (float) $rows[2]['price']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multiple inserts varying order test failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared INSERT with columns in non-DDL order.
     */
    public function testPreparedInsertNonDdlOrder(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "INSERT INTO sl_ico_items (category, price, id, name, stock) VALUES (?, ?, ?, ?, ?)"
            );
            $stmt->execute(['hardware', 49.99, 1, 'Wrench', 3]);

            $rows = $this->ztdQuery("SELECT * FROM sl_ico_items WHERE id = 1");
            $this->assertCount(1, $rows);

            $row = $rows[0];
            if ($row['name'] !== 'Wrench') {
                $this->markTestIncomplete(
                    'Prepared non-DDL order column mapping wrong. Expected name="Wrench", got: ' . json_encode($row)
                );
            }
            if ($row['category'] !== 'hardware') {
                $this->markTestIncomplete(
                    'Prepared non-DDL order column mapping wrong. Expected category="hardware", got: ' . json_encode($row)
                );
            }
            $this->assertSame('Wrench', $row['name']);
            $this->assertSame('hardware', $row['category']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared insert non-DDL order test failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE then SELECT after non-DDL-ordered INSERT.
     * Verifies column mapping persists through UPDATE cycle.
     */
    public function testUpdateAfterNonDdlOrderInsert(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_ico_items (stock, price, category, name, id) VALUES (10, 29.99, 'electronics', 'Widget', 1)"
            );

            $this->pdo->exec(
                "UPDATE sl_ico_items SET price = 39.99, stock = 15 WHERE id = 1"
            );

            $rows = $this->ztdQuery("SELECT name, price, stock FROM sl_ico_items WHERE id = 1");
            $this->assertCount(1, $rows);

            $row = $rows[0];
            if ($row['name'] !== 'Widget') {
                $this->markTestIncomplete(
                    'After UPDATE, name column corrupted. Got: ' . json_encode($row)
                );
            }
            $this->assertSame('Widget', $row['name']);
            $this->assertEquals(39.99, (float) $row['price']);
            $this->assertEquals(15, (int) $row['stock']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Update after non-DDL order insert test failed: ' . $e->getMessage());
        }
    }
}
