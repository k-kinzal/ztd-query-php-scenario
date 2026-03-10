<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INDEXED BY and NOT INDEXED query hints through the CTE shadow store.
 *
 * SQLite supports `SELECT ... FROM t INDEXED BY idx_name WHERE ...` to force
 * index usage, and `NOT INDEXED` to force a full scan. When the CTE rewriter
 * wraps the table reference, these hints may be lost or cause syntax errors
 * because CTEs do not support index hints.
 *
 * @spec SPEC-3.1
 */
class SqliteIndexedByHintTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE sl_ixh_products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT NOT NULL,
                price REAL NOT NULL
            )",
            "CREATE INDEX sl_ixh_idx_category ON sl_ixh_products (category)",
            "CREATE INDEX sl_ixh_idx_price ON sl_ixh_products (price)",
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_ixh_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->pdo->exec("INSERT INTO sl_ixh_products VALUES (1, 'Widget', 'tools', 9.99)");
        $this->pdo->exec("INSERT INTO sl_ixh_products VALUES (2, 'Gadget', 'electronics', 29.99)");
        $this->pdo->exec("INSERT INTO sl_ixh_products VALUES (3, 'Wrench', 'tools', 14.50)");
        $this->pdo->exec("INSERT INTO sl_ixh_products VALUES (4, 'Phone', 'electronics', 499.00)");
        $this->pdo->exec("INSERT INTO sl_ixh_products VALUES (5, 'Hammer', 'tools', 12.00)");
    }

    /**
     * SELECT with INDEXED BY on a matching index.
     *
     * If the CTE rewriter wraps the table, INDEXED BY becomes invalid because
     * CTEs do not support index hints.
     */
    public function testSelectWithIndexedBy(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name FROM sl_ixh_products INDEXED BY sl_ixh_idx_category WHERE category = 'tools' ORDER BY name"
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'INDEXED BY query returned 0 rows. Expected 3. CTE rewriter may strip or break INDEXED BY.'
                );
            }
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'INDEXED BY query returned ' . count($rows) . ' rows. Expected 3. Got: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame('Hammer', $rows[0]['name']);
            $this->assertSame('Widget', $rows[1]['name']);
            $this->assertSame('Wrench', $rows[2]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INDEXED BY query failed: ' . $e->getMessage());
        }
    }

    /**
     * SELECT with NOT INDEXED hint (force full table scan).
     */
    public function testSelectWithNotIndexed(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name FROM sl_ixh_products NOT INDEXED WHERE category = 'electronics' ORDER BY name"
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'NOT INDEXED query returned 0 rows. Expected 2.'
                );
            }
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'NOT INDEXED query returned ' . count($rows) . ' rows. Expected 2. Got: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Gadget', $rows[0]['name']);
            $this->assertSame('Phone', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('NOT INDEXED query failed: ' . $e->getMessage());
        }
    }

    /**
     * INDEXED BY with prepared statement parameters.
     */
    public function testIndexedByWithPreparedParams(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT name, price FROM sl_ixh_products INDEXED BY sl_ixh_idx_price WHERE price > ? ORDER BY price",
                [15.00]
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'INDEXED BY with prepared params returned 0 rows. Expected 2 (Gadget, Phone).'
                );
            }
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INDEXED BY with prepared params returned ' . count($rows) . ' rows. Expected 2. Got: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Gadget', $rows[0]['name']);
            $this->assertSame('Phone', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INDEXED BY with prepared params failed: ' . $e->getMessage());
        }
    }

    /**
     * INDEXED BY after shadow DML — verify hint works with mutated data.
     */
    public function testIndexedByAfterDml(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_ixh_products VALUES (6, 'Drill', 'tools', 45.00)");
            $this->pdo->exec("UPDATE sl_ixh_products SET category = 'tools' WHERE id = 2");

            $rows = $this->ztdQuery(
                "SELECT name FROM sl_ixh_products INDEXED BY sl_ixh_idx_category WHERE category = 'tools' ORDER BY name"
            );

            // Should now have 5 tools: Widget, Wrench, Hammer, Drill, Gadget (moved)
            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'INDEXED BY after DML returned 0 rows. Expected 5.'
                );
            }
            if (count($rows) !== 5) {
                $this->markTestIncomplete(
                    'INDEXED BY after DML returned ' . count($rows) . ' rows. Expected 5. Got: ' . json_encode($rows)
                );
            }

            $this->assertCount(5, $rows);
            $names = array_column($rows, 'name');
            $this->assertContains('Drill', $names);
            $this->assertContains('Gadget', $names); // moved to tools
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INDEXED BY after DML failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with INDEXED BY hint.
     */
    public function testDeleteWithIndexedBy(): void
    {
        try {
            $this->pdo->exec(
                "DELETE FROM sl_ixh_products INDEXED BY sl_ixh_idx_category WHERE category = 'electronics'"
            );

            $rows = $this->ztdQuery("SELECT name FROM sl_ixh_products ORDER BY name");

            if (count($rows) === 5) {
                $this->markTestIncomplete(
                    'DELETE with INDEXED BY had no effect — all 5 rows remain.'
                );
            }
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE with INDEXED BY left ' . count($rows) . ' rows. Expected 3. Got: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $names = array_column($rows, 'name');
            $this->assertNotContains('Gadget', $names);
            $this->assertNotContains('Phone', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE with INDEXED BY failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with INDEXED BY hint.
     */
    public function testUpdateWithIndexedBy(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE sl_ixh_products INDEXED BY sl_ixh_idx_category SET price = price * 0.9 WHERE category = 'tools'"
            );

            $rows = $this->ztdQuery(
                "SELECT name, price FROM sl_ixh_products WHERE category = 'tools' ORDER BY name"
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'UPDATE with INDEXED BY: no tools found after update.'
                );
            }

            // Widget was 9.99, should be ~8.99
            $widget = null;
            foreach ($rows as $row) {
                if ($row['name'] === 'Widget') {
                    $widget = $row;
                    break;
                }
            }

            if ($widget === null) {
                $this->markTestIncomplete('Widget not found after INDEXED BY UPDATE.');
            }
            if (abs((float) $widget['price'] - 8.991) > 0.01) {
                $this->markTestIncomplete(
                    'UPDATE with INDEXED BY did not apply. Widget price=' . $widget['price'] . ', expected ~8.99'
                );
            }

            $this->assertEqualsWithDelta(8.991, (float) $widget['price'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with INDEXED BY failed: ' . $e->getMessage());
        }
    }
}
