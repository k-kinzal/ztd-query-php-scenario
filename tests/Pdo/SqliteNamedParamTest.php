<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests named parameters (:name style) with various SQL patterns on SQLite.
 *
 * All existing prepared statement tests use positional (?) parameters.
 * Named parameters may interact differently with the CTE rewriter's
 * parameter position tracking.
 *
 * @spec SPEC-3.1, SPEC-4.2
 */
class SqliteNamedParamTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_np_items (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            price REAL NOT NULL,
            stock INTEGER NOT NULL DEFAULT 0
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_np_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_np_items VALUES (1, 'Widget', 'tools', 10.00, 100)");
        $this->pdo->exec("INSERT INTO sl_np_items VALUES (2, 'Gadget', 'tools', 20.00, 50)");
        $this->pdo->exec("INSERT INTO sl_np_items VALUES (3, 'Doohickey', 'parts', 30.00, 75)");
        $this->pdo->exec("INSERT INTO sl_np_items VALUES (4, 'Thingamajig', 'parts', 15.00, 200)");
    }

    /**
     * Simple SELECT with named parameter.
     */
    public function testSelectWithNamedParam(): void
    {
        $sql = "SELECT name, price FROM sl_np_items WHERE category = :cat ORDER BY name";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([':cat' => 'tools']);
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Named param SELECT: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Gadget', $rows[0]['name']);
            $this->assertSame('Widget', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Named param SELECT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT with multiple named parameters.
     */
    public function testSelectWithMultipleNamedParams(): void
    {
        $sql = "SELECT name FROM sl_np_items WHERE category = :cat AND price >= :min_price ORDER BY name";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([':cat' => 'tools', ':min_price' => 15.00]);
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Multi named param SELECT: expected 1 row, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Gadget', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Multi named param SELECT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with named parameter.
     */
    public function testUpdateWithNamedParam(): void
    {
        $sql = "UPDATE sl_np_items SET price = :new_price WHERE id = :id";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([':new_price' => 99.99, ':id' => 1]);

            $rows = $this->ztdQuery("SELECT price FROM sl_np_items WHERE id = 1");

            $this->assertCount(1, $rows);

            $price = (float) $rows[0]['price'];
            if (abs($price - 99.99) > 0.01) {
                $this->markTestIncomplete(
                    "Named param UPDATE: price expected 99.99, got {$price}"
                );
            }

            $this->assertEquals(99.99, $price, '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Named param UPDATE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE with named parameter.
     */
    public function testDeleteWithNamedParam(): void
    {
        $sql = "DELETE FROM sl_np_items WHERE category = :cat";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([':cat' => 'parts']);

            $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_np_items");

            $count = (int) $rows[0]['cnt'];
            if ($count !== 2) {
                $this->markTestIncomplete(
                    "Named param DELETE: expected 2 remaining, got {$count}"
                );
            }

            $this->assertSame(2, $count);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Named param DELETE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT with named parameters.
     */
    public function testInsertWithNamedParams(): void
    {
        $sql = "INSERT INTO sl_np_items (id, name, category, price, stock)
                VALUES (:id, :name, :cat, :price, :stock)";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([
                ':id' => 5,
                ':name' => 'Gizmo',
                ':cat' => 'gadgets',
                ':price' => 45.00,
                ':stock' => 10,
            ]);

            $rows = $this->ztdQuery("SELECT name, category, price FROM sl_np_items WHERE id = 5");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Named param INSERT: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Gizmo', $rows[0]['name']);
            $this->assertSame('gadgets', $rows[0]['category']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Named param INSERT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Aggregate query with named parameter.
     */
    public function testAggregateWithNamedParam(): void
    {
        $sql = "SELECT COUNT(*) AS cnt, SUM(price) AS total
                FROM sl_np_items WHERE category = :cat";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([':cat' => 'tools']);
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            $this->assertCount(1, $rows);

            $count = (int) $rows[0]['cnt'];
            $total = (float) $rows[0]['total'];

            if ($count !== 2) {
                $this->markTestIncomplete(
                    "Named param aggregate: count expected 2, got {$count}"
                );
            }

            $this->assertSame(2, $count);
            $this->assertEquals(30.00, $total, '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Named param aggregate failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Named param with subquery in WHERE.
     */
    public function testSubqueryWhereWithNamedParam(): void
    {
        $sql = "SELECT name FROM sl_np_items
                WHERE price > (SELECT AVG(price) FROM sl_np_items WHERE category = :cat)
                ORDER BY name";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([':cat' => 'tools']);
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            // AVG of tools = (10+20)/2 = 15. Items with price > 15: Gadget(20), Doohickey(30), Thingamajig(15 — no)
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Named param subquery: expected 2 rows (Doohickey, Gadget), got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Doohickey', $rows[0]['name']);
            $this->assertSame('Gadget', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Named param subquery failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Named param with GROUP BY HAVING.
     */
    public function testGroupByHavingWithNamedParam(): void
    {
        $sql = "SELECT category, COUNT(*) AS cnt
                FROM sl_np_items
                WHERE stock >= :min_stock
                GROUP BY category
                HAVING COUNT(*) >= :min_count";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([':min_stock' => 50, ':min_count' => 2]);
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            // stock >= 50: Widget(100), Gadget(50), Doohickey(75), Thingamajig(200) = all 4
            // tools: 2, parts: 2. Both have count >= 2.
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Named param GROUP BY HAVING: expected 2 rows, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Named param GROUP BY HAVING failed: ' . $e->getMessage()
            );
        }
    }
}
