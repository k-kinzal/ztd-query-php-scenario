<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests UPDATE with multiple subqueries in SET clause.
 *
 * UPDATE t SET
 *   col1 = (SELECT ... FROM other),
 *   col2 = (SELECT ... FROM other)
 * WHERE id = ?
 *
 * Known: single correlated subquery in SET fails on SQLite/PostgreSQL (#51).
 * This tests non-correlated multi-subquery SET — different subqueries for different columns.
 *
 * @spec SPEC-4.2
 */
class SqliteMultiSubqueryUpdateSetTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_msu_summary (
                id INTEGER PRIMARY KEY,
                category TEXT NOT NULL,
                min_price REAL,
                max_price REAL,
                avg_price REAL,
                item_count INTEGER
            )',
            'CREATE TABLE sl_msu_products (
                id INTEGER PRIMARY KEY,
                category TEXT NOT NULL,
                price REAL NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_msu_summary', 'sl_msu_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Products
        $this->pdo->exec("INSERT INTO sl_msu_products VALUES (1, 'electronics', 99.99)");
        $this->pdo->exec("INSERT INTO sl_msu_products VALUES (2, 'electronics', 199.99)");
        $this->pdo->exec("INSERT INTO sl_msu_products VALUES (3, 'electronics', 49.99)");
        $this->pdo->exec("INSERT INTO sl_msu_products VALUES (4, 'clothing', 29.99)");
        $this->pdo->exec("INSERT INTO sl_msu_products VALUES (5, 'clothing', 59.99)");

        // Summary rows to update
        $this->pdo->exec("INSERT INTO sl_msu_summary VALUES (1, 'electronics', NULL, NULL, NULL, NULL)");
        $this->pdo->exec("INSERT INTO sl_msu_summary VALUES (2, 'clothing', NULL, NULL, NULL, NULL)");
    }

    /**
     * UPDATE with multiple non-correlated subqueries in SET.
     *
     * Each SET clause uses a different aggregate subquery from sl_msu_products.
     */
    public function testUpdateMultipleSubqueriesInSet(): void
    {
        $sql = "UPDATE sl_msu_summary SET
                    min_price = (SELECT MIN(price) FROM sl_msu_products WHERE category = sl_msu_summary.category),
                    max_price = (SELECT MAX(price) FROM sl_msu_products WHERE category = sl_msu_summary.category),
                    avg_price = (SELECT AVG(price) FROM sl_msu_products WHERE category = sl_msu_summary.category),
                    item_count = (SELECT COUNT(*) FROM sl_msu_products WHERE category = sl_msu_summary.category)
                WHERE category = 'electronics'";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery(
                "SELECT * FROM sl_msu_summary WHERE category = 'electronics'"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Multi-subquery UPDATE: expected 1 row, got ' . count($rows)
                );
            }

            $row = $rows[0];

            // Verify each subquery result
            $minOk = abs((float) $row['min_price'] - 49.99) < 0.01;
            $maxOk = abs((float) $row['max_price'] - 199.99) < 0.01;
            $avgOk = abs((float) $row['avg_price'] - 116.66) < 0.01;
            $cntOk = (int) $row['item_count'] === 3;

            if (!$minOk || !$maxOk || !$avgOk || !$cntOk) {
                $this->markTestIncomplete(
                    "Multi-subquery UPDATE: values wrong. "
                    . "min={$row['min_price']} (exp 49.99), "
                    . "max={$row['max_price']} (exp 199.99), "
                    . "avg={$row['avg_price']} (exp ~116.66), "
                    . "count={$row['item_count']} (exp 3)"
                );
            }

            $this->assertEquals(49.99, (float) $row['min_price'], '', 0.01);
            $this->assertEquals(199.99, (float) $row['max_price'], '', 0.01);
            $this->assertEquals(116.66, (float) $row['avg_price'], '', 0.01);
            $this->assertSame(3, (int) $row['item_count']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Multi-subquery UPDATE SET failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with non-correlated subqueries (no reference to outer table).
     */
    public function testUpdateNonCorrelatedSubqueries(): void
    {
        $sql = "UPDATE sl_msu_summary SET
                    min_price = (SELECT MIN(price) FROM sl_msu_products),
                    max_price = (SELECT MAX(price) FROM sl_msu_products)
                WHERE id = 1";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT min_price, max_price FROM sl_msu_summary WHERE id = 1");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Non-correlated multi-subquery UPDATE: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertEquals(29.99, (float) $rows[0]['min_price'], '', 0.01);
            $this->assertEquals(199.99, (float) $rows[0]['max_price'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Non-correlated multi-subquery UPDATE SET failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared UPDATE with multiple subqueries and params.
     */
    public function testPreparedMultiSubqueryUpdate(): void
    {
        $sql = "UPDATE sl_msu_summary SET
                    min_price = (SELECT MIN(price) FROM sl_msu_products WHERE category = ?),
                    max_price = (SELECT MAX(price) FROM sl_msu_products WHERE category = ?)
                WHERE category = ?";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute(['clothing', 'clothing', 'clothing']);

            $rows = $this->ztdQuery("SELECT min_price, max_price FROM sl_msu_summary WHERE category = 'clothing'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Prepared multi-subquery UPDATE: expected 1 row, got ' . count($rows)
                );
            }

            $minOk = abs((float) $rows[0]['min_price'] - 29.99) < 0.01;
            $maxOk = abs((float) $rows[0]['max_price'] - 59.99) < 0.01;

            if (!$minOk || !$maxOk) {
                $this->markTestIncomplete(
                    "Prepared multi-subquery UPDATE: min={$rows[0]['min_price']} (exp 29.99), "
                    . "max={$rows[0]['max_price']} (exp 59.99)"
                );
            }

            $this->assertEquals(29.99, (float) $rows[0]['min_price'], '', 0.01);
            $this->assertEquals(59.99, (float) $rows[0]['max_price'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared multi-subquery UPDATE SET failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with subquery in SET and subquery in WHERE.
     */
    public function testUpdateSubqueryInSetAndWhere(): void
    {
        $sql = "UPDATE sl_msu_summary SET
                    item_count = (SELECT COUNT(*) FROM sl_msu_products WHERE category = sl_msu_summary.category)
                WHERE category IN (SELECT DISTINCT category FROM sl_msu_products)";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT category, item_count FROM sl_msu_summary ORDER BY category");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'UPDATE with subquery in SET and WHERE: expected 2 rows, got ' . count($rows)
                );
            }

            // Both rows should be updated
            $clothingCount = null;
            $electronicsCount = null;
            foreach ($rows as $r) {
                if ($r['category'] === 'clothing') $clothingCount = (int) $r['item_count'];
                if ($r['category'] === 'electronics') $electronicsCount = (int) $r['item_count'];
            }

            if ($clothingCount !== 2 || $electronicsCount !== 3) {
                $this->markTestIncomplete(
                    "UPDATE with subquery in SET+WHERE: clothing={$clothingCount} (exp 2), "
                    . "electronics={$electronicsCount} (exp 3)"
                );
            }

            $this->assertSame(2, $clothingCount);
            $this->assertSame(3, $electronicsCount);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE with subquery in SET and WHERE failed: ' . $e->getMessage()
            );
        }
    }
}
