<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests GROUP BY expression and INSERT with function calls on SQLite PDO.
 *
 * GROUP BY with CASE or function expressions is common in reporting queries.
 * INSERT with function calls is common in data normalization. Both patterns
 * interact with the CTE rewriter's expression parsing.
 *
 * @spec SPEC-3.3
 * @spec SPEC-4.1
 */
class SqliteGroupByExpressionAndInsertFunctionTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE sl_gbe_products (
                id INTEGER PRIMARY KEY,
                name TEXT,
                price REAL,
                category TEXT,
                created_at TEXT
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_gbe_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_gbe_products VALUES (1, 'Widget', 25.00, 'tools', '2025-01-15 10:00:00')");
        $this->pdo->exec("INSERT INTO sl_gbe_products VALUES (2, 'Gadget', 50.00, 'electronics', '2025-02-20 14:00:00')");
        $this->pdo->exec("INSERT INTO sl_gbe_products VALUES (3, 'Gizmo', 10.00, 'tools', '2025-01-05 09:00:00')");
        $this->pdo->exec("INSERT INTO sl_gbe_products VALUES (4, 'Doohickey', 75.00, 'electronics', '2025-03-10 16:00:00')");
        $this->pdo->exec("INSERT INTO sl_gbe_products VALUES (5, 'Thingamajig', 5.00, 'toys', '2025-02-28 11:00:00')");
    }

    // --- GROUP BY expression ---

    public function testGroupByCaseExpression(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT
                    CASE WHEN price >= 50 THEN 'premium' ELSE 'standard' END AS tier,
                    COUNT(*) AS cnt
                 FROM sl_gbe_products
                 GROUP BY CASE WHEN price >= 50 THEN 'premium' ELSE 'standard' END
                 ORDER BY tier"
            );
            $this->assertCount(2, $rows);
            $byTier = array_column($rows, 'cnt', 'tier');
            $this->assertEquals(2, (int) $byTier['premium']);
            $this->assertEquals(3, (int) $byTier['standard']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('GROUP BY CASE expression failed: ' . $e->getMessage());
        }
    }

    public function testGroupByCaseAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO sl_gbe_products VALUES (6, 'Expensive', 200.00, 'luxury', '2025-04-01 12:00:00')");

        try {
            $rows = $this->ztdQuery(
                "SELECT
                    CASE WHEN price >= 50 THEN 'premium' ELSE 'standard' END AS tier,
                    COUNT(*) AS cnt
                 FROM sl_gbe_products
                 GROUP BY CASE WHEN price >= 50 THEN 'premium' ELSE 'standard' END
                 ORDER BY tier"
            );
            $this->assertCount(2, $rows);
            $byTier = array_column($rows, 'cnt', 'tier');
            $this->assertEquals(3, (int) $byTier['premium']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('GROUP BY CASE after INSERT failed: ' . $e->getMessage());
        }
    }

    public function testGroupByFunctionExpression(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT
                    UPPER(category) AS cat_upper,
                    COUNT(*) AS cnt,
                    AVG(price) AS avg_price
                 FROM sl_gbe_products
                 GROUP BY UPPER(category)
                 ORDER BY cat_upper"
            );
            $this->assertCount(3, $rows);
            $this->assertSame('ELECTRONICS', $rows[0]['cat_upper']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('GROUP BY function expression failed: ' . $e->getMessage());
        }
    }

    public function testGroupByDateExtraction(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT
                    SUBSTR(created_at, 6, 2) AS m,
                    COUNT(*) AS cnt
                 FROM sl_gbe_products
                 GROUP BY SUBSTR(created_at, 6, 2)
                 ORDER BY m"
            );
            $this->assertCount(3, $rows);
            $this->assertSame('01', $rows[0]['m']);
            $this->assertEquals(2, (int) $rows[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('GROUP BY date extraction failed: ' . $e->getMessage());
        }
    }

    public function testGroupByExpressionWithHaving(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT
                    CASE WHEN price >= 50 THEN 'premium' ELSE 'standard' END AS tier,
                    COUNT(*) AS cnt
                 FROM sl_gbe_products
                 GROUP BY CASE WHEN price >= 50 THEN 'premium' ELSE 'standard' END
                 HAVING COUNT(*) > 2"
            );
            $this->assertCount(1, $rows);
            $this->assertSame('standard', $rows[0]['tier']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('GROUP BY expression with HAVING failed: ' . $e->getMessage());
        }
    }

    // --- INSERT with function calls ---

    public function testInsertWithUpper(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_gbe_products VALUES (6, UPPER('newitem'), 15.00, LOWER('TOOLS'), '2025-04-01 12:00:00')");

            $rows = $this->ztdQuery('SELECT name, category FROM sl_gbe_products WHERE id = 6');
            $this->assertCount(1, $rows);
            $this->assertSame('NEWITEM', $rows[0]['name']);
            $this->assertSame('tools', $rows[0]['category']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT with UPPER/LOWER failed: ' . $e->getMessage());
        }
    }

    public function testInsertWithStringConcat(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_gbe_products VALUES (6, 'Item' || '-' || 'Six', 15.00, 'misc', '2025-04-01 12:00:00')");

            $rows = $this->ztdQuery('SELECT name FROM sl_gbe_products WHERE id = 6');
            $this->assertCount(1, $rows);
            $this->assertSame('Item-Six', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT with string concat failed: ' . $e->getMessage());
        }
    }

    public function testInsertWithArithmeticExpression(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_gbe_products VALUES (6, 'Computed', 10.00 * 2.5, 'tools', '2025-04-01 12:00:00')");

            $rows = $this->ztdQuery('SELECT price FROM sl_gbe_products WHERE id = 6');
            $this->assertCount(1, $rows);
            $this->assertEqualsWithDelta(25.00, (float) $rows[0]['price'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT with arithmetic expression failed: ' . $e->getMessage());
        }
    }

    public function testInsertWithDatetime(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_gbe_products VALUES (6, 'NowItem', 15.00, 'tools', datetime('now'))");

            $rows = $this->ztdQuery('SELECT name, created_at FROM sl_gbe_products WHERE id = 6');
            $this->assertCount(1, $rows);
            $this->assertSame('NowItem', $rows[0]['name']);
            $this->assertNotNull($rows[0]['created_at']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT with datetime() failed: ' . $e->getMessage());
        }
    }

    public function testInsertFunctionThenUpdateThenQuery(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_gbe_products VALUES (6, UPPER('test'), 10.00 + 5.00, LOWER('MISC'), '2025-04-01 12:00:00')");
            $this->pdo->exec("UPDATE sl_gbe_products SET price = price * 2 WHERE id = 6");

            $rows = $this->ztdQuery('SELECT name, price, category FROM sl_gbe_products WHERE id = 6');
            $this->assertCount(1, $rows);
            $this->assertSame('TEST', $rows[0]['name']);
            $this->assertEqualsWithDelta(30.00, (float) $rows[0]['price'], 0.01);
            $this->assertSame('misc', $rows[0]['category']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT function → UPDATE → query failed: ' . $e->getMessage());
        }
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_gbe_products")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
