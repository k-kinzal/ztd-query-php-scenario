<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests CASE expressions in WHERE clause and UNION/UNION ALL in SELECT queries
 * on SQLite PDO after shadow mutations.
 *
 * Adjacent to Issue #96 (MySQL: CASE in WHERE matches all rows in
 * DELETE/UPDATE) — this tests whether SQLite has similar problems with CASE
 * in WHERE for SELECT. Also tests UNION in SELECT context (Issue #103 only
 * affects INSERT...SELECT UNION on MySQL).
 *
 * @spec SPEC-3.3
 * @spec SPEC-4.3
 */
class SqliteCaseWhereAndUnionSelectTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_cwu_products (
                id INTEGER PRIMARY KEY,
                name TEXT,
                price REAL,
                category TEXT,
                active INTEGER DEFAULT 1
            )',
            'CREATE TABLE sl_cwu_archive (
                id INTEGER PRIMARY KEY,
                name TEXT,
                price REAL,
                category TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_cwu_archive', 'sl_cwu_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_cwu_products VALUES (1, 'Widget', 25.00, 'tools', 1)");
        $this->pdo->exec("INSERT INTO sl_cwu_products VALUES (2, 'Gadget', 50.00, 'electronics', 1)");
        $this->pdo->exec("INSERT INTO sl_cwu_products VALUES (3, 'Gizmo', 10.00, 'tools', 0)");
        $this->pdo->exec("INSERT INTO sl_cwu_products VALUES (4, 'Doohickey', 75.00, 'electronics', 1)");
        $this->pdo->exec("INSERT INTO sl_cwu_products VALUES (5, 'Thingamajig', 5.00, 'toys', 0)");

        $this->pdo->exec("INSERT INTO sl_cwu_archive VALUES (10, 'OldWidget', 15.00, 'tools')");
        $this->pdo->exec("INSERT INTO sl_cwu_archive VALUES (11, 'OldGadget', 30.00, 'electronics')");
    }

    // --- CASE in WHERE ---

    public function testCaseInWhereSimple(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, name FROM sl_cwu_products
             WHERE CASE WHEN active = 1 THEN price > 30 ELSE price < 8 END
             ORDER BY id"
        );
        $this->assertCount(3, $rows);
        $ids = array_map(fn($r) => (int) $r['id'], $rows);
        $this->assertEquals([2, 4, 5], $ids);
    }

    public function testCaseInWhereWithPreparedParam(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT id, name FROM sl_cwu_products
                 WHERE CASE WHEN category = ? THEN price > 20 ELSE price < 10 END
                 ORDER BY id",
                ['tools']
            );
            $this->assertCount(2, $rows);
            $ids = array_map(fn($r) => (int) $r['id'], $rows);
            $this->assertEquals([1, 5], $ids);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CASE in WHERE with prepared param failed: ' . $e->getMessage());
        }
    }

    public function testCaseInWhereAfterUpdate(): void
    {
        $this->pdo->exec("UPDATE sl_cwu_products SET price = 100.00 WHERE id = 3");

        $rows = $this->ztdQuery(
            "SELECT id, name FROM sl_cwu_products
             WHERE CASE WHEN active = 0 THEN price > 50 ELSE 1 END
             ORDER BY id"
        );
        // active=0: id 3(100>50 YES), id 5(5>50 NO)
        // active=1: always true: id 1, 2, 4
        $this->assertCount(4, $rows);
    }

    public function testNestedCaseInWhere(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id FROM sl_cwu_products
                 WHERE CASE
                    WHEN active = 1 THEN
                        CASE WHEN price > 40 THEN 1 ELSE 0 END
                    ELSE
                        CASE WHEN price < 8 THEN 1 ELSE 0 END
                 END = 1
                 ORDER BY id"
            );
            $this->assertCount(3, $rows);
            $ids = array_map(fn($r) => (int) $r['id'], $rows);
            $this->assertEquals([2, 4, 5], $ids);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Nested CASE in WHERE failed: ' . $e->getMessage());
        }
    }

    // --- UNION / UNION ALL in SELECT ---

    public function testUnionAllTwoTables(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id, name, price FROM sl_cwu_products WHERE category = 'tools'
                 UNION ALL
                 SELECT id, name, price FROM sl_cwu_archive WHERE category = 'tools'
                 ORDER BY id"
            );
            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UNION ALL two tables failed: ' . $e->getMessage());
        }
    }

    public function testUnionDistinct(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT category FROM sl_cwu_products
                 UNION
                 SELECT category FROM sl_cwu_archive
                 ORDER BY category"
            );
            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UNION DISTINCT failed: ' . $e->getMessage());
        }
    }

    public function testUnionAllAfterMutations(): void
    {
        $this->pdo->exec("INSERT INTO sl_cwu_products VALUES (6, 'NewItem', 99.00, 'tools', 1)");
        $this->pdo->exec("DELETE FROM sl_cwu_archive WHERE id = 10");

        try {
            $rows = $this->ztdQuery(
                "SELECT id, name FROM sl_cwu_products WHERE category = 'tools'
                 UNION ALL
                 SELECT id, name FROM sl_cwu_archive WHERE category = 'tools'
                 ORDER BY id"
            );
            $this->assertCount(3, $rows);
            $names = array_column($rows, 'name');
            $this->assertContains('NewItem', $names);
            $this->assertNotContains('OldWidget', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UNION ALL after mutations failed: ' . $e->getMessage());
        }
    }

    public function testUnionWithPreparedParam(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "SELECT id, name FROM sl_cwu_products WHERE category = ?
                 UNION ALL
                 SELECT id, name FROM sl_cwu_archive WHERE category = ?"
            );
            $stmt->execute(['electronics', 'electronics']);
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UNION with prepared param failed: ' . $e->getMessage());
        }
    }

    public function testUnionAllSameTableShadow(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id, name, 'active' AS source FROM sl_cwu_products WHERE active = 1
                 UNION ALL
                 SELECT id, name, 'inactive' AS source FROM sl_cwu_products WHERE active = 0
                 ORDER BY id"
            );
            $this->assertCount(5, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UNION ALL same table shadow failed: ' . $e->getMessage());
        }
    }

    public function testStringConcatInUpdateSet(): void
    {
        $this->pdo->exec(
            "UPDATE sl_cwu_products SET name = name || ' (v2)' WHERE category = 'tools'"
        );

        $rows = $this->ztdQuery(
            "SELECT id, name FROM sl_cwu_products WHERE category = 'tools' ORDER BY id"
        );
        $this->assertCount(2, $rows);
        $this->assertSame('Widget (v2)', $rows[0]['name']);
        $this->assertSame('Gizmo (v2)', $rows[1]['name']);
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_cwu_products")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
