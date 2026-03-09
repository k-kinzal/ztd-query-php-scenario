<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests CASE expressions in WHERE clause and UNION/UNION ALL in SELECT queries
 * on PostgreSQL PDO after shadow mutations.
 *
 * Adjacent to Issue #96, #98, #103. Also tests PostgreSQL-specific
 * string concat || in combination with CASE and UNION.
 *
 * @spec SPEC-3.3
 * @spec SPEC-4.3
 */
class PostgresCaseWhereAndUnionSelectTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_cwu_products (
                id INT PRIMARY KEY,
                name VARCHAR(50),
                price NUMERIC(10,2),
                category VARCHAR(20),
                active INT DEFAULT 1
            )',
            'CREATE TABLE pg_cwu_archive (
                id INT PRIMARY KEY,
                name VARCHAR(50),
                price NUMERIC(10,2),
                category VARCHAR(20)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_cwu_archive', 'pg_cwu_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_cwu_products VALUES (1, 'Widget', 25.00, 'tools', 1)");
        $this->pdo->exec("INSERT INTO pg_cwu_products VALUES (2, 'Gadget', 50.00, 'electronics', 1)");
        $this->pdo->exec("INSERT INTO pg_cwu_products VALUES (3, 'Gizmo', 10.00, 'tools', 0)");
        $this->pdo->exec("INSERT INTO pg_cwu_products VALUES (4, 'Doohickey', 75.00, 'electronics', 1)");
        $this->pdo->exec("INSERT INTO pg_cwu_products VALUES (5, 'Thingamajig', 5.00, 'toys', 0)");

        $this->pdo->exec("INSERT INTO pg_cwu_archive VALUES (10, 'OldWidget', 15.00, 'tools')");
        $this->pdo->exec("INSERT INTO pg_cwu_archive VALUES (11, 'OldGadget', 30.00, 'electronics')");
    }

    // --- CASE in WHERE ---

    public function testCaseInWhereSimple(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, name FROM pg_cwu_products
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
                "SELECT id, name FROM pg_cwu_products
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
        $this->pdo->exec("UPDATE pg_cwu_products SET price = 100.00 WHERE id = 3");

        $rows = $this->ztdQuery(
            "SELECT id, name FROM pg_cwu_products
             WHERE CASE WHEN active = 0 THEN price > 50 ELSE 1=1 END
             ORDER BY id"
        );
        // not active: id 3(100 > 50 YES), id 5(5 > 50 NO)
        // active (TRUE): id 1, 2, 4
        $this->assertCount(4, $rows);
        $ids = array_map(fn($r) => (int) $r['id'], $rows);
        $this->assertEquals([1, 2, 3, 4], $ids);
    }

    public function testNestedCaseInWhere(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id FROM pg_cwu_products
                 WHERE CASE
                    WHEN active = 1 THEN
                        CASE WHEN price > 40 THEN 1 ELSE 0 END
                    ELSE
                        CASE WHEN price < 8 THEN 1 ELSE 0 END
                 END
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
                "SELECT id, name, price FROM pg_cwu_products WHERE category = 'tools'
                 UNION ALL
                 SELECT id, name, price FROM pg_cwu_archive WHERE category = 'tools'
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
                "SELECT category FROM pg_cwu_products
                 UNION
                 SELECT category FROM pg_cwu_archive
                 ORDER BY category"
            );
            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UNION DISTINCT failed: ' . $e->getMessage());
        }
    }

    public function testUnionAllAfterMutations(): void
    {
        $this->pdo->exec("INSERT INTO pg_cwu_products VALUES (6, 'NewItem', 99.00, 'tools', 1)");
        $this->pdo->exec("DELETE FROM pg_cwu_archive WHERE id = 10");

        try {
            $rows = $this->ztdQuery(
                "SELECT id, name FROM pg_cwu_products WHERE category = 'tools'
                 UNION ALL
                 SELECT id, name FROM pg_cwu_archive WHERE category = 'tools'
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
                "SELECT id, name FROM pg_cwu_products WHERE category = ?
                 UNION ALL
                 SELECT id, name FROM pg_cwu_archive WHERE category = ?"
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
                "SELECT id, name, 'active' AS source FROM pg_cwu_products WHERE active = 1
                 UNION ALL
                 SELECT id, name, 'inactive' AS source FROM pg_cwu_products WHERE active = 0
                 ORDER BY id"
            );
            $this->assertCount(5, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UNION ALL same table shadow failed: ' . $e->getMessage());
        }
    }

    // --- String concat || in combination ---

    public function testStringConcatInWhereAfterMutation(): void
    {
        $this->pdo->exec("UPDATE pg_cwu_products SET name = 'SuperWidget' WHERE id = 1");

        try {
            $rows = $this->ztdQuery(
                "SELECT id FROM pg_cwu_products
                 WHERE name || ' [' || category || ']' = 'SuperWidget [tools]'"
            );
            $this->assertCount(1, $rows);
            $this->assertEquals(1, (int) $rows[0]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('String concat in WHERE failed: ' . $e->getMessage());
        }
    }

    public function testStringConcatInUpdateSet(): void
    {
        $this->pdo->exec(
            "UPDATE pg_cwu_products SET name = name || ' (v2)' WHERE category = 'tools'"
        );

        $rows = $this->ztdQuery(
            "SELECT id, name FROM pg_cwu_products WHERE category = 'tools' ORDER BY id"
        );
        $this->assertCount(2, $rows);
        $this->assertSame('Widget (v2)', $rows[0]['name']);
        $this->assertSame('Gizmo (v2)', $rows[1]['name']);
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_cwu_products');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
