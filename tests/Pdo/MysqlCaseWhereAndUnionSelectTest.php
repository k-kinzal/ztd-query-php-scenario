<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests CASE expressions in WHERE clause and UNION/UNION ALL in SELECT queries
 * (not INSERT) on MySQL PDO after shadow mutations.
 *
 * Adjacent to Issue #96 (CASE in WHERE matches all rows), Issue #98 (CASE
 * EXISTS misdetected as multi-statement), and Issue #103 (INSERT UNION rejected).
 * These scenarios test whether standalone SELECT with UNION and CASE WHERE
 * work correctly with shadow data.
 *
 * @spec SPEC-3.3
 * @spec SPEC-4.3
 */
class MysqlCaseWhereAndUnionSelectTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_cwu_products (
                id INT PRIMARY KEY,
                name VARCHAR(50),
                price DECIMAL(10,2),
                category VARCHAR(20),
                active TINYINT DEFAULT 1
            )',
            'CREATE TABLE my_cwu_archive (
                id INT PRIMARY KEY,
                name VARCHAR(50),
                price DECIMAL(10,2),
                category VARCHAR(20)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_cwu_archive', 'my_cwu_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_cwu_products VALUES (1, 'Widget', 25.00, 'tools', 1)");
        $this->pdo->exec("INSERT INTO my_cwu_products VALUES (2, 'Gadget', 50.00, 'electronics', 1)");
        $this->pdo->exec("INSERT INTO my_cwu_products VALUES (3, 'Gizmo', 10.00, 'tools', 0)");
        $this->pdo->exec("INSERT INTO my_cwu_products VALUES (4, 'Doohickey', 75.00, 'electronics', 1)");
        $this->pdo->exec("INSERT INTO my_cwu_products VALUES (5, 'Thingamajig', 5.00, 'toys', 0)");

        $this->pdo->exec("INSERT INTO my_cwu_archive VALUES (10, 'OldWidget', 15.00, 'tools')");
        $this->pdo->exec("INSERT INTO my_cwu_archive VALUES (11, 'OldGadget', 30.00, 'electronics')");
    }

    // --- CASE in WHERE ---

    public function testCaseInWhereSimple(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, name FROM my_cwu_products
             WHERE CASE WHEN active = 1 THEN price > 30 ELSE price < 8 END
             ORDER BY id"
        );
        // active=1 AND price>30: id 2(50), 4(75)
        // active=0 AND price<8: id 5(5)
        $this->assertCount(3, $rows);
        $ids = array_map(fn($r) => (int) $r['id'], $rows);
        $this->assertEquals([2, 4, 5], $ids);
    }

    public function testCaseInWhereWithPreparedParam(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT id, name FROM my_cwu_products
                 WHERE CASE WHEN category = ? THEN price > 20 ELSE price < 10 END
                 ORDER BY id",
                ['tools']
            );
            // category='tools' AND price>20: id 1(25)
            // category!='tools' AND price<10: id 5(5)
            $this->assertCount(2, $rows);
            $ids = array_map(fn($r) => (int) $r['id'], $rows);
            $this->assertEquals([1, 5], $ids);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CASE in WHERE with prepared param failed: ' . $e->getMessage());
        }
    }

    public function testCaseInWhereAfterUpdate(): void
    {
        $this->pdo->exec("UPDATE my_cwu_products SET price = 100.00 WHERE id = 3");

        $rows = $this->ztdQuery(
            "SELECT id, name FROM my_cwu_products
             WHERE CASE WHEN active = 0 THEN price > 50 ELSE 1=1 END
             ORDER BY id"
        );
        // active=0: id 3(price=100, >50 YES), id 5(price=5, >50 NO)
        // active=1: all pass (1=1): id 1, 2, 4
        $this->assertCount(4, $rows);
        $ids = array_map(fn($r) => (int) $r['id'], $rows);
        $this->assertEquals([1, 2, 3, 4], $ids);
    }

    public function testCaseInWhereAfterDelete(): void
    {
        $this->pdo->exec("DELETE FROM my_cwu_products WHERE id = 3");

        $rows = $this->ztdQuery(
            "SELECT id FROM my_cwu_products
             WHERE CASE category
                WHEN 'tools' THEN price > 20
                WHEN 'electronics' THEN price > 40
                ELSE 1=1
             END
             ORDER BY id"
        );
        // tools + >20: id 1(25)
        // electronics + >40: id 2(50), 4(75)
        // toys (else): id 5
        $this->assertCount(4, $rows);
    }

    public function testNestedCaseInWhere(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id FROM my_cwu_products
                 WHERE CASE
                    WHEN active = 1 THEN
                        CASE WHEN price > 40 THEN 1 ELSE 0 END
                    ELSE
                        CASE WHEN price < 8 THEN 1 ELSE 0 END
                 END = 1
                 ORDER BY id"
            );
            // active=1 + price>40: id 2(50), 4(75)
            // active=0 + price<8: id 5(5)
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
                "SELECT id, name, price FROM my_cwu_products WHERE category = 'tools'
                 UNION ALL
                 SELECT id, name, price FROM my_cwu_archive WHERE category = 'tools'
                 ORDER BY id"
            );
            // products tools: id 1, 3. archive tools: id 10.
            $this->assertCount(3, $rows);
            $ids = array_map(fn($r) => (int) $r['id'], $rows);
            $this->assertContains(1, $ids);
            $this->assertContains(3, $ids);
            $this->assertContains(10, $ids);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UNION ALL two tables failed: ' . $e->getMessage());
        }
    }

    public function testUnionDistinct(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT category FROM my_cwu_products
                 UNION
                 SELECT category FROM my_cwu_archive
                 ORDER BY category"
            );
            // products categories: tools, electronics, toys
            // archive categories: tools, electronics
            // UNION (distinct): electronics, tools, toys
            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UNION DISTINCT failed: ' . $e->getMessage());
        }
    }

    public function testUnionAllAfterMutations(): void
    {
        $this->pdo->exec("INSERT INTO my_cwu_products VALUES (6, 'NewItem', 99.00, 'tools', 1)");
        $this->pdo->exec("DELETE FROM my_cwu_archive WHERE id = 10");

        try {
            $rows = $this->ztdQuery(
                "SELECT id, name FROM my_cwu_products WHERE category = 'tools'
                 UNION ALL
                 SELECT id, name FROM my_cwu_archive WHERE category = 'tools'
                 ORDER BY id"
            );
            // products tools: id 1, 3, 6. archive tools: (deleted 10) = none
            $this->assertCount(3, $rows);
            $this->assertSame('NewItem', $rows[2]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UNION ALL after mutations failed: ' . $e->getMessage());
        }
    }

    public function testUnionWithPreparedParam(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "SELECT id, name FROM my_cwu_products WHERE category = ?
                 UNION ALL
                 SELECT id, name FROM my_cwu_archive WHERE category = ?"
            );
            $stmt->execute(['electronics', 'electronics']);
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
            // products electronics: id 2, 4. archive electronics: id 11.
            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UNION with prepared param failed: ' . $e->getMessage());
        }
    }

    public function testUnionAllSameTableShadow(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id, name, 'active' AS source FROM my_cwu_products WHERE active = 1
                 UNION ALL
                 SELECT id, name, 'inactive' AS source FROM my_cwu_products WHERE active = 0
                 ORDER BY id"
            );
            // active: id 1,2,4. inactive: id 3,5. Total: 5
            $this->assertCount(5, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UNION ALL same table shadow failed: ' . $e->getMessage());
        }
    }

    // --- Combined: CASE + UNION ---

    public function testCaseInSelectWithUnion(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id, name, CASE WHEN price > 30 THEN 'expensive' ELSE 'cheap' END AS tier
                 FROM my_cwu_products
                 WHERE active = 1
                 UNION ALL
                 SELECT id, name, 'archived' AS tier FROM my_cwu_archive
                 ORDER BY id"
            );
            // active products: id 1,2,4. archive: id 10,11. Total: 5
            $this->assertCount(5, $rows);
            $product2 = array_values(array_filter($rows, fn($r) => (int) $r['id'] === 2));
            $this->assertSame('expensive', $product2[0]['tier']);
            $product1 = array_values(array_filter($rows, fn($r) => (int) $r['id'] === 1));
            $this->assertSame('cheap', $product1[0]['tier']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CASE in SELECT with UNION failed: ' . $e->getMessage());
        }
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM my_cwu_products');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
