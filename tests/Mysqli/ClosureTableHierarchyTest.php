<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests the closure table pattern for deep hierarchy traversal (4+ levels)
 * through ZTD shadow store (MySQLi).
 * Common pattern for category trees, org charts, and nested menus.
 * Covers descendant/ancestor queries, depth filtering, leaf detection,
 * subtree counts, subtree mutation, and physical isolation.
 * @spec SPEC-10.2.121
 */
class ClosureTableHierarchyTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_ch_categories (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255),
                parent_id INT
            )',
            'CREATE TABLE mi_ch_category_closure (
                ancestor_id INT,
                descendant_id INT,
                depth INT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_ch_category_closure', 'mi_ch_categories'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Categories (5-level hierarchy)
        // Electronics (1)
        //   ├─ Computers (2)
        //   │   ├─ Laptops (3)
        //   │   │   ├─ Gaming Laptops (6)
        //   │   │   └─ Business Laptops (7)
        //   │   └─ Desktops (4)
        //   └─ Phones (5)
        //       └─ Smartphones (8)
        //           └─ Flagship (9)
        $this->mysqli->query("INSERT INTO mi_ch_categories (id, name, parent_id) VALUES (1, 'Electronics', NULL)");
        $this->mysqli->query("INSERT INTO mi_ch_categories (id, name, parent_id) VALUES (2, 'Computers', 1)");
        $this->mysqli->query("INSERT INTO mi_ch_categories (id, name, parent_id) VALUES (3, 'Laptops', 2)");
        $this->mysqli->query("INSERT INTO mi_ch_categories (id, name, parent_id) VALUES (4, 'Desktops', 2)");
        $this->mysqli->query("INSERT INTO mi_ch_categories (id, name, parent_id) VALUES (5, 'Phones', 1)");
        $this->mysqli->query("INSERT INTO mi_ch_categories (id, name, parent_id) VALUES (6, 'Gaming Laptops', 3)");
        $this->mysqli->query("INSERT INTO mi_ch_categories (id, name, parent_id) VALUES (7, 'Business Laptops', 3)");
        $this->mysqli->query("INSERT INTO mi_ch_categories (id, name, parent_id) VALUES (8, 'Smartphones', 5)");
        $this->mysqli->query("INSERT INTO mi_ch_categories (id, name, parent_id) VALUES (9, 'Flagship', 8)");

        // Closure table entries: every (ancestor, descendant, depth) triple
        // Self-references (depth 0)
        $this->mysqli->query("INSERT INTO mi_ch_category_closure VALUES (1, 1, 0)");
        $this->mysqli->query("INSERT INTO mi_ch_category_closure VALUES (2, 2, 0)");
        $this->mysqli->query("INSERT INTO mi_ch_category_closure VALUES (3, 3, 0)");
        $this->mysqli->query("INSERT INTO mi_ch_category_closure VALUES (4, 4, 0)");
        $this->mysqli->query("INSERT INTO mi_ch_category_closure VALUES (5, 5, 0)");
        $this->mysqli->query("INSERT INTO mi_ch_category_closure VALUES (6, 6, 0)");
        $this->mysqli->query("INSERT INTO mi_ch_category_closure VALUES (7, 7, 0)");
        $this->mysqli->query("INSERT INTO mi_ch_category_closure VALUES (8, 8, 0)");
        $this->mysqli->query("INSERT INTO mi_ch_category_closure VALUES (9, 9, 0)");

        // Ancestor-descendant pairs from Electronics (1)
        $this->mysqli->query("INSERT INTO mi_ch_category_closure VALUES (1, 2, 1)");
        $this->mysqli->query("INSERT INTO mi_ch_category_closure VALUES (1, 3, 2)");
        $this->mysqli->query("INSERT INTO mi_ch_category_closure VALUES (1, 4, 2)");
        $this->mysqli->query("INSERT INTO mi_ch_category_closure VALUES (1, 5, 1)");
        $this->mysqli->query("INSERT INTO mi_ch_category_closure VALUES (1, 6, 3)");
        $this->mysqli->query("INSERT INTO mi_ch_category_closure VALUES (1, 7, 3)");
        $this->mysqli->query("INSERT INTO mi_ch_category_closure VALUES (1, 8, 2)");
        $this->mysqli->query("INSERT INTO mi_ch_category_closure VALUES (1, 9, 3)");

        // Ancestor-descendant pairs from Computers (2)
        $this->mysqli->query("INSERT INTO mi_ch_category_closure VALUES (2, 3, 1)");
        $this->mysqli->query("INSERT INTO mi_ch_category_closure VALUES (2, 4, 1)");
        $this->mysqli->query("INSERT INTO mi_ch_category_closure VALUES (2, 6, 2)");
        $this->mysqli->query("INSERT INTO mi_ch_category_closure VALUES (2, 7, 2)");

        // Ancestor-descendant pairs from Laptops (3)
        $this->mysqli->query("INSERT INTO mi_ch_category_closure VALUES (3, 6, 1)");
        $this->mysqli->query("INSERT INTO mi_ch_category_closure VALUES (3, 7, 1)");

        // Ancestor-descendant pairs from Phones (5)
        $this->mysqli->query("INSERT INTO mi_ch_category_closure VALUES (5, 8, 1)");
        $this->mysqli->query("INSERT INTO mi_ch_category_closure VALUES (5, 9, 2)");

        // Ancestor-descendant pairs from Smartphones (8)
        $this->mysqli->query("INSERT INTO mi_ch_category_closure VALUES (8, 9, 1)");
    }

    /**
     * All descendants of Electronics (ancestor_id=1, depth > 0) should return 8 nodes.
     */
    public function testAllDescendants(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.id, c.name, cl.depth
             FROM mi_ch_category_closure cl
             JOIN mi_ch_categories c ON c.id = cl.descendant_id
             WHERE cl.ancestor_id = 1 AND cl.depth > 0
             ORDER BY cl.depth, c.name"
        );

        $this->assertCount(8, $rows);

        // Depth 1: Computers, Phones
        $this->assertEquals(1, (int) $rows[0]['depth']);
        $this->assertSame('Computers', $rows[0]['name']);
        $this->assertEquals(1, (int) $rows[1]['depth']);
        $this->assertSame('Phones', $rows[1]['name']);

        // Depth 2: Desktops, Laptops, Smartphones
        $this->assertEquals(2, (int) $rows[2]['depth']);

        // Depth 3: Business Laptops, Flagship, Gaming Laptops
        $names = array_column($rows, 'name');
        $this->assertContains('Gaming Laptops', $names);
        $this->assertContains('Business Laptops', $names);
        $this->assertContains('Flagship', $names);
    }

    /**
     * All ancestors of Gaming Laptops (descendant_id=6, depth > 0)
     * should return Electronics, Computers, Laptops in depth order.
     */
    public function testAllAncestors(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.id, c.name, cl.depth
             FROM mi_ch_category_closure cl
             JOIN mi_ch_categories c ON c.id = cl.ancestor_id
             WHERE cl.descendant_id = 6 AND cl.depth > 0
             ORDER BY cl.depth"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Laptops', $rows[0]['name']);
        $this->assertEquals(1, (int) $rows[0]['depth']);
        $this->assertSame('Computers', $rows[1]['name']);
        $this->assertEquals(2, (int) $rows[1]['depth']);
        $this->assertSame('Electronics', $rows[2]['name']);
        $this->assertEquals(3, (int) $rows[2]['depth']);
    }

    /**
     * Direct children of Computers (depth = 1) should return Laptops and Desktops.
     */
    public function testDirectChildren(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT c.id, c.name
             FROM mi_ch_category_closure cl
             JOIN mi_ch_categories c ON c.id = cl.descendant_id
             WHERE cl.ancestor_id = ? AND cl.depth = 1
             ORDER BY c.name",
            [2]
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Desktops', $rows[0]['name']);
        $this->assertSame('Laptops', $rows[1]['name']);
    }

    /**
     * Descendants of Electronics within depth <= 2 should return 5 nodes:
     * Computers, Phones (depth 1), Laptops, Desktops, Smartphones (depth 2).
     */
    public function testSubtreeDepthFilter(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.id, c.name, cl.depth
             FROM mi_ch_category_closure cl
             JOIN mi_ch_categories c ON c.id = cl.descendant_id
             WHERE cl.ancestor_id = 1 AND cl.depth > 0 AND cl.depth <= 2
             ORDER BY cl.depth, c.name"
        );

        $this->assertCount(5, $rows);
        $names = array_column($rows, 'name');
        $this->assertContains('Computers', $names);
        $this->assertContains('Phones', $names);
        $this->assertContains('Laptops', $names);
        $this->assertContains('Desktops', $names);
        $this->assertContains('Smartphones', $names);
    }

    /**
     * Leaf categories are those that only appear as ancestor_id at depth 0
     * (they have no descendants other than themselves).
     */
    public function testLeafNodes(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.id, c.name
             FROM mi_ch_categories c
             WHERE NOT EXISTS (
                 SELECT 1 FROM mi_ch_category_closure cl
                 WHERE cl.ancestor_id = c.id AND cl.depth > 0
             )
             ORDER BY c.name"
        );

        $this->assertCount(4, $rows);
        $names = array_column($rows, 'name');
        $this->assertSame(['Business Laptops', 'Desktops', 'Flagship', 'Gaming Laptops'], $names);
    }

    /**
     * Count descendants per direct child of Electronics using GROUP BY.
     */
    public function testSubtreeCount(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.name,
                    COUNT(cl2.descendant_id) AS descendant_count
             FROM mi_ch_category_closure cl1
             JOIN mi_ch_categories c ON c.id = cl1.descendant_id
             JOIN mi_ch_category_closure cl2 ON cl2.ancestor_id = cl1.descendant_id AND cl2.depth > 0
             WHERE cl1.ancestor_id = 1 AND cl1.depth = 1
             GROUP BY c.id, c.name
             ORDER BY c.name"
        );

        $this->assertCount(2, $rows);
        // Computers has 4 descendants: Laptops, Desktops, Gaming Laptops, Business Laptops
        $this->assertSame('Computers', $rows[0]['name']);
        $this->assertEquals(4, (int) $rows[0]['descendant_count']);
        // Phones has 2 descendants: Smartphones, Flagship
        $this->assertSame('Phones', $rows[1]['name']);
        $this->assertEquals(2, (int) $rows[1]['descendant_count']);
    }

    /**
     * Move a subtree: insert a new category under Desktops, add closure entries,
     * then verify the new ancestry.
     */
    public function testMoveSubtree(): void
    {
        // Insert a new category "Workstations" (id=10) under Desktops (id=4)
        $this->mysqli->query("INSERT INTO mi_ch_categories (id, name, parent_id) VALUES (10, 'Workstations', 4)");

        // Add closure entries for the new node:
        // Self-reference
        $this->mysqli->query("INSERT INTO mi_ch_category_closure VALUES (10, 10, 0)");
        // Inherit all ancestors of Desktops (4) + 1 depth
        // Desktops ancestors: Electronics(1) at depth 2, Computers(2) at depth 1, Desktops(4) at depth 0
        // So Workstations(10) gets: (4,10,1), (2,10,2), (1,10,3)
        $this->mysqli->query("INSERT INTO mi_ch_category_closure VALUES (4, 10, 1)");
        $this->mysqli->query("INSERT INTO mi_ch_category_closure VALUES (2, 10, 2)");
        $this->mysqli->query("INSERT INTO mi_ch_category_closure VALUES (1, 10, 3)");

        // Verify Workstations appears as descendant of Electronics
        $rows = $this->ztdQuery(
            "SELECT c.id, c.name, cl.depth
             FROM mi_ch_category_closure cl
             JOIN mi_ch_categories c ON c.id = cl.descendant_id
             WHERE cl.ancestor_id = 1 AND cl.depth > 0
             ORDER BY cl.depth, c.name"
        );
        $this->assertCount(9, $rows);
        $names = array_column($rows, 'name');
        $this->assertContains('Workstations', $names);

        // Verify ancestors of Workstations
        $rows = $this->ztdQuery(
            "SELECT c.name, cl.depth
             FROM mi_ch_category_closure cl
             JOIN mi_ch_categories c ON c.id = cl.ancestor_id
             WHERE cl.descendant_id = 10 AND cl.depth > 0
             ORDER BY cl.depth"
        );
        $this->assertCount(3, $rows);
        $this->assertSame('Desktops', $rows[0]['name']);
        $this->assertSame('Computers', $rows[1]['name']);
        $this->assertSame('Electronics', $rows[2]['name']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_ch_categories (id, name, parent_id) VALUES (10, 'Tablets', 1)");
        $this->mysqli->query("INSERT INTO mi_ch_category_closure VALUES (1, 10, 1)");
        $this->mysqli->query("INSERT INTO mi_ch_category_closure VALUES (10, 10, 0)");
        $this->mysqli->query("DELETE FROM mi_ch_categories WHERE id = 9");

        // ZTD shows 9 categories (original 9 + 1 added - 1 deleted)
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_ch_categories");
        $this->assertEquals(9, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_ch_categories');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
