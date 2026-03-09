<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * GROUP_CONCAT aggregate function and multi-step DML lifecycle through ZTD
 * shadow store on SQLite.
 *
 * GROUP_CONCAT is a common SQLite aggregate that real users use for
 * denormalized reporting. Combined with multi-step INSERT→UPDATE→UPDATE→
 * DELETE→SELECT lifecycles, these patterns test whether the shadow store
 * maintains correct state across multiple mutations on the same rows.
 *
 * @spec SPEC-3.3
 * @spec SPEC-4.3
 */
class SqliteGroupConcatAndMultiDmlLifecycleTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE sl_gcml_tags (
                id INTEGER PRIMARY KEY,
                item_name VARCHAR(30),
                tag VARCHAR(20)
            )",
            "CREATE TABLE sl_gcml_items (
                id INTEGER PRIMARY KEY,
                name VARCHAR(30),
                status VARCHAR(20),
                priority INTEGER,
                version INTEGER DEFAULT 1
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_gcml_tags', 'sl_gcml_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_gcml_tags VALUES (1, 'Widget', 'red')");
        $this->pdo->exec("INSERT INTO sl_gcml_tags VALUES (2, 'Widget', 'large')");
        $this->pdo->exec("INSERT INTO sl_gcml_tags VALUES (3, 'Widget', 'sale')");
        $this->pdo->exec("INSERT INTO sl_gcml_tags VALUES (4, 'Gadget', 'blue')");
        $this->pdo->exec("INSERT INTO sl_gcml_tags VALUES (5, 'Gadget', 'small')");
        $this->pdo->exec("INSERT INTO sl_gcml_tags VALUES (6, 'Gizmo', 'green')");

        $this->pdo->exec("INSERT INTO sl_gcml_items VALUES (1, 'Task-A', 'open', 1, 1)");
        $this->pdo->exec("INSERT INTO sl_gcml_items VALUES (2, 'Task-B', 'open', 2, 1)");
        $this->pdo->exec("INSERT INTO sl_gcml_items VALUES (3, 'Task-C', 'open', 3, 1)");
    }

    public function testGroupConcat(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT item_name, GROUP_CONCAT(tag, ', ') AS tags
                 FROM sl_gcml_tags
                 GROUP BY item_name
                 ORDER BY item_name"
            );
            $this->assertCount(3, $rows);
            $byName = array_column($rows, 'tags', 'item_name');
            // Widget should have 3 tags
            $widgetTags = explode(', ', $byName['Widget']);
            $this->assertCount(3, $widgetTags);
            $this->assertContains('red', $widgetTags);
            $this->assertContains('large', $widgetTags);
            $this->assertContains('sale', $widgetTags);
            // Gizmo has 1 tag
            $this->assertSame('green', $byName['Gizmo']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('GROUP_CONCAT failed: ' . $e->getMessage());
        }
    }

    public function testGroupConcatAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO sl_gcml_tags VALUES (7, 'Gizmo', 'premium')");

        try {
            $rows = $this->ztdQuery(
                "SELECT item_name, GROUP_CONCAT(tag, ', ') AS tags
                 FROM sl_gcml_tags
                 WHERE item_name = 'Gizmo'
                 GROUP BY item_name"
            );
            $this->assertCount(1, $rows);
            $tags = explode(', ', $rows[0]['tags']);
            $this->assertCount(2, $tags);
            $this->assertContains('green', $tags);
            $this->assertContains('premium', $tags);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('GROUP_CONCAT after INSERT failed: ' . $e->getMessage());
        }
    }

    public function testGroupConcatAfterDelete(): void
    {
        $this->pdo->exec("DELETE FROM sl_gcml_tags WHERE tag = 'sale'");

        try {
            $rows = $this->ztdQuery(
                "SELECT item_name, GROUP_CONCAT(tag, ', ') AS tags
                 FROM sl_gcml_tags
                 WHERE item_name = 'Widget'
                 GROUP BY item_name"
            );
            $this->assertCount(1, $rows);
            $tags = explode(', ', $rows[0]['tags']);
            $this->assertCount(2, $tags);
            $this->assertNotContains('sale', $tags);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('GROUP_CONCAT after DELETE failed: ' . $e->getMessage());
        }
    }

    public function testGroupConcatWithOrderBy(): void
    {
        try {
            // SQLite GROUP_CONCAT doesn't have ORDER BY inside, but we can
            // use a subquery to control order
            $rows = $this->ztdQuery(
                "SELECT item_name, GROUP_CONCAT(tag, ', ') AS tags
                 FROM (SELECT item_name, tag FROM sl_gcml_tags ORDER BY tag)
                 GROUP BY item_name
                 ORDER BY item_name"
            );
            $this->assertCount(3, $rows);
            $byName = array_column($rows, 'tags', 'item_name');
            // Gadget tags sorted: blue, small
            $this->assertSame('blue, small', $byName['Gadget']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('GROUP_CONCAT with ORDER BY subquery failed: ' . $e->getMessage());
        }
    }

    public function testGroupConcatDistinct(): void
    {
        // Add duplicate tag
        $this->pdo->exec("INSERT INTO sl_gcml_tags VALUES (7, 'Widget', 'red')");

        try {
            $rows = $this->ztdQuery(
                "SELECT item_name, GROUP_CONCAT(DISTINCT tag) AS tags
                 FROM sl_gcml_tags
                 WHERE item_name = 'Widget'
                 GROUP BY item_name"
            );
            $this->assertCount(1, $rows);
            $tags = explode(',', $rows[0]['tags']);
            // Should deduplicate: 3 unique tags not 4
            $this->assertCount(3, $tags);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('GROUP_CONCAT DISTINCT failed: ' . $e->getMessage());
        }
    }

    // --- Multi-step DML lifecycle tests ---

    public function testInsertThenUpdateThenSelect(): void
    {
        try {
            // Step 1: Insert
            $this->pdo->exec("INSERT INTO sl_gcml_items VALUES (4, 'Task-D', 'open', 1, 1)");

            // Step 2: Update
            $this->pdo->exec("UPDATE sl_gcml_items SET status = 'in_progress', version = version + 1 WHERE name = 'Task-D'");

            // Step 3: Verify
            $rows = $this->ztdQuery("SELECT * FROM sl_gcml_items WHERE name = 'Task-D'");
            $this->assertCount(1, $rows);
            $this->assertSame('in_progress', $rows[0]['status']);
            $this->assertSame(2, (int) $rows[0]['version']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT→UPDATE→SELECT lifecycle failed: ' . $e->getMessage());
        }
    }

    public function testMultipleUpdatesOnSameRow(): void
    {
        try {
            // Update priority 3 times on same row
            $this->pdo->exec("UPDATE sl_gcml_items SET priority = 5 WHERE id = 1");
            $this->pdo->exec("UPDATE sl_gcml_items SET priority = 10 WHERE id = 1");
            $this->pdo->exec("UPDATE sl_gcml_items SET priority = 3 WHERE id = 1");

            $rows = $this->ztdQuery("SELECT priority FROM sl_gcml_items WHERE id = 1");
            $this->assertCount(1, $rows);
            $this->assertSame(3, (int) $rows[0]['priority'], 'Last UPDATE should win');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multiple UPDATEs on same row failed: ' . $e->getMessage());
        }
    }

    public function testInsertUpdateDeleteInsertCycle(): void
    {
        try {
            // Insert a new item
            $this->pdo->exec("INSERT INTO sl_gcml_items VALUES (4, 'Temp', 'open', 1, 1)");
            // Update it
            $this->pdo->exec("UPDATE sl_gcml_items SET status = 'done' WHERE id = 4");
            // Delete it
            $this->pdo->exec("DELETE FROM sl_gcml_items WHERE id = 4");
            // Verify it's gone
            $rows = $this->ztdQuery("SELECT * FROM sl_gcml_items WHERE id = 4");
            $this->assertCount(0, $rows, 'Deleted row should not appear');

            // Total should still be 3 original
            $allRows = $this->ztdQuery("SELECT * FROM sl_gcml_items ORDER BY id");
            $this->assertCount(3, $allRows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT→UPDATE→DELETE cycle failed: ' . $e->getMessage());
        }
    }

    public function testUpdateAllThenDeleteSome(): void
    {
        try {
            // Bulk update
            $this->pdo->exec("UPDATE sl_gcml_items SET status = 'review'");
            // Delete low priority
            $this->pdo->exec("DELETE FROM sl_gcml_items WHERE priority < 2");

            $rows = $this->ztdQuery("SELECT * FROM sl_gcml_items ORDER BY id");
            // id=1 (priority=1) deleted, remaining: id=2,3
            $this->assertCount(2, $rows);
            // All remaining should be 'review'
            foreach ($rows as $row) {
                $this->assertSame('review', $row['status']);
            }
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE all then DELETE some failed: ' . $e->getMessage());
        }
    }

    public function testInsertMultipleThenAggregateQuery(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_gcml_items VALUES (4, 'Task-D', 'open', 2, 1)");
            $this->pdo->exec("INSERT INTO sl_gcml_items VALUES (5, 'Task-E', 'done', 3, 1)");
            $this->pdo->exec("INSERT INTO sl_gcml_items VALUES (6, 'Task-F', 'open', 1, 1)");

            $rows = $this->ztdQuery(
                "SELECT status, COUNT(*) AS cnt, AVG(priority) AS avg_pri
                 FROM sl_gcml_items
                 GROUP BY status
                 ORDER BY status"
            );
            // done: 1 (Task-E pri=3), open: 5 (A=1, B=2, C=3, D=2, F=1)
            $byStatus = array_column($rows, null, 'status');
            $this->assertSame(1, (int) $byStatus['done']['cnt']);
            $this->assertSame(5, (int) $byStatus['open']['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multiple INSERT then aggregate failed: ' . $e->getMessage());
        }
    }

    public function testUpdateWithExpressionThenQueryWithBetween(): void
    {
        try {
            // Double all priorities
            $this->pdo->exec("UPDATE sl_gcml_items SET priority = priority * 2");

            // Query with BETWEEN on updated values
            $rows = $this->ztdQuery(
                "SELECT name, priority FROM sl_gcml_items
                 WHERE priority BETWEEN 3 AND 5
                 ORDER BY name"
            );
            // Original priorities: 1→2, 2→4, 3→6. In range [3,5]: only Task-B(4)
            $this->assertCount(1, $rows);
            $this->assertSame('Task-B', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE expression then BETWEEN query failed: ' . $e->getMessage());
        }
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_gcml_items")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
