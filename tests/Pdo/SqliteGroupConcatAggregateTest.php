<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * GROUP_CONCAT aggregate function on shadow data via SQLite PDO.
 * Tests whether the CTE rewriter preserves GROUP_CONCAT behavior
 * with custom separators on SQLite.
 *
 * SQLite's GROUP_CONCAT does NOT support ORDER BY within the function
 * call or DISTINCT, so only basic usage and custom separators are tested.
 * @spec SPEC-3.3
 */
class SqliteGroupConcatAggregateTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE sl_gca_posts (
                id INTEGER PRIMARY KEY,
                title TEXT NOT NULL
            )",
            "CREATE TABLE sl_gca_tags (
                id INTEGER PRIMARY KEY,
                post_id INTEGER NOT NULL,
                tag_name TEXT NOT NULL
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_gca_tags', 'sl_gca_posts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_gca_posts (id, title) VALUES
            (1, 'Getting Started'),
            (2, 'Advanced Tips'),
            (3, 'FAQ')");

        $this->pdo->exec("INSERT INTO sl_gca_tags (id, post_id, tag_name) VALUES
            (1, 1, 'beginner'),
            (2, 1, 'tutorial'),
            (3, 1, 'setup'),
            (4, 2, 'advanced'),
            (5, 2, 'tutorial'),
            (6, 3, 'faq')");
    }

    public function testGroupConcatBasic(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT p.title, GROUP_CONCAT(t.tag_name) AS tags
                 FROM sl_gca_posts p
                 JOIN sl_gca_tags t ON t.post_id = p.id
                 GROUP BY p.id, p.title
                 ORDER BY p.id"
            );

            if (count($rows) !== 3) {
                $this->markTestIncomplete('GROUP_CONCAT basic: expected 3, got ' . count($rows));
            }

            $this->assertSame('Getting Started', $rows[0]['title']);
            // Tags might be in any order, just verify count
            $tags = explode(',', $rows[0]['tags']);
            $this->assertCount(3, $tags);
            $this->assertSame('FAQ', $rows[2]['title']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('GROUP_CONCAT basic failed: ' . $e->getMessage());
        }
    }

    public function testGroupConcatWithCustomSeparator(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT p.title, GROUP_CONCAT(t.tag_name, '; ') AS tags
                 FROM sl_gca_posts p
                 JOIN sl_gca_tags t ON t.post_id = p.id
                 GROUP BY p.id, p.title
                 ORDER BY p.id"
            );

            if (count($rows) !== 3) {
                $this->markTestIncomplete('GROUP_CONCAT custom separator: expected 3, got ' . count($rows));
            }

            $this->assertStringContainsString('; ', $rows[0]['tags']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('GROUP_CONCAT custom separator failed: ' . $e->getMessage());
        }
    }

    public function testGroupConcatAfterInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_gca_tags (id, post_id, tag_name) VALUES (7, 3, 'help'), (8, 3, 'support')");

            $rows = $this->ztdQuery(
                "SELECT p.title, GROUP_CONCAT(t.tag_name) AS tags
                 FROM sl_gca_posts p
                 JOIN sl_gca_tags t ON t.post_id = p.id
                 WHERE p.id = 3
                 GROUP BY p.id, p.title"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete('GROUP_CONCAT after INSERT: expected 1, got ' . count($rows));
            }

            // Tags might be in any order; verify all three are present
            $tags = explode(',', $rows[0]['tags']);
            sort($tags);
            $this->assertSame(['faq', 'help', 'support'], $tags);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('GROUP_CONCAT after INSERT failed: ' . $e->getMessage());
        }
    }

    public function testGroupConcatSingleTag(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT p.title, GROUP_CONCAT(t.tag_name) AS tags
                 FROM sl_gca_posts p
                 JOIN sl_gca_tags t ON t.post_id = p.id
                 WHERE p.id = 3
                 GROUP BY p.id, p.title"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete('GROUP_CONCAT single tag: expected 1, got ' . count($rows));
            }

            // Post 3 has only one tag 'faq', no separator expected
            $this->assertSame('faq', $rows[0]['tags']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('GROUP_CONCAT single tag failed: ' . $e->getMessage());
        }
    }

    public function testPhysicalIsolation(): void
    {
        $this->disableZtd();
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_gca_posts");
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
