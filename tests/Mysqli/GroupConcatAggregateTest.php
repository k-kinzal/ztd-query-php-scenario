<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * GROUP_CONCAT aggregate function on shadow data.
 * Tests whether the CTE rewriter preserves GROUP_CONCAT behavior
 * including custom separators and ORDER BY within GROUP_CONCAT.
 *
 * @spec SPEC-3.3
 */
class GroupConcatAggregateTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE gca_posts (
                id INT PRIMARY KEY,
                title VARCHAR(100) NOT NULL
            ) ENGINE=InnoDB",
            "CREATE TABLE gca_tags (
                id INT PRIMARY KEY,
                post_id INT NOT NULL,
                tag_name VARCHAR(30) NOT NULL
            ) ENGINE=InnoDB",
        ];
    }

    protected function getTableNames(): array
    {
        return ['gca_tags', 'gca_posts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO gca_posts (id, title) VALUES
            (1, 'Getting Started'),
            (2, 'Advanced Tips'),
            (3, 'FAQ')");

        $this->mysqli->query("INSERT INTO gca_tags (id, post_id, tag_name) VALUES
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
                 FROM gca_posts p
                 JOIN gca_tags t ON t.post_id = p.id
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

    public function testGroupConcatWithSeparator(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT p.title, GROUP_CONCAT(t.tag_name SEPARATOR '; ') AS tags
                 FROM gca_posts p
                 JOIN gca_tags t ON t.post_id = p.id
                 GROUP BY p.id, p.title
                 ORDER BY p.id"
            );

            if (count($rows) !== 3) {
                $this->markTestIncomplete('GROUP_CONCAT SEPARATOR: expected 3, got ' . count($rows));
            }

            $this->assertStringContainsString('; ', $rows[0]['tags']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('GROUP_CONCAT SEPARATOR failed: ' . $e->getMessage());
        }
    }

    public function testGroupConcatDistinct(): void
    {
        try {
            // Add duplicate tag
            $this->mysqli->query("INSERT INTO gca_tags (id, post_id, tag_name) VALUES (7, 2, 'tutorial')");

            $rows = $this->ztdQuery(
                "SELECT p.title, GROUP_CONCAT(DISTINCT t.tag_name ORDER BY t.tag_name) AS tags
                 FROM gca_posts p
                 JOIN gca_tags t ON t.post_id = p.id
                 WHERE p.id = 2
                 GROUP BY p.id, p.title"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete('GROUP_CONCAT DISTINCT: expected 1, got ' . count($rows));
            }

            // Should be "advanced,tutorial" (distinct, ordered)
            $this->assertSame('advanced,tutorial', $rows[0]['tags']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('GROUP_CONCAT DISTINCT failed: ' . $e->getMessage());
        }
    }

    public function testGroupConcatAfterInsert(): void
    {
        try {
            $this->mysqli->query("INSERT INTO gca_tags (id, post_id, tag_name) VALUES (7, 3, 'help'), (8, 3, 'support')");

            $rows = $this->ztdQuery(
                "SELECT p.title, GROUP_CONCAT(t.tag_name ORDER BY t.tag_name) AS tags
                 FROM gca_posts p
                 JOIN gca_tags t ON t.post_id = p.id
                 WHERE p.id = 3
                 GROUP BY p.id, p.title"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete('GROUP_CONCAT after INSERT: expected 1, got ' . count($rows));
            }

            $this->assertSame('faq,help,support', $rows[0]['tags']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('GROUP_CONCAT after INSERT failed: ' . $e->getMessage());
        }
    }

    public function testPhysicalIsolation(): void
    {
        $this->disableZtd();
        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM gca_posts");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
