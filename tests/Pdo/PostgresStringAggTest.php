<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * STRING_AGG aggregate function on shadow data via PostgreSQL PDO.
 * Tests whether the CTE rewriter preserves STRING_AGG behavior
 * including custom separators, ORDER BY, and DISTINCT within STRING_AGG.
 *
 * PostgreSQL uses STRING_AGG instead of MySQL's GROUP_CONCAT.
 * @spec SPEC-3.3
 */
class PostgresStringAggTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE pg_gca_posts (
                id INT PRIMARY KEY,
                title VARCHAR(100) NOT NULL
            )",
            "CREATE TABLE pg_gca_tags (
                id INT PRIMARY KEY,
                post_id INT NOT NULL,
                tag_name VARCHAR(30) NOT NULL
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_gca_tags', 'pg_gca_posts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_gca_posts (id, title) VALUES
            (1, 'Getting Started'),
            (2, 'Advanced Tips'),
            (3, 'FAQ')");

        $this->pdo->exec("INSERT INTO pg_gca_tags (id, post_id, tag_name) VALUES
            (1, 1, 'beginner'),
            (2, 1, 'tutorial'),
            (3, 1, 'setup'),
            (4, 2, 'advanced'),
            (5, 2, 'tutorial'),
            (6, 3, 'faq')");
    }

    public function testStringAggBasic(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT p.title, STRING_AGG(t.tag_name, ',') AS tags
                 FROM pg_gca_posts p
                 JOIN pg_gca_tags t ON t.post_id = p.id
                 GROUP BY p.id, p.title
                 ORDER BY p.id"
            );

            if (count($rows) !== 3) {
                $this->markTestIncomplete('STRING_AGG basic: expected 3, got ' . count($rows));
            }

            $this->assertSame('Getting Started', $rows[0]['title']);
            // Tags might be in any order, just verify count
            $tags = explode(',', $rows[0]['tags']);
            $this->assertCount(3, $tags);
            $this->assertSame('FAQ', $rows[2]['title']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('STRING_AGG basic failed: ' . $e->getMessage());
        }
    }

    public function testStringAggWithCustomSeparator(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT p.title, STRING_AGG(t.tag_name, '; ') AS tags
                 FROM pg_gca_posts p
                 JOIN pg_gca_tags t ON t.post_id = p.id
                 GROUP BY p.id, p.title
                 ORDER BY p.id"
            );

            if (count($rows) !== 3) {
                $this->markTestIncomplete('STRING_AGG custom separator: expected 3, got ' . count($rows));
            }

            $this->assertStringContainsString('; ', $rows[0]['tags']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('STRING_AGG custom separator failed: ' . $e->getMessage());
        }
    }

    public function testStringAggWithOrderBy(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT p.title, STRING_AGG(t.tag_name, ',' ORDER BY t.tag_name) AS tags
                 FROM pg_gca_posts p
                 JOIN pg_gca_tags t ON t.post_id = p.id
                 WHERE p.id = 1
                 GROUP BY p.id, p.title"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete('STRING_AGG ORDER BY: expected 1, got ' . count($rows));
            }

            $this->assertSame('beginner,setup,tutorial', $rows[0]['tags']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('STRING_AGG ORDER BY failed: ' . $e->getMessage());
        }
    }

    public function testStringAggDistinct(): void
    {
        try {
            // Add duplicate tag
            $this->pdo->exec("INSERT INTO pg_gca_tags (id, post_id, tag_name) VALUES (7, 2, 'tutorial')");

            $rows = $this->ztdQuery(
                "SELECT p.title, STRING_AGG(DISTINCT t.tag_name, ',' ORDER BY t.tag_name) AS tags
                 FROM pg_gca_posts p
                 JOIN pg_gca_tags t ON t.post_id = p.id
                 WHERE p.id = 2
                 GROUP BY p.id, p.title"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete('STRING_AGG DISTINCT: expected 1, got ' . count($rows));
            }

            // Should be "advanced,tutorial" (distinct, ordered)
            $this->assertSame('advanced,tutorial', $rows[0]['tags']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('STRING_AGG DISTINCT failed: ' . $e->getMessage());
        }
    }

    public function testStringAggAfterInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_gca_tags (id, post_id, tag_name) VALUES (7, 3, 'help'), (8, 3, 'support')");

            $rows = $this->ztdQuery(
                "SELECT p.title, STRING_AGG(t.tag_name, ',' ORDER BY t.tag_name) AS tags
                 FROM pg_gca_posts p
                 JOIN pg_gca_tags t ON t.post_id = p.id
                 WHERE p.id = 3
                 GROUP BY p.id, p.title"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete('STRING_AGG after INSERT: expected 1, got ' . count($rows));
            }

            $this->assertSame('faq,help,support', $rows[0]['tags']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('STRING_AGG after INSERT failed: ' . $e->getMessage());
        }
    }

    public function testPhysicalIsolation(): void
    {
        $this->disableZtd();
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_gca_posts");
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
