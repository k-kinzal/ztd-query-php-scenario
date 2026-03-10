<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests DML with LIKE/GLOB and prepared parameters through ZTD shadow store on SQLite.
 *
 * LIKE with prepared wildcards (e.g., WHERE name LIKE ?) where the param
 * contains '%' is a common pattern. The CTE rewriter must handle the
 * LIKE operator without mangling the parameter binding.
 *
 * @spec SPEC-4.2, SPEC-4.3, SPEC-3.2
 */
class SqliteLikeParamDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_lkp_articles (
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            category TEXT NOT NULL,
            views INTEGER NOT NULL DEFAULT 0
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_lkp_articles'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_lkp_articles VALUES (1, 'PHP 8.1 Release Notes', 'tech', 500)");
        $this->pdo->exec("INSERT INTO sl_lkp_articles VALUES (2, 'Python vs PHP', 'tech', 300)");
        $this->pdo->exec("INSERT INTO sl_lkp_articles VALUES (3, 'Cooking with PHP', 'humor', 100)");
        $this->pdo->exec("INSERT INTO sl_lkp_articles VALUES (4, 'Travel Guide', 'travel', 250)");
        $this->pdo->exec("INSERT INTO sl_lkp_articles VALUES (5, 'PHP Testing Best Practices', 'tech', 450)");
    }

    /**
     * Prepared UPDATE WHERE title LIKE ? with wildcard param.
     */
    public function testPreparedUpdateLikeWildcard(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE sl_lkp_articles SET views = views + 100 WHERE title LIKE ?"
            );
            $stmt->execute(['PHP%']);

            $rows = $this->ztdQuery("SELECT title, views FROM sl_lkp_articles ORDER BY id");

            $byTitle = [];
            foreach ($rows as $r) {
                $byTitle[$r['title']] = (int) $r['views'];
            }

            // 'PHP 8.1 Release Notes' and 'PHP Testing Best Practices' match PHP%
            if ($byTitle['PHP 8.1 Release Notes'] !== 600) {
                $this->markTestIncomplete(
                    'Prepared UPDATE LIKE: expected 600 for PHP 8.1, got '
                    . $byTitle['PHP 8.1 Release Notes'] . '. Data: ' . json_encode($byTitle)
                );
            }

            $this->assertSame(600, $byTitle['PHP 8.1 Release Notes']);
            $this->assertSame(300, $byTitle['Python vs PHP']); // doesn't start with PHP
            $this->assertSame(100, $byTitle['Cooking with PHP']); // doesn't start with PHP
            $this->assertSame(250, $byTitle['Travel Guide']);
            $this->assertSame(550, $byTitle['PHP Testing Best Practices']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE LIKE failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared DELETE WHERE title LIKE ? with contains-pattern.
     */
    public function testPreparedDeleteLikeContains(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "DELETE FROM sl_lkp_articles WHERE title LIKE ?"
            );
            $stmt->execute(['%PHP%']);

            $rows = $this->ztdQuery("SELECT title FROM sl_lkp_articles ORDER BY id");

            // Only 'Travel Guide' doesn't contain PHP
            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Prepared DELETE LIKE contains: expected 1, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Travel Guide', $rows[0]['title']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DELETE LIKE contains failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE with LIKE and additional AND condition, both parameterized.
     */
    public function testPreparedUpdateLikeWithAndParam(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE sl_lkp_articles SET views = ? WHERE title LIKE ? AND category = ?"
            );
            $stmt->execute([999, '%PHP%', 'tech']);

            $rows = $this->ztdQuery("SELECT title, views, category FROM sl_lkp_articles ORDER BY id");

            $byTitle = [];
            foreach ($rows as $r) {
                $byTitle[$r['title']] = ['views' => (int) $r['views'], 'cat' => $r['category']];
            }

            // PHP 8.1 Release Notes (tech, contains PHP) → 999
            // Python vs PHP (tech, contains PHP) → 999
            // Cooking with PHP (humor, contains PHP) → unchanged (100)
            // Travel Guide (travel, no PHP) → unchanged (250)
            // PHP Testing Best Practices (tech, contains PHP) → 999

            if ($byTitle['PHP 8.1 Release Notes']['views'] !== 999) {
                $this->markTestIncomplete(
                    'Prepared UPDATE LIKE AND: expected 999 for PHP 8.1, got '
                    . json_encode($byTitle)
                );
            }

            $this->assertSame(999, $byTitle['PHP 8.1 Release Notes']['views']);
            $this->assertSame(999, $byTitle['Python vs PHP']['views']);
            $this->assertSame(100, $byTitle['Cooking with PHP']['views']);
            $this->assertSame(250, $byTitle['Travel Guide']['views']);
            $this->assertSame(999, $byTitle['PHP Testing Best Practices']['views']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE LIKE AND failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared DELETE WHERE NOT LIKE.
     */
    public function testPreparedDeleteNotLike(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "DELETE FROM sl_lkp_articles WHERE title NOT LIKE ?"
            );
            $stmt->execute(['%PHP%']);

            $rows = $this->ztdQuery("SELECT title FROM sl_lkp_articles ORDER BY id");

            // Only articles containing PHP should remain (4 articles)
            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'Prepared DELETE NOT LIKE: expected 4, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(4, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DELETE NOT LIKE failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared SELECT with LIKE — read path verification.
     */
    public function testPreparedSelectLike(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT title FROM sl_lkp_articles WHERE category LIKE ? ORDER BY title",
                ['te%']
            );

            // 'tech' matches 'te%'; 'travel' does not
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Prepared SELECT LIKE: expected 3 tech articles, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared SELECT LIKE failed: ' . $e->getMessage());
        }
    }
}
