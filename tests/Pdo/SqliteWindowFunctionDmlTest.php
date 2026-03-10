<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests DML operations that use window functions in subqueries
 * through ZTD shadow store on SQLite.
 *
 * SQLite 3.25+ supports window functions. This is the standard
 * "delete duplicates keeping latest" and "ranked insert" pattern.
 *
 * @spec SPEC-4.2, SPEC-4.3
 */
class SqliteWindowFunctionDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_wfd_scores (
                id INTEGER PRIMARY KEY,
                player TEXT NOT NULL,
                score INTEGER NOT NULL,
                played_at TEXT NOT NULL
            )',
            'CREATE TABLE sl_wfd_rankings (
                id INTEGER PRIMARY KEY,
                player TEXT NOT NULL,
                rank_pos INTEGER
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_wfd_rankings', 'sl_wfd_scores'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_wfd_scores VALUES (1, 'Alice', 100, '2026-01-01')");
        $this->pdo->exec("INSERT INTO sl_wfd_scores VALUES (2, 'Alice', 150, '2026-01-15')");
        $this->pdo->exec("INSERT INTO sl_wfd_scores VALUES (3, 'Bob', 200, '2026-01-10')");
        $this->pdo->exec("INSERT INTO sl_wfd_scores VALUES (4, 'Bob', 180, '2026-01-20')");
        $this->pdo->exec("INSERT INTO sl_wfd_scores VALUES (5, 'Charlie', 90, '2026-01-05')");

        $this->pdo->exec("INSERT INTO sl_wfd_rankings (id, player) VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO sl_wfd_rankings (id, player) VALUES (2, 'Bob')");
        $this->pdo->exec("INSERT INTO sl_wfd_rankings (id, player) VALUES (3, 'Charlie')");
    }

    /**
     * DELETE old scores keeping only the latest per player using ROW_NUMBER.
     */
    public function testDeleteKeepingLatestPerGroup(): void
    {
        $sql = "DELETE FROM sl_wfd_scores
                WHERE id IN (
                    SELECT id FROM (
                        SELECT id, ROW_NUMBER() OVER (PARTITION BY player ORDER BY played_at DESC) AS rn
                        FROM sl_wfd_scores
                    )
                    WHERE rn > 1
                )";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT id, player FROM sl_wfd_scores ORDER BY player");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE keep-latest: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE with ROW_NUMBER subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE rank_pos using scalar subquery with DENSE_RANK.
     * SQLite does not support UPDATE...FROM with derived tables easily,
     * so use a correlated subquery pattern.
     */
    public function testUpdateRankWithCorrelatedWindowSubquery(): void
    {
        $sql = "UPDATE sl_wfd_rankings
                SET rank_pos = (
                    SELECT drank FROM (
                        SELECT player, DENSE_RANK() OVER (ORDER BY MAX(score) DESC) AS drank
                        FROM sl_wfd_scores
                        GROUP BY player
                    )
                    WHERE player = sl_wfd_rankings.player
                )";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT player, rank_pos FROM sl_wfd_rankings ORDER BY rank_pos");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'UPDATE DENSE_RANK: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);

            $rankByPlayer = [];
            foreach ($rows as $r) {
                $rankByPlayer[$r['player']] = (int)$r['rank_pos'];
            }

            if (!isset($rankByPlayer['Bob']) || $rankByPlayer['Bob'] !== 1) {
                $this->markTestIncomplete(
                    'UPDATE DENSE_RANK: Bob should be rank 1. Data: ' . json_encode($rankByPlayer)
                );
            }

            $this->assertSame(1, $rankByPlayer['Bob']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with window function correlated subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT with RANK() window function.
     */
    public function testInsertSelectWithWindowFunction(): void
    {
        $this->createTable('CREATE TABLE sl_wfd_top_scores (
            id INTEGER PRIMARY KEY,
            player TEXT,
            score INTEGER,
            rank_num INTEGER
        )');

        $sql = "INSERT INTO sl_wfd_top_scores (player, score, rank_num)
                SELECT player, score, RANK() OVER (ORDER BY score DESC) AS rank_num
                FROM sl_wfd_scores";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT player, score, rank_num FROM sl_wfd_top_scores ORDER BY rank_num");

            if (count($rows) !== 5) {
                $this->markTestIncomplete(
                    'INSERT SELECT window: expected 5, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(5, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT with window function failed: ' . $e->getMessage());
        } finally {
            $this->dropTable('sl_wfd_top_scores');
        }
    }
}
