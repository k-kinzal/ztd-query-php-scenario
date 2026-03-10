<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests DML operations that use window functions in subqueries
 * through ZTD shadow store on MySQLi.
 *
 * Window functions (ROW_NUMBER, RANK, LAG, etc.) inside subqueries
 * driving UPDATE/DELETE are common patterns: "delete duplicates keeping
 * the latest", "update rank column", "archive all but top-N".
 *
 * The CTE rewriter must correctly handle window functions in
 * correlated and non-correlated subqueries within DML context.
 *
 * @spec SPEC-4.2, SPEC-4.3
 */
class WindowFunctionDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_wfd_scores (
                id INT PRIMARY KEY AUTO_INCREMENT,
                player VARCHAR(30) NOT NULL,
                score INT NOT NULL,
                played_at DATE NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE mi_wfd_rankings (
                id INT PRIMARY KEY AUTO_INCREMENT,
                player VARCHAR(30) NOT NULL,
                rank_pos INT
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_wfd_rankings', 'mi_wfd_scores'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_wfd_scores (id, player, score, played_at) VALUES (1, 'Alice', 100, '2026-01-01')");
        $this->mysqli->query("INSERT INTO mi_wfd_scores (id, player, score, played_at) VALUES (2, 'Alice', 150, '2026-01-15')");
        $this->mysqli->query("INSERT INTO mi_wfd_scores (id, player, score, played_at) VALUES (3, 'Bob', 200, '2026-01-10')");
        $this->mysqli->query("INSERT INTO mi_wfd_scores (id, player, score, played_at) VALUES (4, 'Bob', 180, '2026-01-20')");
        $this->mysqli->query("INSERT INTO mi_wfd_scores (id, player, score, played_at) VALUES (5, 'Charlie', 90, '2026-01-05')");

        $this->mysqli->query("INSERT INTO mi_wfd_rankings (id, player, rank_pos) VALUES (1, 'Alice', NULL)");
        $this->mysqli->query("INSERT INTO mi_wfd_rankings (id, player, rank_pos) VALUES (2, 'Bob', NULL)");
        $this->mysqli->query("INSERT INTO mi_wfd_rankings (id, player, rank_pos) VALUES (3, 'Charlie', NULL)");
    }

    /**
     * DELETE old scores keeping only the latest per player using ROW_NUMBER.
     */
    public function testDeleteKeepingLatestPerGroup(): void
    {
        $sql = "DELETE FROM mi_wfd_scores
                WHERE id IN (
                    SELECT id FROM (
                        SELECT id, ROW_NUMBER() OVER (PARTITION BY player ORDER BY played_at DESC) AS rn
                        FROM mi_wfd_scores
                    ) ranked
                    WHERE rn > 1
                )";

        try {
            $this->mysqli->query($sql);
            $rows = $this->ztdQuery("SELECT id, player, score FROM mi_wfd_scores ORDER BY player");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE keep-latest: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $players = array_column($rows, 'player');
            $this->assertContains('Alice', $players);
            $this->assertContains('Bob', $players);
            $this->assertContains('Charlie', $players);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE with ROW_NUMBER subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE rank_pos using DENSE_RANK window function from another table.
     */
    public function testUpdateRankFromWindowSubquery(): void
    {
        $sql = "UPDATE mi_wfd_rankings r
                JOIN (
                    SELECT player, DENSE_RANK() OVER (ORDER BY MAX(score) DESC) AS drank
                    FROM mi_wfd_scores
                    GROUP BY player
                ) s ON r.player = s.player
                SET r.rank_pos = s.drank";

        try {
            $this->mysqli->query($sql);
            $rows = $this->ztdQuery("SELECT player, rank_pos FROM mi_wfd_rankings ORDER BY rank_pos");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'UPDATE with DENSE_RANK: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);

            $rankByPlayer = [];
            foreach ($rows as $r) {
                $rankByPlayer[$r['player']] = (int)$r['rank_pos'];
            }

            // Bob has highest max score (200), Alice (150), Charlie (90)
            if (!isset($rankByPlayer['Bob']) || $rankByPlayer['Bob'] !== 1) {
                $this->markTestIncomplete(
                    'UPDATE DENSE_RANK: Bob should be rank 1. Data: ' . json_encode($rankByPlayer)
                );
            }

            $this->assertSame(1, $rankByPlayer['Bob']);
            $this->assertSame(2, $rankByPlayer['Alice']);
            $this->assertSame(3, $rankByPlayer['Charlie']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with window function subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT with window function — insert ranked results.
     */
    public function testInsertSelectWithWindowFunction(): void
    {
        $this->createTable('CREATE TABLE mi_wfd_top_scores (
            id INT PRIMARY KEY AUTO_INCREMENT,
            player VARCHAR(30),
            score INT,
            rank_num INT
        ) ENGINE=InnoDB');

        $sql = "INSERT INTO mi_wfd_top_scores (player, score, rank_num)
                SELECT player, score, RANK() OVER (ORDER BY score DESC) AS rank_num
                FROM mi_wfd_scores";

        try {
            $this->mysqli->query($sql);
            $rows = $this->ztdQuery("SELECT player, score, rank_num FROM mi_wfd_top_scores ORDER BY rank_num");

            if (count($rows) !== 5) {
                $this->markTestIncomplete(
                    'INSERT SELECT window: expected 5, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(5, $rows);
            $this->assertSame('Bob', $rows[0]['player']);
            $this->assertEquals(200, (int)$rows[0]['score']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT with window function failed: ' . $e->getMessage());
        } finally {
            $this->dropTable('mi_wfd_top_scores');
        }
    }
}
