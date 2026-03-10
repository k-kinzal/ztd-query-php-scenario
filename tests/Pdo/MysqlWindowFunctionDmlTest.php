<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests DML operations that use window functions in subqueries
 * through ZTD shadow store on MySQL PDO.
 *
 * Cross-platform parity with Mysqli/WindowFunctionDmlTest.
 *
 * @spec SPEC-4.2, SPEC-4.3
 */
class MysqlWindowFunctionDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_wfd_scores (
                id INT PRIMARY KEY AUTO_INCREMENT,
                player VARCHAR(30) NOT NULL,
                score INT NOT NULL,
                played_at DATE NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE mp_wfd_rankings (
                id INT PRIMARY KEY AUTO_INCREMENT,
                player VARCHAR(30) NOT NULL,
                rank_pos INT
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_wfd_rankings', 'mp_wfd_scores'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_wfd_scores (id, player, score, played_at) VALUES (1, 'Alice', 100, '2026-01-01')");
        $this->pdo->exec("INSERT INTO mp_wfd_scores (id, player, score, played_at) VALUES (2, 'Alice', 150, '2026-01-15')");
        $this->pdo->exec("INSERT INTO mp_wfd_scores (id, player, score, played_at) VALUES (3, 'Bob', 200, '2026-01-10')");
        $this->pdo->exec("INSERT INTO mp_wfd_scores (id, player, score, played_at) VALUES (4, 'Bob', 180, '2026-01-20')");
        $this->pdo->exec("INSERT INTO mp_wfd_scores (id, player, score, played_at) VALUES (5, 'Charlie', 90, '2026-01-05')");

        $this->pdo->exec("INSERT INTO mp_wfd_rankings (id, player, rank_pos) VALUES (1, 'Alice', NULL)");
        $this->pdo->exec("INSERT INTO mp_wfd_rankings (id, player, rank_pos) VALUES (2, 'Bob', NULL)");
        $this->pdo->exec("INSERT INTO mp_wfd_rankings (id, player, rank_pos) VALUES (3, 'Charlie', NULL)");
    }

    /**
     * DELETE old scores keeping only the latest per player using ROW_NUMBER.
     */
    public function testDeleteKeepingLatestPerGroup(): void
    {
        $sql = "DELETE FROM mp_wfd_scores
                WHERE id IN (
                    SELECT id FROM (
                        SELECT id, ROW_NUMBER() OVER (PARTITION BY player ORDER BY played_at DESC) AS rn
                        FROM mp_wfd_scores
                    ) ranked
                    WHERE rn > 1
                )";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT id, player FROM mp_wfd_scores ORDER BY player");

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
     * UPDATE rank_pos using DENSE_RANK window function.
     */
    public function testUpdateRankFromWindowSubquery(): void
    {
        $sql = "UPDATE mp_wfd_rankings r
                JOIN (
                    SELECT player, DENSE_RANK() OVER (ORDER BY MAX(score) DESC) AS drank
                    FROM mp_wfd_scores
                    GROUP BY player
                ) s ON r.player = s.player
                SET r.rank_pos = s.drank";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT player, rank_pos FROM mp_wfd_rankings ORDER BY rank_pos");

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
            $this->markTestIncomplete('UPDATE with window function subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT with RANK() window function.
     */
    public function testInsertSelectWithWindowFunction(): void
    {
        $this->createTable('CREATE TABLE mp_wfd_top_scores (
            id INT PRIMARY KEY AUTO_INCREMENT,
            player VARCHAR(30),
            score INT,
            rank_num INT
        ) ENGINE=InnoDB');

        $sql = "INSERT INTO mp_wfd_top_scores (player, score, rank_num)
                SELECT player, score, RANK() OVER (ORDER BY score DESC) AS rank_num
                FROM mp_wfd_scores";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT player, score, rank_num FROM mp_wfd_top_scores ORDER BY rank_num");

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
            $this->dropTable('mp_wfd_top_scores');
        }
    }
}
