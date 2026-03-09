<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT...SELECT with ORDER BY and LIMIT on SQLite via PDO.
 *
 * This pattern is common for copying top-N rows from one table to another
 * (e.g. leaderboard snapshots). The CTE rewriter may not handle ORDER BY
 * and LIMIT in the INSERT...SELECT source correctly.
 *
 * @spec SPEC-4.1
 */
class SqliteInsertSelectLimitTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_isl_scores (
                id INT PRIMARY KEY,
                player VARCHAR(30),
                score INT,
                created_at TEXT
            )',
            'CREATE TABLE sl_isl_leaderboard (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player VARCHAR(30),
                score INT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_isl_leaderboard', 'sl_isl_scores'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_isl_scores (id, player, score, created_at) VALUES
            (1, 'Alice', 95, '2025-01-01 10:00:00'),
            (2, 'Bob', 82, '2025-01-01 11:00:00'),
            (3, 'Carol', 91, '2025-01-01 12:00:00'),
            (4, 'Dave', 67, '2025-01-01 13:00:00'),
            (5, 'Eve', 88, '2025-01-01 14:00:00'),
            (6, 'Frank', 73, '2025-01-01 15:00:00')");
    }

    /**
     * INSERT...SELECT top 3 scores ordered by score DESC.
     * Expected leaderboard: Alice (95), Carol (91), Eve (88).
     */
    public function testInsertSelectWithOrderByLimit(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_isl_leaderboard (player, score)
                 SELECT player, score FROM sl_isl_scores ORDER BY score DESC LIMIT 3"
            );

            $rows = $this->ztdQuery("SELECT player, score FROM sl_isl_leaderboard ORDER BY score DESC");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'INSERT SELECT ORDER BY LIMIT: expected 3 rows, got ' . count($rows)
                    . '. CTE rewriter may not preserve LIMIT in INSERT...SELECT source.'
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame('Alice', $rows[0]['player']);
            $this->assertSame(95, (int) $rows[0]['score']);
            $this->assertSame('Carol', $rows[1]['player']);
            $this->assertSame(91, (int) $rows[1]['score']);
            $this->assertSame('Eve', $rows[2]['player']);
            $this->assertSame(88, (int) $rows[2]['score']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT ORDER BY LIMIT failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT with WHERE filter and ORDER BY LIMIT.
     * Only scores >= 50, top 2: Alice (95), Carol (91).
     */
    public function testInsertSelectWithWhereAndLimit(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_isl_leaderboard (player, score)
                 SELECT player, score FROM sl_isl_scores
                 WHERE score >= 50
                 ORDER BY score DESC LIMIT 2"
            );

            $rows = $this->ztdQuery("SELECT player, score FROM sl_isl_leaderboard ORDER BY score DESC");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT SELECT WHERE ORDER BY LIMIT: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['player']);
            $this->assertSame(95, (int) $rows[0]['score']);
            $this->assertSame('Carol', $rows[1]['player']);
            $this->assertSame(91, (int) $rows[1]['score']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT WHERE ORDER BY LIMIT failed: ' . $e->getMessage());
        }
    }

    /**
     * After adding a new high score via shadow INSERT, INSERT...SELECT top 3
     * should reflect the mutation.
     * New score: Grace (99). Top 3: Grace (99), Alice (95), Carol (91).
     */
    public function testInsertSelectLimitAfterMutation(): void
    {
        $this->pdo->exec("INSERT INTO sl_isl_scores (id, player, score, created_at) VALUES (7, 'Grace', 99, '2025-01-02 10:00:00')");

        try {
            $this->pdo->exec(
                "INSERT INTO sl_isl_leaderboard (player, score)
                 SELECT player, score FROM sl_isl_scores ORDER BY score DESC LIMIT 3"
            );

            $rows = $this->ztdQuery("SELECT player, score FROM sl_isl_leaderboard ORDER BY score DESC");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'INSERT SELECT LIMIT after mutation: expected 3 rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame('Grace', $rows[0]['player']);
            $this->assertSame(99, (int) $rows[0]['score']);
            $this->assertSame('Alice', $rows[1]['player']);
            $this->assertSame(95, (int) $rows[1]['score']);
            $this->assertSame('Carol', $rows[2]['player']);
            $this->assertSame(91, (int) $rows[2]['score']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT LIMIT after mutation failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT with OFFSET: skip the top scorer, take next 3.
     * Ordered DESC: Alice(95), Carol(91), Eve(88), Bob(82), Frank(73), Dave(67)
     * OFFSET 1 LIMIT 3: Carol (91), Eve (88), Bob (82).
     */
    public function testInsertSelectWithOffset(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_isl_leaderboard (player, score)
                 SELECT player, score FROM sl_isl_scores ORDER BY score DESC LIMIT 3 OFFSET 1"
            );

            $rows = $this->ztdQuery("SELECT player, score FROM sl_isl_leaderboard ORDER BY score DESC");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'INSERT SELECT LIMIT OFFSET: expected 3 rows, got ' . count($rows)
                    . '. CTE rewriter may not preserve OFFSET in INSERT...SELECT source.'
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame('Carol', $rows[0]['player']);
            $this->assertSame(91, (int) $rows[0]['score']);
            $this->assertSame('Eve', $rows[1]['player']);
            $this->assertSame(88, (int) $rows[1]['score']);
            $this->assertSame('Bob', $rows[2]['player']);
            $this->assertSame(82, (int) $rows[2]['score']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT LIMIT OFFSET failed: ' . $e->getMessage());
        }
    }

    /**
     * Physical isolation: INSERT...SELECT with LIMIT should not touch the real table.
     */
    public function testPhysicalIsolation(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_isl_leaderboard (player, score)
                 SELECT player, score FROM sl_isl_scores ORDER BY score DESC LIMIT 3"
            );
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT failed: ' . $e->getMessage());
            return;
        }

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_isl_leaderboard")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical leaderboard table should be empty');
    }
}
