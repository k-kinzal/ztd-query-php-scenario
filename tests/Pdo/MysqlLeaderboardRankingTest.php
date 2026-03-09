<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests a gaming leaderboard with score tracking, ranking with ties,
 * and position updates through ZTD shadow store (MySQL PDO).
 * Covers DENSE_RANK() window functions, score updates, top-N queries,
 * player rank lookup, score history, tied ranking, and physical isolation.
 * @spec SPEC-10.2.65
 */
class MysqlLeaderboardRankingTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_lb_players (
                id INT PRIMARY KEY,
                username VARCHAR(255),
                score INT,
                games_played INT,
                last_played DATETIME
            )',
            'CREATE TABLE mp_lb_score_history (
                id INT PRIMARY KEY,
                player_id INT,
                points INT,
                reason VARCHAR(255),
                recorded_at DATETIME
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_lb_score_history', 'mp_lb_players'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 5 players: Alice (1500), Bob (1200), Charlie (1500), Diana (800), Eve (2000)
        $this->pdo->exec("INSERT INTO mp_lb_players VALUES (1, 'Alice', 1500, 20, '2026-03-08 14:00:00')");
        $this->pdo->exec("INSERT INTO mp_lb_players VALUES (2, 'Bob', 1200, 15, '2026-03-07 10:00:00')");
        $this->pdo->exec("INSERT INTO mp_lb_players VALUES (3, 'Charlie', 1500, 18, '2026-03-08 16:00:00')");
        $this->pdo->exec("INSERT INTO mp_lb_players VALUES (4, 'Diana', 800, 10, '2026-03-06 09:00:00')");
        $this->pdo->exec("INSERT INTO mp_lb_players VALUES (5, 'Eve', 2000, 25, '2026-03-09 08:00:00')");

        // Score history: 3-4 records per player
        $this->pdo->exec("INSERT INTO mp_lb_score_history VALUES (1, 1, 500, 'Initial placement', '2026-02-01 10:00:00')");
        $this->pdo->exec("INSERT INTO mp_lb_score_history VALUES (2, 1, 400, 'Tournament win', '2026-02-15 14:00:00')");
        $this->pdo->exec("INSERT INTO mp_lb_score_history VALUES (3, 1, 600, 'Season bonus', '2026-03-01 12:00:00')");

        $this->pdo->exec("INSERT INTO mp_lb_score_history VALUES (4, 2, 500, 'Initial placement', '2026-02-01 10:00:00')");
        $this->pdo->exec("INSERT INTO mp_lb_score_history VALUES (5, 2, 300, 'Match victory', '2026-02-10 11:00:00')");
        $this->pdo->exec("INSERT INTO mp_lb_score_history VALUES (6, 2, 400, 'Weekly challenge', '2026-02-20 16:00:00')");

        $this->pdo->exec("INSERT INTO mp_lb_score_history VALUES (7, 3, 600, 'Initial placement', '2026-02-01 10:00:00')");
        $this->pdo->exec("INSERT INTO mp_lb_score_history VALUES (8, 3, 500, 'Ranked match', '2026-02-12 13:00:00')");
        $this->pdo->exec("INSERT INTO mp_lb_score_history VALUES (9, 3, 400, 'Tournament win', '2026-03-05 15:00:00')");

        $this->pdo->exec("INSERT INTO mp_lb_score_history VALUES (10, 4, 300, 'Initial placement', '2026-02-01 10:00:00')");
        $this->pdo->exec("INSERT INTO mp_lb_score_history VALUES (11, 4, 200, 'Match victory', '2026-02-18 09:00:00')");
        $this->pdo->exec("INSERT INTO mp_lb_score_history VALUES (12, 4, 300, 'Weekly challenge', '2026-03-02 11:00:00')");

        $this->pdo->exec("INSERT INTO mp_lb_score_history VALUES (13, 5, 800, 'Initial placement', '2026-02-01 10:00:00')");
        $this->pdo->exec("INSERT INTO mp_lb_score_history VALUES (14, 5, 500, 'Tournament win', '2026-02-14 14:00:00')");
        $this->pdo->exec("INSERT INTO mp_lb_score_history VALUES (15, 5, 400, 'Ranked match', '2026-02-28 17:00:00')");
        $this->pdo->exec("INSERT INTO mp_lb_score_history VALUES (16, 5, 300, 'Season bonus', '2026-03-08 08:00:00')");
    }

    /**
     * DENSE_RANK() window function to rank players by score descending.
     * Alice and Charlie tie at 1500 and should share the same rank.
     */
    public function testLeaderboardRanking(): void
    {
        $rows = $this->ztdQuery(
            "SELECT username, score,
                    DENSE_RANK() OVER (ORDER BY score DESC) AS player_rank
             FROM mp_lb_players
             ORDER BY player_rank, username"
        );

        $this->assertCount(5, $rows);
        // Eve: 2000 -> rank 1
        $this->assertSame('Eve', $rows[0]['username']);
        $this->assertEquals(1, (int) $rows[0]['player_rank']);
        // Alice and Charlie: 1500 -> rank 2 (tied)
        $this->assertSame('Alice', $rows[1]['username']);
        $this->assertEquals(2, (int) $rows[1]['player_rank']);
        $this->assertSame('Charlie', $rows[2]['username']);
        $this->assertEquals(2, (int) $rows[2]['player_rank']);
        // Bob: 1200 -> rank 3
        $this->assertSame('Bob', $rows[3]['username']);
        $this->assertEquals(3, (int) $rows[3]['player_rank']);
        // Diana: 800 -> rank 4
        $this->assertSame('Diana', $rows[4]['username']);
        $this->assertEquals(4, (int) $rows[4]['player_rank']);
    }

    /**
     * Update a player's score, record in history, and verify new ranking.
     */
    public function testScoreUpdate(): void
    {
        // Bob's current score is 1200
        $rows = $this->ztdQuery("SELECT score FROM mp_lb_players WHERE id = 2");
        $this->assertEquals(1200, (int) $rows[0]['score']);

        // Update Bob's score to 2100 and record in history
        $affected = $this->pdo->exec("UPDATE mp_lb_players SET score = 2100, games_played = 16, last_played = '2026-03-09 12:00:00' WHERE id = 2");
        $this->assertSame(1, $affected);

        $this->pdo->exec("INSERT INTO mp_lb_score_history VALUES (17, 2, 900, 'Epic victory', '2026-03-09 12:00:00')");

        // Verify Bob is now rank 1
        $rows = $this->ztdQuery(
            "SELECT username, score,
                    DENSE_RANK() OVER (ORDER BY score DESC) AS player_rank
             FROM mp_lb_players
             ORDER BY player_rank, username"
        );

        $this->assertSame('Bob', $rows[0]['username']);
        $this->assertEquals(2100, (int) $rows[0]['score']);
        $this->assertEquals(1, (int) $rows[0]['player_rank']);
    }

    /**
     * Retrieve top 3 players using LIMIT with window function ordering.
     */
    public function testTopNPlayers(): void
    {
        $rows = $this->ztdQuery(
            "SELECT username, score,
                    DENSE_RANK() OVER (ORDER BY score DESC) AS player_rank
             FROM mp_lb_players
             ORDER BY player_rank, username
             LIMIT 3"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Eve', $rows[0]['username']);
        $this->assertEquals(2000, (int) $rows[0]['score']);
        $this->assertSame('Alice', $rows[1]['username']);
        $this->assertEquals(1500, (int) $rows[1]['score']);
        $this->assertSame('Charlie', $rows[2]['username']);
        $this->assertEquals(1500, (int) $rows[2]['score']);
    }

    /**
     * Look up a specific player's rank using a correlated subquery with prepared statement.
     *
     * Note: Prepared statements with derived tables containing window functions
     * return empty results on MySQL (MySQLi, MySQL-PDO) and SQLite-PDO.
     * PostgreSQL-PDO works correctly. Use correlated subquery for cross-platform rank lookup.
     */
    public function testPlayerRankLookup(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT p.username, p.score,
                    (SELECT COUNT(DISTINCT p2.score) FROM mp_lb_players p2 WHERE p2.score > p.score) + 1 AS player_rank
             FROM mp_lb_players p
             WHERE p.username = ?",
            ['Bob']
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['username']);
        $this->assertEquals(1200, (int) $rows[0]['score']);
        $this->assertEquals(3, (int) $rows[0]['player_rank']);
    }

    /**
     * Score history timeline with JOIN to players, ordered by date descending.
     */
    public function testScoreHistoryTimeline(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.username, sh.points, sh.reason, sh.recorded_at
             FROM mp_lb_score_history sh
             JOIN mp_lb_players p ON p.id = sh.player_id
             ORDER BY sh.recorded_at DESC"
        );

        $this->assertCount(16, $rows);
        // Most recent entry is Eve's season bonus on 2026-03-08
        $this->assertSame('Eve', $rows[0]['username']);
        $this->assertSame('Season bonus', $rows[0]['reason']);
        // Oldest entries are the initial placements on 2026-02-01
        $lastRow = $rows[15];
        $this->assertSame('Initial placement', $lastRow['reason']);
    }

    /**
     * Two players with the same score get the same rank via DENSE_RANK;
     * the next distinct score gets the immediately following rank (no gap).
     */
    public function testTiedRanking(): void
    {
        $rows = $this->ztdQuery(
            "SELECT username, score,
                    DENSE_RANK() OVER (ORDER BY score DESC) AS player_rank
             FROM mp_lb_players
             ORDER BY player_rank, username"
        );

        // Alice (1500) and Charlie (1500) share rank 2
        $aliceRow = null;
        $charlieRow = null;
        $bobRow = null;
        foreach ($rows as $row) {
            if ($row['username'] === 'Alice') {
                $aliceRow = $row;
            }
            if ($row['username'] === 'Charlie') {
                $charlieRow = $row;
            }
            if ($row['username'] === 'Bob') {
                $bobRow = $row;
            }
        }

        $this->assertNotNull($aliceRow);
        $this->assertNotNull($charlieRow);
        $this->assertNotNull($bobRow);

        // Tied players share the same rank
        $this->assertEquals((int) $aliceRow['player_rank'], (int) $charlieRow['player_rank']);
        // DENSE_RANK: next rank after tie is immediately consecutive (no gap)
        $this->assertEquals((int) $aliceRow['player_rank'] + 1, (int) $bobRow['player_rank']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("UPDATE mp_lb_players SET score = 9999 WHERE id = 4");
        $this->pdo->exec("INSERT INTO mp_lb_score_history VALUES (17, 4, 9199, 'Mega bonus', '2026-03-09 20:00:00')");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT score FROM mp_lb_players WHERE id = 4");
        $this->assertEquals(9999, (int) $rows[0]['score']);

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_lb_score_history");
        $this->assertSame(17, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mp_lb_players")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
