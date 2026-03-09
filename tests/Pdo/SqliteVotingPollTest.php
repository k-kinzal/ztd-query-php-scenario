<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests a polling/voting system with result tallying through ZTD shadow store (SQLite PDO).
 * Covers GROUP BY with percentage calculation, HAVING for quorum check, anti-join for non-voters,
 * COUNT DISTINCT, and physical isolation.
 * @spec SPEC-10.2.137
 */
class SqliteVotingPollTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_vp_polls (
                id INTEGER PRIMARY KEY,
                title TEXT,
                created_at TEXT,
                closes_at TEXT,
                status TEXT
            )',
            'CREATE TABLE sl_vp_options (
                id INTEGER PRIMARY KEY,
                poll_id INTEGER,
                option_text TEXT,
                display_order INTEGER
            )',
            'CREATE TABLE sl_vp_votes (
                id INTEGER PRIMARY KEY,
                poll_id INTEGER,
                option_id INTEGER,
                voter_name TEXT,
                voted_at TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_vp_votes', 'sl_vp_options', 'sl_vp_polls'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 2 polls
        $this->pdo->exec("INSERT INTO sl_vp_polls VALUES (1, 'Best Programming Language', '2025-09-01', '2025-09-30', 'closed')");
        $this->pdo->exec("INSERT INTO sl_vp_polls VALUES (2, 'Office Lunch Preference', '2025-10-01', '2025-10-15', 'open')");

        // Options for poll 1
        $this->pdo->exec("INSERT INTO sl_vp_options VALUES (1, 1, 'Python', 1)");
        $this->pdo->exec("INSERT INTO sl_vp_options VALUES (2, 1, 'JavaScript', 2)");
        $this->pdo->exec("INSERT INTO sl_vp_options VALUES (3, 1, 'PHP', 3)");
        $this->pdo->exec("INSERT INTO sl_vp_options VALUES (4, 1, 'Go', 4)");

        // Options for poll 2
        $this->pdo->exec("INSERT INTO sl_vp_options VALUES (5, 2, 'Pizza', 1)");
        $this->pdo->exec("INSERT INTO sl_vp_options VALUES (6, 2, 'Sushi', 2)");
        $this->pdo->exec("INSERT INTO sl_vp_options VALUES (7, 2, 'Salad', 3)");

        // Votes for poll 1: Python=3, JavaScript=2, PHP=1, Go=0
        $this->pdo->exec("INSERT INTO sl_vp_votes VALUES (1, 1, 1, 'Alice', '2025-09-02')");
        $this->pdo->exec("INSERT INTO sl_vp_votes VALUES (2, 1, 1, 'Bob', '2025-09-03')");
        $this->pdo->exec("INSERT INTO sl_vp_votes VALUES (3, 1, 2, 'Carol', '2025-09-04')");
        $this->pdo->exec("INSERT INTO sl_vp_votes VALUES (4, 1, 3, 'Dave', '2025-09-05')");
        $this->pdo->exec("INSERT INTO sl_vp_votes VALUES (5, 1, 1, 'Eve', '2025-09-06')");
        $this->pdo->exec("INSERT INTO sl_vp_votes VALUES (6, 1, 2, 'Frank', '2025-09-07')");

        // Votes for poll 2: Pizza=2, Sushi=1, Salad=1
        $this->pdo->exec("INSERT INTO sl_vp_votes VALUES (7, 2, 5, 'Alice', '2025-10-02')");
        $this->pdo->exec("INSERT INTO sl_vp_votes VALUES (8, 2, 6, 'Bob', '2025-10-03')");
        $this->pdo->exec("INSERT INTO sl_vp_votes VALUES (9, 2, 5, 'Carol', '2025-10-04')");
        $this->pdo->exec("INSERT INTO sl_vp_votes VALUES (10, 2, 7, 'Dave', '2025-10-05')");
    }

    /**
     * For poll 1: GROUP BY option, COUNT votes, calculate percentage (votes * 100.0 / total_votes).
     * Python=3(50%), JavaScript=2(33.3%), PHP=1(16.7%), Go=0(0%).
     * Uses LEFT JOIN options with votes. ORDER BY vote count DESC, option_text.
     */
    public function testPollResults(): void
    {
        $rows = $this->ztdQuery(
            "SELECT o.option_text,
                    COUNT(v.id) AS vote_count,
                    ROUND(COUNT(v.id) * 100.0 / (SELECT COUNT(*) FROM sl_vp_votes WHERE poll_id = 1), 1) AS percentage
             FROM sl_vp_options o
             LEFT JOIN sl_vp_votes v ON v.option_id = o.id AND v.poll_id = 1
             WHERE o.poll_id = 1
             GROUP BY o.id, o.option_text
             ORDER BY vote_count DESC, o.option_text"
        );

        $this->assertCount(4, $rows);

        // Python: 3 votes, 50.0%
        $this->assertSame('Python', $rows[0]['option_text']);
        $this->assertEquals(3, (int) $rows[0]['vote_count']);
        $this->assertEqualsWithDelta(50.0, (float) $rows[0]['percentage'], 0.1);

        // JavaScript: 2 votes, 33.3%
        $this->assertSame('JavaScript', $rows[1]['option_text']);
        $this->assertEquals(2, (int) $rows[1]['vote_count']);
        $this->assertEqualsWithDelta(33.3, (float) $rows[1]['percentage'], 0.1);

        // PHP: 1 vote, 16.7%
        $this->assertSame('PHP', $rows[2]['option_text']);
        $this->assertEquals(1, (int) $rows[2]['vote_count']);
        $this->assertEqualsWithDelta(16.7, (float) $rows[2]['percentage'], 0.1);

        // Go: 0 votes, 0%
        $this->assertSame('Go', $rows[3]['option_text']);
        $this->assertEquals(0, (int) $rows[3]['vote_count']);
    }

    /**
     * For poll 1: option with MAX votes using ORDER BY COUNT DESC LIMIT 1.
     * Assert 'Python' with 3 votes.
     */
    public function testPollWinner(): void
    {
        $rows = $this->ztdQuery(
            "SELECT o.option_text, COUNT(v.id) AS vote_count
             FROM sl_vp_options o
             JOIN sl_vp_votes v ON v.option_id = o.id AND v.poll_id = 1
             WHERE o.poll_id = 1
             GROUP BY o.id, o.option_text
             ORDER BY vote_count DESC, o.option_text
             LIMIT 1"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Python', $rows[0]['option_text']);
        $this->assertEquals(3, (int) $rows[0]['vote_count']);
    }

    /**
     * COUNT DISTINCT voter_name per poll.
     * Poll 1=6, Poll 2=4. ORDER BY poll_id.
     */
    public function testVoterParticipation(): void
    {
        $rows = $this->ztdQuery(
            "SELECT poll_id, COUNT(DISTINCT voter_name) AS unique_voters
             FROM sl_vp_votes
             GROUP BY poll_id
             ORDER BY poll_id"
        );

        $this->assertCount(2, $rows);

        $this->assertEquals(1, (int) $rows[0]['poll_id']);
        $this->assertEquals(6, (int) $rows[0]['unique_voters']);

        $this->assertEquals(2, (int) $rows[1]['poll_id']);
        $this->assertEquals(4, (int) $rows[1]['unique_voters']);
    }

    /**
     * HAVING COUNT >= 2 for poll 1: Python(3), JavaScript(2).
     * Assert 2 rows. ORDER BY option_text.
     */
    public function testOptionsWithMinimumVotes(): void
    {
        $rows = $this->ztdQuery(
            "SELECT o.option_text, COUNT(v.id) AS vote_count
             FROM sl_vp_options o
             JOIN sl_vp_votes v ON v.option_id = o.id AND v.poll_id = 1
             WHERE o.poll_id = 1
             GROUP BY o.id, o.option_text
             HAVING COUNT(v.id) >= 2
             ORDER BY o.option_text"
        );

        $this->assertCount(2, $rows);

        $this->assertSame('JavaScript', $rows[0]['option_text']);
        $this->assertEquals(2, (int) $rows[0]['vote_count']);

        $this->assertSame('Python', $rows[1]['option_text']);
        $this->assertEquals(3, (int) $rows[1]['vote_count']);
    }

    /**
     * For poll 2, find voters from poll 1 who haven't voted in poll 2 using NOT IN subquery.
     * Eve and Frank voted in poll 1 but not poll 2. Assert 2 rows. ORDER BY voter_name.
     */
    public function testNonVotersForPoll(): void
    {
        $rows = $this->ztdQuery(
            "SELECT DISTINCT voter_name
             FROM sl_vp_votes
             WHERE poll_id = 1
               AND voter_name NOT IN (
                   SELECT voter_name FROM sl_vp_votes WHERE poll_id = 2
               )
             ORDER BY voter_name"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Eve', $rows[0]['voter_name']);
        $this->assertSame('Frank', $rows[1]['voter_name']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO sl_vp_votes VALUES (11, 2, 6, 'Eve', '2025-10-06')");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_vp_votes");
        $this->assertSame(11, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_vp_votes")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
