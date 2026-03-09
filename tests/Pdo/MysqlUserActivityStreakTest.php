<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests consecutive-day login streak detection using LAG window functions
 * and date gap analysis (MySQL PDO).
 * @spec SPEC-10.2.116
 */
class MysqlUserActivityStreakTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_uas_users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(255)
            )',
            'CREATE TABLE mp_uas_activity_log (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT,
                activity_date VARCHAR(255),
                activity_type VARCHAR(255)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_uas_activity_log', 'mp_uas_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Users
        $this->pdo->exec("INSERT INTO mp_uas_users VALUES (1, 'alice')");
        $this->pdo->exec("INSERT INTO mp_uas_users VALUES (2, 'bob')");

        // Activity log: alice has a 4-day streak, a gap, then a 2-day streak
        $this->pdo->exec("INSERT INTO mp_uas_activity_log VALUES (1, 1, '2026-03-01', 'login')");
        $this->pdo->exec("INSERT INTO mp_uas_activity_log VALUES (2, 1, '2026-03-02', 'login')");
        $this->pdo->exec("INSERT INTO mp_uas_activity_log VALUES (3, 1, '2026-03-03', 'login')");
        $this->pdo->exec("INSERT INTO mp_uas_activity_log VALUES (4, 1, '2026-03-04', 'login')");
        $this->pdo->exec("INSERT INTO mp_uas_activity_log VALUES (5, 1, '2026-03-07', 'login')");
        $this->pdo->exec("INSERT INTO mp_uas_activity_log VALUES (6, 1, '2026-03-08', 'login')");

        // Activity log: bob has a 3-day streak
        $this->pdo->exec("INSERT INTO mp_uas_activity_log VALUES (7, 2, '2026-03-02', 'login')");
        $this->pdo->exec("INSERT INTO mp_uas_activity_log VALUES (8, 2, '2026-03-03', 'login')");
        $this->pdo->exec("INSERT INTO mp_uas_activity_log VALUES (9, 2, '2026-03-04', 'login')");
    }

    /**
     * Count total distinct activity days per user.
     */
    public function testTotalActivityDays(): void
    {
        $rows = $this->ztdQuery(
            "SELECT user_id, COUNT(DISTINCT activity_date) AS days
             FROM mp_uas_activity_log
             GROUP BY user_id
             ORDER BY user_id"
        );

        $this->assertCount(2, $rows);
        $this->assertEquals(1, (int) $rows[0]['user_id']);
        $this->assertEquals(6, (int) $rows[0]['days']);
        $this->assertEquals(2, (int) $rows[1]['user_id']);
        $this->assertEquals(3, (int) $rows[1]['days']);
    }

    /**
     * Use LAG to retrieve the previous activity date per user.
     */
    public function testLagPreviousDate(): void
    {
        $rows = $this->ztdQuery(
            "SELECT user_id, activity_date,
                    LAG(activity_date) OVER (PARTITION BY user_id ORDER BY activity_date) AS prev_date
             FROM mp_uas_activity_log
             WHERE user_id = 1
             ORDER BY activity_date"
        );

        $this->assertCount(6, $rows);

        // First row: no previous date
        $this->assertSame('2026-03-01', $rows[0]['activity_date']);
        $this->assertNull($rows[0]['prev_date']);

        // Second row: previous is 2026-03-01
        $this->assertSame('2026-03-02', $rows[1]['activity_date']);
        $this->assertSame('2026-03-01', $rows[1]['prev_date']);

        // Third row: previous is 2026-03-02
        $this->assertSame('2026-03-03', $rows[2]['activity_date']);
        $this->assertSame('2026-03-02', $rows[2]['prev_date']);

        // Fourth row: previous is 2026-03-03
        $this->assertSame('2026-03-04', $rows[3]['activity_date']);
        $this->assertSame('2026-03-03', $rows[3]['prev_date']);

        // Fifth row: previous is 2026-03-04 (gap before this date)
        $this->assertSame('2026-03-07', $rows[4]['activity_date']);
        $this->assertSame('2026-03-04', $rows[4]['prev_date']);

        // Sixth row: previous is 2026-03-07
        $this->assertSame('2026-03-08', $rows[5]['activity_date']);
        $this->assertSame('2026-03-07', $rows[5]['prev_date']);
    }

    /**
     * Use LAG to detect gaps: verify that for alice the entry at 2026-03-07
     * has prev_date 2026-03-04 (a gap), while 2026-03-02 has prev_date 2026-03-01 (consecutive).
     */
    public function testGapDetectionViaLag(): void
    {
        // Query all rows with LAG (including NULL prev_date), filter in PHP
        // to avoid derived-table wrapping issues on some platforms.
        $rows = $this->ztdQuery(
            "SELECT activity_date,
                    LAG(activity_date) OVER (PARTITION BY user_id ORDER BY activity_date) AS prev_date
             FROM mp_uas_activity_log
             WHERE user_id = 1
             ORDER BY activity_date"
        );

        $this->assertCount(6, $rows);

        // Filter to non-NULL prev_date rows for gap analysis
        $withPrev = array_values(array_filter($rows, fn($r) => $r['prev_date'] !== null));
        $this->assertCount(5, $withPrev);

        // 2026-03-02 -> prev 2026-03-01 (consecutive)
        $this->assertSame('2026-03-02', $withPrev[0]['activity_date']);
        $this->assertSame('2026-03-01', $withPrev[0]['prev_date']);

        // 2026-03-07 -> prev 2026-03-04 (gap: 3 days apart)
        $this->assertSame('2026-03-07', $withPrev[3]['activity_date']);
        $this->assertSame('2026-03-04', $withPrev[3]['prev_date']);

        // 2026-03-08 -> prev 2026-03-07 (consecutive)
        $this->assertSame('2026-03-08', $withPrev[4]['activity_date']);
        $this->assertSame('2026-03-07', $withPrev[4]['prev_date']);
    }

    /**
     * Find the user with the most activity days.
     */
    public function testUserWithMostActivityDays(): void
    {
        $rows = $this->ztdQuery(
            "SELECT user_id, COUNT(*) AS days
             FROM mp_uas_activity_log
             GROUP BY user_id
             ORDER BY days DESC
             LIMIT 1"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(1, (int) $rows[0]['user_id']);
        $this->assertEquals(6, (int) $rows[0]['days']);
    }

    /**
     * Activity date range per user: first and last activity dates.
     */
    public function testActivityDateRange(): void
    {
        $rows = $this->ztdQuery(
            "SELECT user_id,
                    MIN(activity_date) AS first_date,
                    MAX(activity_date) AS last_date
             FROM mp_uas_activity_log
             GROUP BY user_id
             ORDER BY user_id"
        );

        $this->assertCount(2, $rows);

        // alice: 2026-03-01 to 2026-03-08
        $this->assertEquals(1, (int) $rows[0]['user_id']);
        $this->assertSame('2026-03-01', $rows[0]['first_date']);
        $this->assertSame('2026-03-08', $rows[0]['last_date']);

        // bob: 2026-03-02 to 2026-03-04
        $this->assertEquals(2, (int) $rows[1]['user_id']);
        $this->assertSame('2026-03-02', $rows[1]['first_date']);
        $this->assertSame('2026-03-04', $rows[1]['last_date']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO mp_uas_activity_log VALUES (10, 1, '2026-03-09', 'login')");
        $this->pdo->exec("UPDATE mp_uas_users SET username = 'alice_updated' WHERE id = 1");

        // ZTD sees changes
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_uas_activity_log");
        $this->assertEquals(10, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT username FROM mp_uas_users WHERE id = 1");
        $this->assertSame('alice_updated', $rows[0]['username']);

        // Physical tables untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mp_uas_activity_log")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
