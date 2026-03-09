<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests user cohort analysis with retention metrics — a common pattern
 * for SaaS analytics, marketing analysis, and user engagement tracking (SQLite PDO).
 * @spec SPEC-10.2.124
 */
class SqliteCohortRetentionAnalysisTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_cr_users (
                id INTEGER PRIMARY KEY,
                username TEXT,
                signup_month TEXT
            )',
            'CREATE TABLE sl_cr_user_activities (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                activity_month TEXT,
                action_count INTEGER
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_cr_user_activities', 'sl_cr_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Users across 3 cohorts
        $this->pdo->exec("INSERT INTO sl_cr_users VALUES (1, 'alice',   '2025-01')");
        $this->pdo->exec("INSERT INTO sl_cr_users VALUES (2, 'bob',     '2025-01')");
        $this->pdo->exec("INSERT INTO sl_cr_users VALUES (3, 'charlie', '2025-02')");
        $this->pdo->exec("INSERT INTO sl_cr_users VALUES (4, 'diana',   '2025-02')");
        $this->pdo->exec("INSERT INTO sl_cr_users VALUES (5, 'eve',     '2025-02')");
        $this->pdo->exec("INSERT INTO sl_cr_users VALUES (6, 'frank',   '2025-03')");

        // User activities
        $this->pdo->exec("INSERT INTO sl_cr_user_activities VALUES (1,  1, '2025-01', 10)");
        $this->pdo->exec("INSERT INTO sl_cr_user_activities VALUES (2,  1, '2025-02', 8)");
        $this->pdo->exec("INSERT INTO sl_cr_user_activities VALUES (3,  1, '2025-03', 5)");
        $this->pdo->exec("INSERT INTO sl_cr_user_activities VALUES (4,  2, '2025-01', 15)");
        $this->pdo->exec("INSERT INTO sl_cr_user_activities VALUES (5,  2, '2025-02', 3)");
        $this->pdo->exec("INSERT INTO sl_cr_user_activities VALUES (6,  2, '2025-04', 1)");
        $this->pdo->exec("INSERT INTO sl_cr_user_activities VALUES (7,  3, '2025-02', 12)");
        $this->pdo->exec("INSERT INTO sl_cr_user_activities VALUES (8,  3, '2025-03', 7)");
        $this->pdo->exec("INSERT INTO sl_cr_user_activities VALUES (9,  4, '2025-02', 20)");
        $this->pdo->exec("INSERT INTO sl_cr_user_activities VALUES (10, 4, '2025-03', 15)");
        $this->pdo->exec("INSERT INTO sl_cr_user_activities VALUES (11, 4, '2025-04', 10)");
        $this->pdo->exec("INSERT INTO sl_cr_user_activities VALUES (12, 5, '2025-02', 5)");
        $this->pdo->exec("INSERT INTO sl_cr_user_activities VALUES (13, 6, '2025-03', 25)");
        $this->pdo->exec("INSERT INTO sl_cr_user_activities VALUES (14, 6, '2025-04', 20)");
        $this->pdo->exec("INSERT INTO sl_cr_user_activities VALUES (15, 6, '2025-05', 12)");
    }

    /**
     * Cohort size: count users per signup month.
     */
    public function testCohortSize(): void
    {
        $rows = $this->ztdQuery(
            "SELECT signup_month, COUNT(*) AS cohort_size
             FROM sl_cr_users
             GROUP BY signup_month
             ORDER BY signup_month"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('2025-01', $rows[0]['signup_month']);
        $this->assertEquals(2, (int) $rows[0]['cohort_size']);
        $this->assertSame('2025-02', $rows[1]['signup_month']);
        $this->assertEquals(3, (int) $rows[1]['cohort_size']);
        $this->assertSame('2025-03', $rows[2]['signup_month']);
        $this->assertEquals(1, (int) $rows[2]['cohort_size']);
    }

    /**
     * Retention by month for the January cohort: count distinct active users
     * per activity month.
     */
    public function testRetentionByMonth(): void
    {
        $rows = $this->ztdQuery(
            "SELECT a.activity_month, COUNT(DISTINCT a.user_id) AS active_users
             FROM sl_cr_users u
             JOIN sl_cr_user_activities a ON a.user_id = u.id
             WHERE u.signup_month = '2025-01'
             GROUP BY a.activity_month
             ORDER BY a.activity_month"
        );

        $this->assertCount(4, $rows);

        // Jan: alice + bob
        $this->assertSame('2025-01', $rows[0]['activity_month']);
        $this->assertEquals(2, (int) $rows[0]['active_users']);

        // Feb: alice + bob
        $this->assertSame('2025-02', $rows[1]['activity_month']);
        $this->assertEquals(2, (int) $rows[1]['active_users']);

        // Mar: alice only
        $this->assertSame('2025-03', $rows[2]['activity_month']);
        $this->assertEquals(1, (int) $rows[2]['active_users']);

        // Apr: bob returns
        $this->assertSame('2025-04', $rows[3]['activity_month']);
        $this->assertEquals(1, (int) $rows[3]['active_users']);
    }

    /**
     * Average action count per cohort across all their activity months.
     */
    public function testCohortAvgActivity(): void
    {
        $rows = $this->ztdQuery(
            "SELECT u.signup_month, AVG(a.action_count) AS avg_actions
             FROM sl_cr_users u
             JOIN sl_cr_user_activities a ON a.user_id = u.id
             GROUP BY u.signup_month
             ORDER BY u.signup_month"
        );

        $this->assertCount(3, $rows);

        // Jan cohort: (10+8+5+15+3+1)/6 = 42/6 = 7.0
        $this->assertSame('2025-01', $rows[0]['signup_month']);
        $this->assertEqualsWithDelta(7.0, (float) $rows[0]['avg_actions'], 0.01);

        // Feb cohort: (12+7+20+15+10+5)/6 = 69/6 = 11.5
        $this->assertSame('2025-02', $rows[1]['signup_month']);
        $this->assertEqualsWithDelta(11.5, (float) $rows[1]['avg_actions'], 0.01);

        // Mar cohort: (25+20+12)/3 = 57/3 = 19.0
        $this->assertSame('2025-03', $rows[2]['signup_month']);
        $this->assertEqualsWithDelta(19.0, (float) $rows[2]['avg_actions'], 0.01);
    }

    /**
     * Count distinct active users per activity month across all cohorts.
     */
    public function testActiveUsersPerMonth(): void
    {
        $rows = $this->ztdQuery(
            "SELECT activity_month, COUNT(DISTINCT user_id) AS active_users
             FROM sl_cr_user_activities
             GROUP BY activity_month
             ORDER BY activity_month"
        );

        $this->assertCount(5, $rows);

        // 2025-01: alice, bob
        $this->assertSame('2025-01', $rows[0]['activity_month']);
        $this->assertEquals(2, (int) $rows[0]['active_users']);

        // 2025-02: alice, bob, charlie, diana, eve
        $this->assertSame('2025-02', $rows[1]['activity_month']);
        $this->assertEquals(5, (int) $rows[1]['active_users']);

        // 2025-03: alice, charlie, diana, frank
        $this->assertSame('2025-03', $rows[2]['activity_month']);
        $this->assertEquals(4, (int) $rows[2]['active_users']);

        // 2025-04: bob, diana, frank
        $this->assertSame('2025-04', $rows[3]['activity_month']);
        $this->assertEquals(3, (int) $rows[3]['active_users']);

        // 2025-05: frank
        $this->assertSame('2025-05', $rows[4]['activity_month']);
        $this->assertEquals(1, (int) $rows[4]['active_users']);
    }

    /**
     * Churn detection: find users who were active in their signup month but
     * had NO activity in the month immediately following.
     * Only Eve (signup 2025-02, no activity in 2025-03) should be detected.
     */
    public function testChurnDetection(): void
    {
        $rows = $this->ztdQuery(
            "SELECT u.id, u.username, u.signup_month
             FROM sl_cr_users u
             WHERE EXISTS (
                 SELECT 1 FROM sl_cr_user_activities a1
                 WHERE a1.user_id = u.id
                   AND a1.activity_month = u.signup_month
             )
             AND NOT EXISTS (
                 SELECT 1 FROM sl_cr_user_activities a2
                 WHERE a2.user_id = u.id
                   AND a2.activity_month = CASE u.signup_month
                       WHEN '2025-01' THEN '2025-02'
                       WHEN '2025-02' THEN '2025-03'
                       WHEN '2025-03' THEN '2025-04'
                   END
             )
             ORDER BY u.id"
        );

        // Only Eve churned: active in 2025-02 (signup), no activity in 2025-03
        $this->assertCount(1, $rows);
        $this->assertEquals(5, (int) $rows[0]['id']);
        $this->assertSame('eve', $rows[0]['username']);
        $this->assertSame('2025-02', $rows[0]['signup_month']);
    }

    /**
     * Power users: users whose total action count exceeds 30.
     * Diana (45) and Frank (57) qualify.
     */
    public function testPowerUsers(): void
    {
        $rows = $this->ztdQuery(
            "SELECT u.id, u.username, SUM(a.action_count) AS total_actions
             FROM sl_cr_users u
             JOIN sl_cr_user_activities a ON a.user_id = u.id
             GROUP BY u.id, u.username
             HAVING SUM(a.action_count) > 30
             ORDER BY total_actions"
        );

        $this->assertCount(2, $rows);

        // Diana: 20+15+10 = 45
        $this->assertSame('diana', $rows[0]['username']);
        $this->assertEquals(45, (int) $rows[0]['total_actions']);

        // Frank: 25+20+12 = 57
        $this->assertSame('frank', $rows[1]['username']);
        $this->assertEquals(57, (int) $rows[1]['total_actions']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO sl_cr_users VALUES (7, 'grace', '2025-04')");
        $this->pdo->exec("INSERT INTO sl_cr_user_activities VALUES (16, 7, '2025-04', 30)");

        // ZTD sees changes
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_cr_users");
        $this->assertEquals(7, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_cr_user_activities");
        $this->assertEquals(16, (int) $rows[0]['cnt']);

        // Physical tables untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_cr_users")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
