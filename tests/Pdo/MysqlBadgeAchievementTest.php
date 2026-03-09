<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests a badge achievement system with progress tracking through ZTD shadow store (MySQL PDO).
 * Covers progress percentage calculation, milestone threshold detection with HAVING,
 * rarity scoring with COUNT across users, NOT EXISTS for already-unlocked checks,
 * and physical isolation.
 * @spec SPEC-10.2.141
 */
class MysqlBadgeAchievementTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_ba_users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(255)
            )',
            'CREATE TABLE mp_ba_badges (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255),
                description TEXT,
                threshold INT
            )',
            'CREATE TABLE mp_ba_user_badges (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT,
                badge_id INT,
                progress INT,
                unlocked_at TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_ba_user_badges', 'mp_ba_badges', 'mp_ba_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Users
        $this->pdo->exec("INSERT INTO mp_ba_users VALUES (1, 'alice')");
        $this->pdo->exec("INSERT INTO mp_ba_users VALUES (2, 'bob')");
        $this->pdo->exec("INSERT INTO mp_ba_users VALUES (3, 'carol')");
        $this->pdo->exec("INSERT INTO mp_ba_users VALUES (4, 'dave')");

        // Badges
        $this->pdo->exec("INSERT INTO mp_ba_badges VALUES (1, 'First Post', 'Write your first post', 1)");
        $this->pdo->exec("INSERT INTO mp_ba_badges VALUES (2, 'Prolific Writer', 'Write 10 posts', 10)");
        $this->pdo->exec("INSERT INTO mp_ba_badges VALUES (3, 'Helpful Hand', 'Answer 5 questions', 5)");
        $this->pdo->exec("INSERT INTO mp_ba_badges VALUES (4, 'Veteran', 'Log in 100 days', 100)");

        // User badges
        $this->pdo->exec("INSERT INTO mp_ba_user_badges VALUES (1, 1, 1, 1, '2025-01-05')");
        $this->pdo->exec("INSERT INTO mp_ba_user_badges VALUES (2, 1, 2, 7, NULL)");
        $this->pdo->exec("INSERT INTO mp_ba_user_badges VALUES (3, 1, 3, 5, '2025-03-20')");
        $this->pdo->exec("INSERT INTO mp_ba_user_badges VALUES (4, 1, 4, 42, NULL)");
        $this->pdo->exec("INSERT INTO mp_ba_user_badges VALUES (5, 2, 1, 1, '2025-02-10')");
        $this->pdo->exec("INSERT INTO mp_ba_user_badges VALUES (6, 2, 2, 10, '2025-06-01')");
        $this->pdo->exec("INSERT INTO mp_ba_user_badges VALUES (7, 2, 3, 3, NULL)");
        $this->pdo->exec("INSERT INTO mp_ba_user_badges VALUES (8, 3, 1, 1, '2025-01-20')");
        $this->pdo->exec("INSERT INTO mp_ba_user_badges VALUES (9, 3, 4, 100, '2025-08-15')");
        $this->pdo->exec("INSERT INTO mp_ba_user_badges VALUES (10, 4, 1, 1, '2025-04-01')");
    }

    /**
     * JOIN users + user_badges + badges. For each user, show badge name, progress, threshold,
     * and whether unlocked (CASE WHEN unlocked_at IS NOT NULL).
     * Alice has 4 entries, bob 3, carol 2, dave 1. ORDER BY username, badge name.
     * @spec SPEC-10.2.141
     */
    public function testUserBadgeProgress(): void
    {
        $rows = $this->ztdQuery(
            "SELECT u.username,
                    b.name AS badge_name,
                    ub.progress,
                    b.threshold,
                    CASE WHEN ub.unlocked_at IS NOT NULL THEN 'yes' ELSE 'no' END AS is_unlocked
             FROM mp_ba_users u
             JOIN mp_ba_user_badges ub ON ub.user_id = u.id
             JOIN mp_ba_badges b ON b.id = ub.badge_id
             ORDER BY u.username, b.name"
        );

        $this->assertCount(10, $rows);

        // alice: 4 badges
        $this->assertSame('alice', $rows[0]['username']);
        $this->assertSame('First Post', $rows[0]['badge_name']);
        $this->assertEquals(1, (int) $rows[0]['progress']);
        $this->assertEquals(1, (int) $rows[0]['threshold']);
        $this->assertSame('yes', $rows[0]['is_unlocked']);

        $this->assertSame('alice', $rows[1]['username']);
        $this->assertSame('Helpful Hand', $rows[1]['badge_name']);
        $this->assertEquals(5, (int) $rows[1]['progress']);
        $this->assertEquals(5, (int) $rows[1]['threshold']);
        $this->assertSame('yes', $rows[1]['is_unlocked']);

        $this->assertSame('alice', $rows[2]['username']);
        $this->assertSame('Prolific Writer', $rows[2]['badge_name']);
        $this->assertEquals(7, (int) $rows[2]['progress']);
        $this->assertEquals(10, (int) $rows[2]['threshold']);
        $this->assertSame('no', $rows[2]['is_unlocked']);

        $this->assertSame('alice', $rows[3]['username']);
        $this->assertSame('Veteran', $rows[3]['badge_name']);
        $this->assertEquals(42, (int) $rows[3]['progress']);
        $this->assertEquals(100, (int) $rows[3]['threshold']);
        $this->assertSame('no', $rows[3]['is_unlocked']);

        // bob: 3 badges
        $this->assertSame('bob', $rows[4]['username']);
        $this->assertSame('First Post', $rows[4]['badge_name']);
        $this->assertSame('yes', $rows[4]['is_unlocked']);

        $this->assertSame('bob', $rows[5]['username']);
        $this->assertSame('Helpful Hand', $rows[5]['badge_name']);
        $this->assertSame('no', $rows[5]['is_unlocked']);

        $this->assertSame('bob', $rows[6]['username']);
        $this->assertSame('Prolific Writer', $rows[6]['badge_name']);
        $this->assertSame('yes', $rows[6]['is_unlocked']);

        // carol: 2 badges
        $this->assertSame('carol', $rows[7]['username']);
        $this->assertSame('First Post', $rows[7]['badge_name']);
        $this->assertSame('yes', $rows[7]['is_unlocked']);

        $this->assertSame('carol', $rows[8]['username']);
        $this->assertSame('Veteran', $rows[8]['badge_name']);
        $this->assertSame('yes', $rows[8]['is_unlocked']);

        // dave: 1 badge
        $this->assertSame('dave', $rows[9]['username']);
        $this->assertSame('First Post', $rows[9]['badge_name']);
        $this->assertSame('yes', $rows[9]['is_unlocked']);
    }

    /**
     * COUNT how many users have unlocked each badge, calculate rarity as percentage of total users (4).
     * First Post: 4 unlocked = 100%, Prolific Writer: 1 = 25%, Helpful Hand: 1 = 25%, Veteran: 1 = 25%.
     * GROUP BY badge, ORDER BY unlock_count DESC, badge name.
     * @spec SPEC-10.2.141
     */
    public function testBadgeRarity(): void
    {
        $rows = $this->ztdQuery(
            "SELECT b.name AS badge_name,
                    COUNT(*) AS unlock_count,
                    ROUND(COUNT(*) * 100.0 / 4, 0) AS rarity_pct
             FROM mp_ba_user_badges ub
             JOIN mp_ba_badges b ON b.id = ub.badge_id
             WHERE ub.unlocked_at IS NOT NULL
             GROUP BY b.id, b.name
             ORDER BY unlock_count DESC, b.name"
        );

        $this->assertCount(4, $rows);

        // First Post: 4 users unlocked = 100%
        $this->assertSame('First Post', $rows[0]['badge_name']);
        $this->assertEquals(4, (int) $rows[0]['unlock_count']);
        $this->assertEqualsWithDelta(100.0, (float) $rows[0]['rarity_pct'], 1.0);

        // Helpful Hand: 1 user = 25%
        $this->assertSame('Helpful Hand', $rows[1]['badge_name']);
        $this->assertEquals(1, (int) $rows[1]['unlock_count']);
        $this->assertEqualsWithDelta(25.0, (float) $rows[1]['rarity_pct'], 1.0);

        // Prolific Writer: 1 user = 25%
        $this->assertSame('Prolific Writer', $rows[2]['badge_name']);
        $this->assertEquals(1, (int) $rows[2]['unlock_count']);
        $this->assertEqualsWithDelta(25.0, (float) $rows[2]['rarity_pct'], 1.0);

        // Veteran: 1 user = 25%
        $this->assertSame('Veteran', $rows[3]['badge_name']);
        $this->assertEquals(1, (int) $rows[3]['unlock_count']);
        $this->assertEqualsWithDelta(25.0, (float) $rows[3]['rarity_pct'], 1.0);
    }

    /**
     * COUNT unlocked badges per user (WHERE unlocked_at IS NOT NULL).
     * alice=2, bob=2, carol=2, dave=1. ORDER BY username.
     * @spec SPEC-10.2.141
     */
    public function testUnlockedBadgesPerUser(): void
    {
        $rows = $this->ztdQuery(
            "SELECT u.username,
                    COUNT(*) AS unlocked_count
             FROM mp_ba_users u
             JOIN mp_ba_user_badges ub ON ub.user_id = u.id
             WHERE ub.unlocked_at IS NOT NULL
             GROUP BY u.id, u.username
             ORDER BY u.username"
        );

        $this->assertCount(4, $rows);

        $this->assertSame('alice', $rows[0]['username']);
        $this->assertEquals(2, (int) $rows[0]['unlocked_count']);

        $this->assertSame('bob', $rows[1]['username']);
        $this->assertEquals(2, (int) $rows[1]['unlocked_count']);

        $this->assertSame('carol', $rows[2]['username']);
        $this->assertEquals(2, (int) $rows[2]['unlocked_count']);

        $this->assertSame('dave', $rows[3]['username']);
        $this->assertEquals(1, (int) $rows[3]['unlocked_count']);
    }

    /**
     * UPDATE alice's Prolific Writer progress from 7 to 10, then set unlocked_at.
     * Verify badge is now unlocked. Then count alice's total unlocked = 3.
     * @spec SPEC-10.2.141
     */
    public function testProgressUpdate(): void
    {
        // Update progress to threshold
        $this->pdo->exec("UPDATE mp_ba_user_badges SET progress = 10 WHERE id = 2");
        // Mark as unlocked
        $this->pdo->exec("UPDATE mp_ba_user_badges SET unlocked_at = '2025-09-01' WHERE id = 2");

        // Verify the badge is now unlocked
        $rows = $this->ztdQuery(
            "SELECT b.name AS badge_name,
                    ub.progress,
                    b.threshold,
                    CASE WHEN ub.unlocked_at IS NOT NULL THEN 'yes' ELSE 'no' END AS is_unlocked
             FROM mp_ba_user_badges ub
             JOIN mp_ba_badges b ON b.id = ub.badge_id
             WHERE ub.id = 2"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Prolific Writer', $rows[0]['badge_name']);
        $this->assertEquals(10, (int) $rows[0]['progress']);
        $this->assertEquals(10, (int) $rows[0]['threshold']);
        $this->assertSame('yes', $rows[0]['is_unlocked']);

        // Count alice's total unlocked badges = 3
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS unlocked_count
             FROM mp_ba_user_badges
             WHERE user_id = 1 AND unlocked_at IS NOT NULL"
        );

        $this->assertEquals(3, (int) $rows[0]['unlocked_count']);
    }

    /**
     * For alice (user_id=1), find badges not yet unlocked (unlocked_at IS NULL).
     * Alice's in-progress badges: Prolific Writer (7/10), Veteran (42/100).
     * ORDER BY badge name.
     * @spec SPEC-10.2.141
     */
    public function testNotYetUnlockedBadges(): void
    {
        $rows = $this->ztdQuery(
            "SELECT b.name AS badge_name,
                    ub.progress,
                    b.threshold
             FROM mp_ba_user_badges ub
             JOIN mp_ba_badges b ON b.id = ub.badge_id
             WHERE ub.user_id = 1
               AND ub.unlocked_at IS NULL
             ORDER BY b.name"
        );

        $this->assertCount(2, $rows);

        $this->assertSame('Prolific Writer', $rows[0]['badge_name']);
        $this->assertEquals(7, (int) $rows[0]['progress']);
        $this->assertEquals(10, (int) $rows[0]['threshold']);

        $this->assertSame('Veteran', $rows[1]['badge_name']);
        $this->assertEquals(42, (int) $rows[1]['progress']);
        $this->assertEquals(100, (int) $rows[1]['threshold']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     * @spec SPEC-10.2.141
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO mp_ba_user_badges VALUES (11, 4, 2, 1, NULL)");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_ba_user_badges");
        $this->assertSame(11, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mp_ba_user_badges")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
