<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests referral chain patterns with NULLable self-referencing FK.
 * SQL patterns exercised: NOT IN with NULLable columns, deep self-joins
 * (3 levels), LEFT JOIN + COALESCE for missing references, DELETE WHERE
 * col IS NULL, UPDATE WHERE IN (SELECT from same table).
 * NULL propagation through NOT IN is a classic SQL pitfall: if the subquery
 * contains any NULL, NOT IN returns no rows. This tests whether the CTE
 * shadow store preserves that semantic correctly.
 * @spec SPEC-10.2.181
 */
class MysqlReferralChainTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mp_ref_users (
            id INT PRIMARY KEY,
            name VARCHAR(50),
            referrer_id INT,
            signup_source VARCHAR(20),
            reward_points INT
        )';
    }

    protected function getTableNames(): array
    {
        return ['mp_ref_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_ref_users VALUES (1, 'Alice', NULL, 'organic', 100)");
        $this->pdo->exec("INSERT INTO mp_ref_users VALUES (2, 'Bob', 1, 'referral', 50)");
        $this->pdo->exec("INSERT INTO mp_ref_users VALUES (3, 'Carol', 1, 'referral', 75)");
        $this->pdo->exec("INSERT INTO mp_ref_users VALUES (4, 'Dave', 2, 'referral', 30)");
        $this->pdo->exec("INSERT INTO mp_ref_users VALUES (5, 'Eve', NULL, 'organic', 200)");
    }

    /**
     * LEFT JOIN self-join: pair each user with their referrer.
     * Alice and Eve have NULL referrer (organic). Bob and Carol were referred
     * by Alice. Dave was referred by Bob.
     */
    public function testSelfJoinReferrerLookup(): void
    {
        $rows = $this->ztdQuery(
            "SELECT u.name, r.name AS referrer
             FROM mp_ref_users u
             LEFT JOIN mp_ref_users r ON u.referrer_id = r.id
             ORDER BY u.id"
        );

        $this->assertCount(5, $rows);

        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertNull($rows[0]['referrer']);

        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertSame('Alice', $rows[1]['referrer']);

        $this->assertSame('Carol', $rows[2]['name']);
        $this->assertSame('Alice', $rows[2]['referrer']);

        $this->assertSame('Dave', $rows[3]['name']);
        $this->assertSame('Bob', $rows[3]['referrer']);

        $this->assertSame('Eve', $rows[4]['name']);
        $this->assertNull($rows[4]['referrer']);
    }

    /**
     * Three-level self-join: user -> referrer -> grand-referrer.
     * Dave's grand-referrer is Alice (Dave->Bob->Alice).
     * Bob and Carol have NULL grand-referrer (their referrer Alice has no referrer).
     * Alice and Eve have NULL for both referrer and grand-referrer.
     */
    public function testThreeLevelSelfJoin(): void
    {
        $rows = $this->ztdQuery(
            "SELECT u.name, r.name AS referrer, gr.name AS grand_referrer
             FROM mp_ref_users u
             LEFT JOIN mp_ref_users r ON u.referrer_id = r.id
             LEFT JOIN mp_ref_users gr ON r.referrer_id = gr.id
             ORDER BY u.id"
        );

        $this->assertCount(5, $rows);

        // Alice: no referrer, no grand-referrer
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertNull($rows[0]['referrer']);
        $this->assertNull($rows[0]['grand_referrer']);

        // Bob: referrer=Alice, grand-referrer=NULL (Alice has no referrer)
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertSame('Alice', $rows[1]['referrer']);
        $this->assertNull($rows[1]['grand_referrer']);

        // Carol: referrer=Alice, grand-referrer=NULL
        $this->assertSame('Carol', $rows[2]['name']);
        $this->assertSame('Alice', $rows[2]['referrer']);
        $this->assertNull($rows[2]['grand_referrer']);

        // Dave: referrer=Bob, grand-referrer=Alice
        $this->assertSame('Dave', $rows[3]['name']);
        $this->assertSame('Bob', $rows[3]['referrer']);
        $this->assertSame('Alice', $rows[3]['grand_referrer']);

        // Eve: no referrer, no grand-referrer
        $this->assertSame('Eve', $rows[4]['name']);
        $this->assertNull($rows[4]['referrer']);
        $this->assertNull($rows[4]['grand_referrer']);
    }

    /**
     * NOT IN with NULL-safe subquery: find users who have NOT referred anyone.
     * The WHERE referrer_id IS NOT NULL filter in the subquery is critical.
     * Without it, NOT IN would return 0 rows due to SQL NULL semantics
     * (NULL makes the entire NOT IN evaluate to UNKNOWN).
     * Expected: Carol, Dave, Eve (ids 3, 4, 5 are not in the referrer_id column).
     */
    public function testNotInWithNullableColumn(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name
             FROM mp_ref_users
             WHERE id NOT IN (
                 SELECT referrer_id FROM mp_ref_users WHERE referrer_id IS NOT NULL
             )
             ORDER BY id"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Carol', $rows[0]['name']);
        $this->assertSame('Dave', $rows[1]['name']);
        $this->assertSame('Eve', $rows[2]['name']);
    }

    /**
     * NOT IN without NULL filter: demonstrates the SQL NULL pitfall.
     * When the subquery includes NULL values, NOT IN returns 0 rows because
     * x NOT IN (1, 2, NULL) evaluates to UNKNOWN for every x.
     * This tests whether the shadow store correctly propagates NULL through NOT IN.
     */
    public function testNotInWithoutNullFilter(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name
             FROM mp_ref_users
             WHERE id NOT IN (
                 SELECT referrer_id FROM mp_ref_users
             )
             ORDER BY id"
        );

        // Standard SQL: NOT IN with NULL in subquery returns 0 rows
        $this->assertCount(0, $rows);
    }

    /**
     * COALESCE on LEFT JOIN: replace NULL referrer name with 'Organic'.
     * Alice and Eve show 'Organic'; Bob and Carol show 'Alice'; Dave shows 'Bob'.
     */
    public function testCoalesceOnNullReferrer(): void
    {
        $rows = $this->ztdQuery(
            "SELECT u.name, COALESCE(r.name, 'Organic') AS source
             FROM mp_ref_users u
             LEFT JOIN mp_ref_users r ON u.referrer_id = r.id
             ORDER BY u.id"
        );

        $this->assertCount(5, $rows);

        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Organic', $rows[0]['source']);

        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertSame('Alice', $rows[1]['source']);

        $this->assertSame('Carol', $rows[2]['name']);
        $this->assertSame('Alice', $rows[2]['source']);

        $this->assertSame('Dave', $rows[3]['name']);
        $this->assertSame('Bob', $rows[3]['source']);

        $this->assertSame('Eve', $rows[4]['name']);
        $this->assertSame('Organic', $rows[4]['source']);
    }

    /**
     * Insert a new user referred by Alice (who has the most referrals),
     * then verify the self-join still works with 6 rows and Frank has
     * referrer='Alice'.
     */
    public function testInsertFromSelfJoin(): void
    {
        $this->ztdExec(
            "INSERT INTO mp_ref_users VALUES (6, 'Frank', 1, 'referral', 0)"
        );

        $rows = $this->ztdQuery(
            "SELECT u.name, r.name AS referrer
             FROM mp_ref_users u
             LEFT JOIN mp_ref_users r ON u.referrer_id = r.id
             ORDER BY u.id"
        );

        $this->assertCount(6, $rows);

        $this->assertSame('Frank', $rows[5]['name']);
        $this->assertSame('Alice', $rows[5]['referrer']);
    }

    /**
     * UPDATE with subquery referencing the same table: add 10 reward points
     * to every user who has referred at least one other user.
     * Alice (referrer of Bob, Carol) gets 100->110.
     * Bob (referrer of Dave) gets 50->60.
     * Carol, Dave, Eve are unchanged.
     */
    public function testUpdateArithmeticOnReferrer(): void
    {
        $this->ztdExec(
            "UPDATE mp_ref_users
             SET reward_points = reward_points + 10
             WHERE id IN (
                 SELECT DISTINCT referrer_id
                 FROM mp_ref_users
                 WHERE referrer_id IS NOT NULL
             )"
        );

        $rows = $this->ztdQuery(
            "SELECT name, reward_points FROM mp_ref_users ORDER BY id"
        );

        $this->assertCount(5, $rows);
        $this->assertEquals(110, (int) $rows[0]['reward_points']); // Alice
        $this->assertEquals(60, (int) $rows[1]['reward_points']);   // Bob
        $this->assertEquals(75, (int) $rows[2]['reward_points']);   // Carol
        $this->assertEquals(30, (int) $rows[3]['reward_points']);   // Dave
        $this->assertEquals(200, (int) $rows[4]['reward_points']); // Eve
    }

    /**
     * DELETE WHERE referrer_id IS NULL: removes organic users (Alice, Eve).
     * After deletion, 3 rows remain: Bob, Carol, Dave.
     */
    public function testDeleteWhereReferrerIsNull(): void
    {
        $affected = $this->ztdExec(
            "DELETE FROM mp_ref_users WHERE referrer_id IS NULL"
        );

        $this->assertEquals(2, $affected);

        $rows = $this->ztdQuery(
            "SELECT name FROM mp_ref_users ORDER BY id"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
        $this->assertSame('Carol', $rows[1]['name']);
        $this->assertSame('Dave', $rows[2]['name']);
    }
}
