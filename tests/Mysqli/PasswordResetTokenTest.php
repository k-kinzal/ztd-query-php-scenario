<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests token-based password reset authentication flows through ZTD shadow store (MySQLi).
 * Covers token insertion with expiry dates, valid/expired token lookup, one-time token
 * consumption, expired token cleanup, EXISTS subquery for validity, and physical isolation.
 * @spec SPEC-10.2.113
 */
class PasswordResetTokenTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_prt_users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                email VARCHAR(255),
                status VARCHAR(20)
            )',
            'CREATE TABLE mi_prt_tokens (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT,
                token VARCHAR(255),
                expires_at DATE,
                consumed INT DEFAULT 0
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_prt_tokens', 'mi_prt_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 3 users
        $this->mysqli->query("INSERT INTO mi_prt_users VALUES (1, 'alice@example.com', 'active')");
        $this->mysqli->query("INSERT INTO mi_prt_users VALUES (2, 'bob@example.com', 'active')");
        $this->mysqli->query("INSERT INTO mi_prt_users VALUES (3, 'carol@example.com', 'locked')");

        // 5 tokens
        $this->mysqli->query("INSERT INTO mi_prt_tokens VALUES (1, 1, 'token1', '2026-03-15', 0)");
        $this->mysqli->query("INSERT INTO mi_prt_tokens VALUES (2, 2, 'token2', '2026-02-01', 0)");
        $this->mysqli->query("INSERT INTO mi_prt_tokens VALUES (3, 1, 'token3', '2026-03-20', 0)");
        $this->mysqli->query("INSERT INTO mi_prt_tokens VALUES (4, 3, 'token4', '2026-03-10', 1)");
        $this->mysqli->query("INSERT INTO mi_prt_tokens VALUES (5, 2, 'token5', '2026-01-15', 0)");
    }

    /**
     * Find a valid (unconsumed, not expired) token by value.
     * token1 belongs to alice, expires 2026-03-15, not consumed => 1 row.
     */
    public function testFindValidToken(): void
    {
        $rows = $this->ztdQuery(
            "SELECT t.id, t.user_id, t.token, t.expires_at, t.consumed
             FROM mi_prt_tokens t
             WHERE t.consumed = 0
               AND t.expires_at >= '2026-03-09'
               AND t.token = 'token1'"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(1, (int) $rows[0]['id']);
        $this->assertEquals(1, (int) $rows[0]['user_id']);
        $this->assertSame('token1', $rows[0]['token']);
        $this->assertEquals(0, (int) $rows[0]['consumed']);
    }

    /**
     * Expired token must not be returned even if unconsumed.
     * token2 belongs to bob, expires 2026-02-01 (before 2026-03-09) => 0 rows.
     */
    public function testExpiredTokenNotReturned(): void
    {
        $rows = $this->ztdQuery(
            "SELECT t.id, t.token
             FROM mi_prt_tokens t
             WHERE t.consumed = 0
               AND t.expires_at >= '2026-03-09'
               AND t.token = 'token2'"
        );

        $this->assertCount(0, $rows);
    }

    /**
     * Consume token1: UPDATE consumed=1, verify it is consumed, then check
     * that alice's only remaining valid token is token3.
     */
    public function testConsumeToken(): void
    {
        // Consume token1
        $this->mysqli->query("UPDATE mi_prt_tokens SET consumed = 1 WHERE token = 'token1'");

        // Verify token1 is now consumed
        $rows = $this->ztdQuery("SELECT consumed FROM mi_prt_tokens WHERE token = 'token1'");
        $this->assertCount(1, $rows);
        $this->assertEquals(1, (int) $rows[0]['consumed']);

        // Alice's remaining unconsumed valid tokens
        $rows = $this->ztdQuery(
            "SELECT t.token
             FROM mi_prt_tokens t
             WHERE t.user_id = 1
               AND t.consumed = 0
               AND t.expires_at >= '2026-03-09'
             ORDER BY t.id"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('token3', $rows[0]['token']);
    }

    /**
     * Delete all expired tokens (expires_at < '2026-03-09').
     * Expired: token2 (2026-02-01), token5 (2026-01-15) => deleted.
     * Remaining: token1, token3, token4 => 3 rows.
     */
    public function testDeleteExpiredTokens(): void
    {
        $this->mysqli->query("DELETE FROM mi_prt_tokens WHERE expires_at < '2026-03-09'");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_prt_tokens");
        $this->assertEquals(3, (int) $rows[0]['cnt']);

        // Verify the remaining tokens are token1, token3, token4
        $rows = $this->ztdQuery("SELECT token FROM mi_prt_tokens ORDER BY id");
        $this->assertCount(3, $rows);
        $this->assertSame('token1', $rows[0]['token']);
        $this->assertSame('token3', $rows[1]['token']);
        $this->assertSame('token4', $rows[2]['token']);
    }

    /**
     * EXISTS subquery: find users who have at least one valid unconsumed token.
     * alice has token1 (valid) and token3 (valid). bob has only expired tokens.
     * carol has token4 but it is consumed. => Only alice returned.
     */
    public function testTokenExistsCheck(): void
    {
        $rows = $this->ztdQuery(
            "SELECT u.id, u.email
             FROM mi_prt_users u
             WHERE EXISTS (
                 SELECT 1
                 FROM mi_prt_tokens t
                 WHERE t.user_id = u.id
                   AND t.consumed = 0
                   AND t.expires_at >= '2026-03-09'
             )
             ORDER BY u.id"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(1, (int) $rows[0]['id']);
        $this->assertSame('alice@example.com', $rows[0]['email']);
    }

    /**
     * JOIN tokens with users filtered by active status: only active users'
     * unconsumed valid tokens are returned.
     * alice (active) has token1 and token3. carol (locked) has token4 (consumed anyway).
     * bob (active) has no valid tokens. => token1 and token3.
     */
    public function testLockedUserTokenIgnored(): void
    {
        $rows = $this->ztdQuery(
            "SELECT t.token, u.email
             FROM mi_prt_tokens t
             JOIN mi_prt_users u ON u.id = t.user_id
             WHERE u.status = 'active'
               AND t.consumed = 0
               AND t.expires_at >= '2026-03-09'
             ORDER BY t.id"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('token1', $rows[0]['token']);
        $this->assertSame('alice@example.com', $rows[0]['email']);
        $this->assertSame('token3', $rows[1]['token']);
        $this->assertSame('alice@example.com', $rows[1]['email']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_prt_tokens VALUES (6, 1, 'token6', '2026-04-01', 0)");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_prt_tokens");
        $this->assertSame(6, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT token FROM mi_prt_tokens WHERE id = 6");
        $this->assertSame('token6', $rows[0]['token']);

        // Physical table untouched
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_prt_tokens');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
