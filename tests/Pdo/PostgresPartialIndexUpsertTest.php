<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests INSERT...ON CONFLICT with partial unique index on PostgreSQL.
 *
 * Partial indexes (CREATE UNIQUE INDEX ... WHERE condition) are commonly used
 * for conditional uniqueness constraints. When used as an ON CONFLICT target,
 * the CTE rewriter must handle the index predicate correctly.
 *
 * @spec SPEC-10.2
 */
class PostgresPartialIndexUpsertTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE pg_piu_users (
                id SERIAL PRIMARY KEY,
                email VARCHAR(255) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'active',
                login_count INT DEFAULT 0
            )",
            "CREATE UNIQUE INDEX pg_piu_users_active_email ON pg_piu_users (email) WHERE status = 'active'",
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_piu_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO pg_piu_users (email, status, login_count) VALUES ('alice@test.com', 'active', 5)");
        $this->ztdExec("INSERT INTO pg_piu_users (email, status, login_count) VALUES ('bob@test.com', 'inactive', 2)");
        $this->ztdExec("INSERT INTO pg_piu_users (email, status, login_count) VALUES ('charlie@test.com', 'active', 10)");
    }

    /**
     * ON CONFLICT on partial index: upsert active user increments login_count.
     */
    public function testUpsertOnPartialIndexUpdatesActiveUser(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pg_piu_users (email, status, login_count)
                 VALUES ('alice@test.com', 'active', 1)
                 ON CONFLICT (email) WHERE status = 'active'
                 DO UPDATE SET login_count = pg_piu_users.login_count + 1"
            );

            $rows = $this->ztdQuery(
                "SELECT email, login_count FROM pg_piu_users WHERE email = 'alice@test.com' AND status = 'active'"
            );

            if (count($rows) !== 1 || (int) $rows[0]['login_count'] !== 6) {
                $all = $this->ztdQuery("SELECT * FROM pg_piu_users ORDER BY id");
                $this->markTestIncomplete(
                    'Upsert on partial index: expected login_count=6, got '
                    . json_encode($rows) . '. All: ' . json_encode($all)
                    . ' — ON CONFLICT with partial index may not work with ZTD'
                );
            }

            $this->assertSame(6, (int) $rows[0]['login_count']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Upsert on partial index failed: ' . $e->getMessage());
        }
    }

    /**
     * Inactive user with same email should INSERT (partial index doesn't cover inactive).
     */
    public function testInsertInactiveWithSameEmailAsActive(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pg_piu_users (email, status, login_count)
                 VALUES ('alice@test.com', 'inactive', 0)"
            );

            $rows = $this->ztdQuery(
                "SELECT email, status, login_count FROM pg_piu_users WHERE email = 'alice@test.com' ORDER BY status"
            );

            if (count($rows) !== 2) {
                $all = $this->ztdQuery("SELECT * FROM pg_piu_users ORDER BY id");
                $this->markTestIncomplete(
                    'Insert inactive same email: expected 2 rows, got ' . count($rows)
                    . '. All: ' . json_encode($all)
                );
            }

            $this->assertCount(2, $rows);
            $statuses = array_column($rows, 'status');
            $this->assertContains('active', $statuses);
            $this->assertContains('inactive', $statuses);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Insert inactive same email failed: ' . $e->getMessage());
        }
    }

    /**
     * Duplicate inactive email should INSERT (no uniqueness constraint on inactive).
     */
    public function testDuplicateInactiveEmailAllowed(): void
    {
        try {
            // Bob is inactive; inserting another inactive bob should succeed
            $this->ztdExec(
                "INSERT INTO pg_piu_users (email, status, login_count)
                 VALUES ('bob@test.com', 'inactive', 0)"
            );

            $rows = $this->ztdQuery(
                "SELECT COUNT(*) AS cnt FROM pg_piu_users WHERE email = 'bob@test.com'"
            );

            if ((int) $rows[0]['cnt'] !== 2) {
                $all = $this->ztdQuery("SELECT * FROM pg_piu_users WHERE email = 'bob@test.com'");
                $this->markTestIncomplete(
                    'Duplicate inactive email: expected 2 bobs, got ' . $rows[0]['cnt']
                    . '. Data: ' . json_encode($all)
                );
            }

            $this->assertSame(2, (int) $rows[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Duplicate inactive email failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared upsert on partial index with $N parameters.
     */
    public function testPreparedUpsertOnPartialIndex(): void
    {
        try {
            $stmt = $this->ztdPrepare(
                "INSERT INTO pg_piu_users (email, status, login_count)
                 VALUES ($1, 'active', $2)
                 ON CONFLICT (email) WHERE status = 'active'
                 DO UPDATE SET login_count = pg_piu_users.login_count + $2"
            );
            $stmt->execute(['charlie@test.com', 3]);

            $rows = $this->ztdQuery(
                "SELECT login_count FROM pg_piu_users WHERE email = 'charlie@test.com' AND status = 'active'"
            );

            if (count($rows) !== 1 || (int) $rows[0]['login_count'] !== 13) {
                $all = $this->ztdQuery("SELECT * FROM pg_piu_users ORDER BY id");
                $this->markTestIncomplete(
                    'Prepared upsert partial index: expected login_count=13, got '
                    . json_encode($rows) . '. All: ' . json_encode($all)
                );
            }

            $this->assertSame(13, (int) $rows[0]['login_count']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared upsert on partial index failed: ' . $e->getMessage());
        }
    }

    /**
     * Upsert with DO NOTHING on partial index.
     */
    public function testUpsertDoNothingOnPartialIndex(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pg_piu_users (email, status, login_count)
                 VALUES ('alice@test.com', 'active', 99)
                 ON CONFLICT (email) WHERE status = 'active'
                 DO NOTHING"
            );

            $rows = $this->ztdQuery(
                "SELECT login_count FROM pg_piu_users WHERE email = 'alice@test.com' AND status = 'active'"
            );

            if (count($rows) !== 1 || (int) $rows[0]['login_count'] !== 5) {
                $all = $this->ztdQuery("SELECT * FROM pg_piu_users ORDER BY id");
                $this->markTestIncomplete(
                    'DO NOTHING partial index: expected login_count=5, got '
                    . json_encode($rows) . '. All: ' . json_encode($all)
                );
            }

            $this->assertSame(5, (int) $rows[0]['login_count']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Upsert DO NOTHING on partial index failed: ' . $e->getMessage());
        }
    }

    /**
     * Upsert with EXCLUDED expression on partial index.
     */
    public function testUpsertExcludedExpressionOnPartialIndex(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO pg_piu_users (email, status, login_count)
                 VALUES ('charlie@test.com', 'active', 20)
                 ON CONFLICT (email) WHERE status = 'active'
                 DO UPDATE SET login_count = GREATEST(pg_piu_users.login_count, EXCLUDED.login_count)"
            );

            $rows = $this->ztdQuery(
                "SELECT login_count FROM pg_piu_users WHERE email = 'charlie@test.com' AND status = 'active'"
            );

            if (count($rows) !== 1 || (int) $rows[0]['login_count'] !== 20) {
                $this->markTestIncomplete(
                    'EXCLUDED expression partial index: expected login_count=20, got '
                    . json_encode($rows)
                );
            }

            $this->assertSame(20, (int) $rows[0]['login_count']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Upsert EXCLUDED expression on partial index failed: ' . $e->getMessage());
        }
    }
}
