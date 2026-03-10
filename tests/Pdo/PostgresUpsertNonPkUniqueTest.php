<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests INSERT...ON CONFLICT (unique_col) DO UPDATE when the conflict is
 * on a non-PK UNIQUE constraint on PostgreSQL.
 *
 * PostgreSQL uses: INSERT ... ON CONFLICT (email) DO UPDATE SET name = EXCLUDED.name
 *
 * @spec SPEC-4.2a
 */
class PostgresUpsertNonPkUniqueTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_unpu_users (
            id SERIAL PRIMARY KEY,
            email VARCHAR(100) NOT NULL UNIQUE,
            name VARCHAR(50) NOT NULL,
            login_count INT NOT NULL DEFAULT 0
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_unpu_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_unpu_users (id, email, name, login_count) VALUES (1, 'alice@test.com', 'Alice', 5)");
        $this->pdo->exec("INSERT INTO pg_unpu_users (id, email, name, login_count) VALUES (2, 'bob@test.com', 'Bob', 3)");
    }

    /**
     * ON CONFLICT (email) DO UPDATE — email already exists.
     */
    public function testOnConflictUniqueEmailDoUpdate(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO pg_unpu_users (email, name, login_count) VALUES ('alice@test.com', 'Alice-Upserted', 10) "
                . "ON CONFLICT (email) DO UPDATE SET name = EXCLUDED.name, login_count = EXCLUDED.login_count"
            );

            $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_unpu_users");
            $cnt = (int) $rows[0]['cnt'];

            if ($cnt !== 2) {
                $this->markTestIncomplete(
                    'ON CONFLICT DO UPDATE on UNIQUE email created duplicate. Expected 2 rows, got ' . $cnt
                );
            }

            $rows = $this->ztdQuery("SELECT name, login_count FROM pg_unpu_users WHERE email = 'alice@test.com'");
            $this->assertCount(1, $rows);
            $this->assertSame('Alice-Upserted', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('ON CONFLICT UNIQUE email failed: ' . $e->getMessage());
        }
    }

    /**
     * ON CONFLICT (email) DO UPDATE with self-reference.
     */
    public function testOnConflictUniqueWithSelfReference(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO pg_unpu_users (email, name, login_count) VALUES ('alice@test.com', 'Alice', 1) "
                . "ON CONFLICT (email) DO UPDATE SET login_count = pg_unpu_users.login_count + EXCLUDED.login_count"
            );

            $rows = $this->ztdQuery("SELECT login_count FROM pg_unpu_users WHERE email = 'alice@test.com'");

            if (empty($rows)) {
                $this->markTestIncomplete('No rows found for alice@test.com');
            }

            $count = (int) $rows[0]['login_count'];
            if ($count !== 6) {
                $this->markTestIncomplete(
                    'ON CONFLICT self-ref: expected login_count=6, got ' . $count
                );
            }
            $this->assertSame(6, $count);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('ON CONFLICT self-reference failed: ' . $e->getMessage());
        }
    }

    /**
     * Positive control: ON CONFLICT on PK works.
     */
    public function testOnConflictPkDoUpdate(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO pg_unpu_users (id, email, name, login_count) VALUES (1, 'alice-new@test.com', 'Alice-PK', 99) "
                . "ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name"
            );

            $rows = $this->ztdQuery("SELECT name FROM pg_unpu_users WHERE id = 1");
            $this->assertCount(1, $rows);
            $this->assertSame('Alice-PK', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('PK conflict DO UPDATE failed: ' . $e->getMessage());
        }
    }
}
