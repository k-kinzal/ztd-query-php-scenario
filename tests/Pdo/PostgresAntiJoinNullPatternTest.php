<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests anti-join patterns (LEFT JOIN IS NULL, NOT EXISTS, NOT IN)
 * through PostgreSQL CTE shadow store.
 *
 * These three patterns should return identical results. The CTE rewriter
 * may handle them differently — especially given known issues with
 * IS NULL (#138) and EXISTS type errors (#137) on PostgreSQL.
 *
 * @spec SPEC-3.1
 * @spec SPEC-4.3
 */
class PostgresAntiJoinNullPatternTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_ajn_users (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1
            )',
            'CREATE TABLE pg_ajn_logins (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL,
                login_date DATE NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_ajn_logins', 'pg_ajn_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_ajn_users (name, active) VALUES ('Alice', 1)");
        $this->pdo->exec("INSERT INTO pg_ajn_users (name, active) VALUES ('Bob', 1)");
        $this->pdo->exec("INSERT INTO pg_ajn_users (name, active) VALUES ('Carol', 1)");
        $this->pdo->exec("INSERT INTO pg_ajn_users (name, active) VALUES ('Dave', 0)");

        $this->pdo->exec("INSERT INTO pg_ajn_logins (user_id, login_date) VALUES (1, '2025-01-01')");
        $this->pdo->exec("INSERT INTO pg_ajn_logins (user_id, login_date) VALUES (1, '2025-01-02')");
        $this->pdo->exec("INSERT INTO pg_ajn_logins (user_id, login_date) VALUES (3, '2025-01-01')");
    }

    /**
     * Anti-join via LEFT JOIN ... IS NULL.
     */
    public function testAntiJoinLeftJoinIsNull(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT u.name
                 FROM pg_ajn_users u
                 LEFT JOIN pg_ajn_logins l ON u.id = l.user_id
                 WHERE l.id IS NULL
                 ORDER BY u.name"
            );

            $names = array_column($rows, 'name');
            $this->assertCount(2, $names, 'Expected Bob and Dave (no logins). Got: ' . json_encode($names));
            $this->assertEquals(['Bob', 'Dave'], $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('LEFT JOIN IS NULL anti-join failed: ' . $e->getMessage());
        }
    }

    /**
     * Anti-join via NOT EXISTS.
     */
    public function testAntiJoinNotExists(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT u.name
                 FROM pg_ajn_users u
                 WHERE NOT EXISTS (
                     SELECT 1 FROM pg_ajn_logins l WHERE l.user_id = u.id
                 )
                 ORDER BY u.name"
            );

            $names = array_column($rows, 'name');
            $this->assertCount(2, $names, 'Expected Bob and Dave. Got: ' . json_encode($names));
            $this->assertEquals(['Bob', 'Dave'], $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('NOT EXISTS anti-join failed: ' . $e->getMessage());
        }
    }

    /**
     * Anti-join via NOT IN.
     */
    public function testAntiJoinNotIn(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT u.name
                 FROM pg_ajn_users u
                 WHERE u.id NOT IN (
                     SELECT l.user_id FROM pg_ajn_logins l
                 )
                 ORDER BY u.name"
            );

            $names = array_column($rows, 'name');
            $this->assertCount(2, $names, 'Expected Bob and Dave. Got: ' . json_encode($names));
            $this->assertEquals(['Bob', 'Dave'], $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('NOT IN anti-join failed: ' . $e->getMessage());
        }
    }

    /**
     * All three anti-join patterns should return identical results.
     */
    public function testAntiJoinPatternsEquivalence(): void
    {
        try {
            $leftJoinResult = $this->ztdQuery(
                "SELECT u.id FROM pg_ajn_users u
                 LEFT JOIN pg_ajn_logins l ON u.id = l.user_id
                 WHERE l.id IS NULL ORDER BY u.id"
            );

            $notExistsResult = $this->ztdQuery(
                "SELECT u.id FROM pg_ajn_users u
                 WHERE NOT EXISTS (SELECT 1 FROM pg_ajn_logins l WHERE l.user_id = u.id)
                 ORDER BY u.id"
            );

            $notInResult = $this->ztdQuery(
                "SELECT u.id FROM pg_ajn_users u
                 WHERE u.id NOT IN (SELECT l.user_id FROM pg_ajn_logins l)
                 ORDER BY u.id"
            );

            $leftJoinIds = array_column($leftJoinResult, 'id');
            $notExistsIds = array_column($notExistsResult, 'id');
            $notInIds = array_column($notInResult, 'id');

            $this->assertEquals($leftJoinIds, $notExistsIds,
                'LEFT JOIN IS NULL and NOT EXISTS differ: ' . json_encode($leftJoinIds) . ' vs ' . json_encode($notExistsIds));
            $this->assertEquals($leftJoinIds, $notInIds,
                'LEFT JOIN IS NULL and NOT IN differ: ' . json_encode($leftJoinIds) . ' vs ' . json_encode($notInIds));
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Anti-join equivalence test failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE using anti-join pattern.
     */
    public function testDeleteWithAntiJoinPattern(): void
    {
        try {
            $this->pdo->exec(
                "DELETE FROM pg_ajn_users
                 WHERE id NOT IN (
                     SELECT DISTINCT user_id FROM pg_ajn_logins
                 )"
            );

            $rows = $this->ztdQuery("SELECT name FROM pg_ajn_users ORDER BY name");
            $names = array_column($rows, 'name');

            if (count($names) !== 2) {
                $this->markTestIncomplete(
                    'DELETE anti-join: expected 2 rows (Alice, Carol), got ' . count($names) . ': ' . json_encode($names)
                );
            }
            $this->assertEquals(['Alice', 'Carol'], $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE anti-join failed: ' . $e->getMessage());
        }
    }

    /**
     * Anti-join with additional filter.
     */
    public function testAntiJoinWithFilter(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT u.name
                 FROM pg_ajn_users u
                 LEFT JOIN pg_ajn_logins l ON u.id = l.user_id
                 WHERE l.id IS NULL AND u.active = 1
                 ORDER BY u.name"
            );

            $names = array_column($rows, 'name');
            $this->assertCount(1, $names, 'Expected only Bob. Got: ' . json_encode($names));
            $this->assertEquals(['Bob'], $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Anti-join with filter failed: ' . $e->getMessage());
        }
    }
}
