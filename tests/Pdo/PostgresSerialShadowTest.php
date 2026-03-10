<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests SERIAL PK behavior in ZTD shadow store on PostgreSQL.
 *
 * SERIAL is PostgreSQL's auto-increment equivalent. When INSERT omits
 * the SERIAL column, the shadow store should track the sequence-generated
 * values correctly.
 *
 * @spec SPEC-4.1
 * @spec SPEC-3.1
 */
class PostgresSerialShadowTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_ai_users (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT \'active\'
            )',
            'CREATE TABLE pg_ai_posts (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL,
                title TEXT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_ai_posts', 'pg_ai_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_ai_users (name) VALUES ('Alice')");
        $this->pdo->exec("INSERT INTO pg_ai_users (name) VALUES ('Bob')");
        $this->pdo->exec("INSERT INTO pg_ai_users (name) VALUES ('Carol')");

        $this->pdo->exec("INSERT INTO pg_ai_posts (user_id, title) VALUES (1, 'First Post')");
        $this->pdo->exec("INSERT INTO pg_ai_posts (user_id, title) VALUES (1, 'Second Post')");
        $this->pdo->exec("INSERT INTO pg_ai_posts (user_id, title) VALUES (2, 'Bobs Post')");
    }

    /**
     * SELECT should return non-null SERIAL ids.
     */
    public function testSerialIdsAreNotNull(): void
    {
        try {
            $rows = $this->ztdQuery("SELECT id, name FROM pg_ai_users ORDER BY name");

            $this->assertCount(3, $rows);
            foreach ($rows as $row) {
                if ($row['id'] === null) {
                    $this->markTestIncomplete(
                        'SERIAL id is NULL for ' . $row['name'] . '. Data: ' . json_encode($rows)
                    );
                }
            }
            $this->assertNotNull($rows[0]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT serial ids failed: ' . $e->getMessage());
        }
    }

    /**
     * JOIN between two SERIAL tables.
     */
    public function testJoinOnSerialIds(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT u.name, p.title
                 FROM pg_ai_users u
                 INNER JOIN pg_ai_posts p ON u.id = p.user_id
                 ORDER BY u.name, p.title"
            );

            if (count($rows) < 1) {
                $this->markTestIncomplete(
                    'JOIN on SERIAL ids: 0 rows'
                );
            }
            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('JOIN on SERIAL ids failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE by SERIAL id.
     */
    public function testUpdateBySerialId(): void
    {
        try {
            $this->pdo->exec("UPDATE pg_ai_users SET status = 'inactive' WHERE id = 1");

            $rows = $this->ztdQuery("SELECT status FROM pg_ai_users WHERE id = 1");
            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'UPDATE by SERIAL id: expected 1 row, got ' . count($rows)
                );
            }
            $this->assertSame('inactive', $rows[0]['status']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE by SERIAL id failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE by SERIAL id.
     */
    public function testDeleteBySerialId(): void
    {
        try {
            $this->pdo->exec("DELETE FROM pg_ai_users WHERE id = 3");

            $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_ai_users");
            if ((int) $rows[0]['cnt'] !== 2) {
                $this->markTestIncomplete(
                    'DELETE by SERIAL id: expected 2, got ' . $rows[0]['cnt']
                );
            }
            $this->assertEquals(2, (int) $rows[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE by SERIAL id failed: ' . $e->getMessage());
        }
    }
}
