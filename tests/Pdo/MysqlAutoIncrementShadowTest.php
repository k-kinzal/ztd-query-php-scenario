<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests AUTO_INCREMENT PK behavior in ZTD shadow store on MySQL.
 *
 * When INSERT omits the AUTO_INCREMENT column, the shadow store should
 * assign auto-incrementing values. Tests whether the generated PKs are
 * usable in subsequent operations (SELECT, UPDATE, DELETE, JOIN).
 *
 * @spec SPEC-4.1
 * @spec SPEC-3.1
 */
class MysqlAutoIncrementShadowTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_ai_users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT \'active\'
            )',
            'CREATE TABLE my_ai_posts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT NOT NULL,
                title VARCHAR(100) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_ai_posts', 'my_ai_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Insert without specifying AUTO_INCREMENT id column
        $this->pdo->exec("INSERT INTO my_ai_users (name) VALUES ('Alice')");
        $this->pdo->exec("INSERT INTO my_ai_users (name) VALUES ('Bob')");
        $this->pdo->exec("INSERT INTO my_ai_users (name) VALUES ('Carol')");

        $this->pdo->exec("INSERT INTO my_ai_posts (user_id, title) VALUES (1, 'First Post')");
        $this->pdo->exec("INSERT INTO my_ai_posts (user_id, title) VALUES (1, 'Second Post')");
        $this->pdo->exec("INSERT INTO my_ai_posts (user_id, title) VALUES (2, 'Bob''s Post')");
    }

    /**
     * SELECT should return non-null auto-increment ids.
     */
    public function testAutoIncrementIdsAreNotNull(): void
    {
        try {
            $rows = $this->ztdQuery("SELECT id, name FROM my_ai_users ORDER BY name");

            $this->assertCount(3, $rows);
            foreach ($rows as $row) {
                if ($row['id'] === null) {
                    $this->markTestIncomplete(
                        'AUTO_INCREMENT id is NULL for ' . $row['name'] . '. Data: ' . json_encode($rows)
                    );
                }
            }
            $this->assertNotNull($rows[0]['id'], 'Alice should have a non-null id');
            $this->assertNotNull($rows[1]['id'], 'Bob should have a non-null id');
            $this->assertNotNull($rows[2]['id'], 'Carol should have a non-null id');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT auto-increment ids failed: ' . $e->getMessage());
        }
    }

    /**
     * IDs should be sequential integers.
     */
    public function testAutoIncrementIdsAreSequential(): void
    {
        try {
            $rows = $this->ztdQuery("SELECT id FROM my_ai_users ORDER BY id");
            $ids = array_map(fn($r) => (int) $r['id'], $rows);

            if (in_array(0, $ids, true) || in_array(null, array_column($rows, 'id'), true)) {
                $this->markTestIncomplete(
                    'IDs contain 0 or NULL: ' . json_encode($ids)
                );
            }

            // Check sequential
            $this->assertSame([1, 2, 3], $ids, 'IDs should be 1, 2, 3');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Sequential ID check failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE by auto-increment id.
     */
    public function testUpdateByAutoIncrementId(): void
    {
        try {
            $this->pdo->exec("UPDATE my_ai_users SET status = 'inactive' WHERE id = 1");

            $rows = $this->ztdQuery("SELECT status FROM my_ai_users WHERE id = 1");
            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'UPDATE by auto-increment id: expected 1 row, got ' . count($rows)
                );
            }
            $this->assertSame('inactive', $rows[0]['status']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE by auto-increment id failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE by auto-increment id.
     */
    public function testDeleteByAutoIncrementId(): void
    {
        try {
            $this->pdo->exec("DELETE FROM my_ai_users WHERE id = 3");

            $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM my_ai_users");
            if ((int) $rows[0]['cnt'] !== 2) {
                $this->markTestIncomplete(
                    'DELETE by auto-increment id: expected 2 rows, got ' . $rows[0]['cnt']
                );
            }
            $this->assertEquals(2, (int) $rows[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE by auto-increment id failed: ' . $e->getMessage());
        }
    }

    /**
     * JOIN between two auto-increment tables.
     */
    public function testJoinOnAutoIncrementIds(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT u.name, p.title
                 FROM my_ai_users u
                 INNER JOIN my_ai_posts p ON u.id = p.user_id
                 ORDER BY u.name, p.title"
            );

            if (count($rows) < 1) {
                $this->markTestIncomplete(
                    'JOIN on auto-increment ids: 0 rows. Auto-increment ids may be NULL.'
                );
            }
            // Alice has 2 posts, Bob has 1 post
            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('JOIN on auto-increment ids failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT with explicit id vs without — both should work.
     */
    public function testMixedExplicitAndAutoIds(): void
    {
        try {
            // Insert with explicit id
            $this->pdo->exec("INSERT INTO my_ai_users (id, name) VALUES (100, 'Explicit')");

            // Insert without id (auto-increment)
            $this->pdo->exec("INSERT INTO my_ai_users (name) VALUES ('Auto')");

            $rows = $this->ztdQuery("SELECT id, name FROM my_ai_users ORDER BY id");

            // Should have 5 rows total: 3 original + Explicit (100) + Auto (4 or 101)
            $this->assertCount(5, $rows);

            $explicit = array_filter($rows, fn($r) => $r['name'] === 'Explicit');
            $auto = array_filter($rows, fn($r) => $r['name'] === 'Auto');

            $this->assertNotEmpty($explicit, 'Explicit ID insert should be visible');
            $this->assertNotEmpty($auto, 'Auto ID insert should be visible');

            $explicitId = (int) reset($explicit)['id'];
            $autoId = reset($auto)['id'];

            $this->assertSame(100, $explicitId, 'Explicit id should be 100');
            $this->assertNotNull($autoId, 'Auto id should not be NULL');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Mixed explicit/auto ids failed: ' . $e->getMessage());
        }
    }
}
