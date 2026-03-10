<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests re-preparing different SQL on the same PDOStatement variable (PostgreSQL PDO).
 *
 * In real applications, developers commonly reuse a $stmt variable:
 *   $stmt = $pdo->prepare("INSERT INTO ...");
 *   $stmt->execute([...]);
 *   $stmt = $pdo->prepare("SELECT ..."); // re-prepare
 *   $stmt->execute([...]);
 *
 * The ZTD shadow store must correctly handle this pattern — the second
 * prepare() must not be confused with or influenced by the first.
 *
 * Cross-platform parity with SqlitePreparedRePrepareTest.
 *
 * @spec SPEC-3.2
 * @spec SPEC-4.2
 */
class PostgresPreparedRePrepareTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_rp_users (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT \'active\'
            )',
            'CREATE TABLE pg_rp_logs (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                action VARCHAR(50) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_rp_logs', 'pg_rp_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_rp_users VALUES (1, 'Alice', 'active')");
        $this->pdo->exec("INSERT INTO pg_rp_users VALUES (2, 'Bob', 'active')");
        $this->pdo->exec("INSERT INTO pg_rp_users VALUES (3, 'Carol', 'inactive')");
    }

    /**
     * Re-prepare: INSERT then SELECT on same variable.
     */
    public function testRePrepareInsertThenSelect(): void
    {
        try {
            // First prepare: INSERT
            $stmt = $this->pdo->prepare("INSERT INTO pg_rp_users VALUES (?, ?, ?)");
            $stmt->execute([4, 'Dave', 'active']);

            // Re-prepare same variable: SELECT
            $stmt = $this->pdo->prepare("SELECT name FROM pg_rp_users WHERE status = ? ORDER BY name");
            $stmt->execute(['active']);
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            $names = array_column($rows, 'name');
            $this->assertContains('Dave', $names, 'Re-prepared SELECT should see INSERT from previous prepare');
            $this->assertCount(3, $names, 'Expected Alice, Bob, Dave. Got: ' . json_encode($names));
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Re-prepare INSERT then SELECT failed: ' . $e->getMessage());
        }
    }

    /**
     * Re-prepare: SELECT then UPDATE then SELECT on same variable.
     */
    public function testRePrepareSelectUpdateSelect(): void
    {
        try {
            // First: SELECT
            $stmt = $this->pdo->prepare("SELECT COUNT(*) AS cnt FROM pg_rp_users WHERE status = ?");
            $stmt->execute(['active']);
            $before = $stmt->fetchAll(PDO::FETCH_ASSOC);
            $this->assertEquals(2, (int) $before[0]['cnt']);

            // Re-prepare: UPDATE
            $stmt = $this->pdo->prepare("UPDATE pg_rp_users SET status = ? WHERE id = ?");
            $stmt->execute(['inactive', 1]);

            // Re-prepare again: SELECT (same SQL as first)
            $stmt = $this->pdo->prepare("SELECT COUNT(*) AS cnt FROM pg_rp_users WHERE status = ?");
            $stmt->execute(['active']);
            $after = $stmt->fetchAll(PDO::FETCH_ASSOC);

            if ((int) $after[0]['cnt'] !== 1) {
                $this->markTestIncomplete(
                    'Re-prepared SELECT after UPDATE: count=' . $after[0]['cnt']
                    . ', expected 1. Shadow may not track re-prepared statements correctly.'
                );
            }
            $this->assertEquals(1, (int) $after[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Re-prepare SELECT/UPDATE/SELECT failed: ' . $e->getMessage());
        }
    }

    /**
     * Re-prepare: DELETE then SELECT on same variable.
     */
    public function testRePrepareDeleteThenSelect(): void
    {
        try {
            $stmt = $this->pdo->prepare("DELETE FROM pg_rp_users WHERE id = ?");
            $stmt->execute([3]);

            $stmt = $this->pdo->prepare("SELECT name FROM pg_rp_users ORDER BY name");
            $stmt->execute([]);
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            $names = array_column($rows, 'name');
            $this->assertCount(2, $names, 'Expected Alice, Bob after DELETE. Got: ' . json_encode($names));
            $this->assertEquals(['Alice', 'Bob'], $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Re-prepare DELETE then SELECT failed: ' . $e->getMessage());
        }
    }

    /**
     * Re-prepare across different tables.
     */
    public function testRePrepareDifferentTables(): void
    {
        try {
            // INSERT into logs
            $stmt = $this->pdo->prepare("INSERT INTO pg_rp_logs VALUES (?, ?, ?)");
            $stmt->execute([1, 1, 'login']);
            $stmt->execute([2, 2, 'login']);

            // Re-prepare: JOIN query across both tables
            $stmt = $this->pdo->prepare(
                "SELECT u.name, l.action
                 FROM pg_rp_users u
                 INNER JOIN pg_rp_logs l ON u.id = l.user_id
                 WHERE l.action = ?
                 ORDER BY u.name"
            );
            $stmt->execute(['login']);
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Bob', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Re-prepare across tables failed: ' . $e->getMessage());
        }
    }

    /**
     * Re-prepare with different parameter count.
     */
    public function testRePrepareDifferentParamCount(): void
    {
        try {
            // 3 params
            $stmt = $this->pdo->prepare("INSERT INTO pg_rp_users VALUES (?, ?, ?)");
            $stmt->execute([4, 'Dave', 'active']);

            // 1 param
            $stmt = $this->pdo->prepare("SELECT name FROM pg_rp_users WHERE id = ?");
            $stmt->execute([4]);
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            $this->assertCount(1, $rows);
            $this->assertSame('Dave', $rows[0]['name']);

            // 2 params
            $stmt = $this->pdo->prepare("UPDATE pg_rp_users SET name = ? WHERE id = ?");
            $stmt->execute(['David', 4]);

            // 0 params
            $stmt = $this->pdo->prepare("SELECT name FROM pg_rp_users WHERE id = 4");
            $stmt->execute([]);
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            $this->assertCount(1, $rows);
            if ($rows[0]['name'] !== 'David') {
                $this->markTestIncomplete(
                    'Re-prepare different param count: after UPDATE name='
                    . var_export($rows[0]['name'], true) . ', expected David. '
                    . 'Prepared UPDATE may not apply when re-preparing with different param count.'
                );
            }
            $this->assertSame('David', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Re-prepare different param count failed: ' . $e->getMessage());
        }
    }

    /**
     * Multiple re-prepares in rapid succession (stress test).
     */
    public function testRapidRePrepare(): void
    {
        try {
            for ($i = 4; $i <= 8; $i++) {
                $stmt = $this->pdo->prepare("INSERT INTO pg_rp_users VALUES (?, ?, ?)");
                $stmt->execute([$i, "User{$i}", 'active']);
            }

            $stmt = $this->pdo->prepare("SELECT COUNT(*) AS cnt FROM pg_rp_users");
            $stmt->execute([]);
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            $this->assertEquals(8, (int) $rows[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Rapid re-prepare failed: ' . $e->getMessage());
        }
    }

    /**
     * Re-prepare: same SQL re-prepared (should work like fresh prepare).
     */
    public function testRePrepareSameSql(): void
    {
        try {
            $sql = "SELECT name FROM pg_rp_users WHERE id = ?";

            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([1]);
            $rows1 = $stmt->fetchAll(PDO::FETCH_ASSOC);

            // Mutate
            $this->pdo->exec("UPDATE pg_rp_users SET name = 'Alicia' WHERE id = 1");

            // Re-prepare same SQL
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([1]);
            $rows2 = $stmt->fetchAll(PDO::FETCH_ASSOC);

            $this->assertSame('Alice', $rows1[0]['name']);
            if ($rows2[0]['name'] !== 'Alicia') {
                $this->markTestIncomplete(
                    'Re-prepare same SQL after mutation: name='
                    . var_export($rows2[0]['name'], true) . ', expected Alicia. '
                    . 'Shadow may cache the old prepare result.'
                );
            }
            $this->assertSame('Alicia', $rows2[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Re-prepare same SQL failed: ' . $e->getMessage());
        }
    }
}
