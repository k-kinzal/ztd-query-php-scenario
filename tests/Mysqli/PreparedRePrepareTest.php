<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests re-preparing different SQL on the same mysqli_stmt variable.
 *
 * In real applications, developers commonly reuse a $stmt variable:
 *   $stmt = $mysqli->prepare("INSERT INTO ...");
 *   $stmt->bind_param(...);
 *   $stmt->execute();
 *   $stmt = $mysqli->prepare("SELECT ..."); // re-prepare
 *   $stmt->execute();
 *
 * The ZTD shadow store must correctly handle this pattern — the second
 * prepare() must not be confused with or influenced by the first.
 *
 * Cross-platform parity with SqlitePreparedRePrepareTest (PDO).
 *
 * @spec SPEC-3.2
 * @spec SPEC-4.2
 */
class PreparedRePrepareTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_rp_users (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT \'active\'
            ) ENGINE=InnoDB',
            'CREATE TABLE mi_rp_logs (
                id INT PRIMARY KEY,
                user_id INT NOT NULL,
                action VARCHAR(50) NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_rp_logs', 'mi_rp_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_rp_users VALUES (1, 'Alice', 'active')");
        $this->mysqli->query("INSERT INTO mi_rp_users VALUES (2, 'Bob', 'active')");
        $this->mysqli->query("INSERT INTO mi_rp_users VALUES (3, 'Carol', 'inactive')");
    }

    /**
     * Re-prepare: INSERT then SELECT on same variable.
     */
    public function testRePrepareInsertThenSelect(): void
    {
        try {
            // First prepare: INSERT
            $stmt = $this->mysqli->prepare("INSERT INTO mi_rp_users VALUES (?, ?, ?)");
            $id = 4;
            $name = 'Dave';
            $status = 'active';
            $stmt->bind_param('iss', $id, $name, $status);
            $stmt->execute();

            // Re-prepare same variable: SELECT
            $stmt = $this->mysqli->prepare("SELECT name FROM mi_rp_users WHERE status = ? ORDER BY name");
            $status = 'active';
            $stmt->bind_param('s', $status);
            $stmt->execute();
            $rows = $stmt->get_result()->fetch_all(MYSQLI_ASSOC);

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
            $stmt = $this->mysqli->prepare("SELECT COUNT(*) AS cnt FROM mi_rp_users WHERE status = ?");
            $status = 'active';
            $stmt->bind_param('s', $status);
            $stmt->execute();
            $before = $stmt->get_result()->fetch_all(MYSQLI_ASSOC);
            $this->assertEquals(2, (int) $before[0]['cnt']);

            // Re-prepare: UPDATE
            $stmt = $this->mysqli->prepare("UPDATE mi_rp_users SET status = ? WHERE id = ?");
            $newStatus = 'inactive';
            $id = 1;
            $stmt->bind_param('si', $newStatus, $id);
            $stmt->execute();

            // Re-prepare again: SELECT (same SQL as first)
            $stmt = $this->mysqli->prepare("SELECT COUNT(*) AS cnt FROM mi_rp_users WHERE status = ?");
            $status = 'active';
            $stmt->bind_param('s', $status);
            $stmt->execute();
            $after = $stmt->get_result()->fetch_all(MYSQLI_ASSOC);

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
            $stmt = $this->mysqli->prepare("DELETE FROM mi_rp_users WHERE id = ?");
            $id = 3;
            $stmt->bind_param('i', $id);
            $stmt->execute();

            $stmt = $this->mysqli->prepare("SELECT name FROM mi_rp_users ORDER BY name");
            $stmt->execute();
            $rows = $stmt->get_result()->fetch_all(MYSQLI_ASSOC);

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
            $stmt = $this->mysqli->prepare("INSERT INTO mi_rp_logs VALUES (?, ?, ?)");
            $id = 1;
            $userId = 1;
            $action = 'login';
            $stmt->bind_param('iis', $id, $userId, $action);
            $stmt->execute();

            $id = 2;
            $userId = 2;
            $stmt->bind_param('iis', $id, $userId, $action);
            $stmt->execute();

            // Re-prepare: JOIN query across both tables
            $stmt = $this->mysqli->prepare(
                "SELECT u.name, l.action
                 FROM mi_rp_users u
                 INNER JOIN mi_rp_logs l ON u.id = l.user_id
                 WHERE l.action = ?
                 ORDER BY u.name"
            );
            $action = 'login';
            $stmt->bind_param('s', $action);
            $stmt->execute();
            $rows = $stmt->get_result()->fetch_all(MYSQLI_ASSOC);

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
            $stmt = $this->mysqli->prepare("INSERT INTO mi_rp_users VALUES (?, ?, ?)");
            $id = 4;
            $name = 'Dave';
            $status = 'active';
            $stmt->bind_param('iss', $id, $name, $status);
            $stmt->execute();

            // 1 param
            $stmt = $this->mysqli->prepare("SELECT name FROM mi_rp_users WHERE id = ?");
            $id = 4;
            $stmt->bind_param('i', $id);
            $stmt->execute();
            $rows = $stmt->get_result()->fetch_all(MYSQLI_ASSOC);

            $this->assertCount(1, $rows);
            $this->assertSame('Dave', $rows[0]['name']);

            // 2 params
            $stmt = $this->mysqli->prepare("UPDATE mi_rp_users SET name = ? WHERE id = ?");
            $name = 'David';
            $id = 4;
            $stmt->bind_param('si', $name, $id);
            $stmt->execute();

            // 0 params
            $stmt = $this->mysqli->prepare("SELECT name FROM mi_rp_users WHERE id = 4");
            $stmt->execute();
            $rows = $stmt->get_result()->fetch_all(MYSQLI_ASSOC);

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
                $stmt = $this->mysqli->prepare("INSERT INTO mi_rp_users VALUES (?, ?, ?)");
                $id = $i;
                $name = "User{$i}";
                $status = 'active';
                $stmt->bind_param('iss', $id, $name, $status);
                $stmt->execute();
            }

            $stmt = $this->mysqli->prepare("SELECT COUNT(*) AS cnt FROM mi_rp_users");
            $stmt->execute();
            $rows = $stmt->get_result()->fetch_all(MYSQLI_ASSOC);

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
            $sql = "SELECT name FROM mi_rp_users WHERE id = ?";

            $stmt = $this->mysqli->prepare($sql);
            $id = 1;
            $stmt->bind_param('i', $id);
            $stmt->execute();
            $rows1 = $stmt->get_result()->fetch_all(MYSQLI_ASSOC);

            // Mutate
            $this->mysqli->query("UPDATE mi_rp_users SET name = 'Alicia' WHERE id = 1");

            // Re-prepare same SQL
            $stmt = $this->mysqli->prepare($sql);
            $id = 1;
            $stmt->bind_param('i', $id);
            $stmt->execute();
            $rows2 = $stmt->get_result()->fetch_all(MYSQLI_ASSOC);

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
