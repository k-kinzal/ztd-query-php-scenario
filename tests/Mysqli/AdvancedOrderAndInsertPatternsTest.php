<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests advanced ORDER BY patterns and interleaved prepared statements via MySQLi.
 *
 * Cross-platform parity with MysqlAdvancedOrderAndInsertPatternsTest (PDO).
 * @spec SPEC-4.1
 */
class AdvancedOrderAndInsertPatternsTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_aoi_users (id INT PRIMARY KEY, name VARCHAR(50), role VARCHAR(20), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['mi_aoi_users'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_aoi_users VALUES (1, 'Alice', 'admin', 90)");
        $this->mysqli->query("INSERT INTO mi_aoi_users VALUES (2, 'Bob', 'user', 70)");
        $this->mysqli->query("INSERT INTO mi_aoi_users VALUES (3, 'Charlie', 'moderator', 85)");
        $this->mysqli->query("INSERT INTO mi_aoi_users VALUES (4, 'Diana', 'admin', 95)");
    }

    public function testCaseWhenInOrderBy(): void
    {
        $result = $this->mysqli->query(
            "SELECT name, role FROM mi_aoi_users ORDER BY
             CASE role
                 WHEN 'admin' THEN 1
                 WHEN 'moderator' THEN 2
                 ELSE 3
             END, name"
        );
        $names = [];
        while ($row = $result->fetch_assoc()) {
            $names[] = $row['name'];
        }
        $this->assertSame('Alice', $names[0]);
        $this->assertSame('Diana', $names[1]);
        $this->assertSame('Charlie', $names[2]);
        $this->assertSame('Bob', $names[3]);
    }

    public function testCaseWhenInOrderByWithPrepared(): void
    {
        $stmt = $this->mysqli->prepare(
            "SELECT name FROM mi_aoi_users WHERE score > ?
             ORDER BY CASE role WHEN 'admin' THEN 1 ELSE 2 END, score DESC"
        );
        $score = 80;
        $stmt->bind_param('i', $score);
        $stmt->execute();
        $result = $stmt->get_result();
        $names = [];
        while ($row = $result->fetch_assoc()) {
            $names[] = $row['name'];
        }
        $this->assertSame('Diana', $names[0]);
        $this->assertSame('Alice', $names[1]);
        $this->assertSame('Charlie', $names[2]);
    }

    public function testMultipleInterleavedPreparedStatements(): void
    {
        $stmtByRole = $this->mysqli->prepare('SELECT name FROM mi_aoi_users WHERE role = ? ORDER BY name');
        $stmtByScore = $this->mysqli->prepare('SELECT name FROM mi_aoi_users WHERE score > ? ORDER BY score DESC');

        $role = 'admin';
        $stmtByRole->bind_param('s', $role);
        $stmtByRole->execute();
        $result = $stmtByRole->get_result();
        $admins = [];
        while ($row = $result->fetch_assoc()) {
            $admins[] = $row['name'];
        }
        $this->assertSame(['Alice', 'Diana'], $admins);

        $score = 80;
        $stmtByScore->bind_param('i', $score);
        $stmtByScore->execute();
        $result = $stmtByScore->get_result();
        $highScorers = [];
        while ($row = $result->fetch_assoc()) {
            $highScorers[] = $row['name'];
        }
        $this->assertCount(3, $highScorers);

        $role = 'user';
        $stmtByRole->bind_param('s', $role);
        $stmtByRole->execute();
        $result = $stmtByRole->get_result();
        $users = [];
        while ($row = $result->fetch_assoc()) {
            $users[] = $row['name'];
        }
        $this->assertSame(['Bob'], $users);
    }
}
