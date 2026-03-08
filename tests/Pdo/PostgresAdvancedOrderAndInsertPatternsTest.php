<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests advanced ORDER BY patterns and interleaved prepared statements on PostgreSQL.
 * @spec pending
 */
class PostgresAdvancedOrderAndInsertPatternsTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE aoi_users_pg (id INT PRIMARY KEY, name VARCHAR(50), role VARCHAR(20), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['aoi_users_pg'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO aoi_users_pg VALUES (1, 'Alice', 'admin', 90)");
        $this->pdo->exec("INSERT INTO aoi_users_pg VALUES (2, 'Bob', 'user', 70)");
        $this->pdo->exec("INSERT INTO aoi_users_pg VALUES (3, 'Charlie', 'moderator', 85)");
        $this->pdo->exec("INSERT INTO aoi_users_pg VALUES (4, 'Diana', 'admin', 95)");
    }

    public function testCaseWhenInOrderBy(): void
    {
        $stmt = $this->pdo->query(
            "SELECT name, role FROM aoi_users_pg ORDER BY
             CASE role
                 WHEN 'admin' THEN 1
                 WHEN 'moderator' THEN 2
                 ELSE 3
             END, name"
        );
        $names = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame('Alice', $names[0]);
        $this->assertSame('Diana', $names[1]);
        $this->assertSame('Charlie', $names[2]);
        $this->assertSame('Bob', $names[3]);
    }

    public function testCaseWhenInOrderByWithPrepared(): void
    {
        $stmt = $this->pdo->prepare(
            "SELECT name FROM aoi_users_pg WHERE score > ?
             ORDER BY CASE role WHEN 'admin' THEN 1 ELSE 2 END, score DESC"
        );
        $stmt->execute([80]);
        $names = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame('Diana', $names[0]);
        $this->assertSame('Alice', $names[1]);
        $this->assertSame('Charlie', $names[2]);
    }

    public function testMultipleInterleavedPreparedStatements(): void
    {
        $stmtByRole = $this->pdo->prepare('SELECT name FROM aoi_users_pg WHERE role = ? ORDER BY name');
        $stmtByScore = $this->pdo->prepare('SELECT name FROM aoi_users_pg WHERE score > ? ORDER BY score DESC');

        $stmtByRole->execute(['admin']);
        $admins = $stmtByRole->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Alice', 'Diana'], $admins);

        $stmtByScore->execute([80]);
        $highScorers = $stmtByScore->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(3, $highScorers);

        $stmtByRole->execute(['user']);
        $users = $stmtByRole->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Bob'], $users);
    }
}
