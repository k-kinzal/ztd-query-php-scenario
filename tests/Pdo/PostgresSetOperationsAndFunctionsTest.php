<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests set operations (EXCEPT, INTERSECT), string/math functions, and multiple CTEs on PostgreSQL PDO.
 * @spec SPEC-3.3d
 */
class PostgresSetOperationsAndFunctionsTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_sof_users (id INT PRIMARY KEY, name VARCHAR(255), email VARCHAR(255), score INT)',
            'CREATE TABLE pg_sof_vip (id INT PRIMARY KEY, name VARCHAR(255), email VARCHAR(255))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_sof_vip', 'pg_sof_users'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_sof_users (id, name, email, score) VALUES (1, 'Alice', 'alice@test.com', 90)");
        $this->pdo->exec("INSERT INTO pg_sof_users (id, name, email, score) VALUES (2, 'Bob', 'bob@test.com', 80)");
        $this->pdo->exec("INSERT INTO pg_sof_users (id, name, email, score) VALUES (3, 'Charlie', 'charlie@test.com', 70)");
        $this->pdo->exec("INSERT INTO pg_sof_vip (id, name, email) VALUES (1, 'Alice', 'alice@test.com')");
    }

    public function testExceptReturnsRowsNotInSecondSet(): void
    {
        $stmt = $this->pdo->query("
            SELECT name FROM pg_sof_users
            EXCEPT
            SELECT name FROM pg_sof_vip
            ORDER BY name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
        $this->assertSame('Charlie', $rows[1]['name']);
    }

    public function testIntersectReturnsCommonRows(): void
    {
        $stmt = $this->pdo->query("
            SELECT name FROM pg_sof_users
            INTERSECT
            SELECT name FROM pg_sof_vip
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public function testStringFunctionsConcatUpperLower(): void
    {
        $stmt = $this->pdo->query("SELECT UPPER(name) || ' <' || LOWER(email) || '>' AS display FROM pg_sof_users WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('ALICE <alice@test.com>', $row['display']);
    }

    public function testSubstrAndLength(): void
    {
        $stmt = $this->pdo->query("SELECT LENGTH(name) AS len, SUBSTRING(name FROM 1 FOR 3) AS sub FROM pg_sof_users WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(5, (int) $row['len']);
        $this->assertSame('Ali', $row['sub']);
    }

    public function testStringAgg(): void
    {
        $stmt = $this->pdo->query("SELECT STRING_AGG(name, ', ' ORDER BY name) AS names FROM pg_sof_users");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice, Bob, Charlie', $row['names']);
    }

    public function testMathFunctions(): void
    {
        $this->pdo->exec("UPDATE pg_sof_users SET score = -15 WHERE id = 1");

        $stmt = $this->pdo->query("SELECT ABS(score) AS abs_score, CEIL(score * 1.5) AS ceiled, FLOOR(score * 1.5) AS floored FROM pg_sof_users WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(15, (int) $row['abs_score']);
        $this->assertSame(-22, (int) $row['ceiled']);
        $this->assertSame(-23, (int) $row['floored']);
    }

    public function testAggregatesOnEmptySet(): void
    {
        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt, SUM(score) AS total, AVG(score) AS avg_score FROM pg_sof_users WHERE id > 999");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $row['cnt']);
        $this->assertNull($row['total']);
        $this->assertNull($row['avg_score']);
    }

    public function testNotInWithSubquery(): void
    {
        $stmt = $this->pdo->query("SELECT name FROM pg_sof_users WHERE name NOT IN (SELECT name FROM pg_sof_vip) ORDER BY name");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
    }

    public function testJoinUsingSyntax(): void
    {
        $stmt = $this->pdo->query("SELECT pg_sof_users.name, pg_sof_users.score FROM pg_sof_users JOIN pg_sof_vip USING (id)");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public function testZeroMatchUpdateDelete(): void
    {
        $affected = $this->pdo->exec("UPDATE pg_sof_users SET score = 0 WHERE id = 999");
        $this->assertSame(0, $affected);

        $affected = $this->pdo->exec("DELETE FROM pg_sof_users WHERE id = 999");
        $this->assertSame(0, $affected);
    }
}
