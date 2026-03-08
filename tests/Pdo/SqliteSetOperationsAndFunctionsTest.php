<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests set operations (EXCEPT, INTERSECT), string/math functions, and multiple CTEs on SQLite.
 * @spec SPEC-3.3d
 */
class SqliteSetOperationsAndFunctionsTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, score INTEGER)',
            'CREATE TABLE vip_users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['users', 'vip_users'];
    }


    // --- Set Operations ---

    public function testExceptReturnsRowsNotInSecondSet(): void
    {
        $this->pdo->exec("INSERT INTO users (id, name, email, score) VALUES (1, 'Alice', 'alice@test.com', 90)");
        $this->pdo->exec("INSERT INTO users (id, name, email, score) VALUES (2, 'Bob', 'bob@test.com', 80)");
        $this->pdo->exec("INSERT INTO users (id, name, email, score) VALUES (3, 'Charlie', 'charlie@test.com', 70)");

        $this->pdo->exec("INSERT INTO vip_users (id, name, email) VALUES (1, 'Alice', 'alice@test.com')");

        $stmt = $this->pdo->query("
            SELECT name FROM users
            EXCEPT
            SELECT name FROM vip_users
            ORDER BY name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
        $this->assertSame('Charlie', $rows[1]['name']);
    }

    public function testIntersectReturnsCommonRows(): void
    {
        $this->pdo->exec("INSERT INTO users (id, name, email, score) VALUES (1, 'Alice', 'alice@test.com', 90)");
        $this->pdo->exec("INSERT INTO users (id, name, email, score) VALUES (2, 'Bob', 'bob@test.com', 80)");

        $this->pdo->exec("INSERT INTO vip_users (id, name, email) VALUES (1, 'Alice', 'alice@test.com')");
        $this->pdo->exec("INSERT INTO vip_users (id, name, email) VALUES (3, 'Dave', 'dave@test.com')");

        $stmt = $this->pdo->query("
            SELECT name FROM users
            INTERSECT
            SELECT name FROM vip_users
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public function testExceptAfterMutation(): void
    {
        $this->pdo->exec("INSERT INTO users (id, name, email, score) VALUES (1, 'Alice', 'alice@test.com', 90)");
        $this->pdo->exec("INSERT INTO users (id, name, email, score) VALUES (2, 'Bob', 'bob@test.com', 80)");

        $this->pdo->exec("INSERT INTO vip_users (id, name, email) VALUES (1, 'Alice', 'alice@test.com')");
        $this->pdo->exec("INSERT INTO vip_users (id, name, email) VALUES (2, 'Bob', 'bob@test.com')");

        // Delete Bob from vip_users
        $this->pdo->exec("DELETE FROM vip_users WHERE id = 2");

        $stmt = $this->pdo->query("
            SELECT name FROM users
            EXCEPT
            SELECT name FROM vip_users
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
    }

    // --- String Functions ---

    public function testUpperLower(): void
    {
        $this->pdo->exec("INSERT INTO users (id, name, email, score) VALUES (1, 'Alice', 'alice@test.com', 90)");

        $stmt = $this->pdo->query("SELECT UPPER(name) AS upper_name, LOWER(name) AS lower_name FROM users");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('ALICE', $row['upper_name']);
        $this->assertSame('alice', $row['lower_name']);
    }

    public function testLengthAndSubstr(): void
    {
        $this->pdo->exec("INSERT INTO users (id, name, email, score) VALUES (1, 'Alice', 'alice@test.com', 90)");

        $stmt = $this->pdo->query("SELECT LENGTH(name) AS len, SUBSTR(name, 1, 3) AS sub FROM users");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(5, (int) $row['len']);
        $this->assertSame('Ali', $row['sub']);
    }

    public function testReplace(): void
    {
        $this->pdo->exec("INSERT INTO users (id, name, email, score) VALUES (1, 'Alice', 'alice@test.com', 90)");

        $stmt = $this->pdo->query("SELECT REPLACE(email, '@test.com', '@example.com') AS new_email FROM users");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('alice@example.com', $row['new_email']);
    }

    public function testTrim(): void
    {
        $this->pdo->exec("INSERT INTO users (id, name, email, score) VALUES (1, '  Alice  ', 'alice@test.com', 90)");

        $stmt = $this->pdo->query("SELECT TRIM(name) AS trimmed FROM users");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['trimmed']);
    }

    public function testGroupConcat(): void
    {
        $this->pdo->exec("INSERT INTO users (id, name, email, score) VALUES (1, 'Alice', 'a@test.com', 90)");
        $this->pdo->exec("INSERT INTO users (id, name, email, score) VALUES (2, 'Bob', 'b@test.com', 80)");
        $this->pdo->exec("INSERT INTO users (id, name, email, score) VALUES (3, 'Charlie', 'c@test.com', 70)");

        $stmt = $this->pdo->query("SELECT GROUP_CONCAT(name, ', ') AS names FROM users ORDER BY name");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertNotEmpty($row['names']);
        // GROUP_CONCAT order is not guaranteed, but all names should be present
        $this->assertStringContainsString('Alice', $row['names']);
        $this->assertStringContainsString('Bob', $row['names']);
    }

    // --- Math Functions ---

    public function testAbsAndRound(): void
    {
        $this->pdo->exec("INSERT INTO users (id, name, email, score) VALUES (1, 'Alice', 'a@test.com', -15)");

        $stmt = $this->pdo->query("SELECT ABS(score) AS abs_score, ROUND(score * 1.5) AS rounded FROM users");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(15, (int) $row['abs_score']);
        $this->assertSame(-23, (int) $row['rounded']); // ROUND(-22.5) = -23 on SQLite
    }

    public function testMaxMinOnStrings(): void
    {
        $this->pdo->exec("INSERT INTO users (id, name, email, score) VALUES (1, 'Alice', 'a@test.com', 90)");
        $this->pdo->exec("INSERT INTO users (id, name, email, score) VALUES (2, 'Bob', 'b@test.com', 80)");
        $this->pdo->exec("INSERT INTO users (id, name, email, score) VALUES (3, 'Charlie', 'c@test.com', 70)");

        $stmt = $this->pdo->query("SELECT MIN(name) AS min_name, MAX(name) AS max_name FROM users");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['min_name']);
        $this->assertSame('Charlie', $row['max_name']);
    }

    public function testAggregatesOnEmptySet(): void
    {
        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt, SUM(score) AS total, AVG(score) AS avg_score, MIN(score) AS min_score, MAX(score) AS max_score FROM users");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $row['cnt']);
        $this->assertNull($row['total']);
        $this->assertNull($row['avg_score']);
        $this->assertNull($row['min_score']);
        $this->assertNull($row['max_score']);
    }

    // --- Multiple CTEs ---

    public function testMultipleCtes(): void
    {
        $this->pdo->exec("INSERT INTO users (id, name, email, score) VALUES (1, 'Alice', 'a@test.com', 90)");
        $this->pdo->exec("INSERT INTO users (id, name, email, score) VALUES (2, 'Bob', 'b@test.com', 80)");
        $this->pdo->exec("INSERT INTO users (id, name, email, score) VALUES (3, 'Charlie', 'c@test.com', 60)");

        $stmt = $this->pdo->query("
            WITH high_scorers AS (
                SELECT id, name, score FROM users WHERE score >= 80
            ),
            low_scorers AS (
                SELECT id, name, score FROM users WHERE score < 80
            )
            SELECT
                (SELECT COUNT(*) FROM high_scorers) AS high_count,
                (SELECT COUNT(*) FROM low_scorers) AS low_count
        ");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(2, (int) $row['high_count']);
        $this->assertSame(1, (int) $row['low_count']);
    }

    // --- NOT IN ---

    public function testNotInWithLiterals(): void
    {
        $this->pdo->exec("INSERT INTO users (id, name, email, score) VALUES (1, 'Alice', 'a@test.com', 90)");
        $this->pdo->exec("INSERT INTO users (id, name, email, score) VALUES (2, 'Bob', 'b@test.com', 80)");
        $this->pdo->exec("INSERT INTO users (id, name, email, score) VALUES (3, 'Charlie', 'c@test.com', 70)");

        $stmt = $this->pdo->query("SELECT name FROM users WHERE id NOT IN (1, 3) ORDER BY name");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
    }

    public function testNotInWithSubquery(): void
    {
        $this->pdo->exec("INSERT INTO users (id, name, email, score) VALUES (1, 'Alice', 'a@test.com', 90)");
        $this->pdo->exec("INSERT INTO users (id, name, email, score) VALUES (2, 'Bob', 'b@test.com', 80)");
        $this->pdo->exec("INSERT INTO users (id, name, email, score) VALUES (3, 'Charlie', 'c@test.com', 70)");

        $this->pdo->exec("INSERT INTO vip_users (id, name, email) VALUES (1, 'Alice', 'a@test.com')");

        $stmt = $this->pdo->query("SELECT name FROM users WHERE name NOT IN (SELECT name FROM vip_users) ORDER BY name");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
    }

    // --- JOIN USING ---

    public function testJoinUsingSyntax(): void
    {
        $this->pdo->exec("INSERT INTO users (id, name, email, score) VALUES (1, 'Alice', 'a@test.com', 90)");
        $this->pdo->exec("INSERT INTO users (id, name, email, score) VALUES (2, 'Bob', 'b@test.com', 80)");

        $this->pdo->exec("INSERT INTO vip_users (id, name, email) VALUES (1, 'Alice', 'a@test.com')");

        $stmt = $this->pdo->query("SELECT users.name, users.score FROM users JOIN vip_users USING (id) ORDER BY users.name");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    // --- Zero-match operations ---

    public function testUpdateZeroRows(): void
    {
        $this->pdo->exec("INSERT INTO users (id, name, email, score) VALUES (1, 'Alice', 'a@test.com', 90)");

        $affected = $this->pdo->exec("UPDATE users SET score = 0 WHERE id = 999");
        $this->assertSame(0, $affected);
    }

    public function testDeleteZeroRows(): void
    {
        $this->pdo->exec("INSERT INTO users (id, name, email, score) VALUES (1, 'Alice', 'a@test.com', 90)");

        $affected = $this->pdo->exec("DELETE FROM users WHERE id = 999");
        $this->assertSame(0, $affected);
    }
}
