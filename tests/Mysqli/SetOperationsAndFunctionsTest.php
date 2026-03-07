<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests set operations (EXCEPT, INTERSECT), string/math functions, and multiple CTEs on MySQLi.
 */
class SetOperationsAndFunctionsTest extends TestCase
{
    private ZtdMysqli $mysqli;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS mi_sof_vip');
        $raw->query('DROP TABLE IF EXISTS mi_sof_users');
        $raw->query('CREATE TABLE mi_sof_users (id INT PRIMARY KEY, name VARCHAR(255), email VARCHAR(255), score INT)');
        $raw->query('CREATE TABLE mi_sof_vip (id INT PRIMARY KEY, name VARCHAR(255), email VARCHAR(255))');
        $raw->close();
    }

    protected function setUp(): void
    {
        $this->mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );

        $this->mysqli->query("INSERT INTO mi_sof_users (id, name, email, score) VALUES (1, 'Alice', 'alice@test.com', 90)");
        $this->mysqli->query("INSERT INTO mi_sof_users (id, name, email, score) VALUES (2, 'Bob', 'bob@test.com', 80)");
        $this->mysqli->query("INSERT INTO mi_sof_users (id, name, email, score) VALUES (3, 'Charlie', 'charlie@test.com', 70)");

        $this->mysqli->query("INSERT INTO mi_sof_vip (id, name, email) VALUES (1, 'Alice', 'alice@test.com')");
    }

    /**
     * EXCEPT is treated as multi-statement SQL by MySQL CTE rewriter and throws.
     */
    public function testExceptThrowsMultiStatementError(): void
    {
        $this->expectException(\Throwable::class);

        $this->mysqli->query("
            SELECT name FROM mi_sof_users
            EXCEPT
            SELECT name FROM mi_sof_vip
            ORDER BY name
        ");
    }

    /**
     * INTERSECT is treated as multi-statement SQL by MySQL CTE rewriter and throws.
     */
    public function testIntersectThrowsMultiStatementError(): void
    {
        $this->expectException(\Throwable::class);

        $this->mysqli->query("
            SELECT name FROM mi_sof_users
            INTERSECT
            SELECT name FROM mi_sof_vip
        ");
    }

    public function testStringFunctionsConcatUpperLower(): void
    {
        $result = $this->mysqli->query("SELECT CONCAT(UPPER(name), ' <', LOWER(email), '>') AS display FROM mi_sof_users WHERE id = 1");
        $row = $result->fetch_assoc();
        $this->assertSame('ALICE <alice@test.com>', $row['display']);
    }

    public function testGroupConcat(): void
    {
        $result = $this->mysqli->query("SELECT GROUP_CONCAT(name ORDER BY name SEPARATOR ', ') AS names FROM mi_sof_users");
        $row = $result->fetch_assoc();
        $this->assertSame('Alice, Bob, Charlie', $row['names']);
    }

    public function testMathFunctions(): void
    {
        $this->mysqli->query("UPDATE mi_sof_users SET score = -15 WHERE id = 1");

        $result = $this->mysqli->query("SELECT ABS(score) AS abs_score, CEIL(score * 1.5) AS ceiled, FLOOR(score * 1.5) AS floored FROM mi_sof_users WHERE id = 1");
        $row = $result->fetch_assoc();
        $this->assertSame(15, (int) $row['abs_score']);
        $this->assertSame(-22, (int) $row['ceiled']);
        $this->assertSame(-23, (int) $row['floored']);
    }

    public function testAggregatesOnEmptySet(): void
    {
        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt, SUM(score) AS total, AVG(score) AS avg_score FROM mi_sof_users WHERE id > 999");
        $row = $result->fetch_assoc();
        $this->assertSame(0, (int) $row['cnt']);
        $this->assertNull($row['total']);
        $this->assertNull($row['avg_score']);
    }

    public function testMultipleCtes(): void
    {
        $result = $this->mysqli->query("
            WITH high_scorers AS (
                SELECT id, name, score FROM mi_sof_users WHERE score >= 80
            ),
            low_scorers AS (
                SELECT id, name, score FROM mi_sof_users WHERE score < 80
            )
            SELECT
                (SELECT COUNT(*) FROM high_scorers) AS high_count,
                (SELECT COUNT(*) FROM low_scorers) AS low_count
        ");
        $row = $result->fetch_assoc();
        $this->assertSame(2, (int) $row['high_count']);
        $this->assertSame(1, (int) $row['low_count']);
    }

    public function testNotInWithSubquery(): void
    {
        $result = $this->mysqli->query("SELECT name FROM mi_sof_users WHERE name NOT IN (SELECT name FROM mi_sof_vip) ORDER BY name");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
    }

    public function testJoinUsingSyntax(): void
    {
        $result = $this->mysqli->query("SELECT mi_sof_users.name, mi_sof_users.score FROM mi_sof_users JOIN mi_sof_vip USING (id)");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    protected function tearDown(): void
    {
        $this->mysqli->close();
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS mi_sof_vip');
        $raw->query('DROP TABLE IF EXISTS mi_sof_users');
        $raw->close();
    }
}
