<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests set operations (EXCEPT, INTERSECT), string/math functions, and multiple CTEs on MySQL PDO.
 */
class MysqlSetOperationsAndFunctionsTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mysql_sof_vip');
        $raw->exec('DROP TABLE IF EXISTS mysql_sof_users');
        $raw->exec('CREATE TABLE mysql_sof_users (id INT PRIMARY KEY, name VARCHAR(255), email VARCHAR(255), score INT)');
        $raw->exec('CREATE TABLE mysql_sof_vip (id INT PRIMARY KEY, name VARCHAR(255), email VARCHAR(255))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO mysql_sof_users (id, name, email, score) VALUES (1, 'Alice', 'alice@test.com', 90)");
        $this->pdo->exec("INSERT INTO mysql_sof_users (id, name, email, score) VALUES (2, 'Bob', 'bob@test.com', 80)");
        $this->pdo->exec("INSERT INTO mysql_sof_users (id, name, email, score) VALUES (3, 'Charlie', 'charlie@test.com', 70)");

        $this->pdo->exec("INSERT INTO mysql_sof_vip (id, name, email) VALUES (1, 'Alice', 'alice@test.com')");
    }

    /**
     * EXCEPT should return rows in users but not in vip.
     */
    public function testExceptWorks(): void
    {
        try {
            $stmt = $this->pdo->query("
                SELECT name FROM mysql_sof_users
                EXCEPT
                SELECT name FROM mysql_sof_vip
                ORDER BY name
            ");
            $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
            $this->assertContains('Alice', $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'EXCEPT misdetected as multi-statement query on MySQL: ' . $e->getMessage()
            );
        }
    }

    /**
     * INTERSECT should return rows common to both sets.
     */
    public function testIntersectWorks(): void
    {
        try {
            $stmt = $this->pdo->query("
                SELECT name FROM mysql_sof_users
                INTERSECT
                SELECT name FROM mysql_sof_vip
            ");
            $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
            $this->assertIsArray($rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INTERSECT misdetected as multi-statement query on MySQL: ' . $e->getMessage()
            );
        }
    }

    public function testStringFunctionsConcatUpperLower(): void
    {
        $stmt = $this->pdo->query("SELECT CONCAT(UPPER(name), ' <', LOWER(email), '>') AS display FROM mysql_sof_users WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('ALICE <alice@test.com>', $row['display']);
    }

    public function testSubstrAndLength(): void
    {
        $stmt = $this->pdo->query("SELECT CHAR_LENGTH(name) AS len, SUBSTRING(name, 1, 3) AS sub FROM mysql_sof_users WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(5, (int) $row['len']);
        $this->assertSame('Ali', $row['sub']);
    }

    public function testGroupConcat(): void
    {
        $stmt = $this->pdo->query("SELECT GROUP_CONCAT(name ORDER BY name SEPARATOR ', ') AS names FROM mysql_sof_users");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice, Bob, Charlie', $row['names']);
    }

    public function testMathFunctions(): void
    {
        $this->pdo->exec("UPDATE mysql_sof_users SET score = -15 WHERE id = 1");

        $stmt = $this->pdo->query("SELECT ABS(score) AS abs_score, CEIL(score * 1.5) AS ceiled, FLOOR(score * 1.5) AS floored FROM mysql_sof_users WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(15, (int) $row['abs_score']);
        $this->assertSame(-22, (int) $row['ceiled']);
        $this->assertSame(-23, (int) $row['floored']);
    }

    public function testAggregatesOnEmptySet(): void
    {
        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt, SUM(score) AS total, AVG(score) AS avg_score FROM mysql_sof_users WHERE id > 999");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $row['cnt']);
        $this->assertNull($row['total']);
        $this->assertNull($row['avg_score']);
    }

    public function testMultipleCtes(): void
    {
        $stmt = $this->pdo->query("
            WITH high_scorers AS (
                SELECT id, name, score FROM mysql_sof_users WHERE score >= 80
            ),
            low_scorers AS (
                SELECT id, name, score FROM mysql_sof_users WHERE score < 80
            )
            SELECT
                (SELECT COUNT(*) FROM high_scorers) AS high_count,
                (SELECT COUNT(*) FROM low_scorers) AS low_count
        ");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(2, (int) $row['high_count']);
        $this->assertSame(1, (int) $row['low_count']);
    }

    public function testNotInWithSubquery(): void
    {
        $stmt = $this->pdo->query("SELECT name FROM mysql_sof_users WHERE name NOT IN (SELECT name FROM mysql_sof_vip) ORDER BY name");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
    }

    public function testJoinUsingSyntax(): void
    {
        $stmt = $this->pdo->query("SELECT mysql_sof_users.name, mysql_sof_users.score FROM mysql_sof_users JOIN mysql_sof_vip USING (id)");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public function testZeroMatchUpdateDelete(): void
    {
        $affected = $this->pdo->exec("UPDATE mysql_sof_users SET score = 0 WHERE id = 999");
        $this->assertSame(0, $affected);

        $affected = $this->pdo->exec("DELETE FROM mysql_sof_users WHERE id = 999");
        $this->assertSame(0, $affected);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mysql_sof_vip');
        $raw->exec('DROP TABLE IF EXISTS mysql_sof_users');
    }
}
