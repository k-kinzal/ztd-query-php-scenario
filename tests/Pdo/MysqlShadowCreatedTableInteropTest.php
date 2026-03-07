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
 * Tests interoperability between shadow-created tables and reflected tables (MySQL PDO).
 */
class MysqlShadowCreatedTableInteropTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS mysql_sci_users');
        $raw->exec('CREATE TABLE mysql_sci_users (id INT PRIMARY KEY, name VARCHAR(50), status VARCHAR(20))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
    }

    public function testJoinReflectedAndShadowCreatedTable(): void
    {
        $this->pdo->exec("INSERT INTO mysql_sci_users VALUES (1, 'Alice', 'active')");
        $this->pdo->exec("INSERT INTO mysql_sci_users VALUES (2, 'Bob', 'inactive')");

        $this->pdo->exec('CREATE TABLE mysql_sci_scores (user_id INT PRIMARY KEY, score INT)');
        $this->pdo->exec("INSERT INTO mysql_sci_scores VALUES (1, 95)");
        $this->pdo->exec("INSERT INTO mysql_sci_scores VALUES (2, 72)");

        $stmt = $this->pdo->query("
            SELECT u.name, s.score
            FROM mysql_sci_users u
            JOIN mysql_sci_scores s ON s.user_id = u.id
            ORDER BY u.id
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame(95, (int) $rows[0]['score']);
    }

    public function testInsertSelectFromReflectedToShadowCreated(): void
    {
        $this->pdo->exec("INSERT INTO mysql_sci_users VALUES (1, 'Alice', 'active')");
        $this->pdo->exec("INSERT INTO mysql_sci_users VALUES (2, 'Bob', 'inactive')");

        $this->pdo->exec('CREATE TABLE mysql_sci_active (id INT PRIMARY KEY, name VARCHAR(50))');

        $affected = $this->pdo->exec("INSERT INTO mysql_sci_active (id, name) SELECT id, name FROM mysql_sci_users WHERE status = 'active'");
        $this->assertSame(1, $affected);

        $stmt = $this->pdo->query("SELECT name FROM mysql_sci_active");
        $this->assertSame('Alice', $stmt->fetch(PDO::FETCH_ASSOC)['name']);
    }

    public function testSubqueryReferencingShadowCreatedTable(): void
    {
        $this->pdo->exec("INSERT INTO mysql_sci_users VALUES (1, 'Alice', 'active')");
        $this->pdo->exec("INSERT INTO mysql_sci_users VALUES (2, 'Bob', 'active')");
        $this->pdo->exec("INSERT INTO mysql_sci_users VALUES (3, 'Charlie', 'active')");

        $this->pdo->exec('CREATE TABLE mysql_sci_blacklist (user_id INT PRIMARY KEY)');
        $this->pdo->exec("INSERT INTO mysql_sci_blacklist VALUES (2)");

        $stmt = $this->pdo->query("
            SELECT name FROM mysql_sci_users
            WHERE id NOT IN (SELECT user_id FROM mysql_sci_blacklist)
            ORDER BY id
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Charlie', $rows[1]['name']);
    }

    public function testUpdateWithSubqueryOnShadowCreatedTable(): void
    {
        $this->pdo->exec("INSERT INTO mysql_sci_users VALUES (1, 'Alice', 'active')");
        $this->pdo->exec("INSERT INTO mysql_sci_users VALUES (2, 'Bob', 'active')");

        $this->pdo->exec('CREATE TABLE mysql_sci_deactivate (user_id INT PRIMARY KEY)');
        $this->pdo->exec("INSERT INTO mysql_sci_deactivate VALUES (2)");

        $affected = $this->pdo->exec("UPDATE mysql_sci_users SET status = 'deactivated' WHERE id IN (SELECT user_id FROM mysql_sci_deactivate)");
        $this->assertSame(1, $affected);

        $stmt = $this->pdo->query("SELECT status FROM mysql_sci_users WHERE id = 2");
        $this->assertSame('deactivated', $stmt->fetch(PDO::FETCH_ASSOC)['status']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mysql_sci_users');
    }
}
