<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests interoperability between shadow-created tables and reflected tables (MySQLi).
 */
class ShadowCreatedTableInteropTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_sci_users');
        $raw->query('CREATE TABLE mi_sci_users (id INT PRIMARY KEY, name VARCHAR(50), status VARCHAR(20))');
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
    }

    public function testJoinReflectedAndShadowCreatedTable(): void
    {
        $this->mysqli->query("INSERT INTO mi_sci_users VALUES (1, 'Alice', 'active')");
        $this->mysqli->query("INSERT INTO mi_sci_users VALUES (2, 'Bob', 'inactive')");

        $this->mysqli->query('CREATE TABLE mi_sci_scores (user_id INT PRIMARY KEY, score INT)');
        $this->mysqli->query("INSERT INTO mi_sci_scores VALUES (1, 95)");
        $this->mysqli->query("INSERT INTO mi_sci_scores VALUES (2, 72)");

        $result = $this->mysqli->query("
            SELECT u.name, s.score
            FROM mi_sci_users u
            JOIN mi_sci_scores s ON s.user_id = u.id
            ORDER BY u.id
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame(95, (int) $rows[0]['score']);
    }

    public function testSubqueryReferencingShadowCreatedTable(): void
    {
        $this->mysqli->query("INSERT INTO mi_sci_users VALUES (1, 'Alice', 'active')");
        $this->mysqli->query("INSERT INTO mi_sci_users VALUES (2, 'Bob', 'active')");
        $this->mysqli->query("INSERT INTO mi_sci_users VALUES (3, 'Charlie', 'active')");

        $this->mysqli->query('CREATE TABLE mi_sci_blacklist (user_id INT PRIMARY KEY)');
        $this->mysqli->query("INSERT INTO mi_sci_blacklist VALUES (2)");

        $result = $this->mysqli->query("
            SELECT name FROM mi_sci_users
            WHERE id NOT IN (SELECT user_id FROM mi_sci_blacklist)
            ORDER BY id
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Charlie', $rows[1]['name']);
    }

    public function testUpdateWithSubqueryOnShadowCreatedTable(): void
    {
        $this->mysqli->query("INSERT INTO mi_sci_users VALUES (1, 'Alice', 'active')");
        $this->mysqli->query("INSERT INTO mi_sci_users VALUES (2, 'Bob', 'active')");

        $this->mysqli->query('CREATE TABLE mi_sci_deactivate (user_id INT PRIMARY KEY)');
        $this->mysqli->query("INSERT INTO mi_sci_deactivate VALUES (2)");

        $this->mysqli->query("UPDATE mi_sci_users SET status = 'deactivated' WHERE id IN (SELECT user_id FROM mi_sci_deactivate)");
        $this->assertSame(1, $this->mysqli->lastAffectedRows());

        $result = $this->mysqli->query("SELECT status FROM mi_sci_users WHERE id = 2");
        $this->assertSame('deactivated', $result->fetch_assoc()['status']);
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
        $raw->query('DROP TABLE IF EXISTS mi_sci_users');
        $raw->close();
    }
}
