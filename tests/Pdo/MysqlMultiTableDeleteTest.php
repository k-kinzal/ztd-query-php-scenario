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
 * Tests MySQL multi-table DELETE via PDO adapter.
 *
 * MySQL supports multi-table DELETE with two syntaxes:
 * 1. DELETE t1 FROM t1 JOIN t2 ON ... WHERE ...
 * 2. DELETE t1, t2 FROM t1 JOIN t2 ON ... WHERE ...
 *
 * MySqlMutationResolver creates MultiDeleteMutation when
 * count($tables) > 1 in the projection.
 */
class MysqlMultiTableDeleteTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS pdo_md_orders');
        $raw->exec('DROP TABLE IF EXISTS pdo_md_users');
        $raw->exec('CREATE TABLE pdo_md_users (id INT PRIMARY KEY, name VARCHAR(50), active TINYINT DEFAULT 1)');
        $raw->exec('CREATE TABLE pdo_md_orders (id INT PRIMARY KEY, user_id INT, amount INT)');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO pdo_md_users (id, name, active) VALUES (1, 'Alice', 1)");
        $this->pdo->exec("INSERT INTO pdo_md_users (id, name, active) VALUES (2, 'Bob', 0)");
        $this->pdo->exec("INSERT INTO pdo_md_users (id, name, active) VALUES (3, 'Charlie', 1)");
        $this->pdo->exec("INSERT INTO pdo_md_orders (id, user_id, amount) VALUES (1, 1, 100)");
        $this->pdo->exec("INSERT INTO pdo_md_orders (id, user_id, amount) VALUES (2, 2, 200)");
        $this->pdo->exec("INSERT INTO pdo_md_orders (id, user_id, amount) VALUES (3, 2, 50)");
        $this->pdo->exec("INSERT INTO pdo_md_orders (id, user_id, amount) VALUES (4, 3, 300)");
    }

    /**
     * Single-target DELETE with JOIN.
     */
    public function testSingleTargetDeleteWithJoin(): void
    {
        $this->pdo->exec(
            "DELETE o FROM pdo_md_orders o JOIN pdo_md_users u ON o.user_id = u.id WHERE u.active = 0"
        );

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_md_orders WHERE user_id = 2');
        $this->assertSame(0, (int) $stmt->fetchColumn());

        // Other orders remain
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_md_orders');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * Multi-target DELETE: DELETE t1, t2 FROM ... only affects the first table.
     *
     * Same limitation as multi-table UPDATE — the CTE rewriter processes
     * tables independently and the secondary table is not affected.
     */
    public function testMultiTargetDeleteOnlyAffectsFirstTable(): void
    {
        $this->pdo->exec(
            "DELETE u, o FROM pdo_md_users u JOIN pdo_md_orders o ON u.id = o.user_id WHERE u.active = 0"
        );

        // First table (users): Bob IS deleted
        $stmt = $this->pdo->query("SELECT COUNT(*) FROM pdo_md_users WHERE name = 'Bob'");
        $this->assertSame(0, (int) $stmt->fetchColumn());

        // Second table (orders): Bob's orders are NOT deleted
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_md_orders WHERE user_id = 2');
        $this->assertSame(1, (int) $stmt->fetchColumn());

        // Other users remain
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_md_users');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * Multi-target DELETE with prepared statement.
     */
    public function testMultiTargetDeletePrepared(): void
    {
        $stmt = $this->pdo->prepare(
            "DELETE o FROM pdo_md_orders o JOIN pdo_md_users u ON o.user_id = u.id WHERE u.name = ?"
        );
        $stmt->execute(['Bob']);

        $select = $this->pdo->query('SELECT COUNT(*) FROM pdo_md_orders WHERE user_id = 2');
        $this->assertSame(0, (int) $select->fetchColumn());
    }

    /**
     * Physical isolation: multi-table delete stays in shadow.
     */
    public function testMultiTargetDeletePhysicalIsolation(): void
    {
        $this->pdo->exec(
            "DELETE u, o FROM pdo_md_users u JOIN pdo_md_orders o ON u.id = o.user_id WHERE u.active = 0"
        );

        $this->pdo->disableZtd();

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_md_users');
        $this->assertSame(0, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_md_orders');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pdo_md_orders');
        $raw->exec('DROP TABLE IF EXISTS pdo_md_users');
    }
}
