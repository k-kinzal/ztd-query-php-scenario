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
 * Tests DELETE without WHERE clause on MySQL PDO.
 *
 * MySQL correctly clears the shadow store.
 */
class MysqlDeleteWithoutWhereTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
        $raw->exec('DROP TABLE IF EXISTS pdo_mdww_test');
        $raw->exec('CREATE TABLE pdo_mdww_test (id INT PRIMARY KEY, name VARCHAR(50))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO pdo_mdww_test VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO pdo_mdww_test VALUES (2, 'Bob')");
        $this->pdo->exec("INSERT INTO pdo_mdww_test VALUES (3, 'Charlie')");
    }

    /**
     * DELETE without WHERE works correctly on MySQL.
     */
    public function testDeleteWithoutWhereWorks(): void
    {
        $this->pdo->exec('DELETE FROM pdo_mdww_test');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_mdww_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    /**
     * DELETE with WHERE 1=1 also works.
     */
    public function testDeleteWithWhereTrueWorks(): void
    {
        $this->pdo->exec('DELETE FROM pdo_mdww_test WHERE 1=1');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_mdww_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec('DELETE FROM pdo_mdww_test');

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_mdww_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
            $raw->exec('DROP TABLE IF EXISTS pdo_mdww_test');
        } catch (\Exception $e) {
        }
    }
}
