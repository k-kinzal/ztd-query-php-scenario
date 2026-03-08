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
 * Tests database views behavior through ZTD on MySQL PDO.
 *
 * Views are NOT rewritten by the CTE rewriter — they pass through to physical DB.
 */
class MysqlViewThroughZtdTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
        $raw->exec('DROP VIEW IF EXISTS pdo_vtzt_active');
        $raw->exec('DROP TABLE IF EXISTS pdo_vtzt_users');
        $raw->exec('CREATE TABLE pdo_vtzt_users (id INT PRIMARY KEY, name VARCHAR(50), active TINYINT)');
        $raw->exec("INSERT INTO pdo_vtzt_users VALUES (1, 'Alice', 1)");
        $raw->exec("INSERT INTO pdo_vtzt_users VALUES (2, 'Bob', 0)");
        $raw->exec("INSERT INTO pdo_vtzt_users VALUES (3, 'Charlie', 1)");
        $raw->exec('CREATE VIEW pdo_vtzt_active AS SELECT id, name FROM pdo_vtzt_users WHERE active = 1');
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

    /**
     * View returns physical data (not shadow).
     */
    public function testViewReturnsPhysicalData(): void
    {
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_vtzt_active');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * Shadow insert on base table not visible through view.
     */
    public function testShadowMutationsNotVisibleThroughView(): void
    {
        $this->pdo->exec("INSERT INTO pdo_vtzt_users VALUES (4, 'Diana', 1)");

        // View still reads physical data
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_vtzt_active');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation of base table.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_vtzt_users');
        $this->assertSame(3, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
            $raw->exec('DROP VIEW IF EXISTS pdo_vtzt_active');
            $raw->exec('DROP TABLE IF EXISTS pdo_vtzt_users');
        } catch (\Exception $e) {
        }
    }
}
