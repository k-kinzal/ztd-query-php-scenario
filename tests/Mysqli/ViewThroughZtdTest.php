<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests database views behavior through ZTD via MySQLi.
 *
 * Cross-platform parity with MysqlViewThroughZtdTest (PDO).
 */
class ViewThroughZtdTest extends TestCase
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
        $raw->query('DROP VIEW IF EXISTS mi_vtzt_active');
        $raw->query('DROP TABLE IF EXISTS mi_vtzt_users');
        $raw->query('CREATE TABLE mi_vtzt_users (id INT PRIMARY KEY, name VARCHAR(50), active TINYINT)');
        $raw->query("INSERT INTO mi_vtzt_users VALUES (1, 'Alice', 1)");
        $raw->query("INSERT INTO mi_vtzt_users VALUES (2, 'Bob', 0)");
        $raw->query("INSERT INTO mi_vtzt_users VALUES (3, 'Charlie', 1)");
        $raw->query('CREATE VIEW mi_vtzt_active AS SELECT id, name FROM mi_vtzt_users WHERE active = 1');
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

    /**
     * View returns physical data (not shadow).
     */
    public function testViewReturnsPhysicalData(): void
    {
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_vtzt_active');
        $this->assertSame(2, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Shadow insert on base table not visible through view.
     */
    public function testShadowMutationsNotVisibleThroughView(): void
    {
        $this->mysqli->query("INSERT INTO mi_vtzt_users VALUES (4, 'Diana', 1)");

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_vtzt_active');
        $this->assertSame(2, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Physical isolation of base table.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->disableZtd();

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_vtzt_users');
        $this->assertSame(3, (int) $result->fetch_assoc()['cnt']);
    }

    protected function tearDown(): void
    {
        if (isset($this->mysqli)) {
            $this->mysqli->close();
        }
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new \mysqli(
                MySQLContainer::getHost(),
                'root',
                'root',
                'test',
                MySQLContainer::getPort(),
            );
            $raw->query('DROP VIEW IF EXISTS mi_vtzt_active');
            $raw->query('DROP TABLE IF EXISTS mi_vtzt_users');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
