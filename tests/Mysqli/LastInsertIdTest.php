<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests insert_id behavior with ZTD shadow operations via MySQLi.
 *
 * Cross-platform parity with SqliteLastInsertIdTest (PDO).
 */
class LastInsertIdTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_lid_test');
        $raw->query('CREATE TABLE mi_lid_test (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50))');
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
     * insert_id property access throws Error in ZTD mode.
     *
     * ZtdMysqli does not allow direct property access for insert_id
     * when ZTD is enabled — "Property access is not allowed yet".
     */
    public function testInsertIdPropertyNotAllowed(): void
    {
        $this->mysqli->query("INSERT INTO mi_lid_test (name) VALUES ('Alice')");

        $this->expectException(\Error::class);
        $this->expectExceptionMessage('Property access is not allowed yet');
        $_ = $this->mysqli->insert_id;
    }

    /**
     * Shadow INSERT rows are visible in shadow SELECT.
     */
    public function testShadowInsertRowsVisible(): void
    {
        $this->mysqli->query("INSERT INTO mi_lid_test (name) VALUES ('Alice')");
        $this->mysqli->query("INSERT INTO mi_lid_test (name) VALUES ('Bob')");

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_lid_test');
        $this->assertSame(2, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_lid_test (name) VALUES ('Alice')");

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_lid_test');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
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
            $raw->query('DROP TABLE IF EXISTS mi_lid_test');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
