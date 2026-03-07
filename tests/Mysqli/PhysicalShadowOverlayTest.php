<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests the CTE shadow replacement behavior on MySQLi: physical data is NOT
 * visible through ZTD queries — the shadow store replaces the physical table.
 */
class PhysicalShadowOverlayTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_pso_products');
        $raw->query('CREATE TABLE mi_pso_products (id INT PRIMARY KEY, name VARCHAR(50), price DECIMAL(10,2), category VARCHAR(30))');
        $raw->query("INSERT INTO mi_pso_products VALUES (1, 'Widget', 29.99, 'electronics')");
        $raw->query("INSERT INTO mi_pso_products VALUES (2, 'Gadget', 49.99, 'electronics')");
        $raw->query("INSERT INTO mi_pso_products VALUES (3, 'Gizmo', 19.99, 'toys')");
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

    public function testPhysicalDataNotVisibleThroughZtd(): void
    {
        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_pso_products");
        $row = $result->fetch_assoc();
        $this->assertSame(0, (int) $row['cnt']);
    }

    public function testOnlyShadowInsertedDataVisible(): void
    {
        $this->mysqli->query("INSERT INTO mi_pso_products VALUES (10, 'Shadow Widget', 39.99, 'shadow')");

        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_pso_products");
        $row = $result->fetch_assoc();
        $this->assertSame(1, (int) $row['cnt']);
    }

    public function testPhysicalDataUntouched(): void
    {
        $this->mysqli->query("INSERT INTO mi_pso_products VALUES (10, 'Shadow Only', 99.99, 'shadow')");

        $this->mysqli->disableZtd();

        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_pso_products");
        $row = $result->fetch_assoc();
        $this->assertSame(3, (int) $row['cnt']);
    }

    public function testInsertWithOverlappingPhysicalId(): void
    {
        $this->mysqli->query("INSERT INTO mi_pso_products VALUES (1, 'Shadow Widget', 99.99, 'shadow')");

        $result = $this->mysqli->query("SELECT name FROM mi_pso_products WHERE id = 1");
        $row = $result->fetch_assoc();
        $this->assertSame('Shadow Widget', $row['name']);
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
        $raw->query('DROP TABLE IF EXISTS mi_pso_products');
        $raw->close();
    }
}
