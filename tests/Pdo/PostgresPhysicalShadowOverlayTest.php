<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\PostgreSQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests the CTE shadow replacement behavior on PostgreSQL PDO: physical data is NOT
 * visible through ZTD queries — the shadow store replaces the physical table.
 */
class PostgresPhysicalShadowOverlayTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_pso_products');
        $raw->exec('CREATE TABLE pg_pso_products (id INT PRIMARY KEY, name VARCHAR(50), price NUMERIC(10,2), category VARCHAR(30))');
        // Pre-populate with physical data
        $raw->exec("INSERT INTO pg_pso_products VALUES (1, 'Widget', 29.99, 'electronics')");
        $raw->exec("INSERT INTO pg_pso_products VALUES (2, 'Gadget', 49.99, 'electronics')");
        $raw->exec("INSERT INTO pg_pso_products VALUES (3, 'Gizmo', 19.99, 'toys')");
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
    }

    public function testPhysicalDataNotVisibleThroughZtd(): void
    {
        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_pso_products");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $row['cnt']);
    }

    public function testOnlyShadowInsertedDataVisible(): void
    {
        $this->pdo->exec("INSERT INTO pg_pso_products VALUES (10, 'Shadow Widget', 39.99, 'shadow')");

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_pso_products");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(1, (int) $row['cnt']);
    }

    public function testUpdateOnPhysicalRowMatchesNothing(): void
    {
        $affected = $this->pdo->exec("UPDATE pg_pso_products SET price = 999.99 WHERE id = 1");
        $this->assertSame(0, $affected);
    }

    public function testPhysicalDataUntouched(): void
    {
        $this->pdo->exec("INSERT INTO pg_pso_products VALUES (10, 'Shadow Only', 99.99, 'shadow')");

        $this->pdo->disableZtd();

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_pso_products");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(3, (int) $row['cnt']);

        $stmt = $this->pdo->query("SELECT * FROM pg_pso_products WHERE id = 10");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertFalse($row);
    }

    public function testInsertWithOverlappingPhysicalId(): void
    {
        $this->pdo->exec("INSERT INTO pg_pso_products VALUES (1, 'Shadow Widget', 99.99, 'shadow')");

        $stmt = $this->pdo->query("SELECT name FROM pg_pso_products WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Shadow Widget', $row['name']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_pso_products');
    }
}
