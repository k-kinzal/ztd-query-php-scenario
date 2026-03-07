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
 * Tests debugDumpParams() on ZtdPdoStatement with PostgreSQL PDO.
 * Confirms ZTD rewrites are visible in debug output.
 */
class PostgresDebugDumpParamsTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS ddp_pg');
        $raw->exec('CREATE TABLE ddp_pg (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
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

    public function testDebugDumpParamsOnSelect(): void
    {
        $stmt = $this->pdo->prepare('SELECT * FROM ddp_pg WHERE id = ?');
        $stmt->execute([1]);

        ob_start();
        $stmt->debugDumpParams();
        $output = ob_get_clean();

        $this->assertIsString($output);
        $this->assertStringContainsString('SELECT', $output);
    }

    public function testDebugDumpParamsOnInsertShowsRewrittenSql(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO ddp_pg (id, name, score) VALUES (?, ?, ?)');
        $stmt->execute([1, 'Alice', 100]);

        ob_start();
        $stmt->debugDumpParams();
        $output = ob_get_clean();

        $this->assertIsString($output);
        // ZTD rewrites INSERT to SELECT for shadow store
        $this->assertStringContainsString('SELECT', $output);
    }

    public function testDebugDumpParamsOnUpdateShowsRewrittenSql(): void
    {
        $this->pdo->exec("INSERT INTO ddp_pg VALUES (1, 'Alice', 100)");

        $stmt = $this->pdo->prepare('UPDATE ddp_pg SET score = ? WHERE id = ?');
        $stmt->execute([200, 1]);

        ob_start();
        $stmt->debugDumpParams();
        $output = ob_get_clean();

        $this->assertIsString($output);
        // ZTD rewrites UPDATE to CTE WITH + SELECT
        $this->assertStringContainsString('WITH', $output);
        $this->assertStringContainsString('SELECT', $output);
    }

    public function testDebugDumpParamsWithNamedParams(): void
    {
        $stmt = $this->pdo->prepare('SELECT * FROM ddp_pg WHERE name = :name');
        $stmt->execute([':name' => 'Alice']);

        ob_start();
        $stmt->debugDumpParams();
        $output = ob_get_clean();

        $this->assertIsString($output);
        $this->assertStringContainsString('Params:', $output);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS ddp_pg');
    }
}
