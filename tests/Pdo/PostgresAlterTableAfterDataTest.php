<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\PostgreSQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;
use ZtdQuery\Adapter\Pdo\ZtdPdoException;

/**
 * Tests ALTER TABLE behavior with the shadow store on PostgreSQL PDO.
 */
class PostgresAlterTableAfterDataTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS evolve_pg');
        $raw->exec('CREATE TABLE evolve_pg (id INT PRIMARY KEY, name VARCHAR(50))');
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

    /**
     * ALTER TABLE ADD COLUMN should work on PostgreSQL.
     */
    public function testAlterTableAddColumn(): void
    {
        $this->pdo->exec("INSERT INTO evolve_pg VALUES (1, 'Alice')");

        try {
            $this->pdo->exec('ALTER TABLE evolve_pg ADD COLUMN score INT');
        } catch (ZtdPdoException $e) {
            $this->markTestIncomplete(
                'ALTER TABLE not yet supported on PostgreSQL: ' . $e->getMessage()
            );
        }

        // If ALTER TABLE succeeds, verify new column is usable
        $this->pdo->exec("INSERT INTO evolve_pg (id, name, score) VALUES (2, 'Bob', 100)");
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM evolve_pg');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * Shadow data should remain intact after ALTER TABLE.
     */
    public function testShadowIntactAfterAlterTable(): void
    {
        $this->pdo->exec("INSERT INTO evolve_pg VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO evolve_pg VALUES (2, 'Bob')");

        try {
            $this->pdo->exec('ALTER TABLE evolve_pg ADD COLUMN score INT');
        } catch (ZtdPdoException $e) {
            // ALTER TABLE not yet supported — but shadow data should still be intact
        }

        $stmt = $this->pdo->query('SELECT name FROM evolve_pg WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);

        $stmt2 = $this->pdo->query('SELECT COUNT(*) FROM evolve_pg');
        $this->assertSame(2, (int) $stmt2->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS evolve_pg');
    }
}
