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
 *
 * Discovery: ALTER TABLE is NOT supported by ZTD on PostgreSQL.
 * All ALTER TABLE statements throw ZtdPdoException with
 * "ALTER TABLE not yet supported for PostgreSQL SQL statement."
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
     * ALTER TABLE ADD COLUMN throws ZtdPdoException on PostgreSQL.
     */
    public function testAlterTableThrowsUnsupportedException(): void
    {
        $this->pdo->exec("INSERT INTO evolve_pg VALUES (1, 'Alice')");

        $this->expectException(ZtdPdoException::class);
        $this->expectExceptionMessage('ALTER TABLE not yet supported');
        $this->pdo->exec('ALTER TABLE evolve_pg ADD COLUMN score INT');
    }

    /**
     * Shadow data remains intact after ALTER TABLE error.
     */
    public function testShadowIntactAfterAlterTableError(): void
    {
        $this->pdo->exec("INSERT INTO evolve_pg VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO evolve_pg VALUES (2, 'Bob')");

        try {
            $this->pdo->exec('ALTER TABLE evolve_pg ADD COLUMN score INT');
        } catch (ZtdPdoException $e) {
            // Expected
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
