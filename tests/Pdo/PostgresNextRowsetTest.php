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
 * Tests PDOStatement::nextRowset() behavior with ZTD on PostgreSQL.
 *
 * Discovery: PostgreSQL PDO driver does NOT support nextRowset() — same as SQLite.
 * Throws PDOException "Driver does not support this function".
 * Only MySQL supports nextRowset() (returns false for CTE queries).
 */
class PostgresNextRowsetTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS nr_test_pg');
        $raw->exec('CREATE TABLE nr_test_pg (id INT PRIMARY KEY, name VARCHAR(50))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO nr_test_pg VALUES (1, 'Alice')");
    }

    /**
     * PostgreSQL does not support multiple rowsets — nextRowset() throws.
     */
    public function testNextRowsetThrowsOnPostgres(): void
    {
        $stmt = $this->pdo->query('SELECT * FROM nr_test_pg WHERE id = 1');
        $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->expectException(\PDOException::class);
        $this->expectExceptionMessage('does not support');
        $stmt->nextRowset();
    }

    public function testNextRowsetThrowsOnPrepared(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM nr_test_pg WHERE id = ?');
        $stmt->execute([1]);
        $stmt->fetch(PDO::FETCH_ASSOC);

        $this->expectException(\PDOException::class);
        $this->expectExceptionMessage('does not support');
        $stmt->nextRowset();
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS nr_test_pg');
    }
}
