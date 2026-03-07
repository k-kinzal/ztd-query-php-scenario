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
 * Tests PostgreSQL-specific type edge cases with ZTD shadow store.
 *
 * Documents known limitations:
 * - BOOLEAN false: CTE rewriter generates invalid CAST('' AS BOOLEAN)
 * - BIGINT overflow: CTE rewriter generates CAST(value AS integer) instead of bigint
 */
class PostgresTypeEdgeCaseTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS pg_type_edge');
        $raw->exec('CREATE TABLE pg_type_edge (id INT PRIMARY KEY, flag BOOLEAN, big_num BIGINT)');
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
     * BOOLEAN true works correctly via prepared statement.
     */
    public function testBooleanTrueWorksViaPrepared(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO pg_type_edge (id, flag, big_num) VALUES (?, ?, ?)');
        $stmt->execute([1, true, 0]);

        $sel = $this->pdo->query('SELECT flag FROM pg_type_edge WHERE id = 1');
        $row = $sel->fetch(PDO::FETCH_ASSOC);
        $this->assertNotFalse($row);
    }

    /**
     * BOOLEAN false via prepared statement fails on SELECT.
     * CTE rewriter generates CAST('' AS BOOLEAN) which is invalid PostgreSQL.
     */
    public function testBooleanFalseFailsOnSelect(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO pg_type_edge (id, flag, big_num) VALUES (?, ?, ?)');
        $stmt->execute([2, false, 0]);

        $this->expectException(\PDOException::class);
        $this->pdo->query('SELECT flag FROM pg_type_edge WHERE id = 2');
    }

    /**
     * BIGINT values within integer range work correctly.
     */
    public function testBigintWithinIntegerRange(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO pg_type_edge (id, flag, big_num) VALUES (?, ?, ?)');
        $stmt->execute([3, true, 2147483647]); // max int32

        $sel = $this->pdo->query('SELECT big_num FROM pg_type_edge WHERE id = 3');
        $val = $sel->fetchColumn();
        $this->assertSame(2147483647, (int) $val);
    }

    /**
     * BIGINT values exceeding integer range fail on SELECT.
     * CTE rewriter generates CAST(value AS integer) instead of CAST(value AS bigint).
     */
    public function testBigintOverflowFailsOnSelect(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO pg_type_edge (id, flag, big_num) VALUES (?, ?, ?)');
        $stmt->execute([4, true, 9999999999]); // exceeds int32

        $this->expectException(\PDOException::class);
        $this->pdo->query('SELECT big_num FROM pg_type_edge WHERE id = 4');
    }

    /**
     * BOOLEAN via exec() (not prepared) — also affected since CTE rewriter
     * generates the same CAST expressions.
     */
    public function testBooleanTrueViaExec(): void
    {
        $this->pdo->exec("INSERT INTO pg_type_edge VALUES (5, TRUE, 42)");

        $sel = $this->pdo->query('SELECT flag, big_num FROM pg_type_edge WHERE id = 5');
        $row = $sel->fetch(PDO::FETCH_ASSOC);
        $this->assertNotFalse($row);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_type_edge');
    }
}
