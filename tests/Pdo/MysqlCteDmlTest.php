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
 * Tests CTE-based DML patterns on MySQL PDO ZTD.
 *
 * MySQL 8.0+ supports WITH ... INSERT/UPDATE/DELETE natively, but ZTD
 * does not support these patterns. The mutation resolver cannot produce
 * a shadow mutation for CTE-based DML, throwing RuntimeException
 * "Missing shadow mutation for write simulation".
 */
class MysqlCteDmlTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            sprintf('mysql:host=%s;port=%d;dbname=test', MySQLContainer::getHost(), MySQLContainer::getPort()),
            'root',
            'root',
        );
        $raw->exec('DROP TABLE IF EXISTS pdo_cte_dml_target');
        $raw->exec('DROP TABLE IF EXISTS pdo_cte_dml_source');
        $raw->exec('CREATE TABLE pdo_cte_dml_source (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
        $raw->exec('CREATE TABLE pdo_cte_dml_target (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            sprintf('mysql:host=%s;port=%d;dbname=test', MySQLContainer::getHost(), MySQLContainer::getPort()),
            'root',
            'root',
        );

        $this->pdo->exec("INSERT INTO pdo_cte_dml_source (id, name, score) VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO pdo_cte_dml_source (id, name, score) VALUES (2, 'Bob', 80)");
        $this->pdo->exec("INSERT INTO pdo_cte_dml_source (id, name, score) VALUES (3, 'Charlie', 70)");

        $this->pdo->exec("INSERT INTO pdo_cte_dml_target (id, name, score) VALUES (1, 'Old_Alice', 50)");
        $this->pdo->exec("INSERT INTO pdo_cte_dml_target (id, name, score) VALUES (2, 'Old_Bob', 40)");
    }

    /**
     * WITH ... INSERT fails because mutation resolver cannot handle CTE DML.
     *
     * MySQL QueryGuard classifyWithFallback() correctly identifies WITH...INSERT
     * as WRITE_SIMULATED, but the mutation resolver fails because the parsed
     * statement is a WithStatement, not an InsertStatement.
     */
    public function testWithInsertSelectFails(): void
    {
        $this->expectException(\RuntimeException::class);

        $this->pdo->exec("WITH high_scores AS (SELECT id, name, score FROM pdo_cte_dml_source WHERE score >= 80) INSERT INTO pdo_cte_dml_target (id, name, score) SELECT id + 10, name, score FROM high_scores");
    }

    /**
     * WITH ... DELETE fails on MySQL ZTD.
     */
    public function testWithDeleteFails(): void
    {
        $this->expectException(\RuntimeException::class);

        $this->pdo->exec("WITH low_scores AS (SELECT id FROM pdo_cte_dml_target WHERE score < 45) DELETE FROM pdo_cte_dml_target WHERE id IN (SELECT id FROM low_scores)");
    }

    /**
     * WITH ... UPDATE fails on MySQL ZTD.
     */
    public function testWithUpdateFails(): void
    {
        $this->expectException(\RuntimeException::class);

        $this->pdo->exec("WITH new_scores AS (SELECT id, score FROM pdo_cte_dml_source WHERE id <= 2) UPDATE pdo_cte_dml_target SET score = 100 WHERE id IN (SELECT id FROM new_scores)");
    }

    /**
     * Shadow store is not corrupted by CTE DML failures.
     */
    public function testShadowStoreIntactAfterCteDmlFailure(): void
    {
        try {
            $this->pdo->exec("WITH hs AS (SELECT id FROM pdo_cte_dml_source) INSERT INTO pdo_cte_dml_target (id, name, score) SELECT id + 10, 'x', 0 FROM hs");
        } catch (\Throwable $e) {
            // Expected
        }

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_cte_dml_target');
        $this->assertSame(2, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_cte_dml_source');
        $this->assertSame(3, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation: seed data is only in shadow.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_cte_dml_target');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(
                sprintf('mysql:host=%s;port=%d;dbname=test', MySQLContainer::getHost(), MySQLContainer::getPort()),
                'root',
                'root',
            );
            $raw->exec('DROP TABLE IF EXISTS pdo_cte_dml_target');
            $raw->exec('DROP TABLE IF EXISTS pdo_cte_dml_source');
        } catch (\Exception $e) {
        }
    }
}
