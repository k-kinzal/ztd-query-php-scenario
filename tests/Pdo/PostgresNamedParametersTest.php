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
 * Tests PDO named parameter binding (:param) with ZTD on PostgreSQL.
 *
 * Cross-platform parity with SqliteNamedParametersTest.
 * @spec pending
 */
class PostgresNamedParametersTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            'pgsql:host=' . PostgreSQLContainer::getHost() . ';port=' . PostgreSQLContainer::getPort() . ';dbname=test',
            'test',
            'test',
        );
        $raw->exec('DROP TABLE IF EXISTS pdo_pnp_test');
        $raw->exec('CREATE TABLE pdo_pnp_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
    }

    protected function setUp(): void
    {
        $raw = new PDO(
            'pgsql:host=' . PostgreSQLContainer::getHost() . ';port=' . PostgreSQLContainer::getPort() . ';dbname=test',
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $this->pdo = ZtdPdo::fromPdo($raw);

        $this->pdo->exec("INSERT INTO pdo_pnp_test VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO pdo_pnp_test VALUES (2, 'Bob', 85)");
        $this->pdo->exec("INSERT INTO pdo_pnp_test VALUES (3, 'Charlie', 95)");
    }

    /**
     * Named parameters via execute().
     */
    public function testNamedParamsViaExecute(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM pdo_pnp_test WHERE score > :min_score ORDER BY name');
        $stmt->execute([':min_score' => 88]);

        $names = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Alice', 'Charlie'], $names);
    }

    /**
     * Named parameters via bindValue().
     */
    public function testNamedParamsViaBindValue(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM pdo_pnp_test WHERE id = :id');
        $stmt->bindValue(':id', 2, PDO::PARAM_INT);
        $stmt->execute();

        $this->assertSame('Bob', $stmt->fetchColumn());
    }

    /**
     * Multiple named parameters.
     */
    public function testMultipleNamedParams(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM pdo_pnp_test WHERE score >= :min AND score <= :max ORDER BY name');
        $stmt->execute([':min' => 85, ':max' => 90]);

        $names = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Alice', 'Bob'], $names);
    }

    /**
     * Named parameters in INSERT.
     */
    public function testNamedParamsInInsert(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO pdo_pnp_test VALUES (:id, :name, :score)');
        $stmt->execute([':id' => 4, ':name' => 'Diana', ':score' => 92]);

        $qstmt = $this->pdo->query('SELECT name FROM pdo_pnp_test WHERE id = 4');
        $this->assertSame('Diana', $qstmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_pnp_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(
                'pgsql:host=' . PostgreSQLContainer::getHost() . ';port=' . PostgreSQLContainer::getPort() . ';dbname=test',
                'test',
                'test',
            );
            $raw->exec('DROP TABLE IF EXISTS pdo_pnp_test');
        } catch (\Exception $e) {
        }
    }
}
