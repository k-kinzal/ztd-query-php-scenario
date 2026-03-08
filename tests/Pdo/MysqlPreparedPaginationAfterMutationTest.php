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
 * Tests prepared pagination (LIMIT/OFFSET) with shadow mutations on MySQL PDO.
 *
 * Cross-platform parity with SqlitePreparedPaginationAfterMutationTest.
 * @spec SPEC-3.2
 */
class MysqlPreparedPaginationAfterMutationTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            'mysql:host=' . MySQLContainer::getHost() . ';port=' . MySQLContainer::getPort() . ';dbname=test',
            'root',
            'root',
        );
        $raw->exec('DROP TABLE IF EXISTS pdo_mppag_test');
        $raw->exec('CREATE TABLE pdo_mppag_test (id INT PRIMARY KEY, name VARCHAR(50), category VARCHAR(10))');
    }

    protected function setUp(): void
    {
        $raw = new PDO(
            'mysql:host=' . MySQLContainer::getHost() . ';port=' . MySQLContainer::getPort() . ';dbname=test',
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $this->pdo = ZtdPdo::fromPdo($raw);

        for ($i = 1; $i <= 10; $i++) {
            $cat = $i <= 5 ? 'A' : 'B';
            $this->pdo->exec("INSERT INTO pdo_mppag_test VALUES ($i, 'Item$i', '$cat')");
        }
    }

    /**
     * Parameterized LIMIT and OFFSET.
     */
    public function testPreparedLimitOffset(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM pdo_mppag_test ORDER BY id LIMIT ? OFFSET ?');
        $stmt->bindValue(1, 3, PDO::PARAM_INT);
        $stmt->bindValue(2, 0, PDO::PARAM_INT);
        $stmt->execute();
        $page1 = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(3, $page1);
        $this->assertSame('Item1', $page1[0]);
    }

    /**
     * Pagination after INSERT.
     */
    public function testPaginationAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO pdo_mppag_test VALUES (11, 'Item11', 'A')");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_mppag_test');
        $this->assertSame(11, (int) $stmt->fetchColumn());
    }

    /**
     * Pagination after DELETE.
     */
    public function testPaginationAfterDelete(): void
    {
        $this->pdo->exec("DELETE FROM pdo_mppag_test WHERE category = 'B'");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_mppag_test');
        $this->assertSame(5, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_mppag_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(
                'mysql:host=' . MySQLContainer::getHost() . ';port=' . MySQLContainer::getPort() . ';dbname=test',
                'root',
                'root',
            );
            $raw->exec('DROP TABLE IF EXISTS pdo_mppag_test');
        } catch (\Exception $e) {
        }
    }
}
