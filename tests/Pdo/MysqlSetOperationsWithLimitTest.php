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
 * Tests UNION with LIMIT/OFFSET on MySQL PDO.
 *
 * Cross-platform parity with SqliteSetOperationsWithLimitTest
 * and PostgresSetOperationsWithLimitTest.
 */
class MysqlSetOperationsWithLimitTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
        $raw->exec('DROP TABLE IF EXISTS pdo_mset_a');
        $raw->exec('DROP TABLE IF EXISTS pdo_mset_b');
        $raw->exec('CREATE TABLE pdo_mset_a (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
        $raw->exec('CREATE TABLE pdo_mset_b (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(MySQLContainer::getDsn(), 'root', 'root');

        $this->pdo->exec("INSERT INTO pdo_mset_a VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO pdo_mset_a VALUES (2, 'Bob', 80)");
        $this->pdo->exec("INSERT INTO pdo_mset_a VALUES (3, 'Charlie', 70)");

        $this->pdo->exec("INSERT INTO pdo_mset_b VALUES (4, 'Bob', 80)");
        $this->pdo->exec("INSERT INTO pdo_mset_b VALUES (5, 'Diana', 60)");
        $this->pdo->exec("INSERT INTO pdo_mset_b VALUES (6, 'Eve', 50)");
    }

    /**
     * UNION ALL with LIMIT.
     */
    public function testUnionAllWithLimit(): void
    {
        $stmt = $this->pdo->query('
            SELECT name, score FROM pdo_mset_a
            UNION ALL
            SELECT name, score FROM pdo_mset_b
            LIMIT 4
        ');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(4, $rows);
    }

    /**
     * UNION with ORDER BY, LIMIT, OFFSET.
     */
    public function testUnionWithOrderByLimitOffset(): void
    {
        $stmt = $this->pdo->query('
            SELECT name, score FROM pdo_mset_a
            UNION
            SELECT name, score FROM pdo_mset_b
            ORDER BY name
            LIMIT 3 OFFSET 1
        ');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(3, $rows);
        $this->assertSame('Bob', $rows[0]);
    }

    /**
     * UNION reflects INSERT mutation.
     */
    public function testUnionReflectsInsert(): void
    {
        $this->pdo->exec("INSERT INTO pdo_mset_a VALUES (7, 'Frank', 95)");

        $stmt = $this->pdo->query('
            SELECT name FROM pdo_mset_a
            UNION ALL
            SELECT name FROM pdo_mset_b
            ORDER BY name
        ');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertContains('Frank', $rows);
        $this->assertCount(7, $rows);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_mset_a');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
            $raw->exec('DROP TABLE IF EXISTS pdo_mset_a');
            $raw->exec('DROP TABLE IF EXISTS pdo_mset_b');
        } catch (\Exception $e) {
        }
    }
}
