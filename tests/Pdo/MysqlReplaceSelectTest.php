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
 * Tests REPLACE INTO ... SELECT on MySQL PDO ZTD.
 *
 * MySQL supports REPLACE INTO ... SELECT to replace/insert rows from a SELECT.
 * The ReplaceTransformer handles this via $statement->select->build().
 */
class MysqlReplaceSelectTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
        $raw->exec('DROP TABLE IF EXISTS pdo_rsel_target');
        $raw->exec('DROP TABLE IF EXISTS pdo_rsel_source');
        $raw->exec('CREATE TABLE pdo_rsel_source (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
        $raw->exec('CREATE TABLE pdo_rsel_target (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(MySQLContainer::getDsn(), 'root', 'root');

        $this->pdo->exec("INSERT INTO pdo_rsel_source (id, name, score) VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO pdo_rsel_source (id, name, score) VALUES (2, 'Bob', 80)");
        $this->pdo->exec("INSERT INTO pdo_rsel_source (id, name, score) VALUES (3, 'Charlie', 70)");
    }

    /**
     * REPLACE INTO ... SELECT — all new rows.
     */
    public function testReplaceSelectAllNew(): void
    {
        $this->pdo->exec('REPLACE INTO pdo_rsel_target (id, name, score) SELECT id, name, score FROM pdo_rsel_source');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_rsel_target');
        $this->assertSame(3, (int) $stmt->fetchColumn());
    }

    /**
     * REPLACE INTO ... SELECT — with existing rows.
     */
    public function testReplaceSelectWithConflict(): void
    {
        $this->pdo->exec("INSERT INTO pdo_rsel_target (id, name, score) VALUES (1, 'Old_Alice', 50)");

        $this->pdo->exec('REPLACE INTO pdo_rsel_target (id, name, score) SELECT id, name, score FROM pdo_rsel_source');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_rsel_target');
        $this->assertSame(3, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT name, score FROM pdo_rsel_target WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
        $this->assertSame(90, (int) $row['score']);
    }

    /**
     * REPLACE INTO ... SELECT with WHERE filter.
     */
    public function testReplaceSelectWithWhere(): void
    {
        $this->pdo->exec('REPLACE INTO pdo_rsel_target (id, name, score) SELECT id, name, score FROM pdo_rsel_source WHERE score >= 80');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_rsel_target');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec('REPLACE INTO pdo_rsel_target (id, name, score) SELECT id, name, score FROM pdo_rsel_source');

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_rsel_target');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
            $raw->exec('DROP TABLE IF EXISTS pdo_rsel_target');
            $raw->exec('DROP TABLE IF EXISTS pdo_rsel_source');
        } catch (\Exception $e) {
        }
    }
}
