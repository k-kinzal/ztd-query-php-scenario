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
 * Tests MySQL-specific INSERT ... SET syntax on MySQL PDO ZTD.
 *
 * MySQL supports: INSERT INTO table SET col1 = val1, col2 = val2
 * The InsertTransformer handles this via buildInsertSetSelect().
 */
class MysqlInsertSetSyntaxTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
        $raw->exec('DROP TABLE IF EXISTS pdo_ins_set');
        $raw->exec('CREATE TABLE pdo_ins_set (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(MySQLContainer::getDsn(), 'root', 'root');
    }

    /**
     * Basic INSERT ... SET syntax.
     */
    public function testInsertSetBasic(): void
    {
        $this->pdo->exec("INSERT INTO pdo_ins_set SET id = 1, name = 'Alice', score = 90");

        $stmt = $this->pdo->query('SELECT * FROM pdo_ins_set WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
        $this->assertSame(90, (int) $row['score']);
    }

    /**
     * Multiple INSERT ... SET statements.
     */
    public function testInsertSetMultipleRows(): void
    {
        $this->pdo->exec("INSERT INTO pdo_ins_set SET id = 1, name = 'Alice', score = 90");
        $this->pdo->exec("INSERT INTO pdo_ins_set SET id = 2, name = 'Bob', score = 80");
        $this->pdo->exec("INSERT INTO pdo_ins_set SET id = 3, name = 'Charlie', score = 70");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_ins_set');
        $this->assertSame(3, (int) $stmt->fetchColumn());
    }

    /**
     * INSERT ... SET then update.
     */
    public function testInsertSetThenUpdate(): void
    {
        $this->pdo->exec("INSERT INTO pdo_ins_set SET id = 1, name = 'Alice', score = 90");
        $this->pdo->exec("UPDATE pdo_ins_set SET score = 100 WHERE id = 1");

        $stmt = $this->pdo->query('SELECT score FROM pdo_ins_set WHERE id = 1');
        $this->assertSame(100, (int) $stmt->fetchColumn());
    }

    /**
     * INSERT ... SET ... ON DUPLICATE KEY UPDATE.
     */
    public function testInsertSetOnDuplicateKeyUpdate(): void
    {
        $this->pdo->exec("INSERT INTO pdo_ins_set SET id = 1, name = 'Alice', score = 90");
        $this->pdo->exec("INSERT INTO pdo_ins_set SET id = 1, name = 'Alice V2', score = 95 ON DUPLICATE KEY UPDATE name = VALUES(name), score = VALUES(score)");

        $stmt = $this->pdo->query('SELECT name, score FROM pdo_ins_set WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice V2', $row['name']);
        $this->assertSame(95, (int) $row['score']);
    }

    /**
     * Physical isolation: INSERT ... SET stays in shadow.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pdo_ins_set SET id = 1, name = 'Alice', score = 90");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_ins_set');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
            $raw->exec('DROP TABLE IF EXISTS pdo_ins_set');
        } catch (\Exception $e) {
        }
    }
}
