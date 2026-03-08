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
 * Tests INSERT with DEFAULT keyword on MySQL PDO ZTD.
 *
 * Limitation: InsertTransformer converts INSERT VALUES to SELECT expressions.
 * DEFAULT is only valid in INSERT VALUES context, not in SELECT.
 * So INSERT INTO t (col) VALUES (DEFAULT) becomes SELECT DEFAULT AS `col`
 * which is a MySQL syntax error.
 *
 * INSERT with explicit values works normally.
 */
class MysqlInsertDefaultValuesTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
        $raw->exec('DROP TABLE IF EXISTS pdo_idef_test');
        $raw->exec('CREATE TABLE pdo_idef_test (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(50) DEFAULT \'default_name\',
            score INT DEFAULT 100
        )');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(MySQLContainer::getDsn(), 'root', 'root');
    }

    /**
     * INSERT with DEFAULT keyword fails under ZTD.
     *
     * InsertTransformer converts VALUES(DEFAULT, 50) to SELECT DEFAULT AS `name`, 50 AS `score`
     * which is invalid SQL — DEFAULT is not allowed in SELECT context.
     */
    public function testInsertWithDefaultKeywordFails(): void
    {
        $this->expectException(\Throwable::class);
        $this->pdo->exec("INSERT INTO pdo_idef_test (name, score) VALUES (DEFAULT, 50)");
    }

    /**
     * INSERT with all DEFAULT values fails under ZTD.
     */
    public function testInsertWithAllDefaultsFails(): void
    {
        $this->expectException(\Throwable::class);
        $this->pdo->exec('INSERT INTO pdo_idef_test (name, score) VALUES (DEFAULT, DEFAULT)');
    }

    /**
     * INSERT with mix of explicit and DEFAULT fails under ZTD.
     */
    public function testInsertWithMixedDefaultAndExplicitFails(): void
    {
        $this->expectException(\Throwable::class);
        $this->pdo->exec("INSERT INTO pdo_idef_test (name, score) VALUES ('Alice', DEFAULT)");
    }

    /**
     * INSERT with only explicit values works normally.
     */
    public function testInsertWithExplicitValuesWorks(): void
    {
        $this->pdo->exec("INSERT INTO pdo_idef_test (name, score) VALUES ('Alice', 90)");

        $stmt = $this->pdo->query("SELECT name, score FROM pdo_idef_test WHERE name = 'Alice'");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
        $this->assertSame(90, (int) $row['score']);
    }

    /**
     * Physical isolation with explicit values.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pdo_idef_test (name, score) VALUES ('Bob', 80)");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_idef_test');
        $this->assertGreaterThanOrEqual(1, (int) $stmt->fetchColumn());

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_idef_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
            $raw->exec('DROP TABLE IF EXISTS pdo_idef_test');
        } catch (\Exception $e) {
        }
    }
}
