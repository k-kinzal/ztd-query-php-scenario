<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests INSERT with DEFAULT keyword on MySQLi ZTD.
 *
 * Cross-platform parity with MysqlInsertDefaultValuesTest (PDO).
 * DEFAULT keyword in INSERT VALUES fails under ZTD because InsertTransformer
 * converts VALUES to SELECT expressions where DEFAULT is invalid.
 */
class InsertDefaultValuesTest extends TestCase
{
    private ZtdMysqli $mysqli;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS mi_idef_test');
        $raw->query("CREATE TABLE mi_idef_test (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(50) DEFAULT 'default_name',
            score INT DEFAULT 100
        )");
        $raw->close();
    }

    protected function setUp(): void
    {
        $this->mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
    }

    /**
     * INSERT with DEFAULT keyword fails under ZTD.
     */
    public function testInsertWithDefaultKeywordFails(): void
    {
        $this->expectException(\Throwable::class);
        $this->mysqli->query('INSERT INTO mi_idef_test (name, score) VALUES (DEFAULT, 50)');
    }

    /**
     * INSERT with all DEFAULT values fails under ZTD.
     */
    public function testInsertWithAllDefaultsFails(): void
    {
        $this->expectException(\Throwable::class);
        $this->mysqli->query('INSERT INTO mi_idef_test (name, score) VALUES (DEFAULT, DEFAULT)');
    }

    /**
     * INSERT with mix of explicit and DEFAULT fails under ZTD.
     */
    public function testInsertWithMixedDefaultAndExplicitFails(): void
    {
        $this->expectException(\Throwable::class);
        $this->mysqli->query("INSERT INTO mi_idef_test (name, score) VALUES ('Alice', DEFAULT)");
    }

    /**
     * INSERT with only explicit values works normally.
     */
    public function testInsertWithExplicitValuesWorks(): void
    {
        $this->mysqli->query("INSERT INTO mi_idef_test (name, score) VALUES ('Alice', 90)");

        $result = $this->mysqli->query("SELECT name, score FROM mi_idef_test WHERE name = 'Alice'");
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);
        $this->assertSame(90, (int) $row['score']);
    }

    /**
     * Physical isolation with explicit values.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_idef_test (name, score) VALUES ('Bob', 80)");

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_idef_test');
        $this->assertGreaterThanOrEqual(1, (int) $result->fetch_assoc()['cnt']);

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_idef_test');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }

    protected function tearDown(): void
    {
        if (isset($this->mysqli)) {
            $this->mysqli->close();
        }
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new \mysqli(
                MySQLContainer::getHost(),
                'root',
                'root',
                'test',
                MySQLContainer::getPort(),
            );
            $raw->query('DROP TABLE IF EXISTS mi_idef_test');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
