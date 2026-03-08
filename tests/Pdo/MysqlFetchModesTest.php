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
 * Tests various PDO fetch modes with ZTD shadow operations on MySQL.
 *
 * Cross-platform parity with SqliteFetchModesTest.
 */
class MysqlFetchModesTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS pdo_mfm_test');
        $raw->exec('CREATE TABLE pdo_mfm_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
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

        $this->pdo->exec("INSERT INTO pdo_mfm_test VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO pdo_mfm_test VALUES (2, 'Bob', 85)");
        $this->pdo->exec("INSERT INTO pdo_mfm_test VALUES (3, 'Charlie', 95)");
    }

    /**
     * FETCH_OBJ returns stdClass objects.
     */
    public function testFetchObj(): void
    {
        $stmt = $this->pdo->query('SELECT id, name, score FROM pdo_mfm_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_OBJ);
        $this->assertIsObject($row);
        $this->assertSame('Alice', $row->name);
    }

    /**
     * FETCH_NUM returns numeric-indexed array.
     */
    public function testFetchNum(): void
    {
        $stmt = $this->pdo->query('SELECT id, name, score FROM pdo_mfm_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_NUM);
        $this->assertSame('Alice', $row[1]);
    }

    /**
     * fetchAll with FETCH_COLUMN.
     */
    public function testFetchAllColumn(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM pdo_mfm_test ORDER BY id');
        $names = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Alice', 'Bob', 'Charlie'], $names);
    }

    /**
     * fetchObject returns typed object.
     */
    public function testFetchObject(): void
    {
        $stmt = $this->pdo->query('SELECT id, name, score FROM pdo_mfm_test WHERE id = 2');
        $obj = $stmt->fetchObject();
        $this->assertIsObject($obj);
        $this->assertSame('Bob', $obj->name);
    }

    /**
     * Fetch modes work after shadow mutation.
     */
    public function testFetchModesAfterMutation(): void
    {
        $this->pdo->exec("INSERT INTO pdo_mfm_test VALUES (4, 'Diana', 88)");

        $stmt = $this->pdo->query('SELECT name FROM pdo_mfm_test ORDER BY id');
        $names = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(4, $names);
        $this->assertSame('Diana', $names[3]);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_mfm_test');
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
            $raw->exec('DROP TABLE IF EXISTS pdo_mfm_test');
        } catch (\Exception $e) {
        }
    }
}
