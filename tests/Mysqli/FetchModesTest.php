<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests various MySQLi fetch modes with ZTD shadow operations.
 *
 * Cross-platform parity with SqliteFetchModesTest (PDO).
 */
class FetchModesTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_fm_test');
        $raw->query('CREATE TABLE mi_fm_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
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

        $this->mysqli->query("INSERT INTO mi_fm_test VALUES (1, 'Alice', 90)");
        $this->mysqli->query("INSERT INTO mi_fm_test VALUES (2, 'Bob', 85)");
        $this->mysqli->query("INSERT INTO mi_fm_test VALUES (3, 'Charlie', 95)");
    }

    /**
     * fetch_assoc returns associative array.
     */
    public function testFetchAssoc(): void
    {
        $result = $this->mysqli->query('SELECT id, name, score FROM mi_fm_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);
    }

    /**
     * fetch_row returns numeric-indexed array.
     */
    public function testFetchRow(): void
    {
        $result = $this->mysqli->query('SELECT id, name, score FROM mi_fm_test WHERE id = 1');
        $row = $result->fetch_row();
        $this->assertSame('Alice', $row[1]);
    }

    /**
     * fetch_object returns stdClass.
     */
    public function testFetchObject(): void
    {
        $result = $this->mysqli->query('SELECT id, name, score FROM mi_fm_test WHERE id = 2');
        $obj = $result->fetch_object();
        $this->assertIsObject($obj);
        $this->assertSame('Bob', $obj->name);
    }

    /**
     * fetch_all with MYSQLI_ASSOC.
     */
    public function testFetchAllAssoc(): void
    {
        $result = $this->mysqli->query('SELECT name FROM mi_fm_test ORDER BY id');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Charlie', $rows[2]['name']);
    }

    /**
     * fetch_all with MYSQLI_NUM.
     */
    public function testFetchAllNum(): void
    {
        $result = $this->mysqli->query('SELECT name, score FROM mi_fm_test ORDER BY id');
        $rows = $result->fetch_all(MYSQLI_NUM);
        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0][0]);
    }

    /**
     * Fetch modes work after shadow mutation.
     */
    public function testFetchModesAfterMutation(): void
    {
        $this->mysqli->query("INSERT INTO mi_fm_test VALUES (4, 'Diana', 88)");

        $result = $this->mysqli->query('SELECT name FROM mi_fm_test ORDER BY id');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(4, $rows);
        $this->assertSame('Diana', $rows[3]['name']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_fm_test');
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
            $raw->query('DROP TABLE IF EXISTS mi_fm_test');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
