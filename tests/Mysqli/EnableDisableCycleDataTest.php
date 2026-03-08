<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests ZTD enable/disable cycle with data persistence via MySQLi.
 *
 * Cross-platform parity with SqliteEnableDisableCycleDataTest (PDO).
 */
class EnableDisableCycleDataTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_edc_test');
        $raw->query('CREATE TABLE mi_edc_test (id INT PRIMARY KEY, name VARCHAR(50), val INT)');
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
     * Shadow data persists through disable/re-enable cycle.
     */
    public function testShadowDataPersistsThroughCycle(): void
    {
        $this->mysqli->query("INSERT INTO mi_edc_test VALUES (1, 'Alice', 100)");

        $this->mysqli->disableZtd();
        $this->mysqli->enableZtd();

        $result = $this->mysqli->query('SELECT name FROM mi_edc_test WHERE id = 1');
        $this->assertSame('Alice', $result->fetch_assoc()['name']);
    }

    /**
     * Multiple toggle cycles accumulate shadow data.
     */
    public function testMultipleToggleCyclesAccumulate(): void
    {
        $this->mysqli->query("INSERT INTO mi_edc_test VALUES (1, 'First', 10)");

        $this->mysqli->disableZtd();
        $this->mysqli->enableZtd();

        $this->mysqli->query("INSERT INTO mi_edc_test VALUES (2, 'Second', 20)");

        $this->mysqli->disableZtd();
        $this->mysqli->enableZtd();

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_edc_test');
        $this->assertSame(2, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * UPDATE persists through toggle cycle.
     */
    public function testUpdatePersistsThroughCycle(): void
    {
        $this->mysqli->query("INSERT INTO mi_edc_test VALUES (1, 'Alice', 100)");
        $this->mysqli->query('UPDATE mi_edc_test SET val = 999 WHERE id = 1');

        $this->mysqli->disableZtd();
        $this->mysqli->enableZtd();

        $result = $this->mysqli->query('SELECT val FROM mi_edc_test WHERE id = 1');
        $this->assertSame(999, (int) $result->fetch_assoc()['val']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_edc_test VALUES (1, 'Alice', 100)");

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_edc_test');
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
            $raw->query('DROP TABLE IF EXISTS mi_edc_test');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
