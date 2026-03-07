<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

class SessionIsolationTest extends TestCase
{
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
        $raw->query('DROP TABLE IF EXISTS session_test');
        $raw->query('CREATE TABLE session_test (id INT PRIMARY KEY, val VARCHAR(255))');
        $raw->close();
    }

    public function testShadowDataNotSharedBetweenInstances(): void
    {
        $ztd1 = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $ztd2 = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );

        // Insert in instance 1
        $ztd1->query("INSERT INTO session_test (id, val) VALUES (1, 'from_ztd1')");

        // Instance 1 sees the row
        $result = $ztd1->query('SELECT * FROM session_test WHERE id = 1');
        $this->assertSame(1, $result->num_rows);

        // Instance 2 does NOT see it
        $result = $ztd2->query('SELECT * FROM session_test WHERE id = 1');
        $this->assertSame(0, $result->num_rows);

        $ztd1->close();
        $ztd2->close();
    }

    public function testShadowDataNotPersistedAcrossLifecycle(): void
    {
        $ztd = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $ztd->query("INSERT INTO session_test (id, val) VALUES (1, 'temporary')");

        $result = $ztd->query('SELECT * FROM session_test WHERE id = 1');
        $this->assertSame(1, $result->num_rows);
        $ztd->close();

        // New instance should NOT see previous shadow data
        $ztd2 = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $result = $ztd2->query('SELECT * FROM session_test WHERE id = 1');
        $this->assertSame(0, $result->num_rows);

        $ztd2->close();
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS session_test');
        $raw->close();
    }
}
