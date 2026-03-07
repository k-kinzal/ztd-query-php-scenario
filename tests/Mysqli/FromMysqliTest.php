<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use mysqli;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

class FromMysqliTest extends TestCase
{
    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS wrap_test');
        $raw->query('CREATE TABLE wrap_test (id INT PRIMARY KEY, val VARCHAR(255))');
        $raw->close();
    }

    public function testFromMysqliWrapsExistingConnection(): void
    {
        $rawMysqli = new mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );

        $ztd = ZtdMysqli::fromMysqli($rawMysqli);
        $this->assertTrue($ztd->isZtdEnabled());

        $ztd->query("INSERT INTO wrap_test (id, val) VALUES (1, 'hello')");

        $result = $ztd->query('SELECT * FROM wrap_test WHERE id = 1');
        $row = $result->fetch_assoc();

        $this->assertSame('hello', $row['val']);
    }

    public function testFromMysqliIsolatesFromPhysicalTable(): void
    {
        $rawMysqli = new mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );

        $ztd = ZtdMysqli::fromMysqli($rawMysqli);
        $ztd->query("INSERT INTO wrap_test (id, val) VALUES (1, 'hello')");

        // Physical table should be empty (use a separate connection to verify)
        $verify = new mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $result = $verify->query('SELECT * FROM wrap_test');
        $this->assertSame(0, $result->num_rows);
        $verify->close();
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS wrap_test');
        $raw->close();
    }
}
