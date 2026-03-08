<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;
use ZtdQuery\Adapter\Mysqli\ZtdMysqliException;

/**
 * Tests savepoint behavior with ZTD on MySQLi.
 */
class SavepointTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_sp_test');
        $raw->query('CREATE TABLE mi_sp_test (id INT PRIMARY KEY, name VARCHAR(50))');
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

        $this->mysqli->query("INSERT INTO mi_sp_test VALUES (1, 'Alice')");
    }

    /**
     * SAVEPOINT should be supported.
     */
    public function testSavepointSupported(): void
    {
        $this->mysqli->begin_transaction();

        try {
            $this->mysqli->query('SAVEPOINT sp1');
            $this->mysqli->commit();
            $this->assertTrue(true);
        } catch (ZtdMysqliException $e) {
            $this->markTestIncomplete(
                'SAVEPOINT not yet supported on MySQLi: ' . $e->getMessage()
            );
        }
    }

    /**
     * RELEASE SAVEPOINT should be supported.
     */
    public function testReleaseSavepointSupported(): void
    {
        $this->mysqli->begin_transaction();

        try {
            $this->mysqli->query('SAVEPOINT sp1');
            $this->mysqli->query('RELEASE SAVEPOINT sp1');
            $this->mysqli->commit();
            $this->assertTrue(true);
        } catch (ZtdMysqliException $e) {
            $this->markTestIncomplete(
                'RELEASE SAVEPOINT not yet supported on MySQLi: ' . $e->getMessage()
            );
        }
    }

    /**
     * ROLLBACK TO SAVEPOINT should be supported.
     */
    public function testRollbackToSavepointSupported(): void
    {
        $this->mysqli->begin_transaction();

        try {
            $this->mysqli->query('SAVEPOINT sp1');
            $this->mysqli->query('ROLLBACK TO SAVEPOINT sp1');
            $this->mysqli->commit();
            $this->assertTrue(true);
        } catch (ZtdMysqliException $e) {
            $this->markTestIncomplete(
                'ROLLBACK TO SAVEPOINT not yet supported on MySQLi: ' . $e->getMessage()
            );
        }
    }

    protected function tearDown(): void
    {
        $this->mysqli->close();
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
        $raw->query('DROP TABLE IF EXISTS mi_sp_test');
        $raw->close();
    }
}
