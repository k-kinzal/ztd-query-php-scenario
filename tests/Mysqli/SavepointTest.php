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
 *
 * MySQL rewriter treats SAVEPOINT/RELEASE as unparseable SQL and throws.
 * ROLLBACK TO SAVEPOINT throws "Statement type not supported."
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

    public function testSavepointThrowsUnparseableException(): void
    {
        $this->mysqli->begin_transaction();

        $this->expectException(ZtdMysqliException::class);
        $this->expectExceptionMessage('Empty or unparseable SQL');
        $this->mysqli->query('SAVEPOINT sp1');
    }

    public function testReleaseSavepointThrowsUnparseableException(): void
    {
        $this->expectException(ZtdMysqliException::class);
        $this->expectExceptionMessage('Empty or unparseable SQL');
        $this->mysqli->query('RELEASE SAVEPOINT sp1');
    }

    public function testRollbackToSavepointThrowsUnsupportedSqlException(): void
    {
        $this->expectException(ZtdMysqliException::class);
        $this->expectExceptionMessage('Statement type not supported');
        $this->mysqli->query('ROLLBACK TO SAVEPOINT sp1');
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
