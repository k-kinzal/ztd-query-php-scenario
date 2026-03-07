<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

class TransactionTest extends TestCase
{
    private ZtdMysqli $mysqli;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);
    }

    protected function setUp(): void
    {
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS tx_test');
        $raw->query('CREATE TABLE tx_test (id INT PRIMARY KEY, val VARCHAR(255))');
        $raw->close();

        $this->mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
    }

    public function testBeginTransactionAndCommit(): void
    {
        $this->assertTrue($this->mysqli->begin_transaction());
        $this->mysqli->query("INSERT INTO tx_test (id, val) VALUES (1, 'hello')");
        $this->assertTrue($this->mysqli->commit());

        // Shadow data should still be visible
        $result = $this->mysqli->query('SELECT * FROM tx_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('hello', $row['val']);
    }

    public function testBeginTransactionAndRollback(): void
    {
        $this->mysqli->query("INSERT INTO tx_test (id, val) VALUES (1, 'before_tx')");

        $this->assertTrue($this->mysqli->begin_transaction());
        $this->assertTrue($this->mysqli->rollback());

        // Shadow data from before transaction should still be visible
        $result = $this->mysqli->query('SELECT * FROM tx_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('before_tx', $row['val']);
    }

    public function testSavepointAndRelease(): void
    {
        $this->assertTrue($this->mysqli->begin_transaction());
        $this->assertTrue($this->mysqli->savepoint('sp1'));
        $this->mysqli->query("INSERT INTO tx_test (id, val) VALUES (1, 'hello')");
        $this->assertTrue($this->mysqli->release_savepoint('sp1'));
        $this->assertTrue($this->mysqli->commit());

        $result = $this->mysqli->query('SELECT * FROM tx_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('hello', $row['val']);
    }

    public function testInsertIdPropertyNotAvailableInZtdMode(): void
    {
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS auto_inc_test');
        $raw->query('CREATE TABLE auto_inc_test (id INT AUTO_INCREMENT PRIMARY KEY, val VARCHAR(255))');
        $raw->close();

        $mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );

        $mysqli->query("INSERT INTO auto_inc_test (val) VALUES ('hello')");

        // insert_id property access throws Error when ZTD is enabled
        // because the INSERT was simulated in the shadow store and never
        // hit the physical database
        $this->expectException(\Error::class);
        $this->expectExceptionMessage('Property access is not allowed yet');
        $_ = $mysqli->insert_id;

        $mysqli->close();

        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS auto_inc_test');
        $raw->close();
    }

    public function testRealEscapeString(): void
    {
        $escaped = $this->mysqli->real_escape_string("it's a test");
        $this->assertIsString($escaped);
        $this->assertStringContainsString("\\'", $escaped);
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
        $raw->query('DROP TABLE IF EXISTS tx_test');
        $raw->close();
    }
}
