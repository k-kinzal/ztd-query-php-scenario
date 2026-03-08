<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Scenarios\TransactionScenario;
use Tests\Support\AbstractMysqliTestCase;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

class TransactionTest extends AbstractMysqliTestCase
{
    use TransactionScenario;

    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE tx_test (id INT PRIMARY KEY, val VARCHAR(255))';
    }

    protected function getTableNames(): array
    {
        return ['tx_test'];
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
}
