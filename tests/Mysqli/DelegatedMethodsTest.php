<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/** @spec SPEC-4.9 */
class DelegatedMethodsTest extends TestCase
{
    private ZtdMysqli $mysqli;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);
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

    public function testMultiQueryDelegatesToInner(): void
    {
        $this->mysqli->disableZtd();
        $result = $this->mysqli->multi_query('SELECT 1; SELECT 2');
        $this->assertTrue($result);

        // Consume first result
        $res = $this->mysqli->store_result();
        $this->assertInstanceOf(\mysqli_result::class, $res);
        $res->free();

        // Move to next result
        $this->assertTrue($this->mysqli->more_results());
        $this->assertTrue($this->mysqli->next_result());

        $res = $this->mysqli->store_result();
        $this->assertInstanceOf(\mysqli_result::class, $res);
        $res->free();
    }

    public function testAutocommitDelegatesToInner(): void
    {
        $this->assertTrue($this->mysqli->autocommit(false));
        $this->assertTrue($this->mysqli->autocommit(true));
    }

    public function testSetCharsetDelegatesToInner(): void
    {
        $this->assertTrue($this->mysqli->set_charset('utf8mb4'));
    }

    public function testCharacterSetNameDelegatesToInner(): void
    {
        $name = $this->mysqli->character_set_name();
        $this->assertIsString($name);
        $this->assertNotEmpty($name);
    }

    public function testGetCharsetDelegatesToInner(): void
    {
        $charset = $this->mysqli->get_charset();
        $this->assertIsObject($charset);
        $this->assertObjectHasProperty('charset', $charset);
    }

    public function testEscapeStringDelegatesToInner(): void
    {
        $escaped = $this->mysqli->escape_string("it's a test");
        $this->assertIsString($escaped);
        $this->assertStringContainsString("\\'", $escaped);
    }

    public function testStatDelegatesToInner(): void
    {
        $stat = $this->mysqli->stat();
        $this->assertIsString($stat);
        $this->assertNotEmpty($stat);
    }

    public function testGetServerInfoDelegatesToInner(): void
    {
        $info = $this->mysqli->get_server_info();
        $this->assertIsString($info);
        $this->assertNotEmpty($info);
    }

    public function testGetConnectionStatsDelegatesToInner(): void
    {
        $stats = $this->mysqli->get_connection_stats();
        $this->assertIsArray($stats);
        $this->assertNotEmpty($stats);
    }

    public function testSelectDbDelegatesToInner(): void
    {
        $this->assertTrue($this->mysqli->select_db('test'));
    }

    public function testPingDelegatesToInner(): void
    {
        $result = $this->mysqli->ping();
        $this->assertTrue($result);
    }

    public function testPropertyAccessThrowsError(): void
    {
        // Property access on ZtdMysqli throws Error because the parent mysqli
        // constructor doesn't initialize with a real connection. The C-extension
        // property handler takes precedence over __get.
        $this->expectException(\Error::class);
        $this->expectExceptionMessage('Property access is not allowed yet');
        $_ = $this->mysqli->server_version;
    }

    public function testPropertyAccessOnFromMysqliThrowsError(): void
    {
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $ztd = ZtdMysqli::fromMysqli($raw);

        // fromMysqli also cannot access properties via __get due to
        // C extension property handler taking precedence
        $this->expectException(\Error::class);
        $_ = $ztd->server_version;
    }

    public function testExecuteQueryWithParams(): void
    {
        $this->mysqli->disableZtd();
        $this->mysqli->query('DROP TABLE IF EXISTS exec_query_test');
        $this->mysqli->query('CREATE TABLE exec_query_test (id INT PRIMARY KEY, val VARCHAR(255))');
        $this->mysqli->enableZtd();

        $this->mysqli->query("INSERT INTO exec_query_test (id, val) VALUES (1, 'hello')");

        $result = $this->mysqli->execute_query(
            'SELECT * FROM exec_query_test WHERE id = ?',
            [1]
        );
        $this->assertInstanceOf(\mysqli_result::class, $result);
        $row = $result->fetch_assoc();
        $this->assertSame('hello', $row['val']);

        $this->mysqli->disableZtd();
        $this->mysqli->query('DROP TABLE IF EXISTS exec_query_test');
    }

    protected function tearDown(): void
    {
        $this->mysqli->close();
    }
}
