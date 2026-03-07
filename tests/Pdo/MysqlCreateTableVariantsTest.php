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
 * Tests CREATE TABLE LIKE and CREATE TABLE AS SELECT on MySQL via PDO.
 */
class MysqlCreateTableVariantsTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mysql_ct_target_like');
        $raw->exec('DROP TABLE IF EXISTS mysql_ct_target_ctas');
        $raw->exec('DROP TABLE IF EXISTS mysql_ct_source');
        $raw->exec('CREATE TABLE mysql_ct_source (id INT PRIMARY KEY, val VARCHAR(255))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
    }

    public function testCreateTableLike(): void
    {
        $this->pdo->exec('CREATE TABLE mysql_ct_target_like LIKE mysql_ct_source');

        $this->pdo->exec("INSERT INTO mysql_ct_target_like (id, val) VALUES (1, 'hello')");

        $stmt = $this->pdo->query('SELECT * FROM mysql_ct_target_like WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('hello', $rows[0]['val']);
    }

    public function testCreateTableAsSelect(): void
    {
        $this->pdo->exec("INSERT INTO mysql_ct_source (id, val) VALUES (1, 'hello')");
        $this->pdo->exec("INSERT INTO mysql_ct_source (id, val) VALUES (2, 'world')");

        $this->pdo->exec('CREATE TABLE mysql_ct_target_ctas AS SELECT * FROM mysql_ct_source');

        // On MySQL, CTAS works fully — SELECT from the created table returns data
        $stmt = $this->pdo->query('SELECT * FROM mysql_ct_target_ctas ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('hello', $rows[0]['val']);
        $this->assertSame('world', $rows[1]['val']);
    }

    public function testCreateTableLikeIsolation(): void
    {
        $this->pdo->exec('CREATE TABLE mysql_ct_target_like LIKE mysql_ct_source');
        $this->pdo->exec("INSERT INTO mysql_ct_target_like (id, val) VALUES (1, 'hello')");

        $this->pdo->disableZtd();
        // Physical table should not exist
        try {
            $stmt = $this->pdo->query('SELECT * FROM mysql_ct_target_like');
            $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));
        } catch (\PDOException $e) {
            $this->assertStringContainsString('mysql_ct_target_like', $e->getMessage());
        }
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mysql_ct_target_like');
        $raw->exec('DROP TABLE IF EXISTS mysql_ct_target_ctas');
        $raw->exec('DROP TABLE IF EXISTS mysql_ct_source');
    }
}
