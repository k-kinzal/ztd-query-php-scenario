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
 * Tests schema reflection behavior in ZTD mode on MySQL via PDO.
 * @spec SPEC-1.6
 */
class MysqlSchemaReflectionTest extends TestCase
{
    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);
    }

    public function testAdapterConstructedAfterTableReflectsSchema(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mysql_reflect_test');
        $raw->exec('CREATE TABLE mysql_reflect_test (id INT PRIMARY KEY, val VARCHAR(255))');

        $pdo = ZtdPdo::fromPdo($raw);

        $pdo->exec("INSERT INTO mysql_reflect_test (id, val) VALUES (1, 'original')");
        $pdo->exec("UPDATE mysql_reflect_test SET val = 'updated' WHERE id = 1");

        $stmt = $pdo->query('SELECT val FROM mysql_reflect_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('updated', $rows[0]['val']);

        $pdo->exec("DELETE FROM mysql_reflect_test WHERE id = 1");
        $stmt = $pdo->query('SELECT * FROM mysql_reflect_test');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));

        $raw->exec('DROP TABLE IF EXISTS mysql_reflect_test');
    }

    public function testUpdateFailsWhenSchemaNotReflected(): void
    {
        // Create adapter BEFORE table exists
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mysql_reflect_late');

        $pdo = ZtdPdo::fromPdo($raw);

        // Create table after adapter
        $raw->exec('CREATE TABLE mysql_reflect_late (id INT PRIMARY KEY, val VARCHAR(255))');
        $raw->exec("INSERT INTO mysql_reflect_late VALUES (1, 'physical')");

        // INSERT works (doesn't need primary key info)
        $pdo->exec("INSERT INTO mysql_reflect_late (id, val) VALUES (2, 'shadow')");

        // UPDATE fails because schema was not reflected
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/requires primary keys/i');
        $pdo->exec("UPDATE mysql_reflect_late SET val = 'updated' WHERE id = 1");
    }
}
