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
 * Tests ZTD behavior with MySQL backtick-quoted identifiers and SQL reserved
 * words as column/table names.
 */
class MysqlQuotedIdentifierTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS `mysql_qi_items`');
        $raw->exec('CREATE TABLE `mysql_qi_items` (
            `id` INT PRIMARY KEY,
            `order` INT,
            `group` VARCHAR(30),
            `key` VARCHAR(50),
            `value` VARCHAR(50),
            `select` VARCHAR(50)
        )');
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

    public function testInsertAndSelectWithReservedWordColumns(): void
    {
        $this->pdo->exec("INSERT INTO `mysql_qi_items` (`id`, `order`, `group`, `key`, `value`, `select`) VALUES (1, 10, 'A', 'k1', 'v1', 's1')");

        $stmt = $this->pdo->query("SELECT `order`, `group`, `key`, `value`, `select` FROM `mysql_qi_items` WHERE `id` = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->assertSame(10, (int) $row['order']);
        $this->assertSame('A', $row['group']);
        $this->assertSame('k1', $row['key']);
    }

    public function testUpdateReservedWordColumns(): void
    {
        $this->pdo->exec("INSERT INTO `mysql_qi_items` (`id`, `order`, `group`, `key`, `value`, `select`) VALUES (1, 10, 'A', 'k1', 'v1', 's1')");

        $this->pdo->exec("UPDATE `mysql_qi_items` SET `value` = 'updated', `order` = 20 WHERE `id` = 1");

        $stmt = $this->pdo->query("SELECT `value`, `order` FROM `mysql_qi_items` WHERE `id` = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('updated', $row['value']);
        $this->assertSame(20, (int) $row['order']);
    }

    public function testGroupByOnColumnNamedGroup(): void
    {
        $this->pdo->exec("INSERT INTO `mysql_qi_items` (`id`, `order`, `group`, `key`, `value`, `select`) VALUES (1, 10, 'A', 'k1', 'v1', 's1')");
        $this->pdo->exec("INSERT INTO `mysql_qi_items` (`id`, `order`, `group`, `key`, `value`, `select`) VALUES (2, 20, 'A', 'k2', 'v2', 's2')");
        $this->pdo->exec("INSERT INTO `mysql_qi_items` (`id`, `order`, `group`, `key`, `value`, `select`) VALUES (3, 30, 'B', 'k3', 'v3', 's3')");

        $stmt = $this->pdo->query("SELECT `group`, COUNT(*) AS cnt FROM `mysql_qi_items` GROUP BY `group` ORDER BY `group`");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('A', $rows[0]['group']);
        $this->assertSame(2, (int) $rows[0]['cnt']);
    }

    public function testOrderByOnColumnNamedOrder(): void
    {
        $this->pdo->exec("INSERT INTO `mysql_qi_items` (`id`, `order`, `group`, `key`, `value`, `select`) VALUES (1, 30, 'A', 'k1', 'v1', 's1')");
        $this->pdo->exec("INSERT INTO `mysql_qi_items` (`id`, `order`, `group`, `key`, `value`, `select`) VALUES (2, 10, 'B', 'k2', 'v2', 's2')");
        $this->pdo->exec("INSERT INTO `mysql_qi_items` (`id`, `order`, `group`, `key`, `value`, `select`) VALUES (3, 20, 'C', 'k3', 'v3', 's3')");

        $stmt = $this->pdo->query("SELECT `id`, `order` FROM `mysql_qi_items` ORDER BY `order` ASC");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(2, (int) $rows[0]['id']);
        $this->assertSame(3, (int) $rows[1]['id']);
        $this->assertSame(1, (int) $rows[2]['id']);
    }

    public function testPreparedStatementWithReservedWordColumn(): void
    {
        $this->pdo->exec("INSERT INTO `mysql_qi_items` (`id`, `order`, `group`, `key`, `value`, `select`) VALUES (1, 10, 'A', 'k1', 'v1', 's1')");
        $this->pdo->exec("INSERT INTO `mysql_qi_items` (`id`, `order`, `group`, `key`, `value`, `select`) VALUES (2, 20, 'B', 'k2', 'v2', 's2')");

        $stmt = $this->pdo->prepare("SELECT * FROM `mysql_qi_items` WHERE `group` = ?");
        $stmt->execute(['A']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('k1', $rows[0]['key']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS `mysql_qi_items`');
    }
}
