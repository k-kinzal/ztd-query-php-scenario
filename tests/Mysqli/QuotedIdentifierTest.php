<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests ZTD behavior with MySQLi backtick-quoted identifiers and SQL reserved
 * words as column/table names.
 */
class QuotedIdentifierTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS `mi_qi_items`');
        $raw->query('CREATE TABLE `mi_qi_items` (
            `id` INT PRIMARY KEY,
            `order` INT,
            `group` VARCHAR(30),
            `key` VARCHAR(50),
            `value` VARCHAR(50)
        )');
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
    }

    public function testInsertAndSelectWithReservedWordColumns(): void
    {
        $this->mysqli->query("INSERT INTO `mi_qi_items` (`id`, `order`, `group`, `key`, `value`) VALUES (1, 10, 'A', 'k1', 'v1')");

        $result = $this->mysqli->query("SELECT `order`, `group`, `key`, `value` FROM `mi_qi_items` WHERE `id` = 1");
        $row = $result->fetch_assoc();

        $this->assertSame(10, (int) $row['order']);
        $this->assertSame('A', $row['group']);
        $this->assertSame('k1', $row['key']);
    }

    public function testUpdateReservedWordColumns(): void
    {
        $this->mysqli->query("INSERT INTO `mi_qi_items` (`id`, `order`, `group`, `key`, `value`) VALUES (1, 10, 'A', 'k1', 'v1')");

        $this->mysqli->query("UPDATE `mi_qi_items` SET `value` = 'updated', `order` = 20 WHERE `id` = 1");

        $result = $this->mysqli->query("SELECT `value`, `order` FROM `mi_qi_items` WHERE `id` = 1");
        $row = $result->fetch_assoc();
        $this->assertSame('updated', $row['value']);
        $this->assertSame(20, (int) $row['order']);
    }

    public function testGroupByOnColumnNamedGroup(): void
    {
        $this->mysqli->query("INSERT INTO `mi_qi_items` (`id`, `order`, `group`, `key`, `value`) VALUES (1, 10, 'A', 'k1', 'v1')");
        $this->mysqli->query("INSERT INTO `mi_qi_items` (`id`, `order`, `group`, `key`, `value`) VALUES (2, 20, 'A', 'k2', 'v2')");
        $this->mysqli->query("INSERT INTO `mi_qi_items` (`id`, `order`, `group`, `key`, `value`) VALUES (3, 30, 'B', 'k3', 'v3')");

        $result = $this->mysqli->query("SELECT `group`, COUNT(*) AS cnt FROM `mi_qi_items` GROUP BY `group` ORDER BY `group`");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('A', $rows[0]['group']);
        $this->assertSame(2, (int) $rows[0]['cnt']);
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
        $raw->query('DROP TABLE IF EXISTS `mi_qi_items`');
        $raw->close();
    }
}
