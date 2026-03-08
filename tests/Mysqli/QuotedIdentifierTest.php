<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests ZTD behavior with MySQLi backtick-quoted identifiers and SQL reserved
 * words as column/table names.
 * @spec pending
 */
class QuotedIdentifierTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE `mi_qi_items` (
            `id` INT PRIMARY KEY,
            `order` INT,
            `group` VARCHAR(30),
            `key` VARCHAR(50),
            `value` VARCHAR(50)
        )';
    }

    protected function getTableNames(): array
    {
        return ['mi_qi_items'];
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
}
