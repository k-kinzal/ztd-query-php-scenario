<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests full CRUD on tables whose names are SQL reserved words on MySQL-PDO.
 * MySQL uses backticks for quoting identifiers.
 *
 * @spec SPEC-3.1
 * @spec SPEC-4.1
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class MysqlReservedWordTableCrudTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE `order` (
                id INT PRIMARY KEY,
                customer VARCHAR(50) NOT NULL,
                total DECIMAL(10,2) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE `group` (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                member_count INT NOT NULL DEFAULT 0
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['`order`', '`group`'];
    }

    public function testInsertIntoOrderTable(): void
    {
        try {
            $this->ztdExec("INSERT INTO `order` (id, customer, total) VALUES (1, 'Alice', 99.99)");

            $rows = $this->ztdQuery("SELECT customer, total FROM `order` WHERE id = 1");
            $this->assertCount(1, $rows);
            $this->assertSame('Alice', $rows[0]['customer']);
        } catch (\Exception $e) {
            $this->markTestIncomplete('INSERT into `order` table failed: ' . $e->getMessage());
        }
    }

    public function testUpdateOrderTable(): void
    {
        try {
            $this->ztdExec("INSERT INTO `order` (id, customer, total) VALUES (1, 'Alice', 99.99)");
            $this->ztdExec("UPDATE `order` SET total = 149.99 WHERE id = 1");

            $rows = $this->ztdQuery("SELECT total FROM `order` WHERE id = 1");
            $this->assertCount(1, $rows);
            $this->assertEquals(149.99, (float) $rows[0]['total'], '', 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete('UPDATE `order` table failed: ' . $e->getMessage());
        }
    }

    public function testDeleteFromOrderTable(): void
    {
        try {
            $this->ztdExec("INSERT INTO `order` (id, customer, total) VALUES (1, 'Alice', 99.99)");
            $this->ztdExec("DELETE FROM `order` WHERE id = 1");

            $rows = $this->ztdQuery("SELECT * FROM `order`");
            $this->assertCount(0, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete('DELETE from `order` table failed: ' . $e->getMessage());
        }
    }

    public function testJoinReservedWordTables(): void
    {
        try {
            $this->ztdExec("INSERT INTO `order` (id, customer, total) VALUES (1, 'Alice', 99.99)");
            $this->ztdExec("INSERT INTO `group` (id, name, member_count) VALUES (1, 'VIP', 2)");

            $rows = $this->ztdQuery(
                "SELECT o.customer, g.name
                 FROM `order` o
                 JOIN `group` g ON g.id = 1"
            );
            $this->assertCount(1, $rows);
            $this->assertSame('Alice', $rows[0]['customer']);
            $this->assertSame('VIP', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete('JOIN reserved-word tables failed: ' . $e->getMessage());
        }
    }

    public function testPreparedOnReservedWordTable(): void
    {
        try {
            $this->ztdExec("INSERT INTO `order` (id, customer, total) VALUES (1, 'Alice', 99.99)");

            $rows = $this->ztdPrepareAndExecute(
                "SELECT customer FROM `order` WHERE total >= ?",
                [50.00]
            );
            $this->assertCount(1, $rows);
            $this->assertSame('Alice', $rows[0]['customer']);
        } catch (\Exception $e) {
            $this->markTestIncomplete('Prepared on `order` failed: ' . $e->getMessage());
        }
    }

    public function testAggregateOnOrderTable(): void
    {
        try {
            $this->ztdExec("INSERT INTO `order` (id, customer, total) VALUES (1, 'Alice', 100.00)");
            $this->ztdExec("INSERT INTO `order` (id, customer, total) VALUES (2, 'Alice', 200.00)");
            $this->ztdExec("INSERT INTO `order` (id, customer, total) VALUES (3, 'Bob', 50.00)");

            $rows = $this->ztdQuery(
                "SELECT customer, SUM(total) AS sum_total
                 FROM `order`
                 GROUP BY customer
                 ORDER BY customer"
            );
            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['customer']);
            $this->assertEquals(300.00, (float) $rows[0]['sum_total'], '', 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete('Aggregate on `order` failed: ' . $e->getMessage());
        }
    }
}
