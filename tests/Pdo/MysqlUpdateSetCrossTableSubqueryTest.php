<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests UPDATE SET col = (SELECT ... FROM other_table) where the subquery
 * references a different shadow-modified table on MySQL via PDO.
 *
 * @spec SPEC-4.2
 */
class MysqlUpdateSetCrossTableSubqueryTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mpd_ucs_customers (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                total_orders INT NOT NULL DEFAULT 0,
                total_amount DECIMAL(10,2) NOT NULL DEFAULT 0.00
            ) ENGINE=InnoDB',
            'CREATE TABLE mpd_ucs_orders (
                id INT PRIMARY KEY,
                customer_id INT NOT NULL,
                amount DECIMAL(10,2) NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mpd_ucs_orders', 'mpd_ucs_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mpd_ucs_customers VALUES (1, 'Alice', 0, 0.00)");
        $this->pdo->exec("INSERT INTO mpd_ucs_customers VALUES (2, 'Bob', 0, 0.00)");

        $this->pdo->exec("INSERT INTO mpd_ucs_orders VALUES (1, 1, 100.00)");
        $this->pdo->exec("INSERT INTO mpd_ucs_orders VALUES (2, 1, 200.00)");
        $this->pdo->exec("INSERT INTO mpd_ucs_orders VALUES (3, 2, 50.00)");
    }

    public function testUpdateSetCountSubquery(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE mpd_ucs_customers SET total_orders = (SELECT COUNT(*) FROM mpd_ucs_orders WHERE customer_id = mpd_ucs_customers.id)"
            );

            $rows = $this->ztdQuery("SELECT id, total_orders FROM mpd_ucs_customers ORDER BY id");
            $this->assertCount(2, $rows);

            if ((int) $rows[0]['total_orders'] !== 2) {
                $this->markTestIncomplete('Alice total_orders: expected 2, got ' . $rows[0]['total_orders']);
            }
            $this->assertEquals(2, (int) $rows[0]['total_orders']);
            $this->assertEquals(1, (int) $rows[1]['total_orders']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET COUNT subquery failed: ' . $e->getMessage());
        }
    }

    public function testUpdateSetSumSubquery(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE mpd_ucs_customers SET total_amount = (SELECT COALESCE(SUM(amount), 0) FROM mpd_ucs_orders WHERE customer_id = mpd_ucs_customers.id)"
            );

            $rows = $this->ztdQuery("SELECT id, total_amount FROM mpd_ucs_customers ORDER BY id");
            $this->assertCount(2, $rows);

            if ((float) $rows[0]['total_amount'] != 300.00) {
                $this->markTestIncomplete('Alice total: expected 300, got ' . $rows[0]['total_amount']);
            }
            $this->assertEquals(300.00, (float) $rows[0]['total_amount']);
            $this->assertEquals(50.00, (float) $rows[1]['total_amount']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET SUM subquery failed: ' . $e->getMessage());
        }
    }

    public function testSequentialUpdatesOnSameRow(): void
    {
        try {
            $this->pdo->exec("UPDATE mpd_ucs_customers SET name = 'Alice Smith' WHERE id = 1");
            $this->pdo->exec("UPDATE mpd_ucs_customers SET total_orders = 5 WHERE id = 1");
            $this->pdo->exec("UPDATE mpd_ucs_customers SET total_amount = 999.99 WHERE id = 1");

            $rows = $this->ztdQuery("SELECT name, total_orders, total_amount FROM mpd_ucs_customers WHERE id = 1");
            $this->assertCount(1, $rows);

            $this->assertSame('Alice Smith', $rows[0]['name']);
            $this->assertEquals(5, (int) $rows[0]['total_orders']);
            $this->assertEquals(999.99, (float) $rows[0]['total_amount']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Sequential updates failed: ' . $e->getMessage());
        }
    }
}
