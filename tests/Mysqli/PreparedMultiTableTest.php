<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests multi-table UPDATE/DELETE with prepared statement parameters on MySQLi.
 * @spec SPEC-4.2c
 */
class PreparedMultiTableTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_pmt_users (id INT PRIMARY KEY, name VARCHAR(50), active TINYINT)',
            'CREATE TABLE mi_pmt_orders (id INT PRIMARY KEY, user_id INT, amount DECIMAL(10,2), status VARCHAR(20))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_pmt_orders', 'mi_pmt_users'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_pmt_users (id, name, active) VALUES (1, 'Alice', 1)");
        $this->mysqli->query("INSERT INTO mi_pmt_users (id, name, active) VALUES (2, 'Bob', 1)");
        $this->mysqli->query("INSERT INTO mi_pmt_users (id, name, active) VALUES (3, 'Charlie', 0)");
        $this->mysqli->query("INSERT INTO mi_pmt_orders (id, user_id, amount, status) VALUES (1, 1, 100.0, 'completed')");
        $this->mysqli->query("INSERT INTO mi_pmt_orders (id, user_id, amount, status) VALUES (2, 1, 200.0, 'completed')");
        $this->mysqli->query("INSERT INTO mi_pmt_orders (id, user_id, amount, status) VALUES (3, 2, 50.0, 'pending')");
        $this->mysqli->query("INSERT INTO mi_pmt_orders (id, user_id, amount, status) VALUES (4, 3, 75.0, 'completed')");
    }

    public function testPreparedMultiTableUpdate(): void
    {
        $stmt = $this->mysqli->prepare(
            'UPDATE mi_pmt_users u JOIN mi_pmt_orders o ON u.id = o.user_id SET u.active = 0 WHERE o.amount > ?'
        );
        $amount = 150.0;
        $stmt->bind_param('d', $amount);
        $stmt->execute();
        $this->assertSame(1, $stmt->ztdAffectedRows());

        $result = $this->mysqli->query("SELECT active FROM mi_pmt_users WHERE id = 1");
        $this->assertSame(0, (int) $result->fetch_assoc()['active']);
    }

    public function testPreparedMultiTableDelete(): void
    {
        $stmt = $this->mysqli->prepare(
            'DELETE o FROM mi_pmt_orders o JOIN mi_pmt_users u ON o.user_id = u.id WHERE u.active = ?'
        );
        $active = 0;
        $stmt->bind_param('i', $active);
        $stmt->execute();
        $this->assertSame(1, $stmt->ztdAffectedRows());

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_pmt_orders');
        $this->assertSame(3, (int) $result->fetch_assoc()['cnt']);
    }
}
