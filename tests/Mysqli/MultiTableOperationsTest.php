<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/** @spec SPEC-4.2c, SPEC-4.2d */
class MultiTableOperationsTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255), active TINYINT DEFAULT 1)',
            'CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount DECIMAL(10,2))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['orders', 'users'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO users (id, name, active) VALUES (1, 'Alice', 1)");
        $this->mysqli->query("INSERT INTO users (id, name, active) VALUES (2, 'Bob', 1)");
        $this->mysqli->query("INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 100.00)");
        $this->mysqli->query("INSERT INTO orders (id, user_id, amount) VALUES (2, 1, 200.00)");
        $this->mysqli->query("INSERT INTO orders (id, user_id, amount) VALUES (3, 2, 50.00)");
    }

    public function testMultiTableUpdate(): void
    {
        // Multi-table UPDATE: update users based on join with orders
        $this->mysqli->query("UPDATE users u JOIN orders o ON u.id = o.user_id SET u.active = 0 WHERE o.amount > 150");

        $result = $this->mysqli->query('SELECT active FROM users WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame(0, (int) $row['active']);

        // User 2 should not be affected (no orders > 150)
        $result = $this->mysqli->query('SELECT active FROM users WHERE id = 2');
        $row = $result->fetch_assoc();
        $this->assertSame(1, (int) $row['active']);
    }

    public function testMultiTableUpdateIsolation(): void
    {
        $this->mysqli->query("UPDATE users u JOIN orders o ON u.id = o.user_id SET u.active = 0 WHERE o.amount > 150");

        // Physical table should be unchanged
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT * FROM users');
        $this->assertSame(0, $result->num_rows);
        $this->mysqli->enableZtd();
    }

    public function testMultiTableDelete(): void
    {
        // Multi-table DELETE: delete orders for users with name 'Bob'
        $this->mysqli->query("DELETE o FROM orders o JOIN users u ON o.user_id = u.id WHERE u.name = 'Bob'");

        $result = $this->mysqli->query('SELECT * FROM orders');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        // Only Alice's orders should remain
        $this->assertCount(2, $rows);
    }
}
