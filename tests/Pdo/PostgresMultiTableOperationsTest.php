<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL-specific multi-table operations.
 * PostgreSQL uses UPDATE ... FROM and DELETE ... USING syntax
 * instead of MySQL's JOIN-based syntax.
 * @spec SPEC-4.2c, SPEC-4.2d
 */
class PostgresMultiTableOperationsTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_users (id INT PRIMARY KEY, name VARCHAR(255), active INT DEFAULT 1)',
            'CREATE TABLE pg_orders (id INT PRIMARY KEY, user_id INT, amount DECIMAL(10,2))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_orders', 'pg_users'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_users (id, name, active) VALUES (1, 'Alice', 1)");
        $this->pdo->exec("INSERT INTO pg_users (id, name, active) VALUES (2, 'Bob', 1)");
        $this->pdo->exec("INSERT INTO pg_orders (id, user_id, amount) VALUES (1, 1, 100.00)");
        $this->pdo->exec("INSERT INTO pg_orders (id, user_id, amount) VALUES (2, 1, 200.00)");
        $this->pdo->exec("INSERT INTO pg_orders (id, user_id, amount) VALUES (3, 2, 50.00)");
    }

    public function testUpdateWithFrom(): void
    {
        // PostgreSQL multi-table UPDATE uses FROM clause
        $this->pdo->exec("UPDATE pg_users SET active = 0 FROM pg_orders WHERE pg_users.id = pg_orders.user_id AND pg_orders.amount > 150");

        $stmt = $this->pdo->query('SELECT active FROM pg_users WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['active']);

        $stmt = $this->pdo->query('SELECT active FROM pg_users WHERE id = 2');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(1, (int) $rows[0]['active']);
    }

    public function testDeleteWithUsing(): void
    {
        // PostgreSQL multi-table DELETE uses USING clause
        $this->pdo->exec("DELETE FROM pg_orders USING pg_users WHERE pg_orders.user_id = pg_users.id AND pg_users.name = 'Bob'");

        $stmt = $this->pdo->query('SELECT * FROM pg_orders');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
    }

    public function testUpdateWithFromIsolation(): void
    {
        $this->pdo->exec("UPDATE pg_users SET active = 0 FROM pg_orders WHERE pg_users.id = pg_orders.user_id AND pg_orders.amount > 150");

        // Physical table should be unchanged
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT * FROM pg_users');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));
    }
}
