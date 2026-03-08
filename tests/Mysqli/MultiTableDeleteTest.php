<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests MySQL multi-table DELETE via MySQLi.
 *
 * Cross-platform parity with MysqlMultiTableDeleteTest (PDO).
 * @spec SPEC-4.2d
 */
class MultiTableDeleteTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_md_users (id INT PRIMARY KEY, name VARCHAR(50), active TINYINT DEFAULT 1)',
            'CREATE TABLE mi_md_orders (id INT PRIMARY KEY, user_id INT, amount INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_md_orders', 'mi_md_users'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_md_users (id, name, active) VALUES (1, 'Alice', 1)");
        $this->mysqli->query("INSERT INTO mi_md_users (id, name, active) VALUES (2, 'Bob', 0)");
        $this->mysqli->query("INSERT INTO mi_md_users (id, name, active) VALUES (3, 'Charlie', 1)");
        $this->mysqli->query("INSERT INTO mi_md_orders (id, user_id, amount) VALUES (1, 1, 100)");
        $this->mysqli->query("INSERT INTO mi_md_orders (id, user_id, amount) VALUES (2, 2, 200)");
        $this->mysqli->query("INSERT INTO mi_md_orders (id, user_id, amount) VALUES (3, 2, 50)");
        $this->mysqli->query("INSERT INTO mi_md_orders (id, user_id, amount) VALUES (4, 3, 300)");
    }

    /**
     * Single-target DELETE with JOIN.
     */
    public function testSingleTargetDeleteWithJoin(): void
    {
        $this->mysqli->query(
            'DELETE o FROM mi_md_orders o JOIN mi_md_users u ON o.user_id = u.id WHERE u.active = 0'
        );

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_md_orders WHERE user_id = 2');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_md_orders');
        $this->assertSame(2, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Multi-target DELETE only affects the first table.
     */
    public function testMultiTargetDeleteOnlyAffectsFirstTable(): void
    {
        $this->mysqli->query(
            'DELETE u, o FROM mi_md_users u JOIN mi_md_orders o ON u.id = o.user_id WHERE u.active = 0'
        );

        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_md_users WHERE name = 'Bob'");
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);

        // Second table (orders): Bob's orders are NOT deleted
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_md_orders WHERE user_id = 2');
        $this->assertSame(1, (int) $result->fetch_assoc()['cnt']);

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_md_users');
        $this->assertSame(2, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Multi-target DELETE with prepared statement.
     */
    public function testMultiTargetDeletePrepared(): void
    {
        $stmt = $this->mysqli->prepare(
            'DELETE o FROM mi_md_orders o JOIN mi_md_users u ON o.user_id = u.id WHERE u.name = ?'
        );
        $name = 'Bob';
        $stmt->bind_param('s', $name);
        $stmt->execute();

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_md_orders WHERE user_id = 2');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query(
            'DELETE u, o FROM mi_md_users u JOIN mi_md_orders o ON u.id = o.user_id WHERE u.active = 0'
        );

        $this->mysqli->disableZtd();

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_md_users');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_md_orders');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
