<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests multi-tenant query patterns on MySQL PDO.
 *
 * Cross-platform parity with MultiTenantPatternTest (MySQLi).
 * @spec pending
 */
class MysqlMultiTenantPatternTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_mt_users (id INT PRIMARY KEY, tenant_id INT NOT NULL, name VARCHAR(50), role VARCHAR(20))',
            'CREATE TABLE mp_mt_projects (id INT PRIMARY KEY, tenant_id INT NOT NULL, name VARCHAR(50), owner_id INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_mt_projects', 'mp_mt_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_mt_users VALUES (1, 1, 'Alice', 'admin')");
        $this->pdo->exec("INSERT INTO mp_mt_users VALUES (2, 1, 'Bob', 'member')");
        $this->pdo->exec("INSERT INTO mp_mt_users VALUES (3, 2, 'Charlie', 'admin')");
        $this->pdo->exec("INSERT INTO mp_mt_users VALUES (4, 2, 'Diana', 'member')");
        $this->pdo->exec("INSERT INTO mp_mt_users VALUES (5, 2, 'Eve', 'member')");
        $this->pdo->exec("INSERT INTO mp_mt_projects VALUES (1, 1, 'Project Alpha', 1)");
        $this->pdo->exec("INSERT INTO mp_mt_projects VALUES (2, 1, 'Project Beta', 2)");
        $this->pdo->exec("INSERT INTO mp_mt_projects VALUES (3, 2, 'Project Gamma', 3)");
    }

    public function testSelectByTenant(): void
    {
        $rows = $this->ztdQuery('SELECT name FROM mp_mt_users WHERE tenant_id = 1 ORDER BY name');
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public function testPreparedSelectByTenant(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            'SELECT name FROM mp_mt_users WHERE tenant_id = ? ORDER BY name',
            [2]
        );
        $this->assertCount(3, $rows);
        $this->assertSame('Charlie', $rows[0]['name']);
    }

    public function testInsertWithTenantIsolation(): void
    {
        $this->pdo->exec("INSERT INTO mp_mt_users VALUES (6, 1, 'Frank', 'member')");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM mp_mt_users WHERE tenant_id = 1');
        $this->assertSame(3, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM mp_mt_users WHERE tenant_id = 2');
        $this->assertSame(3, (int) $stmt->fetchColumn());
    }

    public function testUpdateByTenant(): void
    {
        $this->pdo->exec("UPDATE mp_mt_users SET role = 'viewer' WHERE tenant_id = 2 AND role = 'member'");

        $stmt = $this->pdo->query("SELECT COUNT(*) FROM mp_mt_users WHERE tenant_id = 2 AND role = 'viewer'");
        $this->assertSame(2, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT role FROM mp_mt_users WHERE id = 2');
        $this->assertSame('member', $stmt->fetchColumn());
    }

    public function testDeleteByTenant(): void
    {
        $this->pdo->exec("DELETE FROM mp_mt_users WHERE tenant_id = 2 AND role = 'member'");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM mp_mt_users WHERE tenant_id = 2');
        $this->assertSame(1, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM mp_mt_users WHERE tenant_id = 1');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    public function testJoinWithTenantFilter(): void
    {
        $rows = $this->ztdQuery(
            'SELECT p.name AS project, u.name AS owner
             FROM mp_mt_projects p
             JOIN mp_mt_users u ON p.owner_id = u.id
             WHERE p.tenant_id = 1
             ORDER BY p.name'
        );
        $this->assertCount(2, $rows);
        $this->assertSame('Project Alpha', $rows[0]['project']);
        $this->assertSame('Alice', $rows[0]['owner']);
    }

    public function testAggregationPerTenant(): void
    {
        $rows = $this->ztdQuery(
            'SELECT tenant_id, COUNT(*) AS user_count
             FROM mp_mt_users
             GROUP BY tenant_id
             ORDER BY tenant_id'
        );
        $this->assertCount(2, $rows);
        $this->assertSame(2, (int) $rows[0]['user_count']);
        $this->assertSame(3, (int) $rows[1]['user_count']);
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM mp_mt_users');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
