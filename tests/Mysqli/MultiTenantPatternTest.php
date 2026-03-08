<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests multi-tenant query patterns on MySQLi.
 *
 * Multi-tenant applications filter all queries by tenant_id. This is a
 * very common SaaS pattern that exercises WHERE-filtered CRUD, JOINs
 * with tenant isolation, and aggregation per tenant through ZTD.
 * @spec SPEC-10.2.21
 */
class MultiTenantPatternTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_mt_users (id INT PRIMARY KEY, tenant_id INT NOT NULL, name VARCHAR(50), role VARCHAR(20))',
            'CREATE TABLE mi_mt_projects (id INT PRIMARY KEY, tenant_id INT NOT NULL, name VARCHAR(50), owner_id INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_mt_projects', 'mi_mt_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Tenant 1 data
        $this->mysqli->query("INSERT INTO mi_mt_users VALUES (1, 1, 'Alice', 'admin')");
        $this->mysqli->query("INSERT INTO mi_mt_users VALUES (2, 1, 'Bob', 'member')");
        // Tenant 2 data
        $this->mysqli->query("INSERT INTO mi_mt_users VALUES (3, 2, 'Charlie', 'admin')");
        $this->mysqli->query("INSERT INTO mi_mt_users VALUES (4, 2, 'Diana', 'member')");
        $this->mysqli->query("INSERT INTO mi_mt_users VALUES (5, 2, 'Eve', 'member')");
        // Projects
        $this->mysqli->query("INSERT INTO mi_mt_projects VALUES (1, 1, 'Project Alpha', 1)");
        $this->mysqli->query("INSERT INTO mi_mt_projects VALUES (2, 1, 'Project Beta', 2)");
        $this->mysqli->query("INSERT INTO mi_mt_projects VALUES (3, 2, 'Project Gamma', 3)");
    }

    /**
     * SELECT filtered by tenant_id returns only that tenant's data.
     */
    public function testSelectByTenant(): void
    {
        $rows = $this->ztdQuery('SELECT name FROM mi_mt_users WHERE tenant_id = 1 ORDER BY name');
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
    }

    /**
     * Prepared SELECT with tenant_id parameter.
     */
    public function testPreparedSelectByTenant(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            'SELECT name FROM mi_mt_users WHERE tenant_id = ? ORDER BY name',
            [2]
        );
        $this->assertCount(3, $rows);
        $this->assertSame('Charlie', $rows[0]['name']);
    }

    /**
     * INSERT for a specific tenant, then verify tenant isolation.
     */
    public function testInsertWithTenantIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_mt_users VALUES (6, 1, 'Frank', 'member')");

        // Tenant 1 sees new user
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_mt_users WHERE tenant_id = 1');
        $this->assertSame(3, (int) $result->fetch_assoc()['cnt']);

        // Tenant 2 is unaffected
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_mt_users WHERE tenant_id = 2');
        $this->assertSame(3, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * UPDATE filtered by tenant_id only affects that tenant.
     */
    public function testUpdateByTenant(): void
    {
        $this->mysqli->query("UPDATE mi_mt_users SET role = 'viewer' WHERE tenant_id = 2 AND role = 'member'");

        // Tenant 2 members changed
        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_mt_users WHERE tenant_id = 2 AND role = 'viewer'");
        $this->assertSame(2, (int) $result->fetch_assoc()['cnt']);

        // Tenant 1 unaffected
        $result = $this->mysqli->query("SELECT role FROM mi_mt_users WHERE id = 2");
        $this->assertSame('member', $result->fetch_assoc()['role']);
    }

    /**
     * DELETE filtered by tenant_id only affects that tenant.
     */
    public function testDeleteByTenant(): void
    {
        $this->mysqli->query("DELETE FROM mi_mt_users WHERE tenant_id = 2 AND role = 'member'");

        // Tenant 2 lost members
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_mt_users WHERE tenant_id = 2');
        $this->assertSame(1, (int) $result->fetch_assoc()['cnt']);

        // Tenant 1 unaffected
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_mt_users WHERE tenant_id = 1');
        $this->assertSame(2, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * JOIN across tenant-filtered tables.
     */
    public function testJoinWithTenantFilter(): void
    {
        $rows = $this->ztdQuery(
            'SELECT p.name AS project, u.name AS owner
             FROM mi_mt_projects p
             JOIN mi_mt_users u ON p.owner_id = u.id
             WHERE p.tenant_id = 1
             ORDER BY p.name'
        );
        $this->assertCount(2, $rows);
        $this->assertSame('Project Alpha', $rows[0]['project']);
        $this->assertSame('Alice', $rows[0]['owner']);
    }

    /**
     * Aggregation per tenant.
     */
    public function testAggregationPerTenant(): void
    {
        $rows = $this->ztdQuery(
            'SELECT tenant_id, COUNT(*) AS user_count
             FROM mi_mt_users
             GROUP BY tenant_id
             ORDER BY tenant_id'
        );
        $this->assertCount(2, $rows);
        $this->assertSame(2, (int) $rows[0]['user_count']);
        $this->assertSame(3, (int) $rows[1]['user_count']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_mt_users');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
