<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests role-based access control (RBAC) query patterns through ZTD shadow store.
 * Exercises deeply nested EXISTS/NOT EXISTS and 5-table JOINs with junction tables.
 * @spec SPEC-3.3
 * @spec SPEC-4.2
 */
class RbacPermissionCheckTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_rbac_users (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                active INT DEFAULT 1
            )',
            'CREATE TABLE mi_rbac_roles (
                id INT PRIMARY KEY,
                name VARCHAR(255)
            )',
            'CREATE TABLE mi_rbac_permissions (
                id INT PRIMARY KEY,
                resource VARCHAR(255),
                action VARCHAR(255)
            )',
            'CREATE TABLE mi_rbac_user_roles (
                user_id INT,
                role_id INT,
                PRIMARY KEY (user_id, role_id)
            )',
            'CREATE TABLE mi_rbac_role_permissions (
                role_id INT,
                permission_id INT,
                PRIMARY KEY (role_id, permission_id)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return [
            'mi_rbac_role_permissions', 'mi_rbac_user_roles',
            'mi_rbac_permissions', 'mi_rbac_roles', 'mi_rbac_users',
        ];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Users
        $this->mysqli->query("INSERT INTO mi_rbac_users VALUES (1, 'Alice', 1)");
        $this->mysqli->query("INSERT INTO mi_rbac_users VALUES (2, 'Bob', 1)");
        $this->mysqli->query("INSERT INTO mi_rbac_users VALUES (3, 'Carol', 1)");
        $this->mysqli->query("INSERT INTO mi_rbac_users VALUES (4, 'Dave', 0)"); // inactive

        // Roles
        $this->mysqli->query("INSERT INTO mi_rbac_roles VALUES (1, 'admin')");
        $this->mysqli->query("INSERT INTO mi_rbac_roles VALUES (2, 'editor')");
        $this->mysqli->query("INSERT INTO mi_rbac_roles VALUES (3, 'viewer')");

        // Permissions
        $this->mysqli->query("INSERT INTO mi_rbac_permissions VALUES (1, 'posts', 'create')");
        $this->mysqli->query("INSERT INTO mi_rbac_permissions VALUES (2, 'posts', 'read')");
        $this->mysqli->query("INSERT INTO mi_rbac_permissions VALUES (3, 'posts', 'update')");
        $this->mysqli->query("INSERT INTO mi_rbac_permissions VALUES (4, 'posts', 'delete')");
        $this->mysqli->query("INSERT INTO mi_rbac_permissions VALUES (5, 'users', 'manage')");

        // Role -> Permission mappings
        // admin: all permissions
        $this->mysqli->query("INSERT INTO mi_rbac_role_permissions VALUES (1, 1)");
        $this->mysqli->query("INSERT INTO mi_rbac_role_permissions VALUES (1, 2)");
        $this->mysqli->query("INSERT INTO mi_rbac_role_permissions VALUES (1, 3)");
        $this->mysqli->query("INSERT INTO mi_rbac_role_permissions VALUES (1, 4)");
        $this->mysqli->query("INSERT INTO mi_rbac_role_permissions VALUES (1, 5)");
        // editor: create, read, update
        $this->mysqli->query("INSERT INTO mi_rbac_role_permissions VALUES (2, 1)");
        $this->mysqli->query("INSERT INTO mi_rbac_role_permissions VALUES (2, 2)");
        $this->mysqli->query("INSERT INTO mi_rbac_role_permissions VALUES (2, 3)");
        // viewer: read only
        $this->mysqli->query("INSERT INTO mi_rbac_role_permissions VALUES (3, 2)");

        // User -> Role mappings
        // Alice: admin
        $this->mysqli->query("INSERT INTO mi_rbac_user_roles VALUES (1, 1)");
        // Bob: editor
        $this->mysqli->query("INSERT INTO mi_rbac_user_roles VALUES (2, 2)");
        // Carol: viewer
        $this->mysqli->query("INSERT INTO mi_rbac_user_roles VALUES (3, 3)");
        // Dave: admin (but inactive user)
        $this->mysqli->query("INSERT INTO mi_rbac_user_roles VALUES (4, 1)");
    }

    /**
     * Check if user has a specific permission using nested EXISTS.
     */
    public function testUserHasPermissionViaExists(): void
    {
        $rows = $this->ztdQuery(
            "SELECT u.name FROM mi_rbac_users u
             WHERE u.active = 1
             AND EXISTS (
                SELECT 1 FROM mi_rbac_user_roles ur
                JOIN mi_rbac_role_permissions rp ON ur.role_id = rp.role_id
                JOIN mi_rbac_permissions p ON rp.permission_id = p.id
                WHERE ur.user_id = u.id AND p.resource = 'posts' AND p.action = 'delete'
             )
             ORDER BY u.name"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']); // Only admin can delete
    }

    /**
     * 5-table JOIN: list all user-permission pairs.
     */
    public function testFiveTableJoinAllPermissions(): void
    {
        $rows = $this->ztdQuery(
            "SELECT u.name, r.name AS role, p.resource, p.action
             FROM mi_rbac_users u
             JOIN mi_rbac_user_roles ur ON u.id = ur.user_id
             JOIN mi_rbac_roles r ON ur.role_id = r.id
             JOIN mi_rbac_role_permissions rp ON r.id = rp.role_id
             JOIN mi_rbac_permissions p ON rp.permission_id = p.id
             WHERE u.active = 1
             ORDER BY u.name, p.resource, p.action"
        );

        // Alice (admin): 5 permissions, Bob (editor): 3, Carol (viewer): 1
        $this->assertCount(9, $rows);

        $alicePerms = array_filter($rows, fn($r) => $r['name'] === 'Alice');
        $this->assertCount(5, $alicePerms);

        $bobPerms = array_filter($rows, fn($r) => $r['name'] === 'Bob');
        $this->assertCount(3, $bobPerms);

        $carolPerms = array_filter($rows, fn($r) => $r['name'] === 'Carol');
        $this->assertCount(1, $carolPerms);
    }

    /**
     * NOT EXISTS: users without any role assigned.
     */
    public function testUsersWithoutRole(): void
    {
        // Add a user with no role
        $this->mysqli->query("INSERT INTO mi_rbac_users VALUES (5, 'Frank', 1)");

        $rows = $this->ztdQuery(
            "SELECT u.name FROM mi_rbac_users u
             WHERE NOT EXISTS (
                SELECT 1 FROM mi_rbac_user_roles ur WHERE ur.user_id = u.id
             )
             ORDER BY u.name"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Frank', $rows[0]['name']);
    }

    /**
     * Permission count per user using GROUP BY across junction tables.
     */
    public function testPermissionCountPerUser(): void
    {
        $rows = $this->ztdQuery(
            "SELECT u.name, COUNT(DISTINCT p.id) AS perm_count
             FROM mi_rbac_users u
             JOIN mi_rbac_user_roles ur ON u.id = ur.user_id
             JOIN mi_rbac_role_permissions rp ON ur.role_id = rp.role_id
             JOIN mi_rbac_permissions p ON rp.permission_id = p.id
             WHERE u.active = 1
             GROUP BY u.id, u.name
             ORDER BY perm_count DESC"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEquals(5, (int) $rows[0]['perm_count']);
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertEquals(3, (int) $rows[1]['perm_count']);
    }

    /**
     * Grant role: INSERT into junction table then verify permission.
     */
    public function testGrantRoleAndVerify(): void
    {
        // Carol currently can't create posts
        $rows = $this->ztdQuery(
            "SELECT 1 FROM mi_rbac_users u
             WHERE u.id = 3
             AND EXISTS (
                SELECT 1 FROM mi_rbac_user_roles ur
                JOIN mi_rbac_role_permissions rp ON ur.role_id = rp.role_id
                JOIN mi_rbac_permissions p ON rp.permission_id = p.id
                WHERE ur.user_id = u.id AND p.resource = 'posts' AND p.action = 'create'
             )"
        );
        $this->assertCount(0, $rows);

        // Grant editor role to Carol
        $this->mysqli->query("INSERT INTO mi_rbac_user_roles VALUES (3, 2)");

        // Now Carol can create posts
        $rows = $this->ztdQuery(
            "SELECT 1 FROM mi_rbac_users u
             WHERE u.id = 3
             AND EXISTS (
                SELECT 1 FROM mi_rbac_user_roles ur
                JOIN mi_rbac_role_permissions rp ON ur.role_id = rp.role_id
                JOIN mi_rbac_permissions p ON rp.permission_id = p.id
                WHERE ur.user_id = u.id AND p.resource = 'posts' AND p.action = 'create'
             )"
        );
        $this->assertCount(1, $rows);
    }

    /**
     * Revoke role: DELETE from junction table then verify permission removed.
     */
    public function testRevokeRoleAndVerify(): void
    {
        // Bob can currently create posts (editor role)
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt FROM mi_rbac_user_roles ur
             JOIN mi_rbac_role_permissions rp ON ur.role_id = rp.role_id
             JOIN mi_rbac_permissions p ON rp.permission_id = p.id
             WHERE ur.user_id = 2 AND p.resource = 'posts' AND p.action = 'create'"
        );
        $this->assertEquals(1, (int) $rows[0]['cnt']);

        // Revoke editor role from Bob
        $this->mysqli->query("DELETE FROM mi_rbac_user_roles WHERE user_id = 2 AND role_id = 2");

        // Bob can no longer create posts
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt FROM mi_rbac_user_roles ur
             JOIN mi_rbac_role_permissions rp ON ur.role_id = rp.role_id
             JOIN mi_rbac_permissions p ON rp.permission_id = p.id
             WHERE ur.user_id = 2 AND p.resource = 'posts' AND p.action = 'create'"
        );
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }

    /**
     * IN subquery: find all users with a given role.
     */
    public function testUsersWithRole(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name FROM mi_rbac_users
             WHERE id IN (SELECT user_id FROM mi_rbac_user_roles WHERE role_id = 1)
             ORDER BY name"
        );

        $this->assertCount(2, $rows); // Alice and Dave (both have admin)
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Dave', $rows[1]['name']);
    }

    /**
     * Inactive user check: has role but should not have access.
     */
    public function testInactiveUserExcluded(): void
    {
        // Dave has admin role but is inactive
        $rows = $this->ztdQuery(
            "SELECT u.name FROM mi_rbac_users u
             WHERE u.active = 1
             AND EXISTS (
                SELECT 1 FROM mi_rbac_user_roles ur
                WHERE ur.user_id = u.id AND ur.role_id = 1
             )
             ORDER BY u.name"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']); // Dave excluded (inactive)
    }

    /**
     * Prepared permission check with resource and action parameters.
     */
    public function testPreparedPermissionCheck(): void
    {
        $stmt = $this->mysqli->prepare(
            'SELECT u.name FROM mi_rbac_users u
             WHERE u.active = 1
             AND EXISTS (
                SELECT 1 FROM mi_rbac_user_roles ur
                JOIN mi_rbac_role_permissions rp ON ur.role_id = rp.role_id
                JOIN mi_rbac_permissions p ON rp.permission_id = p.id
                WHERE ur.user_id = u.id AND p.resource = ? AND p.action = ?
             )
             ORDER BY u.name'
        );

        // Who can manage users?
        $resource = 'users';
        $action = 'manage';
        $stmt->bind_param('ss', $resource, $action);
        $stmt->execute();
        $rows = $stmt->get_result()->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);

        // Who can read posts?
        $resource = 'posts';
        $action = 'read';
        $stmt->bind_param('ss', $resource, $action);
        $stmt->execute();
        $rows = $stmt->get_result()->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(3, $rows); // Alice, Bob, Carol
    }

    /**
     * Physical isolation: role changes don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_rbac_user_roles VALUES (3, 2)"); // Grant editor to Carol
        $this->mysqli->query("DELETE FROM mi_rbac_user_roles WHERE user_id = 2"); // Revoke all from Bob

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_rbac_user_roles WHERE user_id = 3");
        $this->assertEquals(2, (int) $rows[0]['cnt']); // viewer + editor

        // Physical table is empty (all inserts were via ZTD shadow)
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_rbac_user_roles');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
