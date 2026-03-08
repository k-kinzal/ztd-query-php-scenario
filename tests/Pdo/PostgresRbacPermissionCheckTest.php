<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests role-based access control (RBAC) query patterns through ZTD shadow store.
 * Exercises deeply nested EXISTS/NOT EXISTS and 5-table JOINs with junction tables.
 * @spec SPEC-3.3
 * @spec SPEC-4.2
 */
class PostgresRbacPermissionCheckTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_rbac_users (
                id INTEGER PRIMARY KEY,
                name TEXT,
                active INTEGER DEFAULT 1
            )',
            'CREATE TABLE pg_rbac_roles (
                id INTEGER PRIMARY KEY,
                name TEXT
            )',
            'CREATE TABLE pg_rbac_permissions (
                id INTEGER PRIMARY KEY,
                resource TEXT,
                action TEXT
            )',
            'CREATE TABLE pg_rbac_user_roles (
                user_id INTEGER,
                role_id INTEGER,
                PRIMARY KEY (user_id, role_id)
            )',
            'CREATE TABLE pg_rbac_role_permissions (
                role_id INTEGER,
                permission_id INTEGER,
                PRIMARY KEY (role_id, permission_id)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return [
            'pg_rbac_role_permissions', 'pg_rbac_user_roles',
            'pg_rbac_permissions', 'pg_rbac_roles', 'pg_rbac_users',
        ];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Users
        $this->pdo->exec("INSERT INTO pg_rbac_users VALUES (1, 'Alice', 1)");
        $this->pdo->exec("INSERT INTO pg_rbac_users VALUES (2, 'Bob', 1)");
        $this->pdo->exec("INSERT INTO pg_rbac_users VALUES (3, 'Carol', 1)");
        $this->pdo->exec("INSERT INTO pg_rbac_users VALUES (4, 'Dave', 0)"); // inactive

        // Roles
        $this->pdo->exec("INSERT INTO pg_rbac_roles VALUES (1, 'admin')");
        $this->pdo->exec("INSERT INTO pg_rbac_roles VALUES (2, 'editor')");
        $this->pdo->exec("INSERT INTO pg_rbac_roles VALUES (3, 'viewer')");

        // Permissions
        $this->pdo->exec("INSERT INTO pg_rbac_permissions VALUES (1, 'posts', 'create')");
        $this->pdo->exec("INSERT INTO pg_rbac_permissions VALUES (2, 'posts', 'read')");
        $this->pdo->exec("INSERT INTO pg_rbac_permissions VALUES (3, 'posts', 'update')");
        $this->pdo->exec("INSERT INTO pg_rbac_permissions VALUES (4, 'posts', 'delete')");
        $this->pdo->exec("INSERT INTO pg_rbac_permissions VALUES (5, 'users', 'manage')");

        // Role -> Permission mappings
        // admin: all permissions
        $this->pdo->exec("INSERT INTO pg_rbac_role_permissions VALUES (1, 1)");
        $this->pdo->exec("INSERT INTO pg_rbac_role_permissions VALUES (1, 2)");
        $this->pdo->exec("INSERT INTO pg_rbac_role_permissions VALUES (1, 3)");
        $this->pdo->exec("INSERT INTO pg_rbac_role_permissions VALUES (1, 4)");
        $this->pdo->exec("INSERT INTO pg_rbac_role_permissions VALUES (1, 5)");
        // editor: create, read, update
        $this->pdo->exec("INSERT INTO pg_rbac_role_permissions VALUES (2, 1)");
        $this->pdo->exec("INSERT INTO pg_rbac_role_permissions VALUES (2, 2)");
        $this->pdo->exec("INSERT INTO pg_rbac_role_permissions VALUES (2, 3)");
        // viewer: read only
        $this->pdo->exec("INSERT INTO pg_rbac_role_permissions VALUES (3, 2)");

        // User -> Role mappings
        // Alice: admin
        $this->pdo->exec("INSERT INTO pg_rbac_user_roles VALUES (1, 1)");
        // Bob: editor
        $this->pdo->exec("INSERT INTO pg_rbac_user_roles VALUES (2, 2)");
        // Carol: viewer
        $this->pdo->exec("INSERT INTO pg_rbac_user_roles VALUES (3, 3)");
        // Dave: admin (but inactive user)
        $this->pdo->exec("INSERT INTO pg_rbac_user_roles VALUES (4, 1)");
    }

    /**
     * Check if user has a specific permission using nested EXISTS.
     */
    public function testUserHasPermissionViaExists(): void
    {
        $rows = $this->ztdQuery(
            "SELECT u.name FROM pg_rbac_users u
             WHERE u.active = 1
             AND EXISTS (
                SELECT 1 FROM pg_rbac_user_roles ur
                JOIN pg_rbac_role_permissions rp ON ur.role_id = rp.role_id
                JOIN pg_rbac_permissions p ON rp.permission_id = p.id
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
             FROM pg_rbac_users u
             JOIN pg_rbac_user_roles ur ON u.id = ur.user_id
             JOIN pg_rbac_roles r ON ur.role_id = r.id
             JOIN pg_rbac_role_permissions rp ON r.id = rp.role_id
             JOIN pg_rbac_permissions p ON rp.permission_id = p.id
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
        $this->pdo->exec("INSERT INTO pg_rbac_users VALUES (5, 'Frank', 1)");

        $rows = $this->ztdQuery(
            "SELECT u.name FROM pg_rbac_users u
             WHERE NOT EXISTS (
                SELECT 1 FROM pg_rbac_user_roles ur WHERE ur.user_id = u.id
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
             FROM pg_rbac_users u
             JOIN pg_rbac_user_roles ur ON u.id = ur.user_id
             JOIN pg_rbac_role_permissions rp ON ur.role_id = rp.role_id
             JOIN pg_rbac_permissions p ON rp.permission_id = p.id
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
            "SELECT 1 FROM pg_rbac_users u
             WHERE u.id = 3
             AND EXISTS (
                SELECT 1 FROM pg_rbac_user_roles ur
                JOIN pg_rbac_role_permissions rp ON ur.role_id = rp.role_id
                JOIN pg_rbac_permissions p ON rp.permission_id = p.id
                WHERE ur.user_id = u.id AND p.resource = 'posts' AND p.action = 'create'
             )"
        );
        $this->assertCount(0, $rows);

        // Grant editor role to Carol
        $this->pdo->exec("INSERT INTO pg_rbac_user_roles VALUES (3, 2)");

        // Now Carol can create posts
        $rows = $this->ztdQuery(
            "SELECT 1 FROM pg_rbac_users u
             WHERE u.id = 3
             AND EXISTS (
                SELECT 1 FROM pg_rbac_user_roles ur
                JOIN pg_rbac_role_permissions rp ON ur.role_id = rp.role_id
                JOIN pg_rbac_permissions p ON rp.permission_id = p.id
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
            "SELECT COUNT(*) AS cnt FROM pg_rbac_user_roles ur
             JOIN pg_rbac_role_permissions rp ON ur.role_id = rp.role_id
             JOIN pg_rbac_permissions p ON rp.permission_id = p.id
             WHERE ur.user_id = 2 AND p.resource = 'posts' AND p.action = 'create'"
        );
        $this->assertEquals(1, (int) $rows[0]['cnt']);

        // Revoke editor role from Bob
        $this->pdo->exec("DELETE FROM pg_rbac_user_roles WHERE user_id = 2 AND role_id = 2");

        // Bob can no longer create posts
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt FROM pg_rbac_user_roles ur
             JOIN pg_rbac_role_permissions rp ON ur.role_id = rp.role_id
             JOIN pg_rbac_permissions p ON rp.permission_id = p.id
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
            "SELECT name FROM pg_rbac_users
             WHERE id IN (SELECT user_id FROM pg_rbac_user_roles WHERE role_id = 1)
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
            "SELECT u.name FROM pg_rbac_users u
             WHERE u.active = 1
             AND EXISTS (
                SELECT 1 FROM pg_rbac_user_roles ur
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
        $stmt = $this->pdo->prepare(
            'SELECT u.name FROM pg_rbac_users u
             WHERE u.active = 1
             AND EXISTS (
                SELECT 1 FROM pg_rbac_user_roles ur
                JOIN pg_rbac_role_permissions rp ON ur.role_id = rp.role_id
                JOIN pg_rbac_permissions p ON rp.permission_id = p.id
                WHERE ur.user_id = u.id AND p.resource = ? AND p.action = ?
             )
             ORDER BY u.name'
        );

        // Who can manage users?
        $stmt->execute(['users', 'manage']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);

        // Who can read posts?
        $stmt->execute(['posts', 'read']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows); // Alice, Bob, Carol
    }

    /**
     * Physical isolation: role changes don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_rbac_user_roles VALUES (3, 2)"); // Grant editor to Carol
        $this->pdo->exec("DELETE FROM pg_rbac_user_roles WHERE user_id = 2"); // Revoke all from Bob

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_rbac_user_roles WHERE user_id = 3");
        $this->assertEquals(2, (int) $rows[0]['cnt']); // viewer + editor

        // Physical table is empty (all inserts were via ZTD shadow)
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT user_id FROM pg_rbac_user_roles")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(0, $rows);
    }
}
