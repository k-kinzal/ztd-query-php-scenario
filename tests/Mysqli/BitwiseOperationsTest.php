<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests bitwise operations through ZTD shadow store.
 * Real user pattern: permission flags, feature toggles, bitmask filtering.
 * @spec SPEC-10.2.50
 */
class BitwiseOperationsTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_bw_users (id INT PRIMARY KEY, name VARCHAR(50), permissions INT NOT NULL DEFAULT 0)',
            'CREATE TABLE mi_bw_features (id INT PRIMARY KEY, name VARCHAR(50), flags INT NOT NULL DEFAULT 0)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_bw_users', 'mi_bw_features'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_bw_users VALUES (1, 'Alice', 15)");
        $this->mysqli->query("INSERT INTO mi_bw_users VALUES (2, 'Bob', 3)");
        $this->mysqli->query("INSERT INTO mi_bw_users VALUES (3, 'Charlie', 1)");
        $this->mysqli->query("INSERT INTO mi_bw_users VALUES (4, 'Diana', 5)");
        $this->mysqli->query("INSERT INTO mi_bw_users VALUES (5, 'Eve', 0)");

        $this->mysqli->query("INSERT INTO mi_bw_features VALUES (1, 'Dashboard', 1)");
        $this->mysqli->query("INSERT INTO mi_bw_features VALUES (2, 'Editor', 3)");
        $this->mysqli->query("INSERT INTO mi_bw_features VALUES (3, 'Deploy', 4)");
        $this->mysqli->query("INSERT INTO mi_bw_features VALUES (4, 'Settings', 8)");
    }

    public function testBitwiseAndFlagCheck(): void
    {
        $rows = $this->ztdQuery("
            SELECT name FROM mi_bw_users
            WHERE (permissions & 2) = 2
            ORDER BY name
        ");
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
    }

    public function testBitwiseOrCombineFlags(): void
    {
        $rows = $this->ztdQuery("
            SELECT name, (permissions | 4) AS with_execute
            FROM mi_bw_users
            WHERE id = 2
        ");
        $this->assertEquals(7, (int) $rows[0]['with_execute']);
    }

    public function testBitwiseXorToggle(): void
    {
        // MySQL supports ^ for XOR
        $this->mysqli->query("UPDATE mi_bw_users SET permissions = permissions ^ 2 WHERE id = 2");
        $rows = $this->ztdQuery("SELECT permissions FROM mi_bw_users WHERE id = 2");
        $this->assertEquals(1, (int) $rows[0]['permissions']); // 3 ^ 2 = 1

        $this->mysqli->query("UPDATE mi_bw_users SET permissions = permissions ^ 2 WHERE id = 2");
        $rows = $this->ztdQuery("SELECT permissions FROM mi_bw_users WHERE id = 2");
        $this->assertEquals(3, (int) $rows[0]['permissions']); // 1 ^ 2 = 3
    }

    public function testBitwiseComplementToRemoveFlag(): void
    {
        $rows = $this->ztdQuery("
            SELECT name, (permissions & ~2) AS without_write
            FROM mi_bw_users
            WHERE id = 1
        ");
        $this->assertEquals(13, (int) $rows[0]['without_write']); // 15 & ~2 = 13
    }

    public function testUserFeatureAccessMatrix(): void
    {
        $rows = $this->ztdQuery("
            SELECT u.name AS user_name, f.name AS feature,
                   CASE WHEN (u.permissions & f.flags) = f.flags THEN 'yes' ELSE 'no' END AS has_access
            FROM mi_bw_users u, mi_bw_features f
            WHERE u.id <= 3
            ORDER BY u.name, f.name
        ");
        $this->assertCount(12, $rows);
    }

    public function testGrantPermissionViaBitwiseOr(): void
    {
        $this->mysqli->query("UPDATE mi_bw_users SET permissions = permissions | 4 WHERE id = 2");
        $rows = $this->ztdQuery("SELECT permissions FROM mi_bw_users WHERE id = 2");
        $this->assertEquals(7, (int) $rows[0]['permissions']);
    }

    public function testRevokePermissionViaBitwiseAndNot(): void
    {
        $this->mysqli->query("UPDATE mi_bw_users SET permissions = permissions & ~8 WHERE id = 1");
        $rows = $this->ztdQuery("SELECT permissions FROM mi_bw_users WHERE id = 1");
        $this->assertEquals(7, (int) $rows[0]['permissions']);
    }

    public function testCountUsersByPermissionFlag(): void
    {
        $rows = $this->ztdQuery("
            SELECT
                SUM(CASE WHEN (permissions & 1) = 1 THEN 1 ELSE 0 END) AS can_read,
                SUM(CASE WHEN (permissions & 2) = 2 THEN 1 ELSE 0 END) AS can_write,
                SUM(CASE WHEN (permissions & 4) = 4 THEN 1 ELSE 0 END) AS can_execute,
                SUM(CASE WHEN (permissions & 8) = 8 THEN 1 ELSE 0 END) AS is_admin
            FROM mi_bw_users
        ");
        $this->assertEquals(4, (int) $rows[0]['can_read']);
        $this->assertEquals(2, (int) $rows[0]['can_write']);
        $this->assertEquals(2, (int) $rows[0]['can_execute']);
        $this->assertEquals(1, (int) $rows[0]['is_admin']);
    }

    public function testPreparedBitwiseFilter(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT name FROM mi_bw_users WHERE (permissions & ?) = ? ORDER BY name",
            [4, 4]
        );
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Diana', $rows[1]['name']);
    }

    public function testBitCountFunction(): void
    {
        $rows = $this->ztdQuery("
            SELECT name, BIT_COUNT(permissions) AS flag_count
            FROM mi_bw_users
            ORDER BY flag_count DESC, name
        ");
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEquals(4, (int) $rows[0]['flag_count']); // 15 = 1111 = 4 bits
    }

    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("UPDATE mi_bw_users SET permissions = 15 WHERE id = 5");

        $rows = $this->ztdQuery("SELECT permissions FROM mi_bw_users WHERE id = 5");
        $this->assertEquals(15, (int) $rows[0]['permissions']);

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query("SELECT permissions FROM mi_bw_users WHERE id = 5");
        $row = $result->fetch_assoc();
        $this->assertEquals(0, (int) $row['permissions']);
    }
}
