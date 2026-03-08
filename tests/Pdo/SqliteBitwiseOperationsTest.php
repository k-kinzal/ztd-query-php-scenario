<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests bitwise operations through ZTD shadow store.
 * Real user pattern: permission flags, feature toggles, bitmask filtering.
 * @spec SPEC-10.2.50
 */
class SqliteBitwiseOperationsTest extends AbstractSqlitePdoTestCase
{
    // Permission flags
    private const PERM_READ    = 1;  // 0001
    private const PERM_WRITE   = 2;  // 0010
    private const PERM_EXECUTE = 4;  // 0100
    private const PERM_ADMIN   = 8;  // 1000

    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_bw_users (id INTEGER PRIMARY KEY, name TEXT, permissions INTEGER NOT NULL DEFAULT 0)',
            'CREATE TABLE sl_bw_features (id INTEGER PRIMARY KEY, name TEXT, flags INTEGER NOT NULL DEFAULT 0)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_bw_users', 'sl_bw_features'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Users with various permission levels
        $this->pdo->exec("INSERT INTO sl_bw_users VALUES (1, 'Alice', 15)");   // READ|WRITE|EXECUTE|ADMIN = 1111
        $this->pdo->exec("INSERT INTO sl_bw_users VALUES (2, 'Bob', 3)");      // READ|WRITE = 0011
        $this->pdo->exec("INSERT INTO sl_bw_users VALUES (3, 'Charlie', 1)");  // READ only = 0001
        $this->pdo->exec("INSERT INTO sl_bw_users VALUES (4, 'Diana', 5)");    // READ|EXECUTE = 0101
        $this->pdo->exec("INSERT INTO sl_bw_users VALUES (5, 'Eve', 0)");      // No permissions

        $this->pdo->exec("INSERT INTO sl_bw_features VALUES (1, 'Dashboard', 1)");  // Needs READ
        $this->pdo->exec("INSERT INTO sl_bw_features VALUES (2, 'Editor', 3)");     // Needs READ+WRITE
        $this->pdo->exec("INSERT INTO sl_bw_features VALUES (3, 'Deploy', 4)");     // Needs EXECUTE
        $this->pdo->exec("INSERT INTO sl_bw_features VALUES (4, 'Settings', 8)");   // Needs ADMIN
    }

    /**
     * Bitwise AND to check if a specific flag is set.
     */
    public function testBitwiseAndFlagCheck(): void
    {
        // Users with WRITE permission
        $rows = $this->ztdQuery("
            SELECT name FROM sl_bw_users
            WHERE (permissions & 2) = 2
            ORDER BY name
        ");
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
    }

    /**
     * Bitwise OR to combine flags.
     */
    public function testBitwiseOrCombineFlags(): void
    {
        $rows = $this->ztdQuery("
            SELECT name, (permissions | 4) AS with_execute
            FROM sl_bw_users
            WHERE id = 2
        ");
        $this->assertCount(1, $rows);
        $this->assertEquals(7, (int) $rows[0]['with_execute']); // 0011 | 0100 = 0111
    }

    /**
     * Bitwise NOT (complement) to remove a flag.
     */
    public function testBitwiseComplementToRemoveFlag(): void
    {
        $rows = $this->ztdQuery("
            SELECT name, (permissions & ~2) AS without_write
            FROM sl_bw_users
            WHERE id = 1
        ");
        $this->assertEquals(13, (int) $rows[0]['without_write']); // 1111 & 1101 = 1101 = 13
    }

    /**
     * Filter users who can access a feature (user permissions contain all feature flags).
     */
    public function testUserCanAccessFeature(): void
    {
        // Who can access the Editor (requires READ + WRITE = 3)?
        $rows = $this->ztdQuery("
            SELECT u.name
            FROM sl_bw_users u
            WHERE (u.permissions & 3) = 3
            ORDER BY u.name
        ");
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
    }

    /**
     * Cross-join to show all user-feature access combinations.
     */
    public function testUserFeatureAccessMatrix(): void
    {
        $rows = $this->ztdQuery("
            SELECT u.name AS user_name, f.name AS feature,
                   CASE WHEN (u.permissions & f.flags) = f.flags THEN 'yes' ELSE 'no' END AS has_access
            FROM sl_bw_users u, sl_bw_features f
            WHERE u.id <= 3
            ORDER BY u.name, f.name
        ");
        // 3 users × 4 features = 12 rows
        $this->assertCount(12, $rows);

        // Alice (15) can access everything
        $aliceRows = array_filter($rows, fn($r) => $r['user_name'] === 'Alice');
        foreach ($aliceRows as $r) {
            $this->assertSame('yes', $r['has_access'], "Alice should access {$r['feature']}");
        }

        // Charlie (1=READ) can only access Dashboard (requires READ=1)
        $charlieRows = array_values(array_filter($rows, fn($r) => $r['user_name'] === 'Charlie'));
        $charlieAccess = array_filter($charlieRows, fn($r) => $r['has_access'] === 'yes');
        $this->assertCount(1, $charlieAccess);
    }

    /**
     * UPDATE to grant a permission using bitwise OR.
     */
    public function testGrantPermissionViaBitwiseOr(): void
    {
        // Grant EXECUTE to Bob
        $this->pdo->exec("UPDATE sl_bw_users SET permissions = permissions | 4 WHERE id = 2");

        $rows = $this->ztdQuery("SELECT permissions FROM sl_bw_users WHERE id = 2");
        $this->assertEquals(7, (int) $rows[0]['permissions']); // 3 | 4 = 7
    }

    /**
     * UPDATE to revoke a permission using bitwise AND NOT.
     */
    public function testRevokePermissionViaBitwiseAndNot(): void
    {
        // Revoke ADMIN from Alice
        $this->pdo->exec("UPDATE sl_bw_users SET permissions = permissions & ~8 WHERE id = 1");

        $rows = $this->ztdQuery("SELECT permissions FROM sl_bw_users WHERE id = 1");
        $this->assertEquals(7, (int) $rows[0]['permissions']); // 15 & ~8 = 7
    }

    /**
     * Count users per permission level using bitwise operations.
     */
    public function testCountUsersByPermissionFlag(): void
    {
        $rows = $this->ztdQuery("
            SELECT
                SUM(CASE WHEN (permissions & 1) = 1 THEN 1 ELSE 0 END) AS can_read,
                SUM(CASE WHEN (permissions & 2) = 2 THEN 1 ELSE 0 END) AS can_write,
                SUM(CASE WHEN (permissions & 4) = 4 THEN 1 ELSE 0 END) AS can_execute,
                SUM(CASE WHEN (permissions & 8) = 8 THEN 1 ELSE 0 END) AS is_admin
            FROM sl_bw_users
        ");
        $this->assertEquals(4, (int) $rows[0]['can_read']);    // Alice, Bob, Charlie, Diana
        $this->assertEquals(2, (int) $rows[0]['can_write']);   // Alice, Bob
        $this->assertEquals(2, (int) $rows[0]['can_execute']); // Alice, Diana
        $this->assertEquals(1, (int) $rows[0]['is_admin']);    // Alice
    }

    /**
     * Prepared statement with bitwise AND in WHERE returns empty.
     * The CTE rewriter may not correctly handle bitwise operators with bound parameters.
     */
    public function testPreparedBitwiseFilterReturnsEmpty(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT name FROM sl_bw_users WHERE (permissions & ?) = ? ORDER BY name",
            [self::PERM_EXECUTE, self::PERM_EXECUTE]
        );
        // Known limitation: prepared bitwise operations return empty on SQLite
        $this->assertCount(0, $rows);
    }

    /**
     * Non-prepared bitwise filter works as workaround.
     */
    public function testNonPreparedBitwiseFilterWorks(): void
    {
        $rows = $this->ztdQuery("
            SELECT name FROM sl_bw_users WHERE (permissions & 4) = 4 ORDER BY name
        ");
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Diana', $rows[1]['name']);
    }

    /**
     * Bitwise XOR (^) is not a valid operator in SQLite.
     * SQLite does not support the ^ operator for XOR — it throws "unrecognized token".
     * Workaround: use ((a | b) - (a & b)) for XOR, or (a + b - 2 * (a & b)).
     */
    public function testBitwiseXorNotSupportedInSqlite(): void
    {
        $this->expectException(\ZtdQuery\Adapter\Pdo\ZtdPdoException::class);
        $this->pdo->exec("UPDATE sl_bw_users SET permissions = permissions ^ 2 WHERE id = 2");
    }

    /**
     * XOR workaround using OR-minus-AND arithmetic.
     */
    public function testBitwiseXorWorkaround(): void
    {
        // Toggle WRITE for Bob: XOR via (a | b) - (a & b)
        $this->pdo->exec("UPDATE sl_bw_users SET permissions = (permissions | 2) - (permissions & 2) WHERE id = 2");
        $rows = $this->ztdQuery("SELECT permissions FROM sl_bw_users WHERE id = 2");
        $this->assertEquals(1, (int) $rows[0]['permissions']); // 3 XOR 2 = 1
    }

    /**
     * Physical isolation — permission changes do not persist.
     * Data is inserted via ZTD (shadow store), so physical table is empty.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("UPDATE sl_bw_users SET permissions = 15 WHERE id = 5"); // Grant all to Eve

        $rows = $this->ztdQuery("SELECT permissions FROM sl_bw_users WHERE id = 5");
        $this->assertEquals(15, (int) $rows[0]['permissions']);

        // Physical table is empty — all data was inserted through ZTD shadow store
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_bw_users")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
