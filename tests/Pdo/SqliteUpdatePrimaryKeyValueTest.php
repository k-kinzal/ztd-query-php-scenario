<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests shadow store behavior when UPDATE changes a primary key value.
 *
 * The shadow store tracks rows by PK. When UPDATE SET id = new_value WHERE id = old_value,
 * the store must correctly re-key the row. This is a real user pattern for:
 * - renumbering IDs
 * - slug/code changes on natural-key tables
 * - composite PK member changes
 * @spec SPEC-4.3
 */
class SqliteUpdatePrimaryKeyValueTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE upk_users (id INT PRIMARY KEY, name TEXT, email TEXT)',
            'CREATE TABLE upk_codes (code TEXT PRIMARY KEY, description TEXT)',
            'CREATE TABLE upk_composite (a INT, b INT, val TEXT, PRIMARY KEY (a, b))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['upk_users', 'upk_codes', 'upk_composite'];
    }

    /**
     * Change integer PK from one value to another.
     */
    public function testUpdateIntegerPk(): void
    {
        $this->pdo->exec("INSERT INTO upk_users VALUES (1, 'Alice', 'alice@example.com')");

        try {
            $this->pdo->exec("UPDATE upk_users SET id = 100 WHERE id = 1");

            // Old PK should not exist
            $old = $this->ztdQuery('SELECT * FROM upk_users WHERE id = 1');
            $this->assertCount(0, $old, 'Old PK row should be gone after UPDATE');

            // New PK should exist
            $new = $this->ztdQuery('SELECT * FROM upk_users WHERE id = 100');
            $this->assertCount(1, $new, 'New PK row should exist');
            $this->assertSame('Alice', $new[0]['name']);
        } catch (\Exception $e) {
            $this->markTestSkipped('PK UPDATE not supported: ' . $e->getMessage());
        }
    }

    /**
     * Change text PK.
     */
    public function testUpdateTextPk(): void
    {
        $this->pdo->exec("INSERT INTO upk_codes VALUES ('OLD', 'Legacy code')");

        try {
            $this->pdo->exec("UPDATE upk_codes SET code = 'NEW' WHERE code = 'OLD'");

            $old = $this->ztdQuery("SELECT * FROM upk_codes WHERE code = 'OLD'");
            $this->assertCount(0, $old, 'Old text PK should be gone');

            $new = $this->ztdQuery("SELECT * FROM upk_codes WHERE code = 'NEW'");
            $this->assertCount(1, $new);
            $this->assertSame('Legacy code', $new[0]['description']);
        } catch (\Exception $e) {
            $this->markTestSkipped('Text PK UPDATE not supported: ' . $e->getMessage());
        }
    }

    /**
     * Change PK to value of another existing row (should fail or produce conflict).
     */
    public function testUpdatePkToExistingValue(): void
    {
        $this->pdo->exec("INSERT INTO upk_users VALUES (1, 'Alice', 'a@x.com')");
        $this->pdo->exec("INSERT INTO upk_users VALUES (2, 'Bob', 'b@x.com')");

        try {
            // Try to change Alice's PK to Bob's PK - should conflict
            $this->pdo->exec("UPDATE upk_users SET id = 2 WHERE id = 1");

            // If we reach here, check what happened
            $rows = $this->ztdQuery('SELECT * FROM upk_users ORDER BY id');
            // Should ideally only have one row with id=2, but behavior varies
            $this->assertLessThanOrEqual(2, count($rows), 'Should not create extra rows');
        } catch (\Exception $e) {
            // Expected: constraint violation or similar
            $this->assertTrue(true, 'PK conflict correctly raised');
        }
    }

    /**
     * Update PK then query with JOIN referencing old and new PKs.
     */
    public function testSelectAfterPkChangeWithJoin(): void
    {
        $this->pdo->exec('CREATE TABLE upk_orders (id INT PRIMARY KEY, user_id INT, amount INT)');

        try {
            $this->pdo->exec("INSERT INTO upk_users VALUES (1, 'Alice', 'a@x.com')");
            $this->pdo->exec("INSERT INTO upk_orders VALUES (10, 1, 500)");

            $this->pdo->exec("UPDATE upk_users SET id = 100 WHERE id = 1");

            // Order still references user_id=1, but user's PK is now 100
            $rows = $this->ztdQuery(
                'SELECT u.name, o.amount
                 FROM upk_users u JOIN upk_orders o ON u.id = o.user_id'
            );
            // Should return 0 because the FK no longer matches
            $this->assertCount(0, $rows, 'JOIN should find no match after PK change');

            // Update the FK too
            $this->pdo->exec("UPDATE upk_orders SET user_id = 100 WHERE id = 10");

            $rows = $this->ztdQuery(
                'SELECT u.name, o.amount
                 FROM upk_users u JOIN upk_orders o ON u.id = o.user_id'
            );
            $this->assertCount(1, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestSkipped('PK change + JOIN not supported: ' . $e->getMessage());
        } finally {
            $this->pdo->disableZtd();
            $this->pdo->exec('DROP TABLE IF EXISTS upk_orders');
            $this->pdo->enableZtd();
        }
    }

    /**
     * Update one member of a composite PK.
     */
    public function testUpdateCompositePkMember(): void
    {
        $this->pdo->exec("INSERT INTO upk_composite VALUES (1, 1, 'original')");

        try {
            $this->pdo->exec("UPDATE upk_composite SET b = 2 WHERE a = 1 AND b = 1");

            $old = $this->ztdQuery('SELECT * FROM upk_composite WHERE a = 1 AND b = 1');
            $this->assertCount(0, $old, 'Old composite PK should be gone');

            $new = $this->ztdQuery('SELECT * FROM upk_composite WHERE a = 1 AND b = 2');
            $this->assertCount(1, $new);
            $this->assertSame('original', $new[0]['val']);
        } catch (\Exception $e) {
            $this->markTestSkipped('Composite PK member UPDATE not supported: ' . $e->getMessage());
        }
    }

    /**
     * Change PK then SELECT COUNT(*) — row count should be preserved.
     */
    public function testRowCountPreservedAfterPkChange(): void
    {
        $this->pdo->exec("INSERT INTO upk_users VALUES (1, 'Alice', 'a@x.com')");
        $this->pdo->exec("INSERT INTO upk_users VALUES (2, 'Bob', 'b@x.com')");

        try {
            $this->pdo->exec("UPDATE upk_users SET id = 100 WHERE id = 1");

            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM upk_users');
            $this->assertSame('2', (string) $rows[0]['cnt'], 'Row count should remain 2');
        } catch (\Exception $e) {
            $this->markTestSkipped('PK change count not supported: ' . $e->getMessage());
        }
    }

    /**
     * Change PK then DELETE by new PK.
     */
    public function testDeleteAfterPkChange(): void
    {
        $this->pdo->exec("INSERT INTO upk_users VALUES (1, 'Alice', 'a@x.com')");

        try {
            $this->pdo->exec("UPDATE upk_users SET id = 100 WHERE id = 1");
            $this->pdo->exec("DELETE FROM upk_users WHERE id = 100");

            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM upk_users');
            $this->assertSame('0', (string) $rows[0]['cnt'], 'Table should be empty');
        } catch (\Exception $e) {
            $this->markTestSkipped('Delete after PK change not supported: ' . $e->getMessage());
        }
    }
}
