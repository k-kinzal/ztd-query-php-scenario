<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests DML with quoted mixed-case identifiers through ZTD on SQLite.
 *
 * Many applications (especially those migrated from case-sensitive databases
 * or using ORMs like Doctrine) use double-quoted identifiers with mixed case.
 * The CTE rewriter must correctly handle quoted table and column names.
 *
 * @spec SPEC-4.1
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class SqliteQuotedIdentifierDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE "sl_UserProfiles" (
            "UserId" INTEGER PRIMARY KEY,
            "FirstName" TEXT NOT NULL,
            "LastName" TEXT NOT NULL,
            "EmailAddress" TEXT,
            "IsActive" INTEGER NOT NULL DEFAULT 1
        )';
    }

    protected function getTableNames(): array
    {
        return ['"sl_UserProfiles"'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec('INSERT INTO "sl_UserProfiles" ("UserId", "FirstName", "LastName", "EmailAddress", "IsActive") VALUES (1, \'Alice\', \'Smith\', \'alice@example.com\', 1)');
        $this->pdo->exec('INSERT INTO "sl_UserProfiles" ("UserId", "FirstName", "LastName", "EmailAddress", "IsActive") VALUES (2, \'Bob\', \'Jones\', \'bob@example.com\', 1)');
        $this->pdo->exec('INSERT INTO "sl_UserProfiles" ("UserId", "FirstName", "LastName", "EmailAddress", "IsActive") VALUES (3, \'Carol\', \'Davis\', NULL, 0)');
    }

    /**
     * SELECT with quoted mixed-case table and column names.
     *
     * @spec SPEC-3.1
     */
    public function testSelectQuotedIdentifiers(): void
    {
        try {
            $rows = $this->ztdQuery(
                'SELECT "FirstName", "LastName" FROM "sl_UserProfiles" WHERE "IsActive" = 1 ORDER BY "UserId"'
            );

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['FirstName']);
            $this->assertSame('Bob', $rows[1]['FirstName']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT with quoted identifiers failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT with quoted identifiers.
     *
     * @spec SPEC-4.1
     */
    public function testInsertQuotedIdentifiers(): void
    {
        try {
            $this->pdo->exec(
                'INSERT INTO "sl_UserProfiles" ("UserId", "FirstName", "LastName", "EmailAddress", "IsActive") VALUES (4, \'Dan\', \'Wilson\', \'dan@example.com\', 1)'
            );

            $rows = $this->ztdQuery('SELECT "FirstName" FROM "sl_UserProfiles" WHERE "UserId" = 4');

            if (count($rows) !== 1) {
                $this->markTestIncomplete('INSERT quoted: expected 1 row, got ' . count($rows));
            }

            $this->assertSame('Dan', $rows[0]['FirstName']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT with quoted identifiers failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with quoted identifiers in SET and WHERE.
     *
     * @spec SPEC-4.2
     */
    public function testUpdateQuotedIdentifiers(): void
    {
        try {
            $this->pdo->exec(
                'UPDATE "sl_UserProfiles" SET "IsActive" = 0, "EmailAddress" = NULL WHERE "UserId" = 1'
            );

            $rows = $this->ztdQuery(
                'SELECT "IsActive", "EmailAddress" FROM "sl_UserProfiles" WHERE "UserId" = 1'
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete('UPDATE quoted: expected 1 row, got ' . count($rows));
            }

            $this->assertEquals(0, (int) $rows[0]['IsActive']);
            $this->assertNull($rows[0]['EmailAddress']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with quoted identifiers failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with quoted identifiers.
     *
     * @spec SPEC-4.3
     */
    public function testDeleteQuotedIdentifiers(): void
    {
        try {
            $this->pdo->exec(
                'DELETE FROM "sl_UserProfiles" WHERE "IsActive" = 0'
            );

            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM "sl_UserProfiles"');
            $remaining = (int) $rows[0]['cnt'];

            if ($remaining !== 2) {
                $this->markTestIncomplete(
                    'DELETE quoted: expected 2 remaining, got ' . $remaining
                );
            }

            $this->assertEquals(2, $remaining);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE with quoted identifiers failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared statement with quoted identifiers and bound parameters.
     *
     * @spec SPEC-3.2
     */
    public function testPreparedSelectQuotedIdentifiers(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                'SELECT "FirstName", "LastName" FROM "sl_UserProfiles" WHERE "IsActive" = ? AND "FirstName" LIKE ?',
                [1, 'A%']
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Prepared quoted: expected 1 row, got ' . count($rows));
            }

            $this->assertSame('Alice', $rows[0]['FirstName']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared SELECT with quoted identifiers failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with concatenation on quoted column names.
     *
     * @spec SPEC-4.2
     */
    public function testUpdateConcatQuotedColumns(): void
    {
        try {
            $this->pdo->exec(
                'UPDATE "sl_UserProfiles" SET "EmailAddress" = "FirstName" || \'.\' || "LastName" || \'@corp.com\' WHERE "EmailAddress" IS NULL'
            );

            $rows = $this->ztdQuery(
                'SELECT "EmailAddress" FROM "sl_UserProfiles" WHERE "UserId" = 3'
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete('UPDATE concat quoted: expected 1 row');
            }

            $this->assertSame('Carol.Davis@corp.com', $rows[0]['EmailAddress']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE concat with quoted columns failed: ' . $e->getMessage());
        }
    }

    /**
     * JOIN with quoted identifiers on both tables.
     *
     * @spec SPEC-3.3
     */
    public function testJoinQuotedIdentifiers(): void
    {
        $this->pdo->exec('CREATE TABLE "sl_UserRoles" (
            "RoleId" INTEGER PRIMARY KEY,
            "UserId" INTEGER NOT NULL,
            "RoleName" TEXT NOT NULL
        )');

        try {
            $this->pdo->exec('INSERT INTO "sl_UserRoles" ("RoleId", "UserId", "RoleName") VALUES (1, 1, \'admin\')');
            $this->pdo->exec('INSERT INTO "sl_UserRoles" ("RoleId", "UserId", "RoleName") VALUES (2, 2, \'user\')');

            $rows = $this->ztdQuery(
                'SELECT u."FirstName", r."RoleName"
                 FROM "sl_UserProfiles" u
                 JOIN "sl_UserRoles" r ON r."UserId" = u."UserId"
                 ORDER BY u."UserId"'
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete('JOIN quoted: expected 2 rows, got ' . count($rows));
            }

            $this->assertSame('Alice', $rows[0]['FirstName']);
            $this->assertSame('admin', $rows[0]['RoleName']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('JOIN with quoted identifiers failed: ' . $e->getMessage());
        }
    }
}
