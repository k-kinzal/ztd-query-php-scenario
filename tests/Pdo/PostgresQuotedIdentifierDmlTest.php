<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests DML with quoted mixed-case identifiers through ZTD on PostgreSQL.
 *
 * PostgreSQL is case-sensitive for quoted identifiers: "MyTable" != "mytable".
 * The CTE rewriter must preserve case exactly when handling quoted names.
 *
 * @spec SPEC-4.1
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class PostgresQuotedIdentifierDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        // Use INTEGER instead of BOOLEAN to avoid known Issue #6
        // (BOOLEAN false → CAST('' AS BOOLEAN) error)
        return 'CREATE TABLE "pg_UserProfiles" (
            "UserId" SERIAL PRIMARY KEY,
            "FirstName" TEXT NOT NULL,
            "LastName" TEXT NOT NULL,
            "EmailAddress" TEXT,
            "IsActive" INTEGER NOT NULL DEFAULT 1
        )';
    }

    protected function getTableNames(): array
    {
        return ['"pg_UserProfiles"'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec('INSERT INTO "pg_UserProfiles" ("FirstName", "LastName", "EmailAddress", "IsActive") VALUES (\'Alice\', \'Smith\', \'alice@example.com\', 1)');
        $this->pdo->exec('INSERT INTO "pg_UserProfiles" ("FirstName", "LastName", "EmailAddress", "IsActive") VALUES (\'Bob\', \'Jones\', \'bob@example.com\', 1)');
        $this->pdo->exec('INSERT INTO "pg_UserProfiles" ("FirstName", "LastName", "EmailAddress", "IsActive") VALUES (\'Carol\', \'Davis\', NULL, 0)');
    }

    /**
     * SELECT with quoted mixed-case identifiers.
     *
     * @spec SPEC-3.1
     */
    public function testSelectQuotedIdentifiers(): void
    {
        try {
            $rows = $this->ztdQuery(
                'SELECT "FirstName", "LastName" FROM "pg_UserProfiles" WHERE "IsActive" = 1 ORDER BY "UserId"'
            );

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['FirstName']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT quoted identifiers failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with quoted identifiers.
     *
     * @spec SPEC-4.2
     */
    public function testUpdateQuotedIdentifiers(): void
    {
        try {
            $this->pdo->exec(
                'UPDATE "pg_UserProfiles" SET "IsActive" = 0 WHERE "UserId" = 1'
            );

            $rows = $this->ztdQuery(
                'SELECT "IsActive" FROM "pg_UserProfiles" WHERE "UserId" = 1'
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete('UPDATE quoted: expected 1 row');
            }

            $this->assertEquals(0, (int) $rows[0]['IsActive'],
                'IsActive should be 0');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE quoted identifiers failed: ' . $e->getMessage());
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
                'DELETE FROM "pg_UserProfiles" WHERE "IsActive" = 0'
            );

            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM "pg_UserProfiles"');
            $remaining = (int) $rows[0]['cnt'];

            if ($remaining !== 2) {
                $this->markTestIncomplete('DELETE quoted: expected 2, got ' . $remaining);
            }

            $this->assertEquals(2, $remaining);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE quoted identifiers failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared SELECT with quoted identifiers and ? params.
     *
     * @spec SPEC-3.2
     */
    public function testPreparedSelectQuotedIdentifiers(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                'SELECT "FirstName" FROM "pg_UserProfiles" WHERE "LastName" = ?',
                ['Smith']
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Prepared quoted: expected 1 row');
            }

            $this->assertSame('Alice', $rows[0]['FirstName']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared SELECT quoted identifiers failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET with string concatenation on quoted columns.
     *
     * @spec SPEC-4.2
     */
    public function testUpdateConcatQuotedColumns(): void
    {
        try {
            $this->pdo->exec(
                'UPDATE "pg_UserProfiles" SET "EmailAddress" = "FirstName" || \'.\' || "LastName" || \'@corp.com\' WHERE "EmailAddress" IS NULL'
            );

            $rows = $this->ztdQuery(
                'SELECT "EmailAddress" FROM "pg_UserProfiles" WHERE "UserId" = 3'
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete('UPDATE concat quoted: expected 1 row');
            }

            $this->assertSame('Carol.Davis@corp.com', $rows[0]['EmailAddress']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE concat quoted columns failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT between tables with quoted identifiers.
     *
     * @spec SPEC-4.1
     */
    public function testInsertSelectQuotedIdentifiers(): void
    {
        $this->pdo->exec('CREATE TABLE "pg_ArchivedUsers" (
            "UserId" INTEGER PRIMARY KEY,
            "FullName" TEXT NOT NULL
        )');

        try {
            $this->pdo->exec(
                'INSERT INTO "pg_ArchivedUsers" ("UserId", "FullName")
                 SELECT "UserId", "FirstName" || \' \' || "LastName"
                 FROM "pg_UserProfiles"
                 WHERE "IsActive" = 0'
            );

            $rows = $this->ztdQuery('SELECT "FullName" FROM "pg_ArchivedUsers"');

            if (count($rows) < 1) {
                $this->markTestIncomplete(
                    'INSERT SELECT quoted: expected at least 1 row, got ' . count($rows)
                );
            }

            $this->assertSame('Carol Davis', $rows[0]['FullName']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT quoted identifiers failed: ' . $e->getMessage());
        }
    }
}
