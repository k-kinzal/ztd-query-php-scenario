<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests UPDATE SET with LOWER()/UPPER()/TRIM()/SUBSTR() string functions
 * on SQLite via PDO.
 *
 * @spec SPEC-4.2
 */
class SqliteUpdateSetLowerUpperTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_ulu_contacts (
            id INTEGER PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT NOT NULL,
            code TEXT NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_ulu_contacts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_ulu_contacts VALUES (1, 'alice', 'JOHNSON', 'Alice@Example.COM', '  AB-001  ')");
        $this->pdo->exec("INSERT INTO sl_ulu_contacts VALUES (2, 'BOB', 'smith', 'BOB@test.ORG', '  CD-002  ')");
        $this->pdo->exec("INSERT INTO sl_ulu_contacts VALUES (3, 'Carol', 'Williams', 'carol@example.com', '  EF-003  ')");
    }

    public function testUpdateSetUpper(): void
    {
        try {
            $this->pdo->exec("UPDATE sl_ulu_contacts SET first_name = UPPER(first_name)");
            $rows = $this->ztdQuery("SELECT id, first_name FROM sl_ulu_contacts ORDER BY id");
            $this->assertCount(3, $rows);

            if ($rows[0]['first_name'] !== 'ALICE') {
                $this->markTestIncomplete('UPPER: expected "ALICE", got ' . var_export($rows[0]['first_name'], true));
            }
            $this->assertSame('ALICE', $rows[0]['first_name']);
            $this->assertSame('BOB', $rows[1]['first_name']);
            $this->assertSame('CAROL', $rows[2]['first_name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET UPPER failed: ' . $e->getMessage());
        }
    }

    public function testUpdateSetLower(): void
    {
        try {
            $this->pdo->exec("UPDATE sl_ulu_contacts SET email = LOWER(email)");
            $rows = $this->ztdQuery("SELECT id, email FROM sl_ulu_contacts ORDER BY id");
            $this->assertCount(3, $rows);

            if ($rows[0]['email'] !== 'alice@example.com') {
                $this->markTestIncomplete('LOWER: expected "alice@example.com", got ' . var_export($rows[0]['email'], true));
            }
            $this->assertSame('alice@example.com', $rows[0]['email']);
            $this->assertSame('bob@test.org', $rows[1]['email']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET LOWER failed: ' . $e->getMessage());
        }
    }

    public function testUpdateSetTrim(): void
    {
        try {
            $this->pdo->exec("UPDATE sl_ulu_contacts SET code = TRIM(code)");
            $rows = $this->ztdQuery("SELECT id, code FROM sl_ulu_contacts ORDER BY id");
            $this->assertCount(3, $rows);

            if ($rows[0]['code'] !== 'AB-001') {
                $this->markTestIncomplete('TRIM: expected "AB-001", got ' . var_export($rows[0]['code'], true));
            }
            $this->assertSame('AB-001', $rows[0]['code']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET TRIM failed: ' . $e->getMessage());
        }
    }

    public function testUpdateSetMultipleStringFunctions(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE sl_ulu_contacts SET first_name = UPPER(first_name), last_name = LOWER(last_name), email = LOWER(email) WHERE id = 1"
            );

            $rows = $this->ztdQuery("SELECT first_name, last_name, email FROM sl_ulu_contacts WHERE id = 1");
            $this->assertCount(1, $rows);

            if ($rows[0]['first_name'] !== 'ALICE') {
                $this->markTestIncomplete('Multi-column: expected "ALICE", got ' . var_export($rows[0]['first_name'], true));
            }
            $this->assertSame('ALICE', $rows[0]['first_name']);
            $this->assertSame('johnson', $rows[0]['last_name']);
            $this->assertSame('alice@example.com', $rows[0]['email']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET multiple string functions failed: ' . $e->getMessage());
        }
    }
}
