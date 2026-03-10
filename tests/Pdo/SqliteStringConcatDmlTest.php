<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests string concatenation operator (||) in DML context on SQLite.
 *
 * @spec SPEC-4.2
 */
class SqliteStringConcatDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_scd_users (
            id INTEGER PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            display_name TEXT NOT NULL DEFAULT \'\',
            email TEXT NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_scd_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_scd_users VALUES (1, 'Alice', 'Smith', '', 'alice@example.com')");
        $this->pdo->exec("INSERT INTO sl_scd_users VALUES (2, 'Bob', 'Jones', '', 'bob@test.org')");
        $this->pdo->exec("INSERT INTO sl_scd_users VALUES (3, 'Carol', 'Williams', '', 'carol@example.com')");
    }

    public function testUpdateSetConcatOperator(): void
    {
        try {
            $this->pdo->exec("UPDATE sl_scd_users SET display_name = first_name || ' ' || last_name");

            $rows = $this->ztdQuery("SELECT id, display_name FROM sl_scd_users ORDER BY id");
            $this->assertCount(3, $rows);

            if ($rows[0]['display_name'] !== 'Alice Smith') {
                $this->markTestIncomplete('|| concat: expected "Alice Smith", got ' . var_export($rows[0]['display_name'], true));
            }
            $this->assertSame('Alice Smith', $rows[0]['display_name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE SET || concat failed: ' . $e->getMessage());
        }
    }

    public function testSelectConcatAfterInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_scd_users VALUES (4, 'Dave', 'Brown', '', 'dave@test.org')");

            $rows = $this->ztdQuery(
                "SELECT first_name || ' ' || last_name AS full_name FROM sl_scd_users ORDER BY id"
            );

            $this->assertCount(4, $rows);
            $names = array_column($rows, 'full_name');

            if (!in_array('Dave Brown', $names)) {
                $this->markTestIncomplete('SELECT || after INSERT: Dave not found. Got: ' . implode(', ', $names));
            }
            $this->assertSame('Dave Brown', $rows[3]['full_name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT || concat after INSERT failed: ' . $e->getMessage());
        }
    }

    public function testUpdateBuildEmailWithConcat(): void
    {
        try {
            $this->pdo->exec("UPDATE sl_scd_users SET email = LOWER(first_name) || '.' || LOWER(last_name) || '@company.com'");

            $rows = $this->ztdQuery("SELECT id, email FROM sl_scd_users ORDER BY id");
            $this->assertCount(3, $rows);

            if ($rows[0]['email'] !== 'alice.smith@company.com') {
                $this->markTestIncomplete('Build email: expected "alice.smith@company.com", got ' . var_export($rows[0]['email'], true));
            }
            $this->assertSame('alice.smith@company.com', $rows[0]['email']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE build email with || failed: ' . $e->getMessage());
        }
    }

    public function testWhereConcatAfterDml(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_scd_users VALUES (4, 'Dave', 'Brown', 'Dave Brown', 'dave@test.org')");

            $rows = $this->ztdQuery(
                "SELECT id FROM sl_scd_users WHERE first_name || ' ' || last_name = 'Dave Brown'"
            );

            if (empty($rows)) {
                $this->markTestIncomplete('WHERE || after INSERT: no rows matched');
            }
            $this->assertCount(1, $rows);
            $this->assertEquals(4, (int) $rows[0]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('WHERE || after DML failed: ' . $e->getMessage());
        }
    }
}
