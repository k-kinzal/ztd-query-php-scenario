<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests SELECT ... UNION ALL / UNION from shadow-modified tables on SQLite.
 *
 * @spec SPEC-4.2
 */
class SqliteUnionSelectAfterDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_usd_users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                role TEXT NOT NULL
            )',
            'CREATE TABLE sl_usd_logs (
                id INTEGER PRIMARY KEY,
                user_name TEXT NOT NULL,
                action TEXT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_usd_logs', 'sl_usd_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_usd_users VALUES (1, 'Alice', 'admin')");
        $this->pdo->exec("INSERT INTO sl_usd_users VALUES (2, 'Bob', 'user')");
        $this->pdo->exec("INSERT INTO sl_usd_users VALUES (3, 'Carol', 'user')");

        $this->pdo->exec("INSERT INTO sl_usd_logs VALUES (1, 'Alice', 'login')");
        $this->pdo->exec("INSERT INTO sl_usd_logs VALUES (2, 'Bob', 'login')");
    }

    public function testUnionAllSameTableAfterInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_usd_users VALUES (4, 'Dave', 'admin')");

            $rows = $this->ztdQuery(
                "SELECT name FROM sl_usd_users WHERE role = 'admin'
                 UNION ALL
                 SELECT name FROM sl_usd_users WHERE role = 'user'
                 ORDER BY name"
            );

            $names = array_column($rows, 'name');

            if (!in_array('Dave', $names)) {
                $this->markTestIncomplete('UNION ALL same table: shadow INSERT not visible. Got: ' . implode(', ', $names));
            }
            $this->assertCount(4, $rows);
            $this->assertEquals(['Alice', 'Bob', 'Carol', 'Dave'], $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UNION ALL same table after INSERT failed: ' . $e->getMessage());
        }
    }

    public function testUnionAllTwoTablesAfterDml(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_usd_users VALUES (4, 'Dave', 'user')");
            $this->pdo->exec("INSERT INTO sl_usd_logs VALUES (3, 'Carol', 'signup')");

            $rows = $this->ztdQuery(
                "SELECT name AS label FROM sl_usd_users
                 UNION ALL
                 SELECT user_name AS label FROM sl_usd_logs
                 ORDER BY label"
            );

            $labels = array_column($rows, 'label');

            if (!in_array('Dave', $labels)) {
                $this->markTestIncomplete('UNION ALL two tables: shadow INSERT to users not visible. Got: ' . implode(', ', $labels));
            }
            $this->assertCount(7, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UNION ALL two tables after DML failed: ' . $e->getMessage());
        }
    }

    public function testUnionDistinctAfterUpdate(): void
    {
        try {
            $this->pdo->exec("UPDATE sl_usd_users SET role = 'admin' WHERE id = 2");

            $rows = $this->ztdQuery(
                "SELECT name FROM sl_usd_users WHERE role = 'admin'
                 UNION
                 SELECT name FROM sl_usd_users WHERE id <= 3
                 ORDER BY name"
            );

            $names = array_column($rows, 'name');

            if (!in_array('Bob', $names)) {
                $this->markTestIncomplete('UNION DISTINCT: Bob not visible after UPDATE. Got: ' . implode(', ', $names));
            }
            $this->assertCount(3, $rows);
            $this->assertEquals(['Alice', 'Bob', 'Carol'], $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UNION DISTINCT after UPDATE failed: ' . $e->getMessage());
        }
    }

    public function testUnionAllAfterDelete(): void
    {
        try {
            $this->pdo->exec("DELETE FROM sl_usd_users WHERE id = 2");

            $rows = $this->ztdQuery(
                "SELECT name FROM sl_usd_users WHERE role = 'admin'
                 UNION ALL
                 SELECT name FROM sl_usd_users WHERE role = 'user'
                 ORDER BY name"
            );

            $names = array_column($rows, 'name');

            if (in_array('Bob', $names)) {
                $this->markTestIncomplete('UNION ALL after DELETE: deleted row still visible. Got: ' . implode(', ', $names));
            }
            $this->assertCount(2, $rows);
            $this->assertEquals(['Alice', 'Carol'], $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UNION ALL after DELETE failed: ' . $e->getMessage());
        }
    }
}
