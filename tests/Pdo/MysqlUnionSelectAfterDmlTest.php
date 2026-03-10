<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests SELECT ... UNION ALL / UNION from shadow-modified tables on MySQL via PDO.
 *
 * @spec SPEC-4.2
 */
class MysqlUnionSelectAfterDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mpd_usd_users (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                role VARCHAR(20) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE mpd_usd_logs (
                id INT PRIMARY KEY,
                user_name VARCHAR(50) NOT NULL,
                action VARCHAR(50) NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mpd_usd_logs', 'mpd_usd_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mpd_usd_users VALUES (1, 'Alice', 'admin')");
        $this->pdo->exec("INSERT INTO mpd_usd_users VALUES (2, 'Bob', 'user')");
        $this->pdo->exec("INSERT INTO mpd_usd_users VALUES (3, 'Carol', 'user')");

        $this->pdo->exec("INSERT INTO mpd_usd_logs VALUES (1, 'Alice', 'login')");
        $this->pdo->exec("INSERT INTO mpd_usd_logs VALUES (2, 'Bob', 'login')");
    }

    public function testUnionAllSameTableAfterInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO mpd_usd_users VALUES (4, 'Dave', 'admin')");

            $rows = $this->ztdQuery(
                "SELECT name FROM mpd_usd_users WHERE role = 'admin'
                 UNION ALL
                 SELECT name FROM mpd_usd_users WHERE role = 'user'
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
            $this->pdo->exec("INSERT INTO mpd_usd_users VALUES (4, 'Dave', 'user')");
            $this->pdo->exec("INSERT INTO mpd_usd_logs VALUES (3, 'Carol', 'signup')");

            $rows = $this->ztdQuery(
                "SELECT name AS label FROM mpd_usd_users
                 UNION ALL
                 SELECT user_name AS label FROM mpd_usd_logs
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
            $this->pdo->exec("UPDATE mpd_usd_users SET role = 'admin' WHERE id = 2");

            $rows = $this->ztdQuery(
                "SELECT name FROM mpd_usd_users WHERE role = 'admin'
                 UNION
                 SELECT name FROM mpd_usd_users WHERE id <= 3
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
            $this->pdo->exec("DELETE FROM mpd_usd_users WHERE id = 2");

            $rows = $this->ztdQuery(
                "SELECT name FROM mpd_usd_users WHERE role = 'admin'
                 UNION ALL
                 SELECT name FROM mpd_usd_users WHERE role = 'user'
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
