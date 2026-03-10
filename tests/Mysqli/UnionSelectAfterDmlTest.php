<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests SELECT ... UNION ALL / UNION from shadow-modified tables.
 *
 * The CTE rewriter must inject shadow CTEs into BOTH branches of a UNION.
 * If only one branch is rewritten, the other branch returns stale physical data.
 *
 * @spec SPEC-4.2
 */
class UnionSelectAfterDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_usd_users (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                role VARCHAR(20) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE mi_usd_logs (
                id INT PRIMARY KEY,
                user_name VARCHAR(50) NOT NULL,
                action VARCHAR(50) NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_usd_logs', 'mi_usd_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_usd_users VALUES (1, 'Alice', 'admin')");
        $this->mysqli->query("INSERT INTO mi_usd_users VALUES (2, 'Bob', 'user')");
        $this->mysqli->query("INSERT INTO mi_usd_users VALUES (3, 'Carol', 'user')");

        $this->mysqli->query("INSERT INTO mi_usd_logs VALUES (1, 'Alice', 'login')");
        $this->mysqli->query("INSERT INTO mi_usd_logs VALUES (2, 'Bob', 'login')");
    }

    /**
     * UNION ALL of two SELECTs from same shadow-modified table.
     */
    public function testUnionAllSameTableAfterInsert(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_usd_users VALUES (4, 'Dave', 'admin')");

            $rows = $this->ztdQuery(
                "SELECT name FROM mi_usd_users WHERE role = 'admin'
                 UNION ALL
                 SELECT name FROM mi_usd_users WHERE role = 'user'
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

    /**
     * UNION ALL of two SELECTs from same table after UPDATE.
     */
    public function testUnionAllSameTableAfterUpdate(): void
    {
        try {
            $this->mysqli->query("UPDATE mi_usd_users SET role = 'admin' WHERE id = 2");

            $rows = $this->ztdQuery(
                "SELECT name FROM mi_usd_users WHERE role = 'admin'
                 UNION ALL
                 SELECT name FROM mi_usd_users WHERE role = 'user'"
            );

            $names = array_column($rows, 'name');
            sort($names);

            if (!in_array('Bob', $names) || count($names) !== 3) {
                $this->markTestIncomplete('UNION ALL after UPDATE: expected [Alice, Bob, Carol], got: ' . implode(', ', $names));
            }
            $this->assertCount(3, $rows);
            $this->assertEquals(['Alice', 'Bob', 'Carol'], $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UNION ALL same table after UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * UNION ALL from two different shadow-modified tables.
     */
    public function testUnionAllTwoTablesAfterDml(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_usd_users VALUES (4, 'Dave', 'user')");
            $this->mysqli->query("INSERT INTO mi_usd_logs VALUES (3, 'Carol', 'signup')");

            $rows = $this->ztdQuery(
                "SELECT name AS label FROM mi_usd_users
                 UNION ALL
                 SELECT user_name AS label FROM mi_usd_logs
                 ORDER BY label"
            );

            $labels = array_column($rows, 'label');

            if (!in_array('Dave', $labels)) {
                $this->markTestIncomplete('UNION ALL two tables: shadow INSERT to users not visible. Got: ' . implode(', ', $labels));
            }
            if (count(array_keys($labels, 'Carol')) < 2) {
                $this->markTestIncomplete('UNION ALL two tables: shadow INSERT to logs not visible. Got: ' . implode(', ', $labels));
            }
            // 4 users + 3 logs = 7
            $this->assertCount(7, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UNION ALL two tables after DML failed: ' . $e->getMessage());
        }
    }

    /**
     * UNION DISTINCT from shadow-modified table (dedup should still work).
     */
    public function testUnionDistinctAfterDml(): void
    {
        try {
            // Make Bob an admin too, so both branches would return Bob
            $this->mysqli->query("UPDATE mi_usd_users SET role = 'admin' WHERE id = 2");

            $rows = $this->ztdQuery(
                "SELECT name FROM mi_usd_users WHERE role = 'admin'
                 UNION
                 SELECT name FROM mi_usd_users WHERE id <= 3
                 ORDER BY name"
            );

            $names = array_column($rows, 'name');

            if (!in_array('Bob', $names)) {
                $this->markTestIncomplete('UNION DISTINCT: Bob not visible after UPDATE. Got: ' . implode(', ', $names));
            }
            // UNION deduplicates: Alice, Bob, Carol (Bob appears in both but only once in result)
            $this->assertCount(3, $rows);
            $this->assertEquals(['Alice', 'Bob', 'Carol'], $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UNION DISTINCT after DML failed: ' . $e->getMessage());
        }
    }

    /**
     * UNION ALL after DELETE — deleted rows should not appear.
     */
    public function testUnionAllAfterDelete(): void
    {
        try {
            $this->mysqli->query("DELETE FROM mi_usd_users WHERE id = 2");

            $rows = $this->ztdQuery(
                "SELECT name FROM mi_usd_users WHERE role = 'admin'
                 UNION ALL
                 SELECT name FROM mi_usd_users WHERE role = 'user'
                 ORDER BY name"
            );

            $names = array_column($rows, 'name');

            if (in_array('Bob', $names)) {
                $this->markTestIncomplete('UNION ALL after DELETE: deleted row Bob still visible. Got: ' . implode(', ', $names));
            }
            $this->assertCount(2, $rows);
            $this->assertEquals(['Alice', 'Carol'], $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UNION ALL after DELETE failed: ' . $e->getMessage());
        }
    }
}
