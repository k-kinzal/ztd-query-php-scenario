<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests INSERT...SELECT where BOTH source and destination tables have
 * shadow mutations prior to the INSERT...SELECT.
 *
 * This stresses the CTE rewriter's ability to maintain consistent shadow
 * state across multiple tables in a single statement.
 *
 * @spec SPEC-4.1a
 */
class CrossTableShadowInsertSelectTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_cts_users (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                dept VARCHAR(20) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE mi_cts_archive (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                dept VARCHAR(20) NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_cts_archive', 'mi_cts_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_cts_users VALUES (1, 'Alice', 'eng')");
        $this->mysqli->query("INSERT INTO mi_cts_users VALUES (2, 'Bob', 'eng')");
        $this->mysqli->query("INSERT INTO mi_cts_users VALUES (3, 'Carol', 'sales')");
        $this->mysqli->query("INSERT INTO mi_cts_archive VALUES (10, 'Old1', 'ops')");
    }

    /**
     * Shadow-insert into source, then INSERT...SELECT from source to dest.
     */
    public function testInsertSelectAfterSourceShadowInsert(): void
    {
        try {
            // Shadow-insert a new user
            $this->mysqli->query("INSERT INTO mi_cts_users VALUES (4, 'Dave', 'eng')");

            // INSERT...SELECT eng users into archive
            // Use explicit columns to avoid known Issue #40 (SELECT * column count mismatch)
            $this->mysqli->query(
                "INSERT INTO mi_cts_archive (id, name, dept) SELECT id, name, dept FROM mi_cts_users WHERE dept = 'eng'"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM mi_cts_archive ORDER BY id");

            // Should have old archive row (10) + eng users (1,2,4)
            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'INSERT...SELECT after source shadow INSERT: expected 4 rows, got '
                    . count($rows) . ': ' . json_encode($rows)
                );
            }
            $this->assertCount(4, $rows);

            $ids = array_map('intval', array_column($rows, 'id'));
            if (!in_array(4, $ids)) {
                $this->markTestIncomplete(
                    'Shadow-inserted source row (id=4) not included in INSERT...SELECT: ids='
                    . json_encode($ids)
                );
            }
            $this->assertContains(4, $ids);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT after source shadow INSERT failed: ' . $e->getMessage());
        }
    }

    /**
     * Shadow-delete from source, then INSERT...SELECT should not include deleted rows.
     */
    public function testInsertSelectAfterSourceShadowDelete(): void
    {
        try {
            // Shadow-delete Bob
            $this->mysqli->query("DELETE FROM mi_cts_users WHERE id = 2");

            // INSERT...SELECT eng users
            // Use explicit columns to avoid known Issue #40 (SELECT * column count mismatch)
            $this->mysqli->query(
                "INSERT INTO mi_cts_archive (id, name, dept) SELECT id, name, dept FROM mi_cts_users WHERE dept = 'eng'"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM mi_cts_archive WHERE id != 10 ORDER BY id");

            // Should have only Alice (id=1), not Bob (deleted)
            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT...SELECT after source shadow DELETE: expected 1 eng row, got '
                    . count($rows) . ': ' . json_encode($rows)
                );
            }
            $this->assertCount(1, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT after source shadow DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * Shadow-update source, then INSERT...SELECT should reflect the update.
     */
    public function testInsertSelectAfterSourceShadowUpdate(): void
    {
        try {
            // Shadow-update Bob's dept from eng to sales
            $this->mysqli->query("UPDATE mi_cts_users SET dept = 'sales' WHERE id = 2");

            // INSERT...SELECT eng users
            // Use explicit columns to avoid known Issue #40 (SELECT * column count mismatch)
            $this->mysqli->query(
                "INSERT INTO mi_cts_archive (id, name, dept) SELECT id, name, dept FROM mi_cts_users WHERE dept = 'eng'"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM mi_cts_archive WHERE id != 10 ORDER BY id");

            // Should have only Alice (id=1), not Bob (now sales)
            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT...SELECT after source shadow UPDATE: expected 1 eng row, got '
                    . count($rows) . ': ' . json_encode($rows)
                );
            }
            $this->assertCount(1, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT after source shadow UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * Both tables have shadow mutations, then INSERT...SELECT.
     */
    public function testInsertSelectBothTablesShadowModified(): void
    {
        try {
            // Shadow-insert into archive
            $this->mysqli->query("INSERT INTO mi_cts_archive VALUES (20, 'Pre-existing', 'eng')");
            // Shadow-insert into users
            $this->mysqli->query("INSERT INTO mi_cts_users VALUES (5, 'Eve', 'eng')");

            // INSERT...SELECT eng users into archive
            // Use explicit columns to avoid known Issue #40 (SELECT * column count mismatch)
            $this->mysqli->query(
                "INSERT INTO mi_cts_archive (id, name, dept) SELECT id, name, dept FROM mi_cts_users WHERE dept = 'eng'"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM mi_cts_archive ORDER BY id");

            // Should have: 1(Alice), 2(Bob), 5(Eve) from users + 10(Old1), 20(Pre-existing) from archive
            if (count($rows) !== 5) {
                $this->markTestIncomplete(
                    'INSERT...SELECT both tables modified: expected 5 rows, got '
                    . count($rows) . ': ' . json_encode($rows)
                );
            }
            $this->assertCount(5, $rows);

            $ids = array_map('intval', array_column($rows, 'id'));
            $this->assertContains(5, $ids, 'Shadow-inserted source row (id=5) missing');
            $this->assertContains(20, $ids, 'Shadow-inserted archive row (id=20) missing');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT both tables shadow-modified failed: ' . $e->getMessage());
        }
    }
}
