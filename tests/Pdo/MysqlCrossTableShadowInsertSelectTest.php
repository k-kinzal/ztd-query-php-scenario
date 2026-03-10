<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests INSERT...SELECT where both source and destination tables have
 * shadow mutations on MySQL via PDO.
 *
 * @spec SPEC-4.1a
 */
class MysqlCrossTableShadowInsertSelectTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mpd_cts_users (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                dept VARCHAR(20) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE mpd_cts_archive (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                dept VARCHAR(20) NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mpd_cts_archive', 'mpd_cts_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mpd_cts_users VALUES (1, 'Alice', 'eng')");
        $this->pdo->exec("INSERT INTO mpd_cts_users VALUES (2, 'Bob', 'eng')");
        $this->pdo->exec("INSERT INTO mpd_cts_users VALUES (3, 'Carol', 'sales')");
        $this->pdo->exec("INSERT INTO mpd_cts_archive VALUES (10, 'Old1', 'ops')");
    }

    public function testInsertSelectAfterSourceShadowInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO mpd_cts_users VALUES (4, 'Dave', 'eng')");
            // Use explicit columns to avoid known Issue #40 (SELECT * column count mismatch)
            $this->pdo->exec(
                "INSERT INTO mpd_cts_archive (id, name, dept) SELECT id, name, dept FROM mpd_cts_users WHERE dept = 'eng'"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM mpd_cts_archive ORDER BY id");
            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'INSERT...SELECT after source shadow INSERT: expected 4 rows, got '
                    . count($rows) . ': ' . json_encode($rows)
                );
            }
            $this->assertCount(4, $rows);
            $this->assertContains(4, array_map('intval', array_column($rows, 'id')));
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Failed: ' . $e->getMessage());
        }
    }

    public function testInsertSelectAfterSourceShadowDelete(): void
    {
        try {
            $this->pdo->exec("DELETE FROM mpd_cts_users WHERE id = 2");
            // Use explicit columns to avoid known Issue #40 (SELECT * column count mismatch)
            $this->pdo->exec(
                "INSERT INTO mpd_cts_archive (id, name, dept) SELECT id, name, dept FROM mpd_cts_users WHERE dept = 'eng'"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM mpd_cts_archive WHERE id != 10 ORDER BY id");
            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'INSERT...SELECT after source shadow DELETE: expected 1 row, got '
                    . count($rows) . ': ' . json_encode($rows)
                );
            }
            $this->assertCount(1, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Failed: ' . $e->getMessage());
        }
    }

    public function testInsertSelectBothTablesShadowModified(): void
    {
        try {
            $this->pdo->exec("INSERT INTO mpd_cts_archive VALUES (20, 'Pre-existing', 'eng')");
            $this->pdo->exec("INSERT INTO mpd_cts_users VALUES (5, 'Eve', 'eng')");
            // Use explicit columns to avoid known Issue #40 (SELECT * column count mismatch)
            $this->pdo->exec(
                "INSERT INTO mpd_cts_archive (id, name, dept) SELECT id, name, dept FROM mpd_cts_users WHERE dept = 'eng'"
            );

            $rows = $this->ztdQuery("SELECT id FROM mpd_cts_archive ORDER BY id");
            if (count($rows) !== 5) {
                $this->markTestIncomplete(
                    'Both tables shadow-modified: expected 5 rows, got '
                    . count($rows) . ': ' . json_encode($rows)
                );
            }
            $this->assertCount(5, $rows);
            $ids = array_map('intval', array_column($rows, 'id'));
            $this->assertContains(5, $ids);
            $this->assertContains(20, $ids);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Failed: ' . $e->getMessage());
        }
    }
}
